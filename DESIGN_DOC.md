# Nexus Trading SDK 架构设计文档

## 1. 架构概述

Nexus 是一个多交易所交易 SDK。核心设计原则：

- **模块化按需加载**：从 Nexus 对象按需获取模块，不用不创建
- **底层资源共享**：HTTP Client 和 WS Client 由 Nexus 内部统一持有，多模块指针引用
- **Listener 注入**：策略实现 Listener 接口，Attach 到模块，SDK 只负责"数据到达时触发调用"

### 分工边界

```
SDK 负责：                  调用方负责：
  消息到达 → 解析            posSide / 价格 / 数量过滤
  → 调 Listener 方法         并发策略（go or sync）
  OnceOrder 自动 detach      生命周期管理（调 detach()）
```

### 对外 API 全景

```go
// ── 策略实现 Listener ──
type MyStrategy struct{}

func (s *MyStrategy) OnTicker(t *models.Ticker) {
    // 自己判断多空
    if t.Last < s.entryPrice { ... }
}
func (s *MyStrategy) OnOrder(o *models.OrderUpdate) {
    if o.PosSide != models.PosSideLong { return }
    // 处理多单回调
}
func (s *MyStrategy) OnAccount(acc *models.Account) { ... }
func (s *MyStrategy) OnPosition(pos *models.Position) { ... }

// ── SDK 使用 ──
nexus := nexus.New(nexus.Config{Exchange: nexus.OKX, ...})

market  := nexus.MarketModule()
trading := nexus.TradingModule()
account := nexus.AccountModule()
pos     := nexus.PositionModule()

go nexus.Connect(ctx)

// 注入 Listener
detachTicker := market.AttachTicker("ETH-USDT-SWAP", strategy)
detachOrder  := trading.AttachOrder(strategy)

// 下单
trading.PlaceOrders(ctx, orders)

// 一次性回调
trading.OnceOrder(clOrdId, OrderEventFilled, strategy)

// 策略下线，移除所有监听
detachTicker()
detachOrder()

nexus.Close()
```

### 整体数据流

```
                              OKX API
                         ┌──────┴──────┐
                         │    REST     │  WebSocket
                         └──────┬──────┘
                                │
┌───────────────────────────────┼────────────────────────────────────┐
│                         Nexus (内部)                                │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
│  │ http     │  │ pubWs    │  │ priWs    │  │ bizWs    │          │
│  │ *httpClnt│  │ *wsClient│  │ *wsClient│  │ *wsClient│          │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘          │
│       │              │              │              │               │
│       │              │         ┌────┴────┐         │               │
│       │              │         │ 路由:    │         │               │
│       │        路由: tickers   │ account  │   路由: orders-algo    │
│       │                       │ positions│                         │
│       │                       │ orders   │                         │
│       │              │         └────┬────┘         │               │
│       │              │              │              │               │
│       │         ┌────┴────┐   ┌────┴────┐   ┌────┴────┐          │
│       │         │Ticker   │   │Account  │   │AlgoOrder│          │
│       │         │Component│   │Component│   │Component│          │
│       │         └────┬────┘   │         │   └────┬────┘          │
│       │              │        │Position │         │               │
│       │              │        │Component│         │               │
│       │              │        │         │         │               │
│       │              │        │Order    │         │               │
│       │              │        │Component│         │               │
│       │              │        └────┬────┘         │               │
│       │              │              │              │               │
│       │           调 Listener 方法 (sync 逐个通知)                 │
│       │              │              │              │               │
├───────┴──────────────┴──────────────┴──────────────┴──────────────┤
│                          Modules (Public API)                      │
│  MarketModule    AccountModule   PositionModule   TradingModule    │
└───────────────────────────────────────────────────────────────────┘
```

## 2. 三层职责分离

### 2.1 wsClient — 纯传输层

只做发送、接收、消息路由。零业务逻辑。

```
✅ 建立/重连 WebSocket 连接（指数退避）
✅ Send(msg any) — 发送 JSON 消息
✅ 接收原始消息，按 arg.channel 字段路由到 Component
✅ 心跳 PingMessage
✅ 登录鉴权（私有频道）
✅ Ready() 信号 — 通知连接就绪
❌ 不知道 ticker/account/order 等业务含义
❌ 不管理订阅/取消订阅
```

```go
func (w *wsClient) OnChannel(channel string, handler func([]byte))
```

### 2.2 Component — 业务解析 + 通知层

注册到 wsClient 的对应 channel，收到原始消息后解析为标准模型，同步调用已 Attach 的 Listener。

```
wsClient ──→ Component.handleMessage(rawJSON)
               │
               ├─ json.Unmarshal → models.Ticker / Account / Position / OrderUpdate
               │
               └─ 遍历 Listener 列表，同步调用 listener.OnXxx(data)
```

> 同步调用：SDK 不替策略层做并发决策，策略在 OnXxx 里自己决定是否 `go`。

Component 存储模型——以 Listener 为粒度：

```go
type tickerComponent struct {
    mu        sync.RWMutex
    listeners map[string]map[int]TickerListener  // instId → {id → listener}
    nextID    int
}

func (c *tickerComponent) Attach(instId string, l TickerListener) func() {
    c.mu.Lock()
    id := c.nextID; c.nextID++
    if c.listeners[instId] == nil {
        c.listeners[instId] = make(map[int]TickerListener)
    }
    c.listeners[instId][id] = l
    c.mu.Unlock()

    return func() {
        c.mu.Lock()
        delete(c.listeners[instId], id)
        c.mu.Unlock()
    }
}

func (c *tickerComponent) handleMessage(msg []byte) {
    var ticker models.Ticker
    // ... unmarshal ...
    c.mu.RLock()
    for _, l := range c.listeners[ticker.InstId] { l.OnTicker(&ticker) }
    for _, l := range c.listeners[""]    { l.OnTicker(&ticker) } // instId="" 表示全局
    c.mu.RUnlock()
}
```

Component 类型：

| Component | 注册的 Channel | 调用的 Listener 方法 |
|-----------|---------------|---------------------|
| TickerComponent | `tickers` | `listener.OnTicker(*Ticker)` |
| AccountComponent | `account` | `listener.OnAccount(*Account)` |
| PositionComponent | `positions` | `listener.OnPosition(*Position)` |
| OrderComponent | `orders` | `listener.OnOrder(*OrderUpdate)` |
| AlgoOrderComponent | `orders-algo` | `listener.OnAlgoOrder(*AlgoUpdate)` |

### 2.3 Module — 对外 API 层

组合 Component + wsClient + httpClient，对外暴露 `Attach(listener) → detach()` + `OnceOrder(id, event, listener) → detach()`。

```go
type marketModule struct {
    http *httpClient
    ws   *wsClient           // nexus.pubWs
    comp *tickerComponent
    subs map[string]int      // instId → 引用计数，WS 订阅管理
    mu   sync.Mutex
}

func (m *marketModule) AttachTicker(instId string, l TickerListener) (detach func()) {
    // 1. 注入 Listener 到 Component
    detachComp := m.comp.Attach(instId, l)

    // 2. 首次订阅时发 WS 指令
    m.mu.Lock()
    m.subs[instId]++
    if m.subs[instId] == 1 {
        m.ws.Send(subscribeMsg("tickers", instId))
    }
    m.mu.Unlock()

    // 3. 组合 detach
    return func() {
        detachComp()
        m.mu.Lock()
        m.subs[instId]--
        if m.subs[instId] <= 0 {
            delete(m.subs, instId)
            m.ws.Send(unsubscribeMsg("tickers", instId))
        }
        m.mu.Unlock()
    }
}
```

## 3. Nexus — SDK 唯一入口

```go
type Nexus struct {
    http  *httpClient
    pubWs *wsClient
    priWs *wsClient
    bizWs *wsClient

    market   *marketModule
    account  *accountModule
    position *positionModule
    trading  *tradingModule
}

func New(cfg Config) *Nexus
func (n *Nexus) MarketModule() modules.MarketModule
func (n *Nexus) AccountModule() modules.AccountModule
func (n *Nexus) PositionModule() modules.PositionModule
func (n *Nexus) TradingModule() modules.TradingModule
func (n *Nexus) Connect(ctx context.Context) error
func (n *Nexus) Close() error
```

生命周期：

```
New(cfg)          → 创建 Nexus + 初始化 3 WS + 1 HTTP，不启动连接
ModuleXxx()       → 懒加载，Component 注册到 wsClient，重复调用返回同一实例
Connect(ctx)      → 启动 3 条 WS（goroutine），阻塞直到 ctx 取消
Close()           → 关闭所有 WS + HTTP
```

### 资源分配

| 资源 | 数量 | 使用者 |
|------|------|--------|
| `http` | 1 | MarketModule (OHLCV), TradingModule (REST) |
| `pubWs` | 1 | MarketModule — channel: `tickers` |
| `priWs` | 1 | AccountModule + PositionModule + TradingModule |
| `bizWs` | 1 | TradingModule — channel: `orders-algo` |

## 4. 模块接口

### 4.1 Listener 接口（SDK 定义，调用方实现）

```go
type TickerListener interface {
    OnTicker(ticker *models.Ticker)
}

type AccountListener interface {
    OnAccount(account *models.Account)
}

type PositionListener interface {
    OnPosition(pos *models.Position)
}

type OrderListener interface {
    OnOrder(update *models.OrderUpdate)
}

type AlgoOrderListener interface {
    OnAlgoOrder(update *models.AlgoUpdate)
}
```

### 4.2 MarketModule

```go
type MarketModule interface {
    AttachTicker(instId string, listener TickerListener) (detach func())
    AttachTickers(instIds []string, listener TickerListener) (detach func())
    GetOHLCV(ctx context.Context, instId, timeframe string, limit int) ([]models.Candle, error)
}
```

### 4.3 AccountModule

```go
type AccountModule interface {
    AttachAccount(listener AccountListener) (detach func())
    GetAccount(ctx context.Context) (*models.Account, error)
}
```

### 4.4 PositionModule

```go
type PositionModule interface {
    // instId 为空 = 全账户持仓
    AttachPosition(instId string, listener PositionListener) (detach func())
    GetPositions(ctx context.Context, instId string) ([]models.Position, error)
}
```

### 4.5 TradingModule

```go
type OrderEvent string

const (
    OrderEventAll      OrderEvent = "all"
    OrderEventNew      OrderEvent = "new"
    OrderEventPartial  OrderEvent = "partial"
    OrderEventFilled   OrderEvent = "filled"
    OrderEventCanceled OrderEvent = "canceled"
)

type TradingModule interface {
    // ── 写操作 ──
    PlaceOrders(ctx context.Context, orders []models.PlaceOrderReq) error
    CloseOrders(ctx context.Context, orders []models.PlaceOrderReq) error
    ClosePositions(ctx context.Context, positions []models.ClosePositionReq) error
    AmendOrders(ctx context.Context, orders []models.AmendOrderReq) error
    CancelOrders(ctx context.Context, orders []models.CancelOrderReq) error
    CancelAllOrders(ctx context.Context, instId string) error

    PlaceAlgoOrders(ctx context.Context, orders []models.PlaceAlgoOrderReq) error
    AmendAlgoOrders(ctx context.Context, orders []models.AmendAlgoOrderReq) error
    CancelAlgoOrders(ctx context.Context, orders []models.CancelAlgoOrderReq) error
    CancelAllAlgoOrders(ctx context.Context, instId string) error

    // ── 持久监听 ──
    AttachOrder(listener OrderListener) (detach func())
    AttachAlgoOrder(listener AlgoOrderListener) (detach func())

    // ── 一次性监听（触发后自动 detach）──
    OnceOrder(clOrdId string, event OrderEvent, listener OrderListener) (detach func())
    OnceAlgoOrder(algoClOrdId string, event OrderEvent, listener AlgoOrderListener) (detach func())

    // ── 查询 ──
    GetOrder(ctx context.Context, instId, clOrdId, ordId string) (*models.OrderUpdate, error)
}
```

## 5. OrderComponent — OnceOrder 实现细节

订单回调需按 `clOrdId` + `event` 做一次性匹配，触发后自动移除：

```go
type onceEntry struct {
    clOrdId  string
    event    OrderEvent
    listener OrderListener
}

type orderComponent struct {
    mu         sync.Mutex
    persistent []OrderListener      // AttachOrder 注册的
    onceList   []onceEntry          // OnceOrder 注册的
}

func (c *orderComponent) handleMessage(msg []byte) {
    var update models.OrderUpdate
    // ... unmarshal ...

    // 1. 持久监听
    for _, l := range c.persistent {
        l.OnOrder(&update)
    }

    // 2. 一次性监听：匹配 clOrdId + event，命中后移除
    event := orderStateToEvent(update.State)
    c.mu.Lock()
    remaining := c.onceList[:0]
    for _, e := range c.onceList {
        if e.clOrdId == update.ClOrdId && (e.event == event || e.event == OrderEventAll) {
            e.listener.OnOrder(&update)
        } else {
            remaining = append(remaining, e)
        }
    }
    c.onceList = remaining
    c.mu.Unlock()
}
```

`OnceOrder` 返回的 detach 允许调用方提前取消（组合了好 detach 也没影响）。策略初始化时注册，下单后等通知：

```go
// 策略初始化
trading.AttachOrder(strategy)  // 持久

// 下单前注册
detach := trading.OnceOrder(clOrdId, OrderEventFilled, strategy)
trading.PlaceOrders(ctx, orders)

// 触发后自动 detach，或超时主动取消
time.AfterFunc(30*time.Second, detach)
```

## 6. 重连与订阅恢复

```
wsClient 检测断连
  │
  ├─ cleanupConn() → 重置 Ready channel
  ├─ 指数退避重连
  ├─ login() (私有频道)
  ├─ signalReady() → close(ready channel)
  │
  └─ Module 侧:
       select {
       case <-wsClient.Ready():
           for instId := range m.subs {
               m.ws.Send(subscribeMsg(channel, instId))
           }
       }
```

每个 Module 用 `subs map[string]int` 追踪活跃的 WS 订阅，重连后遍历重发。Listener 对象不变，恢复正常后自动收到推送。

## 7. 目录结构

```text
Nexus/
├── pkg/
│   ├── modules/
│   │   └── modules.go            # Listener 接口 + 4 个模块接口 + Config + OrderEvent
│   ├── client/
│   │   └── nexus.go              # Nexus struct + 工厂函数
│   ├── models/
│   │   ├── stream.go             # Ticker, Account, Position, Candle
│   │   └── order.go              # OrderUpdate, AlgoUpdate, 请求/响应模型
│   └── utils/
│       └── trace.go
├── internal/exchange/okx/
│   ├── ws_client.go              # 纯传输层 (send/recv/route/ping/重连)
│   ├── http_client.go            # REST 客户端 (签名/分片)
│   ├── ticker_component.go       # Ticker 解析 + Listener 通知   [NEW]
│   ├── account_component.go      # Account 解析 + Listener 通知  [NEW]
│   ├── position_component.go     # Position 解析 + Listener 通知 [NEW]
│   ├── order_component.go        # OrderUpdate 解析 + 持久/一次性通知 [NEW]
│   ├── algo_order_component.go   # AlgoUpdate 解析 + 持久/一次性通知 [NEW]
│   ├── market_module.go          # MarketModule 实现              [NEW]
│   ├── account_module.go         # AccountModule 实现             [NEW]
│   ├── position_module.go        # PositionModule 实现            [NEW]
│   ├── trading_module.go         # TradingModule 实现             [NEW]
│   ├── converter.go              # OKX ↔ models
│   ├── order.go                  # OKX 结构体
│   └── sign.go                   # 签名
├── utils/order_tool.go
└── test/main.go
```

## 8. 开发规范

- **日志**：使用 `github.com/YuanJey/go-log`，携带 `OperationID`
- **错误处理**：底层错误通过 `fmt.Errorf("%w")` 包装
- **并发安全**：WS 写操作通过 `sync.Mutex` 保护
- **上下文传递**：所有 I/O 操作接受 `context.Context`
- **同步通知**：Component 直接调 `listener.OnXxx()`，不启 goroutine
