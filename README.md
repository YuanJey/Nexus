# Nexus SDK

Go 语言加密货币交易 SDK，面向策略开发者，提供 **行情监听、账户追踪、持仓同步、订单执行** 四大模块，按需组合使用。

## 架构设计

```
┌────────────────────────────────────────────────────┐
│  策略层 (test/main.go)                              │
│  实现 modules.Listener 接口，注入到各 Module          │
├────────┬────────┬────────┬─────────────────────────┤
│ Market │Account │Position│ Trading                 │  ← Module 层（对外 API）
│ Module │Module  │Module  │ Module                  │
├────────┴────────┴────────┴─────────────────────────┤
│  tickerComp  accountComp  positionComp  orderComp  │  ← Component 层（解析+分发）
│                                    algoOrderComp   │
├────────────────────────────────────────────────────┤
│  wsClient (channel 路由)  │  httpClient (REST)      │  ← 传输层
└────────────────────────────────────────────────────┘
```

**三层职责**：
- **传输层** — wsClient 管理 WebSocket 连接/重连，按 `arg.channel` 字段路由消息到对应 handler；httpClient 封装签名和请求
- **组件层** — 解析 WS 原始报文，维护 listener 列表，按 instId+event 精准分发
- **模块层** — 对外暴露业务 API（下单/撤单/订阅/查询），返回 detach 闭包

**WS 连接分配**（共 4 条）：

| 连接 | 频道 | 使用者 |
|------|------|--------|
| Public WS | `tickers` | MarketModule |
| Private WS #1 | `account`, `positions` | AccountModule + PositionModule |
| Private WS #2 | `orders` | TradingModule |
| Business WS | `orders-algo` | TradingModule |

## 快速开始

### 安装

```bash
go get github.com/YuanJey/nexus
```

### 基础用法

```go
package main

import (
    "context"
    "fmt"
    nexus "github.com/YuanJey/nexus/pkg/client"
    "github.com/YuanJey/nexus/pkg/models"
)

func main() {
    sdk := nexus.New(nexus.Config{
        Exchange:   nexus.OKX,
        APIKey:     "your-api-key",
        SecretKey:  "your-secret-key",
        Passphrase: "your-passphrase",
        Simulated:  true, // 模拟盘
    })

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // 启动 WS 连接（阻塞，放 goroutine）
    go sdk.Start(ctx)

    // 注入监听器
    sdk.Account.AttachAccount(myAccountListener)
    sdk.Position.AttachPosition("ETH-USDT-SWAP", myPositionListener)
    sdk.Trading.AttachOrder(myOrderListener)

    // 下单
    sdk.Trading.PlaceOrders(ctx, []models.PlaceOrderReq{{
        InstId:     "ETH-USDT-SWAP",
        MarginMode: models.MarginCross,
        Side:       models.SideBuy,
        PosSide:    models.PosSideLong,
        OrdType:    models.OrderLimit,
        Sz:         "1",
        Px:         "1800",
        ClOrdId:    "my-order-001",
    }})
}
```

## Module API

### MarketModule — 公共行情

```go
// 订阅单个品种 Ticker
detach := sdk.Market.AttachTicker("ETH-USDT-SWAP", listener)
defer detach()

// 批量订阅（一次注册多个品种，共用 listener）
detach := sdk.Market.AttachTickers([]string{"ETH-USDT-SWAP", "BTC-USDT-SWAP"}, listener)

// REST 查询 K 线
candles, err := sdk.Market.GetOHLCV(ctx, "ETH-USDT-SWAP", "1m", 100)
```

Ticker 订阅采用**引用计数**：同一 instId 多次 Attach 不会重复发送 WS subscribe，均不再 Attach 时自动发送 unsubscribe。

### AccountModule — 账户信息

```go
// 订阅账户推送（余额变动实时通知）
detach := sdk.Account.AttachAccount(listener)

// REST 快照查询
acc, err := sdk.Account.GetAccount(ctx)
```

首次 Attach 时自动发送 WS `subscribe` 请求，后续 Attach 仅注册 listener 不重复订阅。

### PositionModule — 持仓同步

```go
// 按品种订阅持仓推送
detach := sdk.Position.AttachPosition("ETH-USDT-SWAP", listener)
// 全品种订阅（instId 传 ""）
detach := sdk.Position.AttachPosition("", listener)

// REST 快照查询
positions, err := sdk.Position.GetPositions(ctx, "ETH-USDT-SWAP")
```

消息按 instId 精准分发；同时支持全局 listener（instId="" 匹配所有品种）。

### TradingModule — 交易执行

```go
// 下单
err := sdk.Trading.PlaceOrders(ctx, []models.PlaceOrderReq{...})

// 平仓单（自动设置 reduceOnly）
err := sdk.Trading.CloseOrders(ctx, []models.PlaceOrderReq{...})

// 一键平仓
err := sdk.Trading.ClosePositions(ctx, []models.ClosePositionReq{...})

// 改单
err := sdk.Trading.AmendOrders(ctx, []models.AmendOrderReq{...})

// 撤单
err := sdk.Trading.CancelOrders(ctx, []models.CancelOrderReq{...})
err := sdk.Trading.CancelAllOrders(ctx, "ETH-USDT-SWAP")

// 策略委托
err := sdk.Trading.PlaceAlgoOrders(ctx, []models.PlaceAlgoOrderReq{...})
err := sdk.Trading.CancelAlgoOrders(ctx, []models.CancelAlgoOrderReq{...})
err := sdk.Trading.CancelAllAlgoOrders(ctx, "ETH-USDT-SWAP")

// REST 查询单笔订单
order, err := sdk.Trading.GetOrder(ctx, "ETH-USDT-SWAP", "my-cl-ord-id", "")
```

批量操作自动按交易所限制（OKX 最多 20 笔/批）分片发送。

## Listener 接口

SDK 定义 5 个抽象接口（位于 `pkg/modules`），策略层按需实现并注入：

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

**设计原则**：SDK 只负责触发，不做过滤。策略层在 `On*` 方法中自行判断 instId、posSide、state 等。

### 持久监听 vs 一次性监听

```go
// 持久：每次事件都触发，需手动 detach
detach := sdk.Trading.AttachOrder(myListener)

// 一次性：匹配 clOrdId + event 后自动移除
sdk.Trading.OnceOrder("my-cl-ord-id", modules.OrderEventFilled, &onceListener{})
```

一次性监听（OnceOrder/OnceAlgoOrder）适用于"下完单等成交/撤单确认"的场景，触发后自动清理，无需手动管理。

### OrderEvent 常量

| 常量 | 说明 |
|------|------|
| `OrderEventAll` | 通配，匹配所有事件 |
| `OrderEventNew` | 订单已确认（live） |
| `OrderEventPartial` | 部分成交 |
| `OrderEventFilled` | 完全成交 |
| `OrderEventCanceled` | 已撤单 |

## 数据模型

请求/响应模型统一在 `pkg/models` 中定义，与具体交易所解耦。驱动层（`internal/exchange/okx`）负责标准模型与交易所报文的双向转换。

### 关键类型

| 类型 | 说明 |
|------|------|
| `PlaceOrderReq` | 下单请求（开多/开空/平多/平空通用） |
| `ClosePositionReq` | 一键平仓请求 |
| `AmendOrderReq` | 改单请求 |
| `CancelOrderReq` | 撤单请求 |
| `PlaceAlgoOrderReq` | 策略委托请求（止盈止损/触发单） |
| `Ticker` | 实时行情快照 |
| `Account` | 账户资产（总权益 + 各币种明细） |
| `Position` | 单笔持仓 |
| `OrderUpdate` | 订单状态推送 |
| `AlgoUpdate` | 策略委托状态推送 |
| `Candle` | K 线数据 |

### 枚举

```go
// 买卖方向
models.SideBuy   // "buy"
models.SideSell  // "sell"

// 持仓方向
models.PosSideLong   // "long"
models.PosSideShort  // "short"
models.PosSideNet    // "net"（单向持仓）

// 订单类型
models.OrderMarket    // 市价
models.OrderLimit     // 限价
models.OrderPostOnly  // 只做 Maker
models.OrderFok       // 全部成交或立即取消
models.OrderIoc       // 立即成交并取消剩余

// 保证金模式
models.MarginIsolated  // 逐仓
models.MarginCross     // 全仓

// 触发价类型
models.TriggerMark   // 标记价格（推荐）
models.TriggerLast   // 最新成交价
models.TriggerIndex  // 指数价格
```

## Configuration

```go
type Config struct {
    Exchange   ExchangeType  // nexus.OKX / nexus.BINANCE
    APIKey     string
    SecretKey  string
    Passphrase string        // OKX 专用
    Simulated  bool          // true = 模拟盘
}
```

## 项目结构

```
nexus/
├── pkg/
│   ├── client/nexus.go        # SDK 统一入口，New() 工厂函数
│   ├── modules/modules.go     # 4 个 Module 接口 + 5 个 Listener 接口
│   ├── models/
│   │   ├── order.go           # 请求/响应模型 + 枚举定义
│   │   └── stream.go          # Ticker/Account/Position/Candle 模型
│   └── utils/trace.go         # trace ID 工具
├── internal/exchange/okx/
│   ├── factory.go             # Modules 容器，组装 4 个 Module + Start/Stop
│   ├── market_module.go       # MarketModule 实现
│   ├── account_module.go      # AccountModule 实现
│   ├── position_module.go     # PositionModule 实现
│   ├── trading_module.go      # TradingModule 实现
│   ├── *_component.go         # 5 个 Component（解析 WS 消息+分发 listener）
│   ├── ws_client.go           # WebSocket 客户端（连接/重连/鉴权/channel 路由）
│   ├── ws_types.go            # WS 推送结构体
│   ├── http_client.go         # HTTP 客户端（签名/批量/错误处理）
│   ├── converter.go           # models 标准模型 ↔ OKX 结构双向转换
│   ├── order.go               # OKX 订单/改单/撤单/策略单内部结构
│   └── sign.go                # HMAC-SHA256 签名
├── test/main.go               # 集成测试示例
└── utils/order_tool.go        # ClOrdId 生成工具
```

## License

MIT
