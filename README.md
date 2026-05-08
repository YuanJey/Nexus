# Nexus SDK

Nexus 是一个高性能、工业级的合约交易 SDK，专为高频交易与复杂交易系统接入设计，目前只实现了OKX V5相关接口，后续会支持更多交易所。

## 🚀 核心特性

- **Hybrid 架构**：RESTful 接口负责指令下发，多路 WebSocket 负责极速状态回报。
- **异步双闭环**：基于 `Observer` 模式的订单生命周期管理，支持 `OnceOrder` 一次性回调及全局持久监听。
- **可注入式设计**：第三方系统（风控、审计、资管）可无缝注入自定义监听器到账户、持仓及行情流。
- **工业级稳定性**：内置指数退避（Exponential Backoff）自动重连机制，支持订阅状态自动恢复。
- **全链路追踪**：集成 Context 级的 `OperationID` 追踪，配合结构化日志库，实现从信号到成交的完整审计。

## 📦 安装

```bash
go get github.com/YuanJey/nexus
```

## 🛠 快速开始

### 1. 初始化 SDK

```go
import nexus "github.com/YuanJey/nexus/pkg/client"

sdk := nexus.New(nexus.Config{
    Exchange:   nexus.OKX,
    APIKey:     "your_api_key",
    SecretKey:  "your_secret_key",
    Passphrase: "your_passphrase",
    Simulated:  true, // 开启模拟盘
})

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

// 启动监听
go sdk.Execution.Start(ctx)
go sdk.Stream.Start(ctx)
```

### 2. 注入第三方监听 (Observer 模式)

```go
// 监听账户资产变动
sdk.Stream.AccountObserver().Subscribe(func(acc *models.Account) {
    fmt.Printf("账户权益更新: %s\n", acc.TotalEq)
})

// 监听特定订单状态
sdk.Execution.Observer().OnceOrder("my_cl_ord_id", listener.OrderEventFilled, func(update *listener.Identifiable) {
    fmt.Println("订单已完全成交！")
})
```

### 3. 下单与撤单

```go
err := sdk.Execution.PlaceOrders(ctx, []models.PlaceOrderReq{
    {
        InstId:     "ETH-USDT-SWAP",
        Side:       models.SideBuy,
        OrdType:    models.OrderLimit,
        Px:         "2500.5",
        Sz:         "10",
        ClOrdId:    "unique_id_123",
    },
})
```

## 🏗 项目结构

- `/pkg/client`: SDK 入口与统一客户端。
- `/pkg/execution`: 交易执行接口与订单观察者。
- `/pkg/stream`: 行情、账户、持仓订阅接口与观察者。
- `/pkg/listener`: 统一的观察者模式（Observer）实现。
- `/internal/exchange/okx`: OKX V5 协议的具体驱动实现。

## 📜 许可证

MIT License
