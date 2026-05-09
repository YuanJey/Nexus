package execution

import (
	"context"

	"github.com/YuanJey/nexus/pkg/listener"
	"github.com/YuanJey/nexus/pkg/models"
)

// ──────────────────────────────────────────────────────────────
// Execution 执行层核心接口（exchange-agnostic，公开 API）
//
// 设计原则（参见 DESIGN_DOC §3.1 / §3.2）：
//   - 参数全部使用 pkg/models 标准模型，不依赖任何具体交易所结构
//   - 驱动层（internal/exchange/okx、binance）负责标准模型 ↔ 交易所报文的双向转换
//   - 写操作返回仅代表"指令已送出（Ack）"，最终状态通过 Observer 异步回调
//   - 批量操作驱动层自动按交易所限制（如 OKX 最多 20 笔/批）进行分片
// ──────────────────────────────────────────────────────────────

// Execution 统一执行接口（由各交易所驱动 internal/exchange/* 实现）
type Execution interface {

	// ─── 生命周期 ─────────────────────────────────────────────

	// Start 启动 WebSocket 连接，订阅私有频道（阻塞直到 ctx 取消）
	Start(ctx context.Context) error

	// Stop 优雅关闭所有连接，释放资源
	Stop() error

	// ─── 开仓 ─────────────────────────────────────────────────

	// PlaceOrders 批量开仓下单（开多/开空）
	// 使用 models.PlaceOrderReq，ReduceOnly=false
	PlaceOrders(ctx context.Context, orders []models.PlaceOrderReq) error

	// ─── 平仓 ─────────────────────────────────────────────────

	// CloseOrders 批量平仓下单（精确控制平仓数量）
	// 使用 models.PlaceOrderReq，ReduceOnly=true
	CloseOrders(ctx context.Context, orders []models.PlaceOrderReq) error

	// ClosePositions 一键平仓（推荐：交易所自动全平，无需指定数量）
	ClosePositions(ctx context.Context, positions []models.ClosePositionReq) error

	// ─── 改单 / 撤单 ──────────────────────────────────────────

	// AmendOrders 批量改单（修改价格/数量/止盈止损）
	// OrdId 与 ClOrdId 必须提供其中一个
	AmendOrders(ctx context.Context, orders []models.AmendOrderReq) error

	// CancelOrders 批量撤单
	// OrdId 与 ClOrdId 必须提供其中一个
	CancelOrders(ctx context.Context, orders []models.CancelOrderReq) error

	// CancelAllOrders 撤销指定产品所有普通挂单（instId 为空则撤全账户）
	CancelAllOrders(ctx context.Context, instId string) error

	// ─── 策略委托 (Algo Orders / TP/SL) ─────────────────────────

	// PlaceAlgoOrders 批量下单策略单（如独立止盈止损、触发单）
	PlaceAlgoOrders(ctx context.Context, orders []models.PlaceAlgoOrderReq) error

	// AmendAlgoOrders 批量修改策略单
	AmendAlgoOrders(ctx context.Context, orders []models.AmendAlgoOrderReq) error

	// CancelAlgoOrders 批量撤销策略单
	CancelAlgoOrders(ctx context.Context, orders []models.CancelAlgoOrderReq) error

	// CancelAllAlgoOrders 撤销指定产品所有策略委托（instId 为空则撤全账户）
	CancelAllAlgoOrders(ctx context.Context, instId string) error

	// ─── 订单状态订阅（异步双闭环）────────────────────────────

	// Observer 返回订单事件观察者
	// 支持监听普通单 (*models.OrderUpdate) 和 策略单 (*models.AlgoUpdate)
	Observer() *listener.OrderObserver[listener.Identifiable]

	// ─── 查询（对账补偿）─────────────────────────────────────

	// GetOrder 查询单笔订单（优先用 clOrdId，ordId 可为空）
	GetOrder(ctx context.Context, instId, clOrdId, ordId string) (*models.OrderUpdate, error)

	GetOHLCV(ctx context.Context, instId string, timeframe string, limit int) ([]models.Candle, error)
}
