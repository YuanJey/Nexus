package models

// ══════════════════════════════════════════════════════════════
// 标准枚举（exchange-agnostic，驱动层负责双向转换）
// ══════════════════════════════════════════════════════════════

// OrderSide 买卖方向
type OrderSide string

const (
	SideBuy  OrderSide = "buy"
	SideSell OrderSide = "sell"
)

// PosSide 持仓方向
type PosSide string

const (
	PosSideLong  PosSide = "long"
	PosSideShort PosSide = "short"
	PosSideNet   PosSide = "net" // 单向持仓模式
)

// OrderType 订单类型
type OrderType string

const (
	OrderMarket   OrderType = "market"    // 市价单
	OrderLimit    OrderType = "limit"     // 限价单
	OrderPostOnly OrderType = "post_only" // 只做 Maker
	OrderFok      OrderType = "fok"       // 全部成交或立即取消
	OrderIoc      OrderType = "ioc"       // 立即成交并取消剩余
)

// MarginMode 保证金模式
type MarginMode string

const (
	MarginIsolated MarginMode = "isolated" // 逐仓
	MarginCross    MarginMode = "cross"    // 全仓
)

// TriggerPxType 触发价类型
type TriggerPxType string

const (
	TriggerLast  TriggerPxType = "last"  // 最新成交价（易受插针影响）
	TriggerMark  TriggerPxType = "mark"  // 标记价格（推荐）
	TriggerIndex TriggerPxType = "index" // 指数价格
)

// ══════════════════════════════════════════════════════════════
// 止盈止损参数（嵌入到下单/改单请求中）
// ══════════════════════════════════════════════════════════════

// TPSL 止盈止损参数（均为空字符串表示不设置）
type TPSL struct {
	TpTriggerPx     string        // 止盈触发价
	TpOrdPx         string        // 止盈委托价（"-1" 为市价）
	TpTriggerPxType TriggerPxType // 建议 TriggerMark
	SlTriggerPx     string        // 止损触发价
	SlOrdPx         string        // 止损委托价（"-1" 为市价）
	SlTriggerPxType TriggerPxType
}

// ══════════════════════════════════════════════════════════════
// 标准请求模型（Execution 接口参数）
// ══════════════════════════════════════════════════════════════

// PlaceOrderReq 标准下单请求（开多/开空/平多/平空通用）
type PlaceOrderReq struct {
	InstId     string     // 产品 ID，如 ETH-USDT-SWAP
	MarginMode MarginMode // 逐仓/全仓
	Side       OrderSide  // buy / sell
	PosSide    PosSide    // long / short
	OrdType    OrderType  // 订单类型
	Sz         string     // 委托数量（decimal string）
	Px         string     // 委托价（市价单传 ""）
	ClOrdId    string     // 客户自定义 ID（建议格式：Project_Strategy_Timestamp）
	ReduceOnly bool       // true = 只减仓（平仓时使用）
	TPSL       *TPSL      // 可选止盈止损
}

// ClosePositionReq 标准一键平仓请求（交易所自动全平，无需指定数量）
type ClosePositionReq struct {
	InstId     string     // 产品 ID
	MarginMode MarginMode // 保证金模式
	PosSide    PosSide    // 平多=long，平空=short
	Ccy        string     // 可选，全仓单币种时必填
	ClOrdId    string     // 可选，自定义 ID
	AutoCxl    bool       // 平仓触发失败时是否自动撤关联止盈止损
}

// AmendOrderReq 标准改单请求（OrdId 与 ClOrdId 二选一）
type AmendOrderReq struct {
	InstId    string // 产品 ID
	OrdId     string // 平台订单 ID（与 ClOrdId 二选一）
	ClOrdId   string // 客户自定义 ID（与 OrdId 二选一）
	ReqId     string // 可选，请求追踪 ID
	NewSz     string // 新数量（不修改传 ""）
	NewPx     string // 新价格（不修改传 ""）
	CxlOnFail bool   // 改单失败时自动撤单
	// 修改附带止盈止损（AlgoId 必填，其余传 "" 表示不修改）
	AlgoId             string
	NewTpTriggerPx     string
	NewTpOrdPx         string
	NewTpTriggerPxType TriggerPxType
	NewSlTriggerPx     string
	NewSlOrdPx         string
	NewSlTriggerPxType TriggerPxType
}

// CancelOrderReq 标准撤单请求（OrdId 与 ClOrdId 二选一）
type CancelOrderReq struct {
	InstId  string // 产品 ID
	OrdId   string // 平台订单 ID
	ClOrdId string // 客户自定义 ID
}

// ══════════════════════════════════════════════════════════════
// 标准响应/推送模型
// ══════════════════════════════════════════════════════════════

// OrderUpdate 订单状态推送（WebSocket 实时推送，驱动层转换后分发）
// 实现 common.Identifiable 接口，供 OrderObserver 按 clOrdId 路由
type OrderUpdate struct {
	ClOrdId  string // 客户自定义 ID
	OrdId    string // 平台订单 ID
	InstId   string // 产品 ID
	Side     OrderSide
	PosSide  PosSide
	OrdType  OrderType
	State    string // 订单状态原始值（filled/canceled/live/partially_filled）
	Sz       string // 委托数量
	FillSz   string // 已成交数量
	Px       string // 委托价
	FillPx   string // 成交价
	AvgPx    string // 平均成交价
	Pnl      string // 收益（平仓时）
	Fee      string // 手续费
	UpdateAt int64  // 更新时间戳（ms）
}

// GetClOrdId 实现 listener.Identifiable，供 OrderObserver 路由
func (o *OrderUpdate) GetClOrdId() string { return o.ClOrdId }

// ══════════════════════════════════════════════════════════════
// 策略委托请求模型 (Algo Orders)
// ══════════════════════════════════════════════════════════════

// AlgoOrdType 策略单类型
type AlgoOrdType string

const (
	AlgoConditional AlgoOrdType = "conditional" // 止盈止损单
	AlgoTrigger     AlgoOrdType = "trigger"     // 触发单
	AlgoOCO         AlgoOrdType = "oco"         // OCO 订单
)

// PlaceAlgoOrderReq 策略委托下单请求
type PlaceAlgoOrderReq struct {
	InstId     string      // 产品 ID
	MarginMode MarginMode  // 逐仓/全仓
	Side       OrderSide   // buy / sell
	PosSide    PosSide     // long / short
	OrdType    AlgoOrdType // conditional / trigger
	Sz         string      // 委托数量
	ReduceOnly bool        // 是否只减仓
	ClOrdId    string      // 客户自定义策略 ID（对应 OKX 的 algoClOrdId）

	// 止盈相关
	TpTriggerPx     string
	TpOrdPx         string
	TpTriggerPxType TriggerPxType

	// 止损相关
	SlTriggerPx     string
	SlOrdPx         string
	SlTriggerPxType TriggerPxType

	// 触发单相关 (Trigger Order)
	TriggerPx     string
	TriggerPxType TriggerPxType
	OrderPx       string // 触发后的委托价（"-1" 为市价）
}

// AmendAlgoOrderReq 策略委托改单请求
type AmendAlgoOrderReq struct {
	InstId      string // 产品 ID
	AlgoId      string // 平台分配的策略 ID（与 AlgoClOrdId 二选一）
	AlgoClOrdId string // 客户自定义 ID
	NewSz       string // 新数量

	NewTpTriggerPx string
	NewTpOrdPx     string
	NewSlTriggerPx string
	NewSlOrdPx     string
}

// CancelAlgoOrderReq 策略委托撤单请求
type CancelAlgoOrderReq struct {
	InstId string // 产品 ID
	AlgoId string // 平台分配的策略 ID
}

// AlgoUpdate 策略委托状态推送（对应 OKX 的订单/策略推送转换后）
type AlgoUpdate struct {
	AlgoId      string
	AlgoClOrdId string
	InstId      string
	State       string // effective / canceled / order_failed / ...
	UpdateAt    int64
}

// GetClOrdId 实现 listener.Identifiable
func (a *AlgoUpdate) GetClOrdId() string { return a.AlgoClOrdId }
