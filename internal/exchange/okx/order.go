package okx

// ══════════════════════════════════════════════════════
// OKX 特定结构（内部驱动层，外部不可见）
// 由 converter.go 负责与 pkg/models 标准模型双向转换
// ══════════════════════════════════════════════════════

type Side string

const (
	SideBuy  Side = "buy"
	SideSell Side = "sell"
)

type OrderType string

const (
	OrderMarket   OrderType = "market"    // 市价单
	OrderLimit    OrderType = "limit"     // 限价单
	OrderPostOnly OrderType = "post_only" // 只做Maker单
	OrderFok      OrderType = "fok"       // 全部成交或立即取消
	OrderIoc      OrderType = "ioc"       // 立即成交或立即取消
)

// 附带止盈止损( attachAlgoOrds) 关键点
// tpOrdPx/slOrdPx: 填"-1"执行市价止盈止损
// tpTriggerPxType/slTriggerPxType: 建议使用mark（标记价格）防止插针
type PosSide string

const (
	PosSideLong  PosSide = "long"
	PosSideShort PosSide = "short"
)

type Order struct {
	InstId           string    `json:"instId"`  // 产品ID，如 ETH-USDT
	TdMode           string    `json:"tdMode"`  // isolated：逐仓 cross：全仓
	Side             Side      `json:"side"`    // buy：买， sell：卖
	OrdType          OrderType `json:"ordType"` // market：市价单 limit：限价单
	Sz               string    `json:"sz"`
	Px               string    `json:"px"`
	Ccy              *string   `json:"ccy"`
	ClOrdId          string    `json:"clOrdId"` // 客户自定义订单ID，1-32位字母数字
	Tag              *string   `json:"tag"`     // 订单标签
	PosSide          PosSide   `json:"posSide"`
	ReduceOnly       *bool     `json:"reduceOnly"`
	TgtCcy           *string   `json:"tgtCcy"`
	BanAmend         *bool     `json:"banAmend"`
	PxAmendType      *string   `json:"pxAmendType"`
	StpMode          *string   `json:"stpMode"`
	TradeQuoteCcy    *string   `json:"tradeQuoteCcy"`
	IsElpTakerAccess *bool     `json:"isElpTakerAccess"`
	AttachAlgoOrds   []AlgoOrd `json:"attachAlgoOrds"`
}

// AlgoOrd 附带止盈止损算法单
type AlgoOrd struct {
	AttachAlgoClOrdId    string `json:"attachAlgoClOrdId"`
	TpTriggerPx          string `json:"tpTriggerPx"` // 止盈触发价
	TpOrdPx              string `json:"tpOrdPx"`     // 止盈委托价（-1 为市价）
	TpOrdKind            string `json:"tpOrdKind"`
	TpTriggerPxType      string `json:"tpTriggerPxType"` // last / mark / index
	SlTriggerPx          string `json:"slTriggerPx"`     // 止损触发价
	SlOrdPx              string `json:"slOrdPx"`         // 止损委托价（-1 为市价）
	SlTriggerPxType      string `json:"slTriggerPxType"` // last / mark / index
	Sz                   string `json:"sz"`
	AmendPxOnTriggerType string `json:"amendPxOnTriggerType"`
}

// ──────────────────────────────────────────
// 止盈止损选项（函数选项模式）
// ──────────────────────────────────────────

// OrderOption 构造函数可选参数
type OrderOption func(*Order)

// WithTakeProfit 设置止盈（触发价 tpPx，委托价 tpOrdPx，-1 为市价）
// triggerType: last | mark | index，建议使用 mark 防止插针
func WithTakeProfit(algoClOrdId, tpPx, tpOrdPx, triggerType string) OrderOption {
	return func(o *Order) {
		if len(o.AttachAlgoOrds) == 0 {
			o.AttachAlgoOrds = []AlgoOrd{{}}
		}
		o.AttachAlgoOrds[0].AttachAlgoClOrdId = algoClOrdId
		o.AttachAlgoOrds[0].TpTriggerPx = tpPx
		o.AttachAlgoOrds[0].TpOrdPx = tpOrdPx
		o.AttachAlgoOrds[0].TpTriggerPxType = triggerType
	}
}

// WithStopLoss 设置止损（触发价 slPx，委托价 slOrdPx，-1 为市价）
func WithStopLoss(algoClOrdId, slPx, slOrdPx, triggerType string) OrderOption {
	return func(o *Order) {
		if len(o.AttachAlgoOrds) == 0 {
			o.AttachAlgoOrds = []AlgoOrd{{}}
		}
		o.AttachAlgoOrds[0].AttachAlgoClOrdId = algoClOrdId
		o.AttachAlgoOrds[0].SlTriggerPx = slPx
		o.AttachAlgoOrds[0].SlOrdPx = slOrdPx
		o.AttachAlgoOrds[0].SlTriggerPxType = triggerType
	}
}

// ──────────────────────────────────────────
// 开多 / 开空 / 平多 / 平空 构造函数
// ──────────────────────────────────────────

// newBaseOrder 构建公共字段
func newBaseOrder(instId, tdMode, clOrdId, sz, px string, ordType OrderType, posSide PosSide, side Side, opts []OrderOption) *Order {
	o := &Order{
		InstId:  instId,
		TdMode:  tdMode,
		Side:    side,
		OrdType: ordType,
		Sz:      sz,
		Px:      px,
		ClOrdId: clOrdId,
		PosSide: posSide,
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// OpenLong 开多单（buy / long）
func OpenLong(instId, tdMode, clOrdId, sz, px string, ordType OrderType, opts ...OrderOption) *Order {
	return newBaseOrder(instId, tdMode, clOrdId, sz, px, ordType, PosSideLong, SideBuy, opts)
}

// OpenShort 开空单（sell / short）
func OpenShort(instId, tdMode, clOrdId, sz, px string, ordType OrderType, opts ...OrderOption) *Order {
	return newBaseOrder(instId, tdMode, clOrdId, sz, px, ordType, PosSideShort, SideSell, opts)
}

// CloseLong 平多单（sell / long，自动设置 reduceOnly=true）
func CloseLong(instId, tdMode, clOrdId, sz, px string, ordType OrderType, opts ...OrderOption) *Order {
	t := true
	o := newBaseOrder(instId, tdMode, clOrdId, sz, px, ordType, PosSideLong, SideSell, opts)
	o.ReduceOnly = &t
	return o
}

// CloseShort 平空单（buy / short，自动设置 reduceOnly=true）
func CloseShort(instId, tdMode, clOrdId, sz, px string, ordType OrderType, opts ...OrderOption) *Order {
	t := true
	o := newBaseOrder(instId, tdMode, clOrdId, sz, px, ordType, PosSideShort, SideBuy, opts)
	o.ReduceOnly = &t
	return o
}

// ──────────────────────────────────────────
// ClosePosition 一键平仓
// ──────────────────────────────────────────

type ClosePosition struct {
	InstId  string   `json:"instId"`
	MgnMode string   `json:"mgnMode"`
	PosSide *PosSide `json:"posSide,omitempty"`
	Ccy     *string  `json:"ccy,omitempty"`
	AutoCxl *bool    `json:"autoCxl,omitempty"`
	ClOrdId *string  `json:"clOrdId,omitempty"`
	Tag     *string  `json:"tag,omitempty"`
}

// ClosePositionOption ClosePosition 可选参数
type ClosePositionOption func(*ClosePosition)

// WithCcy 设置保证金币种（全仓单币种保证金时必填）
func WithCcy(ccy string) ClosePositionOption {
	return func(c *ClosePosition) { c.Ccy = &ccy }
}

// WithAutoCxl 平仓触发失败时自动撤销关联止盈止损
func WithAutoCxl() ClosePositionOption {
	t := true
	return func(c *ClosePosition) { c.AutoCxl = &t }
}

// WithCloseClOrdId 设置客户自定义 ID
func WithCloseClOrdId(clOrdId string) ClosePositionOption {
	return func(c *ClosePosition) { c.ClOrdId = &clOrdId }
}

// WithCloseTag 设置订单标签
func WithCloseTag(tag string) ClosePositionOption {
	return func(c *ClosePosition) { c.Tag = &tag }
}

func newClosePosition(instId, mgnMode string, posSide PosSide, opts []ClosePositionOption) *ClosePosition {
	c := &ClosePosition{
		InstId:  instId,
		MgnMode: mgnMode,
		PosSide: &posSide,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// NewCloseLongPosition 一键平多（posSide=long）
func NewCloseLongPosition(instId, mgnMode string, opts ...ClosePositionOption) *ClosePosition {
	return newClosePosition(instId, mgnMode, PosSideLong, opts)
}

// NewCloseShortPosition 一键平空（posSide=short）
func NewCloseShortPosition(instId, mgnMode string, opts ...ClosePositionOption) *ClosePosition {
	return newClosePosition(instId, mgnMode, PosSideShort, opts)
}

// ──────────────────────────────────────────
// AmendOrder 改单
// ──────────────────────────────────────────

type AmendOrder struct {
	InstId      string         `json:"instId"`
	CxlOnFail   *bool          `json:"cxlOnFail,omitempty"`
	OrdId       *string        `json:"ordId,omitempty"`
	ClOrdId     *string        `json:"clOrdId,omitempty"`
	ReqId       *string        `json:"reqId,omitempty"`
	NewSz       *string        `json:"newSz,omitempty"`
	NewPx       *string        `json:"newPx,omitempty"`
	PxAmendType *string        `json:"pxAmendType,omitempty"`
	AttachAlgo  []AmendAlgoOrd `json:"attachAlgoOrds,omitempty"`
}

type AmendAlgoOrd struct {
	AttachAlgoId       string  `json:"attachAlgoId"` // 改单时必填
	NewTpTriggerPx     *string `json:"newTpTriggerPx,omitempty"`
	NewTpOrdPx         *string `json:"newTpOrdPx,omitempty"`
	NewTpTriggerPxType *string `json:"newTpTriggerPxType,omitempty"`
	NewSlTriggerPx     *string `json:"newSlTriggerPx,omitempty"`
	NewSlOrdPx         *string `json:"newSlOrdPx,omitempty"`
	NewSlTriggerPxType *string `json:"newSlTriggerPxType,omitempty"`
}

// AmendOrderOption AmendOrder 可选参数
type AmendOrderOption func(*AmendOrder)

// WithAmendReqId 设置请求 ID
func WithAmendReqId(reqId string) AmendOrderOption {
	return func(a *AmendOrder) { a.ReqId = &reqId }
}

// WithAmendNewSz 修改委托数量
func WithAmendNewSz(sz string) AmendOrderOption {
	return func(a *AmendOrder) { a.NewSz = &sz }
}

// WithAmendNewPx 修改委托价格
func WithAmendNewPx(px string) AmendOrderOption {
	return func(a *AmendOrder) { a.NewPx = &px }
}

// WithAmendCxlOnFail 改单失败时自动撤单
func WithAmendCxlOnFail() AmendOrderOption {
	t := true
	return func(a *AmendOrder) { a.CxlOnFail = &t }
}

// WithAmendPxAmendType 价格修改类型
func WithAmendPxAmendType(t string) AmendOrderOption {
	return func(a *AmendOrder) { a.PxAmendType = &t }
}

// WithAmendAlgo 修改附带止盈止损（传 "" 表示不修改该字段）
func WithAmendAlgo(attachAlgoId, newTpTriggerPx, newTpOrdPx, newTpTriggerPxType, newSlTriggerPx, newSlOrdPx, newSlTriggerPxType string) AmendOrderOption {
	return func(a *AmendOrder) {
		algo := AmendAlgoOrd{AttachAlgoId: attachAlgoId}
		if newTpTriggerPx != "" {
			algo.NewTpTriggerPx = &newTpTriggerPx
		}
		if newTpOrdPx != "" {
			algo.NewTpOrdPx = &newTpOrdPx
		}
		if newTpTriggerPxType != "" {
			algo.NewTpTriggerPxType = &newTpTriggerPxType
		}
		if newSlTriggerPx != "" {
			algo.NewSlTriggerPx = &newSlTriggerPx
		}
		if newSlOrdPx != "" {
			algo.NewSlOrdPx = &newSlOrdPx
		}
		if newSlTriggerPxType != "" {
			algo.NewSlTriggerPxType = &newSlTriggerPxType
		}
		a.AttachAlgo = append(a.AttachAlgo, algo)
	}
}

func newAmendOrder(instId string, opts []AmendOrderOption) *AmendOrder {
	a := &AmendOrder{InstId: instId}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// NewAmendOrderByOrdId 通过平台订单 ID 改单
func NewAmendOrderByOrdId(instId, ordId string, opts ...AmendOrderOption) *AmendOrder {
	a := newAmendOrder(instId, opts)
	a.OrdId = &ordId
	return a
}

// NewAmendOrderByClOrdId 通过客户自定义订单 ID 改单
func NewAmendOrderByClOrdId(instId, clOrdId string, opts ...AmendOrderOption) *AmendOrder {
	a := newAmendOrder(instId, opts)
	a.ClOrdId = &clOrdId
	return a
}

// ──────────────────────────────────────────
// Cancel 撤单
// ──────────────────────────────────────────

// Cancel ordId和clOrdId必须传一个，若传两个，以ordId为主
type Cancel struct {
	InstId  string  `json:"instId"`
	OrdId   *string `json:"ordId,omitempty"`
	ClOrdId *string `json:"clOrdId,omitempty"`
}

// NewCancelByOrdId 通过平台订单 ID 撤单
func NewCancelByOrdId(instId, ordId string) *Cancel {
	return &Cancel{InstId: instId, OrdId: &ordId}
}

// NewCancelByClOrdId 通过客户自定义订单 ID 撤单
func NewCancelByClOrdId(instId, clOrdId string) *Cancel {
	return &Cancel{InstId: instId, ClOrdId: &clOrdId}
}

// ──────────────────────────────────────────
// Resp OKX 通用响应
// ──────────────────────────────────────────

type Resp struct {
	Code    string `json:"code"`
	Msg     string `json:"msg"`
	Data    any    `json:"data"`
	InTime  string `json:"inTime"`
	OutTime string `json:"outTime"`
}
