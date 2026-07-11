package models

// ══════════════════════════════════════════════════════════════
// 行情 / 账户 / 持仓 标准推送模型（exchange-agnostic）
// 驱动层负责从交易所原始报文转换到这里
// ══════════════════════════════════════════════════════════════

// Ticker 实时行情快照
type Ticker struct {
	InstId    string // 产品 ID，如 ETH-USDT-SWAP
	Last      string // 最新成交价
	BidPx     string // 买一价
	AskPx     string // 卖一价
	Open24H   string // 24H 开盘价
	High24H   string // 24H 最高价
	Low24H    string // 24H 最低价
	Vol24H    string // 24H 成交量（张）
	VolCcy24H string // 24H 成交额（计价币）
	Ts        int64  // 时间戳（ms）
}

// AccountBalance 单个币种余额
type AccountBalance struct {
	Ccy       string // 币种
	Eq        string // 权益（含未实现盈亏）
	AvailEq   string // 可用权益
	FrozenBal string // 冻结余额
	UPL       string // 未实现盈亏
}

// Account 账户资产快照（含所有币种）
type Account struct {
	TotalEq  string           // 账户总权益（USD）
	Balances []AccountBalance // 各币种明细
	Ts       int64            // 时间戳（ms）
}

// Position 单笔持仓
type Position struct {
	InstId   string  // 产品 ID
	PosSide  PosSide // long / short / net
	Pos      string  // 持仓量（张）
	AvailPos string  // 可平量
	AvgPx    string  // 开仓均价
	UPL      string  // 未实现盈亏
	Lever    string  // 杠杆倍数
	LiqPx    string  // 预估强平价
	Margin   string  // 保证金
	Ts       int64   // 时间戳（ms）
}

// Candle K 线数据
type Candle struct {
	Ts          int64   `json:"ts"` // 开始时间（单位：毫秒）
	Open        float64 `json:"o"`
	High        float64 `json:"h"`
	Low         float64 `json:"l"`
	Close       float64 `json:"c"`
	Volume      float64 `json:"v"`           // 成交张数 (vol)
	VolCcy      float64 `json:"volCcy"`      // 成交量 (币)
	VolCcyQuote float64 `json:"volCcyQuote"` // 成交额 (计价币)
	Confirm     int     `json:"confirm"`     // 是否确认 (0/1)
}

// Instrument 交易产品信息
type Instrument struct {
	InstID string  // 产品 ID
	CtVal  float64 // 合约面值（如 ETH-USDT-SWAP = 0.1）
	CtMult float64 // 合约乘数
	LotSz  float64 // 最小下单量
}

// OptSummary 期权概要信息（对应 GET /api/v5/public/opt-summary）
type OptSummary struct {
	InstID   string  `json:"instId"`   // 期权合约 ID，如 ETH-USD-260712-1720-C
	InstType string  `json:"instType"` // 产品类型，OPTION
	Uly      string  `json:"uly"`      // 标的资产，如 ETH-USD
	Delta    float64 `json:"delta"`    // 模型 delta
	DeltaBS  float64 `json:"deltaBS"`  // BS delta
	Gamma    float64 `json:"gamma"`    // 模型 gamma
	GammaBS  float64 `json:"gammaBS"`  // BS gamma
	Theta    float64 `json:"theta"`    // 模型 theta
	ThetaBS  float64 `json:"thetaBS"`  // BS theta
	Vega     float64 `json:"vega"`     // 模型 vega
	VegaBS   float64 `json:"vegaBS"`   // BS vega
	MarkVol  float64 `json:"markVol"`  // 标记波动率
	RealVol  float64 `json:"realVol"`  // 已实现波动率
	VolLv    float64 `json:"volLv"`    // 波动率水平
	Distance float64 `json:"distance"` // 距离
	Lever    float64 `json:"lever"`    // 杠杆倍数
	FwdPx    float64 `json:"fwdPx"`    // 远期价格
	AskVol   string  `json:"askVol"`   // 卖盘波动率
	BidVol   string  `json:"bidVol"`   // 买盘波动率
	BuyAPR   string  `json:"buyApr"`   // 买入年化收益率
	SellAPR  string  `json:"sellApr"`  // 卖出年化收益率
	Ts       string  `json:"ts"`       // 时间戳（ms）
}
