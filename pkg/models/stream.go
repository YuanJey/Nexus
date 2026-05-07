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
