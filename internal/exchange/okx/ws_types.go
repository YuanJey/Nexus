package okx

import "encoding/json"

// WS 响应信封
type wsResponse struct {
	Event string            `json:"event,omitempty"`
	Arg   wsArg             `json:"arg"`
	Data  []json.RawMessage `json:"data,omitempty"`
}

type wsArg struct {
	Channel string `json:"channel"`
	InstId  string `json:"instId,omitempty"`
}

// 账户推送
type okxWsAccount struct {
	UTime   int64  `json:"uTime,string"`
	TotalEq string `json:"totalEq"`
	Details []struct {
		Ccy       string `json:"ccy"`
		Eq        string `json:"eq"`
		AvailEq   string `json:"availEq"`
		FrozenBal string `json:"frozenBal"`
		Upl       string `json:"upl"`
	} `json:"details"`
}

// 持仓推送
type okxWsPosition struct {
	InstId   string `json:"instId"`
	PosSide  string `json:"posSide"`
	Pos      string `json:"pos"`
	AvailPos string `json:"availPos"`
	AvgPx    string `json:"avgPx"`
	Upl      string `json:"upl"`
	Lever    string `json:"lever"`
	LiqPx    string `json:"liqPx"`
	Margin   string `json:"margin"`
	UTime    int64  `json:"uTime,string"`
}
