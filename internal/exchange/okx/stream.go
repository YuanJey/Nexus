package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	logger "github.com/YuanJey/go-log/pkg/log"

	"github.com/YuanJey/nexus/pkg/listener"
	"github.com/YuanJey/nexus/pkg/models"
)

// ──────────────────────────────────────────────────────────────
// Stream 实现 execution.Stream 接口
// ──────────────────────────────────────────────────────────────

type Stream struct {
	apiKey     string
	secretKey  string
	passphrase string
	simulated  bool

	tickerObserver   *listener.StreamObserver[models.Ticker]
	accountObserver  *listener.StreamObserver[models.Account]
	positionObserver *listener.StreamObserver[models.Position]

	pubWs *wsClient
	priWs *wsClient

	mu          sync.Mutex
	tickerSubs  map[string]int
	accountSub  bool
	positionSub bool
}

// NewStream 创建 OKX Stream 实例
func NewStream(apiKey, secretKey, passphrase string, simulated bool) *Stream {
	pubUrl := "wss://ws.okx.com:8443/ws/v5/public"
	priUrl := "wss://ws.okx.com:8443/ws/v5/private"
	if simulated {
		pubUrl = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
		priUrl = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
	}

	s := &Stream{
		apiKey:           apiKey,
		secretKey:        secretKey,
		passphrase:       passphrase,
		simulated:        simulated,
		tickerObserver:   listener.NewStreamObserver[models.Ticker](),
		accountObserver:  listener.NewStreamObserver[models.Account](),
		positionObserver: listener.NewStreamObserver[models.Position](),
		tickerSubs:       make(map[string]int),
	}

	s.pubWs = newWSClient(pubUrl, s.handlePublicMsg)
	s.priWs = newWSClient(priUrl, s.handlePrivateMsg)
	// 注入认证信息用于私有频道登录
	s.priWs.setAuth(apiKey, secretKey, passphrase)

	return s
}

func (s *Stream) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		if err := s.pubWs.Start(ctx); err != nil {
			logger.NewError("", fmt.Sprintf("Public WS error: %v", err))
		}
	}()

	go func() {
		defer wg.Done()
		if err := s.priWs.Start(ctx); err != nil {
			logger.NewError("", fmt.Sprintf("Private WS error: %v", err))
		}
	}()

	// Wait until ctx is done
	<-ctx.Done()
	return ctx.Err()
}

// ─── 订阅实现 ──────────────────────────────────────────────────

func (s *Stream) SubscribeTicker(instId string, handler func(*models.Ticker)) listener.HandlerID {
	id := s.tickerObserver.Subscribe(handler)

	s.mu.Lock()
	s.tickerSubs[instId]++
	if s.tickerSubs[instId] == 1 {
		// 发送订阅请求
		s.pubWs.Subscribe([]map[string]string{{"channel": "tickers", "instId": instId}})
	}
	s.mu.Unlock()
	return id
}

func (s *Stream) UnsubscribeTicker(instId string, id listener.HandlerID) {
	s.tickerObserver.Unsubscribe(id)

	s.mu.Lock()
	s.tickerSubs[instId]--
	if s.tickerSubs[instId] <= 0 {
		delete(s.tickerSubs, instId)
		// 取消订阅
		s.pubWs.Unsubscribe([]map[string]string{{"channel": "tickers", "instId": instId}})
	}
	s.mu.Unlock()
}

func (s *Stream) SubscribeAccount(handler func(*models.Account)) listener.HandlerID {
	id := s.accountObserver.Subscribe(handler)

	s.mu.Lock()
	if !s.accountSub {
		s.accountSub = true
		s.priWs.Subscribe([]map[string]string{{"channel": "account"}})
	}
	s.mu.Unlock()
	return id
}

func (s *Stream) UnsubscribeAccount(id listener.HandlerID) {
	s.accountObserver.Unsubscribe(id)
	// 为简单起见，暂不实现完全取消订阅，因为可能有其他 listener
}

func (s *Stream) SubscribePosition(instId string, handler func(*models.Position)) listener.HandlerID {
	id := s.positionObserver.Subscribe(handler)

	s.mu.Lock()
	if !s.positionSub {
		s.positionSub = true
		s.priWs.Subscribe([]map[string]string{{"channel": "positions", "instType": "ANY"}})
	}
	s.mu.Unlock()
	return id
}

func (s *Stream) UnsubscribePosition(instId string, id listener.HandlerID) {
	s.positionObserver.Unsubscribe(id)
}

// ─── 消息处理 ──────────────────────────────────────────────────

func (s *Stream) handlePublicMsg(msg []byte) {
	var resp wsResponse
	if err := json.Unmarshal(msg, &resp); err != nil {
		return
	}
	if resp.Arg.Channel == "tickers" && len(resp.Data) > 0 {
		for _, rawData := range resp.Data {
			var okxTicker map[string]interface{}
			if err := json.Unmarshal(rawData, &okxTicker); err == nil {
				ticker := s.parseTicker(okxTicker)
				s.tickerObserver.Dispatch(&ticker)
			}
		}
	}
}

func (s *Stream) handlePrivateMsg(msg []byte) {
	var resp wsResponse
	if err := json.Unmarshal(msg, &resp); err != nil {
		return
	}

	if len(resp.Data) == 0 {
		return
	}

	switch resp.Arg.Channel {
	case "account":
		for _, rawData := range resp.Data {
			var okxAcc okxWsAccount
			if err := json.Unmarshal(rawData, &okxAcc); err == nil {
				acc := s.parseAccount(&okxAcc)
				s.accountObserver.Dispatch(&acc)
			}
		}
	case "positions":
		for _, rawData := range resp.Data {
			var okxPos okxWsPosition
			if err := json.Unmarshal(rawData, &okxPos); err == nil {
				pos := s.parsePosition(&okxPos)
				s.positionObserver.Dispatch(&pos)
			}
		}
	}
}

// ─── 解析逻辑 ──────────────────────────────────────────────────

func (s *Stream) parseTicker(data map[string]interface{}) models.Ticker {
	getString := func(k string) string {
		if v, ok := data[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
		return ""
	}
	tsStr := getString("ts")
	var ts int64
	fmt.Sscanf(tsStr, "%d", &ts)

	return models.Ticker{
		InstId:    getString("instId"),
		Last:      getString("last"),
		BidPx:     getString("bidPx"),
		AskPx:     getString("askPx"),
		Open24H:   getString("open24h"),
		High24H:   getString("high24h"),
		Low24H:    getString("low24h"),
		Vol24H:    getString("vol24h"),
		VolCcy24H: getString("volCcy24h"),
		Ts:        ts,
	}
}

func (s *Stream) parseAccount(data *okxWsAccount) models.Account {
	acc := models.Account{
		TotalEq: data.TotalEq,
		Ts:      data.UTime,
	}

	for _, details := range data.Details {
		acc.Balances = append(acc.Balances, models.AccountBalance{
			Ccy:       details.Ccy,
			Eq:        details.Eq,
			AvailEq:   details.AvailEq,
			FrozenBal: details.FrozenBal,
			UPL:       details.Upl,
		})
	}
	return acc
}

func (s *Stream) parsePosition(data *okxWsPosition) models.Position {
	posSide := models.PosSideNet
	if data.PosSide == "long" {
		posSide = models.PosSideLong
	} else if data.PosSide == "short" {
		posSide = models.PosSideShort
	}

	return models.Position{
		InstId:   data.InstId,
		PosSide:  posSide,
		Pos:      data.Pos,
		AvailPos: data.AvailPos,
		AvgPx:    data.AvgPx,
		UPL:      data.Upl,
		Lever:    data.Lever,
		LiqPx:    data.LiqPx,
		Margin:   data.Margin,
		Ts:       data.UTime,
	}
}

// ─── 结构体定义 ──────────────────────────────────────────────────

type wsResponse struct {
	Event string            `json:"event,omitempty"`
	Arg   wsArg             `json:"arg"`
	Data  []json.RawMessage `json:"data,omitempty"`
}

type wsArg struct {
	Channel string `json:"channel"`
	InstId  string `json:"instId,omitempty"`
}

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
