package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/YuanJey/nexus/pkg/models"
	"github.com/YuanJey/nexus/pkg/modules"
)

type marketModule struct {
	http       *httpClient
	ws         *wsClient
	comp       *tickerComponent
	candleComp *candleComponent
	subs       map[string]int
	mu         sync.Mutex
}

func newMarketModule(http *httpClient, ws *wsClient) *marketModule {
	m := &marketModule{
		http:       http,
		ws:         ws,
		comp:       newTickerComponent(),
		candleComp: newCandleComponent(),
		subs:       make(map[string]int),
	}
	ws.OnChannel("tickers", m.comp.handleMessage)
	// candle 频道名包含时间周期，需要在 AttachCandle 时动态注册
	return m
}

// ─── GetInstrument ─────────────────────────────────────────────

type okxInstrument struct {
	CtVal  string `json:"ctVal"`
	CtMult string `json:"ctMult"`
	LotSz  string `json:"lotSz"`
}

func (m *marketModule) GetInstrument(ctx context.Context, instId string) (*models.Instrument, error) {
	params := map[string]string{"instType": "SWAP", "instId": instId}
	resp, err := m.http.get(ctx, "/api/v5/public/instruments", params)
	if err != nil {
		return nil, fmt.Errorf("get instrument %s: %w", instId, err)
	}

	var data []okxInstrument
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshal instrument %s: %w", instId, err)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("instrument %s not found", instId)
	}

	ins := &models.Instrument{InstID: instId}
	ins.CtVal, _ = strconv.ParseFloat(data[0].CtVal, 64)
	ins.CtMult, _ = strconv.ParseFloat(data[0].CtMult, 64)
	ins.LotSz, _ = strconv.ParseFloat(data[0].LotSz, 64)
	return ins, nil
}

// ─── Attach ───────────────────────────────────────────────────

func (m *marketModule) AttachTicker(instId string, l modules.TickerListener) func() {
	detachComp := m.comp.Attach(instId, l)

	m.mu.Lock()
	m.subs[instId]++
	if m.subs[instId] == 1 {
		_ = m.ws.Subscribe([]map[string]string{{"channel": "tickers", "instId": instId}})
	}
	m.mu.Unlock()

	return func() {
		detachComp()
		m.mu.Lock()
		m.subs[instId]--
		if m.subs[instId] <= 0 {
			delete(m.subs, instId)
			m.ws.SendMsg(context.Background(), map[string]interface{}{
				"op":   "unsubscribe",
				"args": []map[string]string{{"channel": "tickers", "instId": instId}},
			})
		}
		m.mu.Unlock()
	}
}

func (m *marketModule) AttachTickers(instIds []string, l modules.TickerListener) func() {
	var cancels []func()
	for _, id := range instIds {
		cancels = append(cancels, m.AttachTicker(id, l))
	}
	return func() {
		for _, c := range cancels {
			c()
		}
	}
}

// AttachCandle 订阅 K 线频道，推送已闭合的 K 线
func (m *marketModule) AttachCandle(instId, timeframe string, l modules.CandleListener) func() {
	channel := "candle" + timeframe

	// 动态注册 candle 频道处理器（只需注册一次）
	m.ws.OnChannel(channel, m.candleComp.handleMessage)

	detachComp := m.candleComp.Attach(instId, l)

	// 通过 channel+instId 作为 sub key 区分不同时间周期
	subKey := channel + ":" + instId
	m.mu.Lock()
	m.subs[subKey]++
	if m.subs[subKey] == 1 {
		_ = m.ws.Subscribe([]map[string]string{{"channel": channel, "instId": instId}})
	}
	m.mu.Unlock()

	return func() {
		detachComp()
		m.mu.Lock()
		m.subs[subKey]--
		if m.subs[subKey] <= 0 {
			delete(m.subs, subKey)
			m.ws.SendMsg(context.Background(), map[string]interface{}{
				"op":   "unsubscribe",
				"args": []map[string]string{{"channel": channel, "instId": instId}},
			})
		}
		m.mu.Unlock()
	}
}

// ─── GetOHLCV ─────────────────────────────────────────────────

func (m *marketModule) GetOHLCV(ctx context.Context, instId, timeframe string, limit int, after ...string) ([]models.Candle, error) {
	params := map[string]string{
		"instId": instId,
		"bar":    timeframe,
	}
	if limit > 0 {
		params["limit"] = strconv.Itoa(limit)
	}
	if len(after) > 0 && after[0] != "" {
		params["after"] = after[0]
	}

	resp, err := m.http.get(ctx, "/api/v5/market/candles", params)
	if err != nil {
		return nil, err
	}

	var data [][]string
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshal candles: %w", err)
	}

	result := make([]models.Candle, len(data))
	for i := range data {
		row := data[len(data)-1-i]
		c := models.Candle{}
		c.Ts, _ = strconv.ParseInt(row[0], 10, 64)
		c.Open, _ = strconv.ParseFloat(row[1], 64)
		c.High, _ = strconv.ParseFloat(row[2], 64)
		c.Low, _ = strconv.ParseFloat(row[3], 64)
		c.Close, _ = strconv.ParseFloat(row[4], 64)
		c.Volume, _ = strconv.ParseFloat(row[5], 64)
		if len(row) > 6 {
			c.VolCcy, _ = strconv.ParseFloat(row[6], 64)
		}
		if len(row) > 7 {
			c.VolCcyQuote, _ = strconv.ParseFloat(row[7], 64)
		}
		if len(row) > 8 {
			conf, _ := strconv.Atoi(row[8])
			c.Confirm = conf
		}
		result[i] = c
	}
	return result, nil
}
