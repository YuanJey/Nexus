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
	http *httpClient
	ws   *wsClient
	comp *tickerComponent
	subs map[string]int
	mu   sync.Mutex
}

func newMarketModule(http *httpClient, ws *wsClient) *marketModule {
	m := &marketModule{
		http: http,
		ws:   ws,
		comp: newTickerComponent(),
		subs: make(map[string]int),
	}
	ws.OnChannel("tickers", m.comp.handleMessage)
	return m
}

// ─── Attach ───────────────────────────────────────────────────

func (m *marketModule) AttachTicker(instId string, l modules.TickerListener) func() {
	detachComp := m.comp.Attach(instId, l)

	m.mu.Lock()
	m.subs[instId]++
	if m.subs[instId] == 1 {
		m.ws.SendMsg(context.Background(), map[string]interface{}{
			"op":   "subscribe",
			"args": []map[string]string{{"channel": "tickers", "instId": instId}},
		})
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

// ─── GetOHLCV ─────────────────────────────────────────────────

func (m *marketModule) GetOHLCV(ctx context.Context, instId, timeframe string, limit int) ([]models.Candle, error) {
	params := map[string]string{
		"instId": instId,
		"bar":    timeframe,
	}
	if limit > 0 {
		params["limit"] = strconv.Itoa(limit)
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
