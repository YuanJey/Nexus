package okx

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/YuanJey/nexus/pkg/models"
	"github.com/YuanJey/nexus/pkg/modules"
)

type tickerComponent struct {
	mu        sync.RWMutex
	listeners map[string]map[int]modules.TickerListener
	nextID    int
}

func newTickerComponent() *tickerComponent {
	return &tickerComponent{
		listeners: make(map[string]map[int]modules.TickerListener),
	}
}

func (c *tickerComponent) Attach(instId string, l modules.TickerListener) func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextID
	c.nextID++
	if c.listeners[instId] == nil {
		c.listeners[instId] = make(map[int]modules.TickerListener)
	}
	c.listeners[instId][id] = l

	return func() {
		c.mu.Lock()
		delete(c.listeners[instId], id)
		c.mu.Unlock()
	}
}

func (c *tickerComponent) handleMessage(msg []byte) {
	var resp wsResponse
	if err := json.Unmarshal(msg, &resp); err != nil {
		return
	}
	if resp.Arg.Channel != "tickers" || len(resp.Data) == 0 {
		return
	}

	for _, rawData := range resp.Data {
		var data map[string]interface{}
		if err := json.Unmarshal(rawData, &data); err != nil {
			continue
		}
		ticker := parseTicker(data)

		c.mu.RLock()
		// 按品种分发
		if ls, ok := c.listeners[ticker.InstId]; ok {
			for _, l := range ls {
				l.OnTicker(&ticker)
			}
		}
		// 全局分发（instId="" 表示全品种）
		if ls, ok := c.listeners[""]; ok {
			for _, l := range ls {
				l.OnTicker(&ticker)
			}
		}
		c.mu.RUnlock()
	}
}

func parseTicker(data map[string]interface{}) models.Ticker {
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
