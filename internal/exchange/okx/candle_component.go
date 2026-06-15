package okx

import (
	"encoding/json"
	"strconv"
	"sync"

	"github.com/YuanJey/nexus/pkg/models"
	"github.com/YuanJey/nexus/pkg/modules"
)

type candleComponent struct {
	mu        sync.RWMutex
	listeners map[string]map[int]modules.CandleListener
	nextID    int
}

func newCandleComponent() *candleComponent {
	return &candleComponent{
		listeners: make(map[string]map[int]modules.CandleListener),
	}
}

func (c *candleComponent) Attach(instId string, l modules.CandleListener) func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextID
	c.nextID++
	if c.listeners[instId] == nil {
		c.listeners[instId] = make(map[int]modules.CandleListener)
	}
	c.listeners[instId][id] = l

	return func() {
		c.mu.Lock()
		delete(c.listeners[instId], id)
		c.mu.Unlock()
	}
}

func (c *candleComponent) handleMessage(msg []byte) {
	var resp wsResponse
	if err := json.Unmarshal(msg, &resp); err != nil {
		return
	}
	if len(resp.Data) == 0 {
		return
	}

	for _, rawData := range resp.Data {
		// OKX data 格式: "data":[["ts","o","h","l","c","vol","volCcy","volCcyQuote","confirm"]]
		// 每个 rawData 是一个 []string，不是 [][]string
		var row []string
		if err := json.Unmarshal(rawData, &row); err != nil {
			continue
		}
		if len(row) < 6 {
			continue
		}
		candle := models.Candle{}
		candle.Ts, _ = strconv.ParseInt(row[0], 10, 64)
		candle.Open, _ = strconv.ParseFloat(row[1], 64)
		candle.High, _ = strconv.ParseFloat(row[2], 64)
		candle.Low, _ = strconv.ParseFloat(row[3], 64)
		candle.Close, _ = strconv.ParseFloat(row[4], 64)
		candle.Volume, _ = strconv.ParseFloat(row[5], 64)
		if len(row) > 6 {
			candle.VolCcy, _ = strconv.ParseFloat(row[6], 64)
		}
		if len(row) > 7 {
			candle.VolCcyQuote, _ = strconv.ParseFloat(row[7], 64)
		}
		if len(row) > 8 {
			conf, _ := strconv.Atoi(row[8])
			candle.Confirm = conf
		}

		// 只推送已闭合的 K 线
		if candle.Confirm != 1 {
			continue
		}

		c.mu.RLock()
		if ls, ok := c.listeners[resp.Arg.InstId]; ok {
			for _, l := range ls {
				l.OnCandle(&candle)
			}
		}
		if ls, ok := c.listeners[""]; ok {
			for _, l := range ls {
				l.OnCandle(&candle)
			}
		}
		c.mu.RUnlock()
	}
}
