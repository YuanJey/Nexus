package okx

import (
	"sync"

	"github.com/YuanJey/nexus/pkg/models"
	"github.com/YuanJey/nexus/pkg/modules"
)

type onceAlgoEntry struct {
	algoClOrdId string
	event       modules.OrderEvent
	listener    modules.AlgoOrderListener
}

type algoOrderComponent struct {
	mu         sync.Mutex
	persistent []modules.AlgoOrderListener
	onceList   []onceAlgoEntry
}

func newAlgoOrderComponent() *algoOrderComponent {
	return &algoOrderComponent{}
}

func (c *algoOrderComponent) Attach(l modules.AlgoOrderListener) func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.persistent = append(c.persistent, l)
	idx := len(c.persistent) - 1

	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.persistent[idx] = nil
	}
}

func (c *algoOrderComponent) Once(algoClOrdId string, event modules.OrderEvent, l modules.AlgoOrderListener) func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onceList = append(c.onceList, onceAlgoEntry{algoClOrdId: algoClOrdId, event: event, listener: l})

	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i, e := range c.onceList {
			if e.algoClOrdId == algoClOrdId && e.event == event {
				c.onceList = append(c.onceList[:i], c.onceList[i+1:]...)
				return
			}
		}
	}
}

func (c *algoOrderComponent) handleMessage(msg []byte) {
	type okxAlgoUpdateMsg struct {
		AlgoId      string `json:"algoId"`
		AlgoClOrdId string `json:"algoClOrdId"`
		InstId      string `json:"instId"`
		State       string `json:"state"`
		UTime       int64  `json:"uTime,string"`
	}

	var detail okxAlgoUpdateMsg
	if err := jsonUnmarshalData(msg, "orders-algo", &detail); err != nil {
		return
	}
	if detail.AlgoId == "" {
		return
	}

	update := &models.AlgoUpdate{
		AlgoId:      detail.AlgoId,
		AlgoClOrdId: detail.AlgoClOrdId,
		InstId:      detail.InstId,
		State:       detail.State,
		UpdateAt:    detail.UTime,
	}

	// 持久监听
	c.mu.Lock()
	for _, l := range c.persistent {
		if l != nil {
			l.OnAlgoOrder(update)
		}
	}

	// 一次性监听
	remaining := c.onceList[:0]
	for _, e := range c.onceList {
		if e.algoClOrdId == update.AlgoClOrdId && (e.event == modules.OrderEventAll) {
			e.listener.OnAlgoOrder(update)
		} else {
			remaining = append(remaining, e)
		}
	}
	c.onceList = remaining
	c.mu.Unlock()
}
