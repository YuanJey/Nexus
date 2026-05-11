package okx

import (
	"encoding/json"
	"sync"

	"github.com/YuanJey/nexus/pkg/modules"
)

type onceOrderEntry struct {
	clOrdId  string
	event    modules.OrderEvent
	listener modules.OrderListener
}

type orderComponent struct {
	mu         sync.Mutex
	persistent []modules.OrderListener
	onceList   []onceOrderEntry
}

func newOrderComponent() *orderComponent {
	return &orderComponent{}
}

func (c *orderComponent) Attach(l modules.OrderListener) func() {
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

func (c *orderComponent) Once(clOrdId string, event modules.OrderEvent, l modules.OrderListener) func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onceList = append(c.onceList, onceOrderEntry{clOrdId: clOrdId, event: event, listener: l})

	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i, e := range c.onceList {
			if e.clOrdId == clOrdId && e.event == event {
				c.onceList = append(c.onceList[:i], c.onceList[i+1:]...)
				return
			}
		}
	}
}

func (c *orderComponent) handleMessage(msg []byte) {
	var detail okxOrderDetail
	if err := jsonUnmarshalData(msg, "orders", &detail); err != nil {
		return
	}

	update := toOrderUpdate(detail)

	// 持久监听
	c.mu.Lock()
	for _, l := range c.persistent {
		if l != nil {
			l.OnOrder(update)
		}
	}

	// 一次性监听
	event := stateToOrderEvent(update.State)
	remaining := c.onceList[:0]
	for _, e := range c.onceList {
		if e.clOrdId == update.ClOrdId && (e.event == event || e.event == modules.OrderEventAll) {
			e.listener.OnOrder(update)
		} else {
			remaining = append(remaining, e)
		}
	}
	c.onceList = remaining
	c.mu.Unlock()
}

func stateToOrderEvent(state string) modules.OrderEvent {
	switch state {
	case "live":
		return modules.OrderEventNew
	case "partially_filled":
		return modules.OrderEventPartial
	case "filled":
		return modules.OrderEventFilled
	case "canceled":
		return modules.OrderEventCanceled
	default:
		return modules.OrderEventAll
	}
}

// jsonUnmarshalData 从 WS 响应中提取第一个 data 元素并反序列化
func jsonUnmarshalData(msg []byte, channel string, target interface{}) error {
	var resp wsResponse
	if err := json.Unmarshal(msg, &resp); err != nil {
		return err
	}
	if resp.Arg.Channel != channel || len(resp.Data) == 0 {
		return nil
	}
	return json.Unmarshal(resp.Data[0], target)
}
