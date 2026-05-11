package okx

import (
	"encoding/json"
	"sync"

	"github.com/YuanJey/nexus/pkg/models"
	"github.com/YuanJey/nexus/pkg/modules"
)

type accountComponent struct {
	mu        sync.RWMutex
	listeners map[int]modules.AccountListener
	nextID    int
}

func newAccountComponent() *accountComponent {
	return &accountComponent{
		listeners: make(map[int]modules.AccountListener),
	}
}

func (c *accountComponent) Attach(l modules.AccountListener) func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextID
	c.nextID++
	c.listeners[id] = l

	return func() {
		c.mu.Lock()
		delete(c.listeners, id)
		c.mu.Unlock()
	}
}

func (c *accountComponent) handleMessage(msg []byte) {
	var resp wsResponse
	if err := json.Unmarshal(msg, &resp); err != nil {
		return
	}
	if resp.Arg.Channel != "account" || len(resp.Data) == 0 {
		return
	}

	for _, rawData := range resp.Data {
		var okxAcc okxWsAccount
		if err := json.Unmarshal(rawData, &okxAcc); err != nil {
			continue
		}
		acc := parseAccount(&okxAcc)

		c.mu.RLock()
		for _, l := range c.listeners {
			l.OnAccount(&acc)
		}
		c.mu.RUnlock()
	}
}

func parseAccount(data *okxWsAccount) models.Account {
	acc := models.Account{
		TotalEq: data.TotalEq,
		Ts:      data.UTime,
	}
	for _, d := range data.Details {
		acc.Balances = append(acc.Balances, models.AccountBalance{
			Ccy:       d.Ccy,
			Eq:        d.Eq,
			AvailEq:   d.AvailEq,
			FrozenBal: d.FrozenBal,
			UPL:       d.Upl,
		})
	}
	return acc
}
