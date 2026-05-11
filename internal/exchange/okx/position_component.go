package okx

import (
	"encoding/json"
	"sync"

	"github.com/YuanJey/nexus/pkg/models"
	"github.com/YuanJey/nexus/pkg/modules"
)

type positionComponent struct {
	mu        sync.RWMutex
	listeners map[string]map[int]modules.PositionListener
	nextID    int
}

func newPositionComponent() *positionComponent {
	return &positionComponent{
		listeners: make(map[string]map[int]modules.PositionListener),
	}
}

func (c *positionComponent) Attach(instId string, l modules.PositionListener) func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextID
	c.nextID++
	if c.listeners[instId] == nil {
		c.listeners[instId] = make(map[int]modules.PositionListener)
	}
	c.listeners[instId][id] = l

	return func() {
		c.mu.Lock()
		delete(c.listeners[instId], id)
		c.mu.Unlock()
	}
}

func (c *positionComponent) handleMessage(msg []byte) {
	var resp wsResponse
	if err := json.Unmarshal(msg, &resp); err != nil {
		return
	}
	if resp.Arg.Channel != "positions" || len(resp.Data) == 0 {
		return
	}

	for _, rawData := range resp.Data {
		var okxPos okxWsPosition
		if err := json.Unmarshal(rawData, &okxPos); err != nil {
			continue
		}
		pos := parsePosition(&okxPos)

		c.mu.RLock()
		if ls, ok := c.listeners[pos.InstId]; ok {
			for _, l := range ls {
				l.OnPosition(&pos)
			}
		}
		if ls, ok := c.listeners[""]; ok {
			for _, l := range ls {
				l.OnPosition(&pos)
			}
		}
		c.mu.RUnlock()
	}
}

func parsePosition(data *okxWsPosition) models.Position {
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
