package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/YuanJey/nexus/pkg/models"
	"github.com/YuanJey/nexus/pkg/modules"
)

type positionModule struct {
	http  *httpClient
	ws    *wsClient
	comp  *positionComponent
	subMu sync.Mutex
	sub   bool
}

func newPositionModule(http *httpClient, ws *wsClient) *positionModule {
	m := &positionModule{
		http: http,
		ws:   ws,
		comp: newPositionComponent(),
	}
	ws.OnChannel("positions", m.comp.handleMessage)
	return m
}

func (m *positionModule) AttachPosition(instId string, l modules.PositionListener) func() {
	detachComp := m.comp.Attach(instId, l)

	m.subMu.Lock()
	if !m.sub {
		m.sub = true
		_ = m.ws.Subscribe([]map[string]string{{"channel": "positions", "instType": "ANY"}})
	}
	m.subMu.Unlock()

	return detachComp
}

func (m *positionModule) GetPositions(ctx context.Context, instId string) ([]models.Position, error) {
	params := map[string]string{"instType": "SWAP"}
	if instId != "" {
		params["instId"] = instId
	}
	resp, err := m.http.get(ctx, "/api/v5/account/positions", params)
	if err != nil {
		return nil, err
	}

	var data []struct {
		InstId   string `json:"instId"`
		PosSide  string `json:"posSide"`
		Pos      string `json:"pos"`
		AvailPos string `json:"availPos"`
		AvgPx    string `json:"avgPx"`
		Upl      string `json:"upl"`
		Lever    string `json:"lever"`
		LiqPx    string `json:"liqPx"`
		Margin   string `json:"margin"`
		UTime    string `json:"uTime"`
	}
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshal positions: %w", err)
	}

	result := make([]models.Position, 0, len(data))
	for _, d := range data {
		ps := models.PosSideNet
		switch d.PosSide {
		case "long":
			ps = models.PosSideLong
		case "short":
			ps = models.PosSideShort
		}
		result = append(result, models.Position{
			InstId:   d.InstId,
			PosSide:  ps,
			Pos:      d.Pos,
			AvailPos: d.AvailPos,
			AvgPx:    d.AvgPx,
			UPL:      d.Upl,
			Lever:    d.Lever,
			LiqPx:    d.LiqPx,
			Margin:   d.Margin,
		})
	}
	return result, nil
}
