package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/YuanJey/nexus/pkg/models"
	"github.com/YuanJey/nexus/pkg/modules"
)

type accountModule struct {
	http  *httpClient
	ws    *wsClient
	comp  *accountComponent
	subMu sync.Mutex
	sub   bool
}

func newAccountModule(http *httpClient, ws *wsClient) *accountModule {
	m := &accountModule{
		http: http,
		ws:   ws,
		comp: newAccountComponent(),
	}
	ws.OnChannel("account", m.comp.handleMessage)
	return m
}

func (m *accountModule) AttachAccount(l modules.AccountListener) func() {
	detachComp := m.comp.Attach(l)

	m.subMu.Lock()
	if !m.sub {
		m.sub = true
		m.ws.SendMsg(context.Background(), map[string]interface{}{
			"op":   "subscribe",
			"args": []map[string]string{{"channel": "account"}},
		})
	}
	m.subMu.Unlock()

	return detachComp
}

func (m *accountModule) GetAccount(ctx context.Context) (*models.Account, error) {
	// REST 查询账户配置获取余额 — 注意：OKX 需要私有频道 WS 的账户推送获取实时数据
	// 这里通过查询余额接口实现快照
	resp, err := m.http.get(ctx, "/api/v5/account/balance", nil)
	if err != nil {
		return nil, err
	}

	var data struct {
		TotalEq string `json:"totalEq"`
		Details []struct {
			Ccy       string `json:"ccy"`
			Eq        string `json:"eq"`
			AvailEq   string `json:"availEq"`
			FrozenBal string `json:"frozenBal"`
			Upl       string `json:"upl"`
		} `json:"details"`
	}
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshal account: %w", err)
	}

	acc := &models.Account{TotalEq: data.TotalEq}
	for _, d := range data.Details {
		acc.Balances = append(acc.Balances, models.AccountBalance{
			Ccy: d.Ccy, Eq: d.Eq, AvailEq: d.AvailEq,
			FrozenBal: d.FrozenBal, UPL: d.Upl,
		})
	}
	return acc, nil
}
