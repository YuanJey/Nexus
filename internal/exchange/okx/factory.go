package okx

import (
	"context"
	"sync"

	"github.com/YuanJey/nexus/pkg/modules"
)

// Modules 聚合 4 个模块，共享底层连接
type Modules struct {
	Market   modules.MarketModule
	Account  modules.AccountModule
	Position modules.PositionModule
	Trading  modules.TradingModule

	wsClients []*wsClient
}

// NewModules 创建共享连接的模块集合
//
//	WS 连接: 1 public + 2 private + 1 business = 4 条
//	pubWs  → MarketModule (tickers)
//	priWs1 → AccountModule (account) + PositionModule (positions)
//	priWs2 → TradingModule (orders)
//	bizWs  → TradingModule (orders-algo)
func NewModules(apiKey, secretKey, passphrase string, simulated bool) *Modules {
	http := newHTTPClient(apiKey, secretKey, passphrase, simulated)

	pubURL := "wss://ws.okx.com:8443/ws/v5/public"
	priURL := "wss://ws.okx.com:8443/ws/v5/private"
	bizURL := "wss://ws.okx.com:8443/ws/v5/business"
	if simulated {
		pubURL = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
		priURL = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
		bizURL = "wss://wspap.okx.com:8443/ws/v5/business?brokerId=9999"
	}

	pubWs := newWSClient(pubURL)

	priWs1 := newWSClient(priURL)
	priWs1.setAuth(apiKey, secretKey, passphrase)

	priWs2 := newWSClient(priURL)
	priWs2.setAuth(apiKey, secretKey, passphrase)

	bizWs := newWSClient(bizURL)
	bizWs.setAuth(apiKey, secretKey, passphrase)

	return &Modules{
		Market:    newMarketModule(http, pubWs),
		Account:   newAccountModule(http, priWs1),
		Position:  newPositionModule(http, priWs1),
		Trading:   newTradingModule(http, priWs2, bizWs),
		wsClients: []*wsClient{pubWs, priWs1, priWs2, bizWs},
	}
}

// Ready 返回一个 channel，在所有 WS 连接就绪后关闭
func (m *Modules) Ready() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		for _, ws := range m.wsClients {
			<-ws.Ready()
		}
		close(ch)
	}()
	return ch
}

// Start 启动所有 WebSocket 连接（阻塞直到 ctx 取消）
func (m *Modules) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, ws := range m.wsClients {
		wg.Add(1)
		go func(w *wsClient) {
			defer wg.Done()
			_ = w.Start(ctx)
		}(ws)
	}
	wg.Wait()
	return ctx.Err()
}

// Stop 关闭所有连接
func (m *Modules) Stop() error {
	for _, ws := range m.wsClients {
		_ = ws.Stop()
	}
	return nil
}
