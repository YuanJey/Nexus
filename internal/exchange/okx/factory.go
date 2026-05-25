package okx

import (
	"context"
	"sync"

	"github.com/YuanJey/nexus/pkg/modules"
)

// Modules 聚合模块集合，按需创建
type Modules struct {
	Market   modules.MarketModule
	Account  modules.AccountModule
	Position modules.PositionModule
	Trading  modules.TradingModule

	wsClients []*wsClient
}

// ─── ModuleOption ───────────────────────────────────────────────

type moduleOptions struct {
	market   bool
	account  bool
	position bool
	trading  bool
}

// ModuleOption 模块启用选项
type ModuleOption func(*moduleOptions)

func WithMarket(v bool) ModuleOption   { return func(o *moduleOptions) { o.market = v } }
func WithAccount(v bool) ModuleOption  { return func(o *moduleOptions) { o.account = v } }
func WithPosition(v bool) ModuleOption { return func(o *moduleOptions) { o.position = v } }
func WithTrading(v bool) ModuleOption  { return func(o *moduleOptions) { o.trading = v } }

// NewModules 创建模块集合
//
//	默认启用全部 4 个模块，可通过 ModuleOption 按需关闭：
//	  NewModules(key, secret, pass, sim, WithPosition(false))
func NewModules(apiKey, secretKey, passphrase string, simulated bool, opts ...ModuleOption) *Modules {
	o := moduleOptions{market: true, account: true, position: true, trading: true}
	for _, opt := range opts {
		opt(&o)
	}

	http := newHTTPClient(apiKey, secretKey, passphrase, simulated)

	pubURL := "wss://ws.okx.com:8443/ws/v5/public"
	priURL := "wss://ws.okx.com:8443/ws/v5/private"
	bizURL := "wss://ws.okx.com:8443/ws/v5/business"
	if simulated {
		pubURL = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
		priURL = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
		bizURL = "wss://wspap.okx.com:8443/ws/v5/business?brokerId=9999"
	}

	var wsClients []*wsClient
	m := &Modules{}

	// Market — public WS
	if o.market {
		pubWs := newWSClient(pubURL)
		m.Market = newMarketModule(http, pubWs)
		wsClients = append(wsClients, pubWs)
	}

	// Account / Position — 共享一条 private WS
	if o.account || o.position {
		priWs1 := newWSClient(priURL)
		priWs1.setAuth(apiKey, secretKey, passphrase)
		if o.account {
			m.Account = newAccountModule(http, priWs1)
		}
		if o.position {
			m.Position = newPositionModule(http, priWs1)
		}
		wsClients = append(wsClients, priWs1)
	}

	// Trading — private WS (orders) + business WS (orders-algo)
	if o.trading {
		priWs2 := newWSClient(priURL)
		priWs2.setAuth(apiKey, secretKey, passphrase)

		bizWs := newWSClient(bizURL)
		bizWs.setAuth(apiKey, secretKey, passphrase)

		m.Trading = newTradingModule(http, priWs2, bizWs)
		wsClients = append(wsClients, priWs2, bizWs)
	}

	m.wsClients = wsClients
	return m
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
