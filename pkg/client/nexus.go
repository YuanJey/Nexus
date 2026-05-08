package nexus

import (
	"github.com/YuanJey/nexus/internal/exchange/okx"
	"github.com/YuanJey/nexus/pkg/execution"
	"github.com/YuanJey/nexus/pkg/stream"
)

// ──────────────────────────────────────────────────────────────
// Nexus SDK 统一入口
//
// 策略层通过 New(config) 获取 Client，Client 组合了：
//   - Execution: 下单/改单/撤单/平仓 + 订单回调
//   - Stream:    行情/账户/持仓 持久订阅
//
// ──────────────────────────────────────────────────────────────

type ExchangeType string

const (
	OKX     ExchangeType = "OKX"
	BINANCE ExchangeType = "BINANCE"
)

// Config SDK 初始化配置
type Config struct {
	Exchange   ExchangeType // OKX / BINANCE
	APIKey     string
	SecretKey  string
	Passphrase string // OKX 专用
	Simulated  bool   // true = 模拟盘
}

// Client SDK 核心客户端，策略层唯一交互入口
type Client struct {
	Execution execution.Execution
	Stream    stream.Stream
	cfg       Config
}

// New 创建 SDK 客户端
func New(cfg Config) *Client {
	c := &Client{cfg: cfg}
	switch cfg.Exchange {
	case OKX:
		c.Execution = okx.NewExecutor(cfg.APIKey, cfg.SecretKey, cfg.Passphrase, cfg.Simulated)
		c.Stream = okx.NewStream(cfg.APIKey, cfg.SecretKey, cfg.Passphrase, cfg.Simulated)
	}
	return c
}
