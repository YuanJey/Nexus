package nexus

import (
	"github.com/YuanJey/nexus/internal/exchange/okx"
	"github.com/YuanJey/nexus/pkg/modules"
)

// Modules SDK 模块集合，按需使用
type Modules = okx.Modules

// Config 类型别名
type Config = modules.Config

// ExchangeType 类型别名
type ExchangeType = modules.ExchangeType

const (
	OKX     = modules.OKX
	BINANCE = modules.BINANCE
)

// New 创建 SDK 模块集合
func New(cfg Config) *Modules {
	return okx.NewModules(cfg.APIKey, cfg.SecretKey, cfg.Passphrase, cfg.Simulated)
}
