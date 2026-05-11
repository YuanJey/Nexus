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

// ModuleOption 模块启用选项
type ModuleOption = okx.ModuleOption

const (
	OKX     = modules.OKX
	BINANCE = modules.BINANCE
)

// 模块启用选项（默认全部启用）
var (
	WithMarket   = okx.WithMarket   // 是否开启行情
	WithAccount  = okx.WithAccount  // 是否开启账户
	WithPosition = okx.WithPosition // 是否开启持仓
	WithTrading  = okx.WithTrading  // 是否开启交易
)

// 默认全部启用（向后兼容）
// sdk := nexus.New(cfg)
// 只需行情
// sdk := nexus.New(cfg, nexus.WithAccount(false), nexus.WithPosition(false), nexus.WithTrading(false))
// 只需交易
// sdk := nexus.New(cfg, nexus.WithMarket(false))
// 行情 + 交易（不要账户/持仓推送）
// sdk := nexus.New(cfg, nexus.WithAccount(false), nexus.WithPosition(false))
// Ready()/Start()/Stop() 自动只操作实际创建的连接，未启用的模块字段为 nil
// New 创建 SDK 模块集合
func New(cfg Config, opts ...ModuleOption) *Modules {
	return okx.NewModules(cfg.APIKey, cfg.SecretKey, cfg.Passphrase, cfg.Simulated, opts...)
}
