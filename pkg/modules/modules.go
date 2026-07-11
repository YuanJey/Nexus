package modules

import (
	"context"

	"github.com/YuanJey/nexus/pkg/models"
)

// ─── Listener 接口（SDK 定义，调用方实现）─────────────────────────

type TickerListener interface {
	OnTicker(ticker *models.Ticker)
}

type CandleListener interface {
	OnCandle(candle *models.Candle)
}

type AccountListener interface {
	OnAccount(account *models.Account)
}

type PositionListener interface {
	OnPosition(pos *models.Position)
}

type OrderListener interface {
	OnOrder(update *models.OrderUpdate)
}

type AlgoOrderListener interface {
	OnAlgoOrder(update *models.AlgoUpdate)
}

// ─── OrderEvent ─────────────────────────────────────────────────

type OrderEvent string

const (
	OrderEventAll      OrderEvent = "all"
	OrderEventNew      OrderEvent = "new"
	OrderEventPartial  OrderEvent = "partial"
	OrderEventFilled   OrderEvent = "filled"
	OrderEventCanceled OrderEvent = "canceled"
)

// ─── Module 接口 ────────────────────────────────────────────────

type MarketModule interface {
	AttachTicker(instId string, listener TickerListener) (detach func())
	AttachTickers(instIds []string, listener TickerListener) (detach func())
	AttachCandle(instId, timeframe string, listener CandleListener) (detach func())
	GetOHLCV(ctx context.Context, instId, timeframe string, limit int, after ...string) ([]models.Candle, error)
	GetInstrument(ctx context.Context, instId string) (*models.Instrument, error)
	GetOptSummary(ctx context.Context, instFamily, expTime string) ([]models.OptSummary, error)
}

type AccountModule interface {
	AttachAccount(listener AccountListener) (detach func())
	GetAccount(ctx context.Context) (*models.Account, error)
}

type PositionModule interface {
	AttachPosition(instId string, listener PositionListener) (detach func())
	GetPositions(ctx context.Context, instId string) ([]models.Position, error)
}

type TradingModule interface {
	SetLeverage(ctx context.Context, instId string, lever string, mgnMode string) error
	SetPositionMode(ctx context.Context, posMode string) error

	PlaceOrders(ctx context.Context, orders []models.PlaceOrderReq) error
	CloseOrders(ctx context.Context, orders []models.PlaceOrderReq) error
	ClosePositions(ctx context.Context, positions []models.ClosePositionReq) error
	AmendOrders(ctx context.Context, orders []models.AmendOrderReq) error
	CancelOrders(ctx context.Context, orders []models.CancelOrderReq) error
	CancelAllOrders(ctx context.Context, instId string) error

	PlaceAlgoOrders(ctx context.Context, orders []models.PlaceAlgoOrderReq) error
	AmendAlgoOrders(ctx context.Context, orders []models.AmendAlgoOrderReq) error
	CancelAlgoOrders(ctx context.Context, orders []models.CancelAlgoOrderReq) error
	CancelAllAlgoOrders(ctx context.Context, instId string) error
	CancelAlgoByClOrdId(ctx context.Context, instId, algoClOrdId string) error

	AttachOrder(listener OrderListener) (detach func())
	AttachAlgoOrder(listener AlgoOrderListener) (detach func())

	OnceOrder(clOrdId string, event OrderEvent, listener OrderListener) (detach func())
	OnceAlgoOrder(algoClOrdId string, event OrderEvent, listener AlgoOrderListener) (detach func())

	GetOrder(ctx context.Context, instId, clOrdId, ordId string) (*models.OrderUpdate, error)
}

// ─── Config ─────────────────────────────────────────────────────

type ExchangeType string

const (
	OKX     ExchangeType = "OKX"
	BINANCE ExchangeType = "BINANCE"
)

type Config struct {
	Exchange   ExchangeType
	APIKey     string
	SecretKey  string
	Passphrase string
	Simulated  bool
}
