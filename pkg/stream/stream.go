package stream

import (
	"context"

	"github.com/YuanJey/nexus/pkg/listener"
	"github.com/YuanJey/nexus/pkg/models"
)

// ──────────────────────────────────────────────────────────────
// Stream 行情/账户/持仓订阅接口（exchange-agnostic，公开 API）
//
// 所有订阅均为持久监听，数据通过 StreamObserver 异步分发。
// 策略层调用 Subscribe 拿到 HandlerID，热重载时可 Unsubscribe。
// ──────────────────────────────────────────────────────────────

// Stream 统一行情与账户推送接口
type Stream interface {

	// ─── 生命周期 ─────────────────────────────────────────────

	// Start 启动 WebSocket 连接，开始接收推送（阻塞直到 ctx 取消）
	Start(ctx context.Context) error

	// ─── 行情订阅 ─────────────────────────────────────────────

	// SubscribeTicker 订阅产品实时行情
	// 返回 HandlerID，可通过 UnsubscribeTicker 注销
	SubscribeTicker(instId string, handler func(*models.Ticker)) listener.HandlerID

	// UnsubscribeTicker 注销行情订阅
	UnsubscribeTicker(instId string, id listener.HandlerID)

	// ─── 账户订阅 ─────────────────────────────────────────────

	// SubscribeAccount 订阅账户资产变动（余额/可用/冻结）
	SubscribeAccount(handler func(*models.Account)) listener.HandlerID

	// UnsubscribeAccount 注销账户订阅
	UnsubscribeAccount(id listener.HandlerID)

	// ─── 持仓订阅 ─────────────────────────────────────────────

	// SubscribePosition 订阅持仓变动
	// instId 为空则订阅全账户所有产品持仓
	SubscribePosition(instId string, handler func(*models.Position)) listener.HandlerID

	// UnsubscribePosition 注销持仓订阅
	UnsubscribePosition(instId string, id listener.HandlerID)
}
