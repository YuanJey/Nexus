package okx

import "context"

// ──────────────────────────────────────────────────────────────
// wsClient WebSocket 客户端（预留）
//
// 后续实现：连接池 + 心跳 + 断线重连 + 私有频道登录
// 交易指令（下单/改单/撤单）可通过 SendMsg 走 WS 通道，延迟更低
// ──────────────────────────────────────────────────────────────

type wsClient struct {
	// TODO: 连接池、登录状态、心跳 ticker
}

func newWSClient() *wsClient {
	return &wsClient{}
}

// SendMsg 发送 WS 消息（预留，后续由 WS 交易指令调用）
// msg 将被序列化为 JSON 后发送到 OKX WS 交易频道
func (w *wsClient) SendMsg(ctx context.Context, msg any) error {
	// TODO: 序列化 msg，从连接池取连接，写入 WS frame
	return nil
}

// Start 启动 WS 连接（预留：dial → login → subscribe → heartbeat → reconnect）
func (w *wsClient) Start(ctx context.Context) error {
	// TODO: 实现 WS 生命周期管理
	<-ctx.Done()
	return ctx.Err()
}

// Stop 优雅关闭 WS 连接
func (w *wsClient) Stop() error {
	// TODO: 发送 close frame，等待 goroutine 退出
	return nil
}
