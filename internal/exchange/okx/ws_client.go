package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	logger "github.com/YuanJey/go-log/pkg/log"

	"github.com/gorilla/websocket"
)

type wsClient struct {
	url        string
	apiKey     string
	secretKey  string
	passphrase string
	auth       bool

	conn *websocket.Conn
	mu   sync.Mutex

	channelHandlers map[string]func([]byte)
	done            chan struct{}
	stopOnce        sync.Once

	ready     chan struct{}
	readyOnce sync.Once

	subscriptions []map[string]string
}

func (w *wsClient) Ready() <-chan struct{} {
	return w.ready
}

func (w *wsClient) signalReady() {
	w.readyOnce.Do(func() {
		close(w.ready)
	})
}

// OnChannel 注册频道消息处理器
func (w *wsClient) OnChannel(channel string, handler func([]byte)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.channelHandlers[channel] = handler
}

// newWSClient 创建 WebSocket 客户端
func newWSClient(url string) *wsClient {
	return &wsClient{
		url:             url,
		channelHandlers: make(map[string]func([]byte)),
		done:            make(chan struct{}),
		ready:           make(chan struct{}),
	}
}

func (w *wsClient) setAuth(apiKey, secretKey, passphrase string) {
	w.apiKey = apiKey
	w.secretKey = secretKey
	w.passphrase = passphrase
	w.auth = true
}

func (w *wsClient) SendMsg(ctx context.Context, msg any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.conn == nil {
		return nil
	}
	return w.conn.WriteJSON(msg)
}

func (w *wsClient) sendMsg(op string, args []map[string]string) error {
	req := map[string]interface{}{
		"op":   op,
		"args": args,
	}
	return w.conn.WriteJSON(req)
}

func (w *wsClient) Start(ctx context.Context) error {
	backoff := 1 * time.Second
	maxBackoff := 60 * time.Second

	for {
		select {
		case <-w.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := w.connect(ctx)
		if err != nil {
			logger.NewError("", fmt.Sprintf("WS connect failed: %v, retrying in %v...", err, backoff))
		} else {
			// 如果 connect 返回 nil，说明是正常关闭或 ctx done
			select {
			case <-w.done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			default:
				// 意外断开，重置 backoff 或保持？通常连接成功后重置
				backoff = 1 * time.Second
			}
		}

		select {
		case <-w.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func (w *wsClient) connect(ctx context.Context) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, w.url, nil)
	if err != nil {
		return err
	}
	w.mu.Lock()
	w.conn = conn
	w.mu.Unlock()
	defer w.cleanupConn()

	// 1. 登录 (私有频道)
	if w.apiKey != "" {
		if err := w.login(); err != nil {
			return fmt.Errorf("login failed: %w", err)
		}
	}

	// 2. 恢复订阅
	w.mu.Lock()
	if len(w.subscriptions) > 0 {
		if err := w.sendMsg("subscribe", w.subscriptions); err != nil {
			w.mu.Unlock()
			return fmt.Errorf("resubscribe failed: %w", err)
		}
	}
	w.mu.Unlock()

	// 通知连接已就绪
	w.signalReady()

	// 3. 启动读写循环
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go w.pingLoop(readCtx)

	// readLoop 阻塞直到断开
	return w.readLoop(readCtx)
}

func (w *wsClient) Stop() error {
	w.stopOnce.Do(func() {
		close(w.done)
	})
	return w.cleanupConn()
}

func (w *wsClient) cleanupConn() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.readyOnce = sync.Once{}
	w.ready = make(chan struct{})
	if w.conn != nil {
		err := w.conn.Close()
		w.conn = nil
		return err
	}
	return nil
}

func (w *wsClient) login() error {
	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	signStr := sign(timestamp, "GET", "/users/self/verify", "", w.secretKey)

	loginReq := map[string]interface{}{
		"op": "login",
		"args": []map[string]string{
			{
				"apiKey":     w.apiKey,
				"passphrase": w.passphrase,
				"timestamp":  timestamp,
				"sign":       signStr,
			},
		},
	}
	return w.SendMsg(context.Background(), loginReq)
}

func (w *wsClient) Subscribe(args []map[string]string) error {
	w.mu.Lock()
	w.subscriptions = append(w.subscriptions, args...)
	w.mu.Unlock()

	req := map[string]interface{}{
		"op":   "subscribe",
		"args": args,
	}
	return w.SendMsg(context.Background(), req)
}

func (w *wsClient) Unsubscribe(args []map[string]string) error {
	req := map[string]interface{}{
		"op":   "unsubscribe",
		"args": args,
	}
	return w.SendMsg(context.Background(), req)
}

func (w *wsClient) readLoop(ctx context.Context) error {
	for {
		select {
		case <-w.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		w.mu.Lock()
		conn := w.conn
		w.mu.Unlock()

		if conn == nil {
			return fmt.Errorf("connection is nil")
		}

		_, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}

		w.routeMsg(msg)
	}
}

// routeMsg 解析 arg.channel 字段，路由到已注册的处理器
func (w *wsClient) routeMsg(msg []byte) {
	var envelope struct {
		Arg struct {
			Channel string `json:"channel"`
		} `json:"arg"`
	}
	if err := json.Unmarshal(msg, &envelope); err != nil {
		return
	}
	w.mu.Lock()
	h, ok := w.channelHandlers[envelope.Arg.Channel]
	w.mu.Unlock()
	if ok {
		h(msg)
	}
}

func (w *wsClient) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.mu.Lock()
			if w.conn != nil {
				_ = w.conn.WriteMessage(websocket.PingMessage, nil)
			}
			w.mu.Unlock()
		}
	}
}
