package listener

import "sync"

// ══════════════════════════════════════════════════════
// 公共基础类型
// ══════════════════════════════════════════════════════

// HandlerID 用于注销回调
type HandlerID uint64

type handlerEntry[T any] struct {
	id      HandlerID
	handler func(*T)
}

// ══════════════════════════════════════════════════════
// StreamObserver —— 持久订阅（行情 / 账户 / 持仓）
// 每次 Dispatch 时触发全部已注册 handler，无事件分类。
// ══════════════════════════════════════════════════════

type StreamObserver[T any] struct {
	mu       sync.RWMutex
	handlers []handlerEntry[T]
	nextID   HandlerID
}

func NewStreamObserver[T any]() *StreamObserver[T] {
	return &StreamObserver[T]{}
}

// Subscribe 注册持久 handler，返回 HandlerID 用于注销
func (s *StreamObserver[T]) Subscribe(handler func(*T)) HandlerID {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	s.handlers = append(s.handlers, handlerEntry[T]{id: s.nextID, handler: handler})
	return s.nextID
}

// Unsubscribe 注销指定 handler
func (s *StreamObserver[T]) Unsubscribe(id HandlerID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, e := range s.handlers {
		if e.id == id {
			s.handlers = append(s.handlers[:i], s.handlers[i+1:]...)
			return
		}
	}
}

// Dispatch 分发数据到所有已注册 handler（异步，不阻塞调用方）
func (s *StreamObserver[T]) Dispatch(data *T) {
	s.mu.RLock()
	snapshot := make([]handlerEntry[T], len(s.handlers))
	copy(snapshot, s.handlers)
	s.mu.RUnlock()

	for _, e := range snapshot {
		go e.handler(data)
	}
}

// ══════════════════════════════════════════════════════
// Identifiable —— 订单类型必须实现此接口
// OrderObserver 通过 clOrdId 路由单笔订单回调
// ══════════════════════════════════════════════════════

type Identifiable interface {
	GetClOrdId() string
}

// OrderEvent 订单状态事件类型
type OrderEvent string

const (
	OrderEventAll      OrderEvent = "all"      // 通配，订阅所有事件
	OrderEventNew      OrderEvent = "new"      // 新订单确认
	OrderEventPartial  OrderEvent = "partial"  // 部分成交
	OrderEventFilled   OrderEvent = "filled"   // 完全成交
	OrderEventCanceled OrderEvent = "canceled" // 已撤单
	OrderEventAmended  OrderEvent = "amended"  // 改单成功
)

// onceKey 单笔订单一次性回调的索引键
type onceKey struct {
	clOrdId string
	event   OrderEvent
}

// ══════════════════════════════════════════════════════
// OrderObserver —— 订单专用观察者
//
//   持久监听（OnOrder）：策略层，监听所有同类事件
//   一次性监听（OnceOrder）：下单/改单/撤单后注册，
//                            特定 clOrdId + event 触发一次后自动注销
//
// ══════════════════════════════════════════════════════

type OrderObserver[T Identifiable] struct {
	mu             sync.RWMutex
	persistent     map[OrderEvent][]handlerEntry[T] // 持久 handler
	once           map[onceKey][]func(*T)           // 一次性 handler
	persistentNext HandlerID
}

func NewOrderObserver[T Identifiable]() *OrderObserver[T] {
	return &OrderObserver[T]{
		persistent: make(map[OrderEvent][]handlerEntry[T]),
		once:       make(map[onceKey][]func(*T)),
	}
}

// OnOrder 注册持久 handler（策略层，监听所有该类型事件）
// 返回 HandlerID 可用于 Unsubscribe
func (o *OrderObserver[T]) OnOrder(event OrderEvent, handler func(*T)) HandlerID {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.persistentNext++
	o.persistent[event] = append(o.persistent[event], handlerEntry[T]{
		id:      o.persistentNext,
		handler: handler,
	})
	return o.persistentNext
}

// Unsubscribe 注销持久 handler
func (o *OrderObserver[T]) Unsubscribe(event OrderEvent, id HandlerID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	entries := o.persistent[event]
	for i, e := range entries {
		if e.id == id {
			o.persistent[event] = append(entries[:i], entries[i+1:]...)
			return
		}
	}
}

// OnceOrder 注册一次性 handler（下单/改单/撤单后调用）
// 触发条件：data.GetClOrdId() == clOrdId && event 匹配 → 执行后自动注销
func (o *OrderObserver[T]) OnceOrder(clOrdId string, event OrderEvent, handler func(*T)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	key := onceKey{clOrdId: clOrdId, event: event}
	o.once[key] = append(o.once[key], handler)
}

// Dispatch 分发订单事件（由 WebSocket 推送层调用）
//  1. 触发特定 event 的持久 handler
//  2. 触发 OrderEventAll 通配持久 handler
//  3. 触发匹配 clOrdId + event 的一次性 handler（执行后注销）
func (o *OrderObserver[T]) Dispatch(event OrderEvent, data *T) {
	clOrdId := (*data).GetClOrdId()

	o.mu.Lock()
	// 收集持久 handler（读写锁内复制，避免长时间持锁）
	var persistentTargets []func(*T)
	for _, e := range o.persistent[event] {
		persistentTargets = append(persistentTargets, e.handler)
	}
	if event != OrderEventAll {
		for _, e := range o.persistent[OrderEventAll] {
			persistentTargets = append(persistentTargets, e.handler)
		}
	}

	// 收集并注销一次性 handler
	key := onceKey{clOrdId: clOrdId, event: event}
	onceHandlers := o.once[key]
	delete(o.once, key)
	o.mu.Unlock()

	// 异步分发持久 handler
	for _, h := range persistentTargets {
		h := h
		go h(data)
	}
	// 异步分发一次性 handler
	for _, h := range onceHandlers {
		h := h
		go h(data)
	}
}
