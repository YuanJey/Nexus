package okx

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/YuanJey/nexus/pkg/listener"
	"github.com/YuanJey/nexus/pkg/models"
)

// ──────────────────────────────────────────────────────────────
// Executor 实现 execution.Execution 接口
// 当前：HTTP 全量实现；WS 通道预留（通过 ws.SendMsg 扩展）
// ──────────────────────────────────────────────────────────────

type Executor struct {
	http     *httpClient
	ws       *wsClient
	observer *listener.OrderObserver[*models.OrderUpdate]
}

// NewExecutor 创建 OKX 执行器
func NewExecutor(apiKey, secretKey, passphrase string, simulated bool) *Executor {
	return &Executor{
		http:     newHTTPClient(apiKey, secretKey, passphrase, simulated),
		ws:       newWSClient(),
		observer: listener.NewOrderObserver[*models.OrderUpdate](),
	}
}

// ─── 生命周期 ─────────────────────────────────────────────────

func (e *Executor) Start(ctx context.Context) error {
	// TODO: 启动 WS，当前仅阻塞等待 ctx 取消
	return e.ws.Start(ctx)
}

func (e *Executor) Stop() error {
	return e.ws.Stop()
}

// ─── 开仓 ─────────────────────────────────────────────────────

func (e *Executor) PlaceOrders(ctx context.Context, orders []models.PlaceOrderReq) error {
	okxOrders := make([]Order, 0, len(orders))
	for _, r := range orders {
		okxOrders = append(okxOrders, toOKXOrder(r))
	}
	return e.batchPlaceOrders(ctx, okxOrders)
}

// ─── 平仓 ─────────────────────────────────────────────────────

func (e *Executor) CloseOrders(ctx context.Context, orders []models.PlaceOrderReq) error {
	okxOrders := make([]Order, 0, len(orders))
	for _, r := range orders {
		r.ReduceOnly = true // 强制 reduceOnly
		okxOrders = append(okxOrders, toOKXOrder(r))
	}
	return e.batchPlaceOrders(ctx, okxOrders)
}

func (e *Executor) ClosePositions(ctx context.Context, positions []models.ClosePositionReq) error {
	for _, p := range positions {
		body := toOKXClosePosition(p)
		if _, err := e.http.post(ctx, "/api/v5/trade/close-position", body); err != nil {
			return fmt.Errorf("close position %s: %w", p.InstId, err)
		}
	}
	return nil
}

// ─── 改单 ─────────────────────────────────────────────────────

func (e *Executor) AmendOrders(ctx context.Context, orders []models.AmendOrderReq) error {
	okxOrders := make([]AmendOrder, 0, len(orders))
	for _, r := range orders {
		okxOrders = append(okxOrders, toOKXAmendOrder(r))
	}
	for _, batch := range chunk(okxOrders, MaxBatch) {
		resp, err := e.http.post(ctx, "/api/v5/trade/amend-batch-orders", batch)
		if err != nil {
			return err
		}
		if err := checkBatchResult(resp); err != nil {
			return err
		}
	}
	return nil
}

// ─── 撤单 ─────────────────────────────────────────────────────

func (e *Executor) CancelOrders(ctx context.Context, orders []models.CancelOrderReq) error {
	okxOrders := make([]Cancel, 0, len(orders))
	for _, r := range orders {
		okxOrders = append(okxOrders, toOKXCancel(r))
	}
	for _, batch := range chunk(okxOrders, MaxBatch) {
		resp, err := e.http.post(ctx, "/api/v5/trade/cancel-batch-orders", batch)
		if err != nil {
			return err
		}
		if err := checkBatchResult(resp); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) CancelAllOrders(ctx context.Context, instId string) error {
	// 1. 查询当前所有挂单
	params := map[string]string{"instType": "SWAP"}
	if instId != "" {
		params["instId"] = instId
	}
	resp, err := e.http.get(ctx, "/api/v5/trade/orders-pending", params)
	if err != nil {
		return fmt.Errorf("get pending orders: %w", err)
	}

	var pending []pendingOrderItem
	if err := json.Unmarshal(resp.Data, &pending); err != nil {
		return fmt.Errorf("unmarshal pending orders: %w", err)
	}
	if len(pending) == 0 {
		return nil
	}

	// 2. 构建撤单列表
	cancels := make([]Cancel, 0, len(pending))
	for _, p := range pending {
		id := p.OrdId
		cancels = append(cancels, Cancel{InstId: p.InstId, OrdId: &id})
	}

	// 3. 分批撤单
	for _, batch := range chunk(cancels, MaxBatch) {
		if _, err := e.http.post(ctx, "/api/v5/trade/cancel-batch-orders", batch); err != nil {
			return err
		}
	}
	return nil
}

// ─── Observer ─────────────────────────────────────────────────

func (e *Executor) Observer() *listener.OrderObserver[*models.OrderUpdate] {
	return e.observer
}

// ─── 查询 ─────────────────────────────────────────────────────

func (e *Executor) GetOrder(ctx context.Context, instId, clOrdId, ordId string) (*models.OrderUpdate, error) {
	params := map[string]string{"instId": instId}
	if ordId != "" {
		params["ordId"] = ordId
	} else {
		params["clOrdId"] = clOrdId
	}

	resp, err := e.http.get(ctx, "/api/v5/trade/order", params)
	if err != nil {
		return nil, err
	}

	var details []okxOrderDetail
	if err := json.Unmarshal(resp.Data, &details); err != nil {
		return nil, fmt.Errorf("unmarshal order detail: %w", err)
	}
	if len(details) == 0 {
		return nil, fmt.Errorf("order not found: instId=%s clOrdId=%s ordId=%s", instId, clOrdId, ordId)
	}
	return toOrderUpdate(details[0]), nil
}

// ─── 内部工具 ─────────────────────────────────────────────────

// batchPlaceOrders 分批调用 batch-orders 接口
func (e *Executor) batchPlaceOrders(ctx context.Context, orders []Order) error {
	for _, batch := range chunk(orders, MaxBatch) {
		resp, err := e.http.post(ctx, "/api/v5/trade/batch-orders", batch)
		if err != nil {
			return err
		}
		if err := checkBatchResult(resp); err != nil {
			return err
		}
	}
	return nil
}

// checkBatchResult 检查批量操作中是否有单笔失败
func checkBatchResult(resp *apiResp) error {
	var items []batchItemResult
	if err := json.Unmarshal(resp.Data, &items); err != nil {
		return fmt.Errorf("unmarshal batch result: %w", err)
	}
	for _, item := range items {
		if item.SCode != "0" {
			return fmt.Errorf("order %s failed: sCode=%s sMsg=%s", item.ClOrdId, item.SCode, item.SMsg)
		}
	}
	return nil
}
