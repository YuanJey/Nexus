package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/YuanJey/nexus/pkg/models"
	"github.com/YuanJey/nexus/pkg/modules"
)

type tradingModule struct {
	http  *httpClient
	priWs *wsClient
	bizWs *wsClient
	order *orderComponent
	algo  *algoOrderComponent

	subMu    sync.Mutex
	subOrder bool
	subAlgo  bool
}

func newTradingModule(http *httpClient, priWs, bizWs *wsClient) *tradingModule {
	m := &tradingModule{
		http:  http,
		priWs: priWs,
		bizWs: bizWs,
		order: newOrderComponent(),
		algo:  newAlgoOrderComponent(),
	}
	priWs.OnChannel("orders", m.order.handleMessage)
	bizWs.OnChannel("orders-algo", m.algo.handleMessage)
	return m
}

// ─── 账户设置 ──────────────────────────────────────────────────

func (t *tradingModule) SetLeverage(ctx context.Context, instId string, lever string, mgnMode string) error {
	body := map[string]string{
		"instId":  instId,
		"lever":   lever,
		"mgnMode": mgnMode,
	}
	resp, err := t.http.post(ctx, "/api/v5/account/set-leverage", body)
	if err != nil {
		return fmt.Errorf("set leverage %s: %w", instId, err)
	}
	_ = resp
	return nil
}

func (t *tradingModule) SetPositionMode(ctx context.Context, posMode string) error {
	body := map[string]string{"posMode": posMode}
	resp, err := t.http.post(ctx, "/api/v5/account/set-position-mode", body)
	if err != nil {
		return fmt.Errorf("set position mode: %w", err)
	}
	_ = resp
	return nil
}

// ─── 下单/撤单/改单 ──────────────────────────────────────────────

func (t *tradingModule) PlaceOrders(ctx context.Context, orders []models.PlaceOrderReq) error {
	okxOrders := make([]Order, 0, len(orders))
	for _, r := range orders {
		okxOrders = append(okxOrders, toOKXOrder(r))
	}
	return batchWrite(ctx, t.http, "/api/v5/trade/batch-orders", okxOrders)
}

func (t *tradingModule) CloseOrders(ctx context.Context, orders []models.PlaceOrderReq) error {
	okxOrders := make([]Order, 0, len(orders))
	for _, r := range orders {
		r.ReduceOnly = true
		okxOrders = append(okxOrders, toOKXOrder(r))
	}
	return batchWrite(ctx, t.http, "/api/v5/trade/batch-orders", okxOrders)
}

func (t *tradingModule) ClosePositions(ctx context.Context, positions []models.ClosePositionReq) error {
	for _, p := range positions {
		body := toOKXClosePosition(p)
		if _, err := t.http.post(ctx, "/api/v5/trade/close-position", body); err != nil {
			return fmt.Errorf("close position %s: %w", p.InstId, err)
		}
	}
	return nil
}

func (t *tradingModule) AmendOrders(ctx context.Context, orders []models.AmendOrderReq) error {
	okxOrders := make([]AmendOrder, 0, len(orders))
	for _, r := range orders {
		okxOrders = append(okxOrders, toOKXAmendOrder(r))
	}
	return batchWrite(ctx, t.http, "/api/v5/trade/amend-batch-orders", okxOrders)
}

func (t *tradingModule) CancelOrders(ctx context.Context, orders []models.CancelOrderReq) error {
	okxOrders := make([]Cancel, 0, len(orders))
	for _, r := range orders {
		okxOrders = append(okxOrders, toOKXCancel(r))
	}
	return batchWrite(ctx, t.http, "/api/v5/trade/cancel-batch-orders", okxOrders)
}

func (t *tradingModule) CancelAllOrders(ctx context.Context, instId string) error {
	params := map[string]string{"instType": "SWAP"}
	if instId != "" {
		params["instId"] = instId
	}
	resp, err := t.http.get(ctx, "/api/v5/trade/orders-pending", params)
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

	cancels := make([]Cancel, 0, len(pending))
	for _, p := range pending {
		id := p.OrdId
		cancels = append(cancels, Cancel{InstId: p.InstId, OrdId: &id})
	}
	for _, batch := range chunk(cancels, MaxBatch) {
		resp, err := t.http.post(ctx, "/api/v5/trade/cancel-batch-orders", batch)
		if err != nil {
			return err
		}
		if err := checkBatchResult(resp); err != nil {
			return err
		}
	}
	return nil
}

// ─── 策略委托 ────────────────────────────────────────────────────

func (t *tradingModule) PlaceAlgoOrders(ctx context.Context, orders []models.PlaceAlgoOrderReq) error {
	for _, r := range orders {
		body := toOKXAlgoOrder(r)
		if _, err := t.http.post(ctx, "/api/v5/trade/order-algo", body); err != nil {
			return fmt.Errorf("place algo order %s: %w", r.ClOrdId, err)
		}
	}
	return nil
}

func (t *tradingModule) AmendAlgoOrders(ctx context.Context, orders []models.AmendAlgoOrderReq) error {
	// OKX: /api/v5/trade/amend-algos
	if len(orders) == 0 {
		return nil
	}
	return fmt.Errorf("amend algo orders not yet implemented")
}

func (t *tradingModule) CancelAlgoOrders(ctx context.Context, orders []models.CancelAlgoOrderReq) error {
	okxCancels := make([]CancelAlgo, 0, len(orders))
	for _, r := range orders {
		okxCancels = append(okxCancels, toOKXCancelAlgo(r))
	}
	return batchWrite(ctx, t.http, "/api/v5/trade/cancel-algos", okxCancels)
}

// CancelAlgoByClOrdId 按 algoClOrdId 精准撤销单个策略委托。
func (t *tradingModule) CancelAlgoByClOrdId(ctx context.Context, instId, algoClOrdId string) error {
	for _, ordType := range allAlgoOrdTypes {
		params := map[string]string{"instType": "SWAP", "ordType": ordType, "algoClOrdId": algoClOrdId}
		if instId != "" {
			params["instId"] = instId
		}
		resp, err := t.http.get(ctx, "/api/v5/trade/orders-algo-pending", params)
		if err != nil {
			return err
		}
		var pending []okxAlgoPendingItem
		if err := json.Unmarshal(resp.Data, &pending); err != nil {
			continue
		}
		if len(pending) == 0 {
			continue
		}
		return t.CancelAlgoOrders(ctx, toCancelAlgoOrderReqs(pending))
	}
	return nil
}

// allAlgoOrdTypes 全部策略单类型。
var allAlgoOrdTypes = []string{"conditional", "trigger", "oco", "move_order_stop"}

func (t *tradingModule) CancelAllAlgoOrders(ctx context.Context, instId string) error {
	var allPending []okxAlgoPendingItem
	for _, ordType := range allAlgoOrdTypes {
		params := map[string]string{"instType": "SWAP", "ordType": ordType}
		if instId != "" {
			params["instId"] = instId
		}
		resp, err := t.http.get(ctx, "/api/v5/trade/orders-algo-pending", params)
		if err != nil {
			return err
		}
		var pending []okxAlgoPendingItem
		if err := json.Unmarshal(resp.Data, &pending); err != nil {
			continue
		}
		allPending = append(allPending, pending...)
	}
	if len(allPending) == 0 {
		return nil
	}
	return t.CancelAlgoOrders(ctx, toCancelAlgoOrderReqs(allPending))
}

// ─── Listener ────────────────────────────────────────────────────

func (t *tradingModule) AttachOrder(l modules.OrderListener) func() {
	detach := t.order.Attach(l)

	t.subMu.Lock()
	if !t.subOrder {
		t.subOrder = true
		_ = t.priWs.Subscribe([]map[string]string{{"channel": "orders", "instType": "ANY"}})
	}
	t.subMu.Unlock()

	return detach
}

func (t *tradingModule) AttachAlgoOrder(l modules.AlgoOrderListener) func() {
	detach := t.algo.Attach(l)

	t.subMu.Lock()
	if !t.subAlgo {
		t.subAlgo = true
		_ = t.bizWs.Subscribe([]map[string]string{{"channel": "orders-algo", "instType": "ANY"}})
	}
	t.subMu.Unlock()

	return detach
}

func (t *tradingModule) OnceOrder(clOrdId string, event modules.OrderEvent, l modules.OrderListener) func() {
	return t.order.Once(clOrdId, event, l)
}

func (t *tradingModule) OnceAlgoOrder(algoClOrdId string, event modules.OrderEvent, l modules.AlgoOrderListener) func() {
	return t.algo.Once(algoClOrdId, event, l)
}

// ─── 查询 ────────────────────────────────────────────────────────

func (t *tradingModule) GetOrder(ctx context.Context, instId, clOrdId, ordId string) (*models.OrderUpdate, error) {
	params := map[string]string{"instId": instId}
	if ordId != "" {
		params["ordId"] = ordId
	} else {
		params["clOrdId"] = clOrdId
	}
	resp, err := t.http.get(ctx, "/api/v5/trade/order", params)
	if err != nil {
		return nil, err
	}
	var details []okxOrderDetail
	if err := json.Unmarshal(resp.Data, &details); err != nil {
		return nil, fmt.Errorf("unmarshal order detail: %w", err)
	}
	if len(details) == 0 {
		return nil, fmt.Errorf("order not found")
	}
	return toOrderUpdate(details[0]), nil
}

// ─── 内部工具 ────────────────────────────────────────────────────

func batchWrite[T any](ctx context.Context, http *httpClient, path string, orders []T) error {
	for _, batch := range chunk(orders, MaxBatch) {
		resp, err := http.post(ctx, path, batch)
		if err != nil {
			return err
		}
		if err := checkBatchResult(resp); err != nil {
			return err
		}
	}
	return nil
}

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
