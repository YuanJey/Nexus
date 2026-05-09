package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	logger "github.com/YuanJey/go-log/pkg/log"

	"github.com/YuanJey/nexus/pkg/listener"
	"github.com/YuanJey/nexus/pkg/models"
)

// ──────────────────────────────────────────────────────────────
// Executor 实现 execution.Execution 接口
// ──────────────────────────────────────────────────────────────

type Executor struct {
	http     *httpClient
	ws       *wsClient // Private channel (orders)
	bizWs    *wsClient // Business channel (orders-algo)
	observer *listener.OrderObserver[listener.Identifiable]
}

// NewExecutor 创建 OKX 执行器
func NewExecutor(apiKey, secretKey, passphrase string, simulated bool) *Executor {
	priUrl := "wss://ws.okx.com:8443/ws/v5/private"
	bizUrl := "wss://ws.okx.com:8443/ws/v5/business"
	if simulated {
		priUrl = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
		bizUrl = "wss://wspap.okx.com:8443/ws/v5/business?brokerId=9999"
	}

	e := &Executor{
		http:     newHTTPClient(apiKey, secretKey, passphrase, simulated),
		observer: listener.NewOrderObserver[listener.Identifiable](),
	}

	e.ws = newWSClient(priUrl, e.handleWsMsg)
	e.ws.setAuth(apiKey, secretKey, passphrase)

	e.bizWs = newWSClient(bizUrl, e.handleWsMsg)
	e.bizWs.setAuth(apiKey, secretKey, passphrase)

	return e
}

// ─── 生命周期 ─────────────────────────────────────────────────

func (e *Executor) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		go func() {
			if err := e.ws.Start(ctx); err != nil {
				logger.NewError("", fmt.Sprintf("Private WS error: %v", err))
			}
		}()
		time.Sleep(2 * time.Second)
		e.ws.Subscribe([]map[string]string{{"channel": "orders", "instType": "ANY"}})
	}()

	go func() {
		defer wg.Done()
		go func() {
			if err := e.bizWs.Start(ctx); err != nil {
				logger.NewError("", fmt.Sprintf("Business WS error: %v", err))
			}
		}()
		time.Sleep(2 * time.Second)
		e.bizWs.Subscribe([]map[string]string{{"channel": "orders-algo", "instType": "ANY"}})
	}()

	<-ctx.Done()
	return ctx.Err()
}

func (e *Executor) Stop() error {
	_ = e.ws.Stop()
	_ = e.bizWs.Stop()
	return nil
}

// ─── 普通订单 (Normal Orders) ─────────────────────────────────

func (e *Executor) PlaceOrders(ctx context.Context, orders []models.PlaceOrderReq) error {
	okxOrders := make([]Order, 0, len(orders))
	for _, r := range orders {
		okxOrders = append(okxOrders, toOKXOrder(r))
	}
	return e.batchPlaceOrders(ctx, okxOrders)
}

func (e *Executor) CloseOrders(ctx context.Context, orders []models.PlaceOrderReq) error {
	okxOrders := make([]Order, 0, len(orders))
	for _, r := range orders {
		r.ReduceOnly = true
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

	cancels := make([]Cancel, 0, len(pending))
	for _, p := range pending {
		id := p.OrdId
		cancels = append(cancels, Cancel{InstId: p.InstId, OrdId: &id})
	}

	for _, batch := range chunk(cancels, MaxBatch) {
		if _, err := e.http.post(ctx, "/api/v5/trade/cancel-batch-orders", batch); err != nil {
			return err
		}
	}
	return nil
}

// ─── 策略委托 (Algo Orders) ───────────────────────────────────

func (e *Executor) PlaceAlgoOrders(ctx context.Context, orders []models.PlaceAlgoOrderReq) error {
	// OKX 策略委托目前仅支持单笔接口
	for _, r := range orders {
		body := toOKXAlgoOrder(r)
		if _, err := e.http.post(ctx, "/api/v5/trade/order-algo", body); err != nil {
			return fmt.Errorf("place algo order %s: %w", r.ClOrdId, err)
		}
	}
	return nil
}

func (e *Executor) AmendAlgoOrders(ctx context.Context, orders []models.AmendAlgoOrderReq) error {
	// TODO: 实现 AmendAlgoOrder 的内部结构转换与接口调用
	// OKX 接口: /api/v5/trade/amend-algos
	return nil
}

func (e *Executor) CancelAlgoOrders(ctx context.Context, orders []models.CancelAlgoOrderReq) error {
	okxCancels := make([]CancelAlgo, 0, len(orders))
	for _, r := range orders {
		okxCancels = append(okxCancels, toOKXCancelAlgo(r))
	}
	for _, batch := range chunk(okxCancels, MaxBatch) {
		if _, err := e.http.post(ctx, "/api/v5/trade/cancel-algos", batch); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) CancelAllAlgoOrders(ctx context.Context, instId string) error {
	// 1. 查询所有策略挂单
	params := map[string]string{"instType": "SWAP", "ordType": "conditional"}
	if instId != "" {
		params["instId"] = instId
	}
	resp, err := e.http.get(ctx, "/api/v5/trade/orders-algo-pending", params)
	if err != nil {
		return err
	}

	var pending []okxAlgoPendingItem
	if err := json.Unmarshal(resp.Data, &pending); err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}

	// 2. 批量撤销
	return e.CancelAlgoOrders(ctx, toCancelAlgoOrderReqs(pending))
}

// ─── Observer ─────────────────────────────────────────────────

func (e *Executor) Observer() *listener.OrderObserver[listener.Identifiable] {
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
		return nil, fmt.Errorf("order not found")
	}
	return toOrderUpdate(details[0]), nil
}

func (e *Executor) GetOHLCV(ctx context.Context, instId string, timeframe string, limit int) ([]models.Candle, error) {
	params := map[string]string{
		"instId": instId,
		"bar":    timeframe,
	}
	if limit > 0 {
		params["limit"] = strconv.Itoa(limit)
	}

	resp, err := e.http.get(ctx, "/api/v5/market/candles", params)
	if err != nil {
		return nil, err
	}

	var data [][]string
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshal candles: %w", err)
	}

	result := make([]models.Candle, len(data))
	for i := range data {
		row := data[len(data)-1-i] // 正序

		c := models.Candle{}
		c.Ts, _ = strconv.ParseInt(row[0], 10, 64)
		c.Open, _ = strconv.ParseFloat(row[1], 64)
		c.High, _ = strconv.ParseFloat(row[2], 64)
		c.Low, _ = strconv.ParseFloat(row[3], 64)
		c.Close, _ = strconv.ParseFloat(row[4], 64)
		c.Volume, _ = strconv.ParseFloat(row[5], 64)

		if len(row) > 6 {
			c.VolCcy, _ = strconv.ParseFloat(row[6], 64)
		}
		if len(row) > 7 {
			c.VolCcyQuote, _ = strconv.ParseFloat(row[7], 64)
		}
		if len(row) > 8 {
			conf, _ := strconv.Atoi(row[8])
			c.Confirm = conf
		}

		result[i] = c
	}

	return result, nil
}

// ─── 内部工具 ─────────────────────────────────────────────────

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

// ─── WebSocket 消息处理 ──────────────────────────────────────────

func (e *Executor) handleWsMsg(msg []byte) {
	var resp struct {
		Event string `json:"event"`
		Code  string `json:"code"`
		Msg   string `json:"msg"`
		Arg   struct {
			Channel string `json:"channel"`
		} `json:"arg"`
		Data []json.RawMessage `json:"data"`
	}

	if err := json.Unmarshal(msg, &resp); err != nil {
		return
	}

	if resp.Event != "" {
		if resp.Event == "error" {
			logger.NewError("", fmt.Sprintf("⚠️ [WS Error] Code: %s, Msg: %s", resp.Code, resp.Msg))
		} else {
			logger.NewInfo("", fmt.Sprintf("ℹ️ [WS Event] %s: channel=%s", resp.Event, resp.Arg.Channel))
		}
		return
	}

	if len(resp.Data) == 0 {
		return
	}

	switch resp.Arg.Channel {
	case "orders":
		for _, rawData := range resp.Data {
			var detail okxOrderDetail
			if err := json.Unmarshal(rawData, &detail); err == nil {
				update := toOrderUpdate(detail)
				var identifiable listener.Identifiable = update
				// 根据订单状态映射到通用 OrderEvent
				event := listener.OrderEventAll
				switch detail.State {
				case "live":
					event = listener.OrderEventNew
				case "partially_filled":
					event = listener.OrderEventPartial
				case "filled":
					event = listener.OrderEventFilled
				case "canceled":
					event = listener.OrderEventCanceled
				}
				e.observer.Dispatch(event, &identifiable)
			}
		}
	case "orders-algo":
		type okxAlgoUpdateMsg struct {
			AlgoId      string `json:"algoId"`
			AlgoClOrdId string `json:"algoClOrdId"`
			InstId      string `json:"instId"`
			State       string `json:"state"`
			UTime       int64  `json:"uTime,string"`
		}
		for _, rawData := range resp.Data {
			var detail okxAlgoUpdateMsg
			if err := json.Unmarshal(rawData, &detail); err == nil {
				update := &models.AlgoUpdate{
					AlgoId:      detail.AlgoId,
					AlgoClOrdId: detail.AlgoClOrdId,
					InstId:      detail.InstId,
					State:       detail.State,
					UpdateAt:    detail.UTime,
				}
				var identifiable listener.Identifiable = update
				e.observer.Dispatch(listener.OrderEventAll, &identifiable)
			}
		}
	}
}
