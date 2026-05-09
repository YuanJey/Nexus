package okx

import (
	"strconv"

	"github.com/YuanJey/nexus/pkg/models"
)

// ──────────────────────────────────────────────────────────────
// Converter：pkg/models 标准模型 ↔ OKX 内部结构双向转换（ACL 层）
// ──────────────────────────────────────────────────────────────

// toOKXOrder 将标准下单请求转换为 OKX Order 结构
func toOKXOrder(r models.PlaceOrderReq) Order {
	o := Order{
		InstId:  r.InstId,
		TdMode:  string(r.MarginMode),
		Side:    Side(r.Side),
		PosSide: PosSide(r.PosSide),
		OrdType: OrderType(r.OrdType),
		Sz:      r.Sz,
		Px:      r.Px,
		ClOrdId: r.ClOrdId,
	}
	if r.ReduceOnly {
		t := true
		o.ReduceOnly = &t
	}
	if tp := r.TPSL; tp != nil {
		algo := AlgoOrd{
			TpTriggerPx:     tp.TpTriggerPx,
			TpOrdPx:         tp.TpOrdPx,
			TpTriggerPxType: string(tp.TpTriggerPxType),
			SlTriggerPx:     tp.SlTriggerPx,
			SlOrdPx:         tp.SlOrdPx,
			SlTriggerPxType: string(tp.SlTriggerPxType),
		}
		o.AttachAlgoOrds = []AlgoOrd{algo}
	}
	return o
}

// toOKXClosePosition 将标准一键平仓请求转换为 OKX ClosePosition 结构
func toOKXClosePosition(r models.ClosePositionReq) ClosePosition {
	ps := PosSide(r.PosSide)
	c := ClosePosition{
		InstId:  r.InstId,
		MgnMode: string(r.MarginMode),
		PosSide: &ps,
	}
	if r.Ccy != "" {
		c.Ccy = &r.Ccy
	}
	if r.AutoCxl {
		t := true
		c.AutoCxl = &t
	}
	if r.ClOrdId != "" {
		c.ClOrdId = &r.ClOrdId
	}
	return c
}

// toOKXAmendOrder 将标准改单请求转换为 OKX AmendOrder 结构
func toOKXAmendOrder(r models.AmendOrderReq) AmendOrder {
	a := AmendOrder{InstId: r.InstId}
	if r.OrdId != "" {
		a.OrdId = &r.OrdId
	}
	if r.ClOrdId != "" {
		a.ClOrdId = &r.ClOrdId
	}
	if r.ReqId != "" {
		a.ReqId = &r.ReqId
	}
	if r.NewSz != "" {
		a.NewSz = &r.NewSz
	}
	if r.NewPx != "" {
		a.NewPx = &r.NewPx
	}
	if r.CxlOnFail {
		t := true
		a.CxlOnFail = &t
	}
	if r.AlgoId != "" {
		algo := AmendAlgoOrd{AttachAlgoId: r.AlgoId}
		if r.NewTpTriggerPx != "" {
			algo.NewTpTriggerPx = &r.NewTpTriggerPx
		}
		if r.NewTpOrdPx != "" {
			algo.NewTpOrdPx = &r.NewTpOrdPx
		}
		if r.NewTpTriggerPxType != "" {
			s := string(r.NewTpTriggerPxType)
			algo.NewTpTriggerPxType = &s
		}
		if r.NewSlTriggerPx != "" {
			algo.NewSlTriggerPx = &r.NewSlTriggerPx
		}
		if r.NewSlOrdPx != "" {
			algo.NewSlOrdPx = &r.NewSlOrdPx
		}
		if r.NewSlTriggerPxType != "" {
			s := string(r.NewSlTriggerPxType)
			algo.NewSlTriggerPxType = &s
		}
		a.AttachAlgo = []AmendAlgoOrd{algo}
	}
	return a
}

// toOKXCancel 将标准撤单请求转换为 OKX Cancel 结构
func toOKXCancel(r models.CancelOrderReq) Cancel {
	c := Cancel{InstId: r.InstId}
	if r.OrdId != "" {
		c.OrdId = &r.OrdId
	}
	if r.ClOrdId != "" {
		c.ClOrdId = &r.ClOrdId
	}
	return c
}

// toOKXAlgoOrder 将标准策略单请求转换为 OKX AlgoOrder 结构
func toOKXAlgoOrder(r models.PlaceAlgoOrderReq) AlgoOrder {
	return AlgoOrder{
		InstId:          r.InstId,
		TdMode:          string(r.MarginMode),
		Side:            Side(r.Side),
		PosSide:         PosSide(r.PosSide),
		OrdType:         string(r.OrdType),
		Sz:              r.Sz,
		ReduceOnly:      r.ReduceOnly,
		AlgoClOrdId:     r.ClOrdId,
		TpTriggerPx:     r.TpTriggerPx,
		TpOrdPx:         r.TpOrdPx,
		TpTriggerPxType: string(r.TpTriggerPxType),
		SlTriggerPx:     r.SlTriggerPx,
		SlOrdPx:         r.SlOrdPx,
		SlTriggerPxType: string(r.SlTriggerPxType),
		TriggerPx:       r.TriggerPx,
		TriggerPxType:   string(r.TriggerPxType),
		OrderPx:         r.OrderPx,
	}
}

// toOKXCancelAlgo 将标准策略撤单请求转换为 OKX CancelAlgo 结构
func toOKXCancelAlgo(r models.CancelAlgoOrderReq) CancelAlgo {
	return CancelAlgo{
		InstId: r.InstId,
		AlgoId: r.AlgoId,
	}
}

// toCancelAlgoOrderReqs 辅助函数：内部挂单列表 -> 标准撤单请求列表
func toCancelAlgoOrderReqs(items []okxAlgoPendingItem) []models.CancelAlgoOrderReq {
	reqs := make([]models.CancelAlgoOrderReq, 0, len(items))
	for _, item := range items {
		reqs = append(reqs, models.CancelAlgoOrderReq{
			InstId: item.InstId,
			AlgoId: item.AlgoId,
		})
	}
	return reqs
}

// toOrderUpdate 将 OKX 订单详情转换为标准 OrderUpdate
func toOrderUpdate(d okxOrderDetail) *models.OrderUpdate {
	return &models.OrderUpdate{
		ClOrdId:  d.ClOrdId,
		OrdId:    d.OrdId,
		InstId:   d.InstId,
		Side:     models.OrderSide(d.Side),
		PosSide:  models.PosSide(d.PosSide),
		OrdType:  models.OrderType(d.OrdType),
		State:    d.State,
		Sz:       d.Sz,
		FillSz:   d.FillSz,
		Px:       d.Px,
		AvgPx:    d.AvgPx,
		Pnl:      d.Pnl,
		Fee:      d.Fee,
		UpdateAt: parseUnixMs(d.UTime),
	}
}

func parseUnixMs(s string) int64 {
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}
