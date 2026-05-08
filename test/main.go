package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	logger "github.com/YuanJey/go-log/pkg/log"
	utils2 "github.com/YuanJey/nexus/utils"

	nexus "github.com/YuanJey/nexus/pkg/client"
	"github.com/YuanJey/nexus/pkg/listener"
	"github.com/YuanJey/nexus/pkg/models"
	"github.com/YuanJey/nexus/pkg/utils"
)

func main() {
	env := loadEnv(".env")
	sdk := nexus.New(nexus.Config{
		Exchange:   nexus.OKX,
		APIKey:     env["OKX_API_KEY"],
		SecretKey:  env["OKX_SECRET_KEY"],
		Passphrase: env["OKX_PASSPHRASE"],
		Simulated:  env["OKX_SIMULATED"] == "true",
	})
	ctx, cancel := context.WithCancel(context.Background())
	ctx, rootOpID := utils.GenOperationID(ctx)
	defer cancel()

	// 启动 WebSocket 监听（执行层：订单私有频道）
	go func() {
		logger.NewInfo("", "📡 启动 Execution 私有频道监听...")
		if err := sdk.Execution.Start(ctx); err != nil {
			logger.NewError("", fmt.Sprintf("❌ Execution 监听器异常退出: %v", err))
		}
	}()

	// 启动 Stream 监听（行情、账户、持仓频道）
	go func() {
		logger.NewInfo("", "📡 启动 Stream 行情/账户/持仓监听...")
		if err := sdk.Stream.Start(ctx); err != nil {
			logger.NewError("", fmt.Sprintf("❌ Stream 监听器异常退出: %v", err))
		}
	}()

	// 等待连接建立和订阅生效
	time.Sleep(4 * time.Second)

	instId := "ETH-USDT-SWAP"

	logger.NewInfo(rootOpID, fmt.Sprintf("🔧 初始化测试: 交易所=OKX, 模拟盘=%v\n", env["OKX_SIMULATED"]))

	// 1. 注入账户更新监听（多点注入示例）
	sdk.Stream.AccountObserver().Subscribe(func(acc *models.Account) {
		logger.NewInfo(rootOpID, fmt.Sprintf("💰 [OBSERVER-1] 账户更新: 总权益=%s USD\n", acc.TotalEq))
	})
	sdk.Stream.AccountObserver().Subscribe(func(acc *models.Account) {
		// 模拟另一个系统的审计日志
		logger.NewInfo(rootOpID, fmt.Sprintf("💰 [OBSERVER-2/Audit] 资产对账完成，Ts=%d\n", acc.Ts))
	})

	// 2. 注入持仓更新监听
	sdk.Stream.PositionObserver().Subscribe(func(pos *models.Position) {
		logger.NewInfo(rootOpID, fmt.Sprintf("📊 [OBSERVER] 持仓更新: 产品=%s, 方向=%s, 仓位=%s\n", pos.InstId, pos.PosSide, pos.Pos))
	})

	// 3. 注入行情监听
	sdk.Stream.TickerObserver().Subscribe(func(t *models.Ticker) {
		// 仅作为调试输出
	})

	// 触发订阅逻辑（通过旧的 API 触发，或者直接在 Observer 模式下自动按需触发）
	sdk.Stream.SubscribeAccount(func(acc *models.Account) {})
	sdk.Stream.SubscribePosition(instId, func(pos *models.Position) {})
	sdk.Stream.SubscribeTicker(instId, func(t *models.Ticker) {})

	// 注册全局持久监听
	sdk.Execution.Observer().OnOrder(listener.OrderEventAll, func(update *listener.Identifiable) {
		switch o := (*update).(type) {
		case *models.OrderUpdate:
			logger.NewInfo("", fmt.Sprintf("🔔 [PERSISTENT] 普通单更新: 自定义ID=%s, 平台ID=%s, 状态=%s, 价格=%s, 数量=%s/%s\n",
				o.ClOrdId, o.OrdId, o.State, o.FillPx, o.FillSz, o.Sz))
		case *models.AlgoUpdate:
			logger.NewInfo("", fmt.Sprintf("🔔 [PERSISTENT] 策略单更新: 自定义ID=%s, 平台ID=%s, 状态=%s\n",
				o.AlgoClOrdId, o.AlgoId, o.State))
		}
	})

	// 1. 普通订单测试 (限价挂单 + 一键全撤)
	testNormalOrders(ctx, sdk, instId)

	// 2. 策略委托测试 (独立止损单 + 策略单全撤)
	testAlgoOrders(ctx, sdk, instId)

	// 3. 一键平仓测试
	testClosePositions(ctx, sdk, instId)

	// 保持运行一段时间查看异步回调
	logger.NewInfo(rootOpID, "\n⏳ 等待 5 秒观察后续异步推送...")
	time.Sleep(5 * time.Second)

	logger.NewInfo(rootOpID, "\n✨ 测试流程结束")
}

func testNormalOrders(ctx context.Context, sdk *nexus.Client, instId string) {
	ctx, opID := utils.GenOperationID(ctx)
	logger.NewInfo(opID, "\n--- [1] 普通订单测试 (Normal Orders) ---")

	clOrdId := fmt.Sprintf("NormalTest%d", time.Now().Unix())
	req := models.PlaceOrderReq{
		InstId:     instId,
		MarginMode: models.MarginCross,
		Side:       models.SideBuy,
		PosSide:    models.PosSideLong,
		OrdType:    models.OrderLimit,
		Sz:         "1",
		Px:         "1000", // 设低价不成交
		ClOrdId:    clOrdId,
	}

	logger.NewInfo(opID, fmt.Sprintf("👉 下普通限价单: %s, 价: %s, ID: %s\n", instId, req.Px, clOrdId))

	// 注册一次性监听：监听该订单成交 (filled)
	sdk.Execution.Observer().OnceOrder(clOrdId, listener.OrderEventFilled, func(update *listener.Identifiable) {
		o := (*update).(*models.OrderUpdate)
		logger.NewInfo(opID, fmt.Sprintf("🎯 [ONCE] 订单已成交: ID=%s, 成交均价=%s\n", o.ClOrdId, o.AvgPx))
	})

	// 注册一次性监听：监听该订单撤单 (canceled)
	sdk.Execution.Observer().OnceOrder(clOrdId, listener.OrderEventCanceled, func(update *listener.Identifiable) {
		o := (*update).(*models.OrderUpdate)
		logger.NewInfo(opID, fmt.Sprintf("🛑 [ONCE] 订单已撤单: ID=%s\n", o.ClOrdId))
	})

	if err := sdk.Execution.PlaceOrders(ctx, []models.PlaceOrderReq{req}); err != nil {
		logger.NewError(opID, fmt.Sprintf("❌ 下单失败: %v", err))
	} else {
		logger.NewInfo(opID, "✅ 下单指令已送出")
	}

	time.Sleep(1 * time.Second)
	logger.NewInfo(opID, "👉 正在全撤当前产品的所有普通挂单...")
	if err := sdk.Execution.CancelAllOrders(ctx, instId); err != nil {
		logger.NewError(opID, fmt.Sprintf("❌ 全撤失败: %v", err))
	} else {
		logger.NewInfo(opID, "✅ 普通挂单已清空")
	}
}

func testAlgoOrders(ctx context.Context, sdk *nexus.Client, instId string) {
	ctx, opID := utils.GenOperationID(ctx)
	logger.NewInfo(opID, "\n--- [2] 策略委托测试 (Algo Orders) ---")

	algoClOrdId := fmt.Sprintf("AlgoTest%d", time.Now().Unix())
	req := models.PlaceAlgoOrderReq{
		InstId:          instId,
		MarginMode:      models.MarginCross,
		Side:            models.SideSell,
		PosSide:         models.PosSideLong,
		OrdType:         models.AlgoConditional,
		Sz:              "1",
		ClOrdId:         algoClOrdId,
		SlTriggerPx:     "2000",
		SlOrdPx:         "-1",
		SlTriggerPxType: models.TriggerMark,
	}

	logger.NewInfo(opID, fmt.Sprintf("👉 下独立止损策略单: %s, 触发价: %s, ID: %s\n", instId, req.SlTriggerPx, algoClOrdId))

	sdk.Execution.Observer().OnceOrder(algoClOrdId, listener.OrderEventAll, func(update *listener.Identifiable) {
		a := (*update).(*models.AlgoUpdate)
		logger.NewInfo(opID, fmt.Sprintf("🎯 [ONCE] 策略单状态变更: ID=%s, 当前状态=%s\n", a.AlgoClOrdId, a.State))
	})

	if err := sdk.Execution.PlaceAlgoOrders(ctx, []models.PlaceAlgoOrderReq{req}); err != nil {
		logger.NewError(opID, fmt.Sprintf("❌ 策略单下单失败: %v", err))
	} else {
		logger.NewInfo(opID, "✅ 策略单指令已送出")
	}

	time.Sleep(1 * time.Second)
	logger.NewInfo(opID, "👉 正在全撤当前产品的所有策略委托...")
	if err := sdk.Execution.CancelAllAlgoOrders(ctx, instId); err != nil {
		logger.NewError(opID, fmt.Sprintf("❌ 策略单全撤失败: %v", err))
	} else {
		logger.NewInfo(opID, "✅ 策略委托已清空")
	}
}

func testClosePositions(ctx context.Context, sdk *nexus.Client, instId string) {
	ctx, opID := utils.GenOperationID(ctx)
	logger.NewInfo(opID, "\n--- [3] 一键平仓测试 (Market Close) ---")
	placeOrders(ctx, sdk)
	req := models.ClosePositionReq{
		InstId:     instId,
		MarginMode: models.MarginCross,
		PosSide:    models.PosSideLong,
		AutoCxl:    true,
	}

	logger.NewInfo(opID, fmt.Sprintf("👉 尝试一键全平多仓: %s\n", instId))
	if err := sdk.Execution.ClosePositions(ctx, []models.ClosePositionReq{req}); err != nil {
		logger.NewError(opID, fmt.Sprintf("⚠️ 平仓指令反馈: %v (可能因无持仓报错，属正常)", err))
	} else {
		logger.NewInfo(opID, "✅ 平仓指令已发送")
	}
}
func placeOrders(ctx context.Context, sdk *nexus.Client) error {
	opID := utils.GetOperationID(ctx)
	clOrdId := utils2.GenerateClOrdId("placeOrderTest")
	// 1. 下单测试
	req := models.PlaceOrderReq{
		InstId:     "ETH-USDT-SWAP",
		MarginMode: models.MarginCross,
		Side:       models.SideBuy,
		PosSide:    models.PosSideLong,
		OrdType:    models.OrderMarket,
		Sz:         "1",
		ClOrdId:    clOrdId,
		TPSL: &models.TPSL{
			TpTriggerPx:     "3000",
			TpOrdPx:         "-1",
			TpTriggerPxType: "mark",
			SlTriggerPx:     "2000",
			SlOrdPx:         "-1",
			SlTriggerPxType: "mark",
		},
	}
	// 注册一次性监听：监听该订单成交 (filled)
	sdk.Execution.Observer().OnceOrder(clOrdId, listener.OrderEventFilled, func(update *listener.Identifiable) {
		o := (*update).(*models.OrderUpdate)
		logger.NewInfo(opID, fmt.Sprintf("🎯 [ONCE] 订单已成交: ID=%s, 成交均价=%s\n", o.ClOrdId, o.AvgPx))
	})
	logger.NewInfo(opID, fmt.Sprintf("🚀 正在发送测试订单: %s %s...\n", req.InstId, req.Side))
	err := sdk.Execution.PlaceOrders(ctx, []models.PlaceOrderReq{req})
	if err != nil {
		logger.NewError(opID, fmt.Sprintf("⚠️ 下单失败 (可能由于模拟盘余额不足或其他原因): %v", err))
	} else {
		logger.NewInfo(opID, "✅ 订单发送成功")
	}
	return err
}
func loadEnv(path string) map[string]string {
	env := make(map[string]string)
	file, err := os.Open(path)
	if err != nil {
		return env
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, ";") || !strings.Contains(line, "=") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		env[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	return env
}
