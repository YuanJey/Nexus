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
	"github.com/YuanJey/nexus/pkg/models"
	"github.com/YuanJey/nexus/pkg/modules"
	"github.com/YuanJey/nexus/pkg/utils"
)

// ─── 测试用 Listener 实现 ──────────────────────────────────────────

type testTickerListener struct {
	rootOpID string
}

func (l *testTickerListener) OnTicker(t *models.Ticker) {
	logger.NewInfo(l.rootOpID, fmt.Sprintf("📊 行情更新: 产品=%s, 收盘价=%s", t.InstId, t.Last))
}

type testAccountListener struct {
	rootOpID string
}

func (l *testAccountListener) OnAccount(acc *models.Account) {
	logger.NewInfo(l.rootOpID, fmt.Sprintf("💰 账户更新: 总权益=%s USD", acc.TotalEq))
}

type testAuditAccountListener struct {
	rootOpID string
}

func (l *testAuditAccountListener) OnAccount(acc *models.Account) {
	logger.NewInfo(l.rootOpID, fmt.Sprintf("💰 [Audit] 资产对账完成，Ts=%d", acc.Ts))
}

type testPositionListener struct {
	rootOpID string
}

func (l *testPositionListener) OnPosition(pos *models.Position) {
	logger.NewInfo(l.rootOpID, fmt.Sprintf("📊 持仓更新: 产品=%s, 方向=%s, 仓位=%s", pos.InstId, pos.PosSide, pos.Pos))
}

type testOrderListener struct{}

func (l *testOrderListener) OnOrder(update *models.OrderUpdate) {
	logger.NewInfo("", fmt.Sprintf("🔔 普通单更新: 自定义ID=%s, 平台ID=%s, 状态=%s, 价格=%s, 数量=%s/%s",
		update.ClOrdId, update.OrdId, update.State, update.FillPx, update.FillSz, update.Sz))
}

type testAlgoOrderListener struct{}

func (l *testAlgoOrderListener) OnAlgoOrder(update *models.AlgoUpdate) {
	logger.NewInfo("", fmt.Sprintf("🔔 策略单更新: 自定义ID=%s, 平台ID=%s, 状态=%s",
		update.AlgoClOrdId, update.AlgoId, update.State))
}

// ─── main ─────────────────────────────────────────────────────────

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
	getOHLCV(ctx, sdk, "ETH-USDT-SWAP")

	// 启动 WebSocket 连接
	go func() {
		logger.NewInfo("", "📡 启动所有 WebSocket 监听...")
		if err := sdk.Start(ctx); err != nil {
			logger.NewError("", fmt.Sprintf("❌ 监听器异常退出: %v", err))
		}
	}()

	// 等待所有 WS 连接就绪（登录+恢复订阅完成）
	<-sdk.Ready()

	instId := "ETH-USDT-SWAP"

	logger.NewInfo(rootOpID, fmt.Sprintf("🔧 初始化测试: 交易所=OKX, 模拟盘=%v\n", env["OKX_SIMULATED"]))

	// 1. 注入账户更新监听
	sdk.Account.AttachAccount(&testAccountListener{rootOpID: rootOpID})
	sdk.Account.AttachAccount(&testAuditAccountListener{rootOpID: rootOpID})

	// 2. 注入持仓更新监听
	sdk.Position.AttachPosition(instId, &testPositionListener{rootOpID: rootOpID})

	// 3. 注入行情监听
	sdk.Market.AttachTicker(instId, &testTickerListener{rootOpID: rootOpID})

	// 4. 注册全局持久订单监听
	sdk.Trading.AttachOrder(&testOrderListener{})
	sdk.Trading.AttachAlgoOrder(&testAlgoOrderListener{})

	// 1. 普通订单测试
	testNormalOrders(ctx, sdk, instId)

	// 2. 策略委托测试
	testAlgoOrders(ctx, sdk, instId)

	// 3. 一键平仓测试
	testClosePositions(ctx, sdk, instId)

	logger.NewInfo(rootOpID, "\n⏳ 等待 5 秒观察后续异步推送...")
	time.Sleep(5 * time.Second)

	logger.NewInfo(rootOpID, "\n✨ 测试流程结束")
}

func testNormalOrders(ctx context.Context, sdk *nexus.Modules, instId string) {
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
		Px:         "1000",
		ClOrdId:    clOrdId,
	}

	logger.NewInfo(opID, fmt.Sprintf("👉 下普通限价单: %s, 价: %s, ID: %s\n", instId, req.Px, clOrdId))

	sdk.Trading.OnceOrder(clOrdId, modules.OrderEventFilled, &testOrderListener{
		// 这里实际上不需要额外字段，OnOrder 已打印
	})
	sdk.Trading.OnceOrder(clOrdId, modules.OrderEventCanceled, &testOrderListener{})

	if err := sdk.Trading.PlaceOrders(ctx, []models.PlaceOrderReq{req}); err != nil {
		logger.NewError(opID, fmt.Sprintf("❌ 下单失败: %v", err))
	} else {
		logger.NewInfo(opID, "✅ 下单指令已送出")
	}

	time.Sleep(1 * time.Second)
	logger.NewInfo(opID, "👉 正在全撤当前产品的所有普通挂单...")
	if err := sdk.Trading.CancelAllOrders(ctx, instId); err != nil {
		logger.NewError(opID, fmt.Sprintf("❌ 全撤失败: %v", err))
	} else {
		logger.NewInfo(opID, "✅ 普通挂单已清空")
	}
}

func testAlgoOrders(ctx context.Context, sdk *nexus.Modules, instId string) {
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

	sdk.Trading.OnceAlgoOrder(algoClOrdId, modules.OrderEventAll, &testAlgoOrderListener{})

	if err := sdk.Trading.PlaceAlgoOrders(ctx, []models.PlaceAlgoOrderReq{req}); err != nil {
		logger.NewError(opID, fmt.Sprintf("❌ 策略单下单失败: %v", err))
	} else {
		logger.NewInfo(opID, "✅ 策略单指令已送出")
	}

	time.Sleep(1 * time.Second)
	logger.NewInfo(opID, "👉 正在全撤当前产品的所有策略委托...")
	if err := sdk.Trading.CancelAllAlgoOrders(ctx, instId); err != nil {
		logger.NewError(opID, fmt.Sprintf("❌ 策略单全撤失败: %v", err))
	} else {
		logger.NewInfo(opID, "✅ 策略委托已清空")
	}
}

func testClosePositions(ctx context.Context, sdk *nexus.Modules, instId string) {
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
	if err := sdk.Trading.ClosePositions(ctx, []models.ClosePositionReq{req}); err != nil {
		logger.NewError(opID, fmt.Sprintf("⚠️ 平仓指令反馈: %v (可能因无持仓报错，属正常)", err))
	} else {
		logger.NewInfo(opID, "✅ 平仓指令已发送")
	}
}

func placeOrders(ctx context.Context, sdk *nexus.Modules) error {
	opID := utils.GetOperationID(ctx)
	clOrdId := utils2.GenerateClOrdId("placeOrderTest")
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
	sdk.Trading.OnceOrder(clOrdId, modules.OrderEventFilled, &testOrderListener{})
	logger.NewInfo(opID, fmt.Sprintf("🚀 正在发送测试订单: %s %s...\n", req.InstId, req.Side))
	err := sdk.Trading.PlaceOrders(ctx, []models.PlaceOrderReq{req})
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

func getOHLCV(ctx context.Context, sdk *nexus.Modules, instId string) {
	data, err := sdk.Market.GetOHLCV(ctx, instId, "1m", 10)
	if err != nil {
		logger.NewError("", fmt.Sprintf("❌ 获取 K 线数据失败: %v", err))
	} else {
		logger.NewInfo("", fmt.Sprintf("✅ 获取 K 线数据成功: %v", data))
	}
}
