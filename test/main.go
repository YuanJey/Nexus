package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/YuanJey/nexus/pkg/client"
	"github.com/YuanJey/nexus/pkg/models"
)

// 加载 .env 文件到 map
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
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			env[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	return env
}

func main() {
	env := loadEnv(".env")

	cfg := nexus.Config{
		Exchange:   nexus.OKX,
		APIKey:     env["OKX_API_KEY"],
		SecretKey:  env["OKX_SECRET_KEY"],
		Passphrase: env["OKX_PASSPHRASE"],
		Simulated:  env["OKX_SIMULATED"] == "true",
	}

	if cfg.APIKey == "" {
		log.Fatal("❌ 错误: .env 文件中未找到 OKX_API_KEY")
	}

	fmt.Printf("🔧 初始化 SDK: 交易所=%s, 模拟盘=%v\n", cfg.Exchange, cfg.Simulated)
	sdk := nexus.New(cfg)
	ctx := context.Background()

	//placeOrders(ctx, sdk)
	cancelOrders(ctx, sdk)
}

func placeOrders(ctx context.Context, sdk *nexus.Client) error {
	// 1. 下单测试
	req := models.PlaceOrderReq{
		InstId:     "ETH-USDT-SWAP",
		MarginMode: models.MarginCross,
		Side:       models.SideBuy,
		PosSide:    models.PosSideLong,
		OrdType:    models.OrderMarket,
		Sz:         "1",
		ClOrdId:    fmt.Sprintf("test%d", 12345),
		TPSL: &models.TPSL{
			TpTriggerPx:     "3000",
			TpOrdPx:         "-1",
			TpTriggerPxType: "mark",
			SlTriggerPx:     "2000",
			SlOrdPx:         "-1",
			SlTriggerPxType: "mark",
		},
	}

	fmt.Printf("🚀 正在发送测试订单: %s %s...\n", req.InstId, req.Side)
	err := sdk.Execution.PlaceOrders(ctx, []models.PlaceOrderReq{req})
	if err != nil {
		log.Printf("⚠️ 下单失败 (可能由于模拟盘余额不足或其他原因): %v", err)
	} else {
		fmt.Println("✅ 订单发送成功")
	}
	return err
}
func cancelOrders(ctx context.Context, sdk *nexus.Client) error {
	// 2. 撤单测试
	fmt.Println("🚀 正在测试撤单...")
	err := sdk.Execution.CancelAllOrders(ctx, "ETH-USDT-SWAP")
	if err != nil {
		log.Printf("⚠️ 撤单失败 (可能由于无订单或其他原因): %v", err)
	} else {
		fmt.Println("✅ 撤单成功")
	}
	return err
}
