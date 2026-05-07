package okx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	baseURL  = "https://www.okx.com"
	MaxBatch = 20 // OKX 批量接口单次上限
)

// apiResp OKX 标准响应外层
type apiResp struct {
	Code json.RawMessage `json:"code"` // 使用 RawMessage 兼容 string 和 number (404等异常情况)
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

// GetCode 将 Code 统一转换为 string
func (r *apiResp) GetCode() string {
	var s string
	if err := json.Unmarshal(r.Code, &s); err == nil {
		return s
	}
	var i int
	if err := json.Unmarshal(r.Code, &i); err == nil {
		return fmt.Sprintf("%d", i)
	}
	return string(r.Code)
}

// batchItemResult 批量操作单笔结果
type batchItemResult struct {
	ClOrdId string `json:"clOrdId"`
	OrdId   string `json:"ordId"`
	SCode   string `json:"sCode"` // "0" 表示成功
	SMsg    string `json:"sMsg"`
}

// okxOrderDetail 订单详情
type okxOrderDetail struct {
	InstId  string `json:"instId"`
	OrdId   string `json:"ordId"`
	ClOrdId string `json:"clOrdId"`
	Side    string `json:"side"`
	PosSide string `json:"posSide"`
	OrdType string `json:"ordType"`
	Sz      string `json:"sz"`
	FillSz  string `json:"fillSz"`
	Px      string `json:"px"`
	AvgPx   string `json:"avgPx"`
	State   string `json:"state"`
	Pnl     string `json:"pnl"`
	Fee     string `json:"fee"`
	UTime   string `json:"uTime"`
}

// pendingOrderItem 待成交订单
type pendingOrderItem struct {
	InstId  string `json:"instId"`
	OrdId   string `json:"ordId"`
	ClOrdId string `json:"clOrdId"`
}

// ──────────────────────────────────────────
// httpClient
// ──────────────────────────────────────────

type httpClient struct {
	apiKey     string
	secretKey  string
	passphrase string
	simulated  bool
	cli        *http.Client
}

func newHTTPClient(apiKey, secretKey, passphrase string, simulated bool) *httpClient {
	return &httpClient{
		apiKey:     apiKey,
		secretKey:  secretKey,
		passphrase: passphrase,
		simulated:  simulated,
		cli:        &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *httpClient) post(ctx context.Context, path string, body any) (*apiResp, error) {
	return c.do(ctx, http.MethodPost, path, body)
}

func (c *httpClient) get(ctx context.Context, path string, params map[string]string) (*apiResp, error) {
	if len(params) > 0 {
		var sb strings.Builder
		sb.WriteString(path)
		sb.WriteByte('?')
		first := true
		for k, v := range params {
			if !first {
				sb.WriteByte('&')
			}
			sb.WriteString(k)
			sb.WriteByte('=')
			sb.WriteString(v)
			first = false
		}
		path = sb.String()
	}
	return c.do(ctx, http.MethodGet, path, nil)
}

func (c *httpClient) do(ctx context.Context, method, path string, body any) (*apiResp, error) {
	var bodyBytes []byte
	var bodyStr string
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request: %w", err)
		}
		bodyStr = string(bodyBytes)
	}

	ts := utcTimestamp()
	sig := sign(ts, method, path, bodyStr, c.secretKey)

	fullURL := baseURL + "/" + strings.TrimPrefix(path, "/")
	req, err := http.NewRequestWithContext(ctx, method, fullURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("OK-ACCESS-KEY", c.apiKey)
	req.Header.Set("OK-ACCESS-SIGN", sig)
	req.Header.Set("OK-ACCESS-TIMESTAMP", ts)
	req.Header.Set("OK-ACCESS-PASSPHRASE", c.passphrase)
	if c.simulated {
		req.Header.Set("x-simulated-trading", "1")
	}

	resp, err := c.cli.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	var result apiResp
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w (body=%s)", err, raw)
	}

	code := result.GetCode()
	if code != "0" {
		return nil, fmt.Errorf("okx error: code=%s msg=%s", code, result.Msg)
	}
	return &result, nil
}

// chunk 分批工具
func chunk[T any](s []T, size int) [][]T {
	var out [][]T
	for len(s) > 0 {
		n := size
		if n > len(s) {
			n = len(s)
		}
		out = append(out, s[:n])
		s = s[n:]
	}
	return out
}
