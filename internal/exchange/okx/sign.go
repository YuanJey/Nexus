package okx

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"time"
)

// sign 生成 OKX 签名：base64(HMAC-SHA256(ts+method+path+body, secretKey))
func sign(timestamp, method, requestPath, body, secretKey string) string {
	msg := timestamp + method + requestPath + body
	mac := hmac.New(sha256.New, []byte(secretKey))
	mac.Write([]byte(msg))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// utcTimestamp OKX 要求的时间戳格式：ISO8601 UTC
func utcTimestamp() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
}
