package utils

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

type contextKey string

const opIDKey contextKey = "OperationID"

// WithOperationID 将指定的 OperationID 注入 Context
func WithOperationID(ctx context.Context, opID string) context.Context {
	return context.WithValue(ctx, opIDKey, opID)
}

// GenOperationID 自动生成一个 16 字符的 OperationID 并注入 Context
func GenOperationID(ctx context.Context) (context.Context, string) {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	opID := "trace-" + hex.EncodeToString(b)
	return WithOperationID(ctx, opID), opID
}

// GetOperationID 从 Context 中提取 OperationID，若不存在则返回空字符串
func GetOperationID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if opID, ok := ctx.Value(opIDKey).(string); ok {
		return opID
	}
	return ""
}
