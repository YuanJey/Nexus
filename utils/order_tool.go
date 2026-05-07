package utils

import (
	"fmt"
	"time"
)

// 前缀+字母（区分大小写）与数字的组合，可以是纯字母、纯数字且长度要在1-32位之间
func GenerateClOrdId(Prefix string) string {
	return fmt.Sprintf("%s%d", Prefix, time.Now().UnixMilli())
}
