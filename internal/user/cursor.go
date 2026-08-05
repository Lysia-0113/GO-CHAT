package user

import (
	"encoding/base64"
	"strconv"
	"strings"
)

// 不透明游标：base64(十进制 user_id)。
// 对客户端不透明，避免暴露分页实现；服务端可逆解析用于 SQL 定位。

func encodeOpaqueCursor(id int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(id, 10)))
}

func parseOpaqueCursor(cursor string) (int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(cursor))
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(raw), 10, 64)
}
