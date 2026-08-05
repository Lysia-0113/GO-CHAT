// Package middleware 是 HTTP 中间件：链路 ID、鉴权、恢复。
package middleware

import (
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/Lysia-0113/GO-CHAT/internal/auth"
	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/transport/http/resp"
)

// gin.Context 使用字符串 Key；使用未导出常量避免其他包误读
// （GOCHAT_API.md §11.3.1：链路信息使用私有 Key 承载）。
const (
	ctxUserID   = "ctx_user_id"
	ctxDeviceID = "ctx_device_id"
)

// RequestID 为每个请求注入 request_id 并记录耗时。
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		rid := c.GetHeader("X-Request-ID")
		if rid == "" {
			rid = "req_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		}
		c.Set("request_id", rid)
		start := time.Now()
		c.Next()
		c.Set("latency_ms", time.Since(start).Milliseconds())
	}
}

// Auth 校验 Bearer Token 并注入 user_id / device_id。
func Auth(tokens *auth.TokenManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(header, prefix) {
			resp.Err(c, errs.New(errs.AuthRequired, "缺少访问令牌"))
			c.Abort()
			return
		}
		claims, err := tokens.Parse(strings.TrimPrefix(header, prefix))
		if err != nil {
			resp.Err(c, err)
			c.Abort()
			return
		}
		c.Set(ctxUserID, claims.UserID)
		c.Set(ctxDeviceID, claims.DeviceID)
		c.Next()
	}
}

// CurrentUserID 从鉴权中间件注入的 context 读取用户 ID。
func CurrentUserID(c *gin.Context) int64 {
	v, ok := c.Get(ctxUserID)
	if !ok {
		return 0
	}
	if id, ok := v.(int64); ok {
		return id
	}
	return 0
}

// CurrentDeviceID 读取设备 ID。
func CurrentDeviceID(c *gin.Context) string {
	v, _ := c.Get(ctxDeviceID)
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// Recovery 捕获 panic 并返回统一 500（避免进程崩溃）。
func Recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				resp.Err(c, errs.Internal(nil))
				c.Abort()
			}
		}()
		c.Next()
	}
}
