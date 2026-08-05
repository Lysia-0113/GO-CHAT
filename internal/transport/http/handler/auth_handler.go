// Package handler 是 HTTP 处理器：参数解析、调用应用服务、转换响应
// （GOCHAT_API.md §11.2：Gin 只做这四件事，业务层禁止依赖 gin.Context）。
package handler

import (
	"context"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Lysia-0113/GO-CHAT/internal/auth"
	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/redis"
	"github.com/Lysia-0113/GO-CHAT/internal/transport/http/middleware"
	"github.com/Lysia-0113/GO-CHAT/internal/transport/http/resp"
	"github.com/Lysia-0113/GO-CHAT/internal/user"
)

// AuthHandler 处理注册与登录。
type AuthHandler struct {
	users   *user.Service
	tokens  *auth.TokenManager
	limiter *redis.RateLimiter
}

func NewAuthHandler(users *user.Service, tokens *auth.TokenManager, limiter *redis.RateLimiter) *AuthHandler {
	return &AuthHandler{users: users, tokens: tokens, limiter: limiter}
}

// Register 处理 POST /api/v1/auth/register（GOCHAT_API.md §5.1）。
func (h *AuthHandler) Register(c *gin.Context) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Nickname string `json:"nickname"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		resp.Err(c, errs.New(errs.InvalidArgument, "请求体格式错误"))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	result, err := h.users.Register(ctx, user.RegisterCommand{
		Username: req.Username,
		Password: req.Password,
		Nickname: req.Nickname,
	})
	if err != nil {
		resp.Err(c, err)
		return
	}
	resp.Created(c, gin.H{
		"user_id":    formatID(result.User.UserID),
		"username":   result.User.Username,
		"nickname":   result.User.Nickname,
		"created_at": result.User.CreatedAt,
	})
}

// Login 处理 POST /api/v1/auth/login（GOCHAT_API.md §5.2）。
func (h *AuthHandler) Login(c *gin.Context) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
		DeviceID string `json:"device_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		resp.Err(c, errs.New(errs.InvalidArgument, "请求体格式错误"))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	// 登录限流：IP + 规范化用户名（GOCHAT_RESILIENCE.md §5.2）
	if h.limiter != nil {
		ok, retryAfter, _ := h.limiter.AllowLogin(ctx, c.ClientIP(), req.Username, 5)
		if !ok {
			resp.Err(c, errs.Retryable(errs.RateLimited, "登录尝试过于频繁", retryAfter))
			return
		}
	}

	result, err := h.users.Login(ctx, user.LoginCommand{
		Username: req.Username,
		Password: req.Password,
		DeviceID: req.DeviceID,
	})
	if err != nil {
		resp.Err(c, err)
		return
	}
	resp.OK(c, gin.H{
		"access_token": result.AccessToken,
		"token_type":   "Bearer",
		"expires_in":   result.ExpiresIn,
		"user": gin.H{
			"user_id":  formatID(result.User.UserID),
			"username": result.User.Username,
			"nickname": result.User.Nickname,
		},
	})
}

// UserHandler 处理用户查询。
type UserHandler struct {
	users *user.Service
}

func NewUserHandler(users *user.Service) *UserHandler {
	return &UserHandler{users: users}
}

// Me 处理 GET /api/v1/users/me（GOCHAT_API.md §5.3）。
func (h *UserHandler) Me(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	u, err := h.users.Get(ctx, middleware.CurrentUserID(c))
	if err != nil {
		resp.Err(c, err)
		return
	}
	resp.OK(c, u.ToPublic())
}

// Search 处理 GET /api/v1/users/search（GOCHAT_API.md §5.4）。
func (h *UserHandler) Search(c *gin.Context) {
	keyword := c.Query("keyword")
	cursor := c.Query("cursor")
	limit := atoiDefault(c.Query("limit"), 20)

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	page, err := h.users.Search(ctx, keyword, cursor, limit)
	if err != nil {
		resp.Err(c, err)
		return
	}
	items := make([]gin.H, 0, len(page.Items))
	for _, u := range page.Items {
		items = append(items, gin.H{
			"user_id":    formatID(u.UserID),
			"username":   u.Username,
			"nickname":   u.Nickname,
			"avatar_url": u.AvatarURL,
		})
	}
	resp.OK(c, gin.H{
		"items":       items,
		"next_cursor": page.NextCursor,
		"has_more":    page.HasMore,
	})
}

// formatID 将 BIGINT ID 序列化为十进制字符串。
func formatID(id int64) string {
	return strconv.FormatInt(id, 10)
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return def
	}
	return v
}
