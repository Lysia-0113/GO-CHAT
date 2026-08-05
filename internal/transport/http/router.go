// Package http 是 HTTP 传输层路由注册（GOCHAT_API.md §11.2）。
package http

import (
	"github.com/gin-gonic/gin"

	"github.com/Lysia-0113/GO-CHAT/internal/auth"
	"github.com/Lysia-0113/GO-CHAT/internal/transport/http/handler"
	"github.com/Lysia-0113/GO-CHAT/internal/transport/http/middleware"
	ws "github.com/Lysia-0113/GO-CHAT/internal/transport/websocket"
)

// Router 是 HTTP 路由构造器。
type Router struct {
	Tokens              *auth.TokenManager
	AuthHandler         *handler.AuthHandler
	UserHandler         *handler.UserHandler
	ConversationHandler *handler.ConversationHandler
	MessageHandler      *handler.MessageHandler
	HealthHandler       *handler.HealthHandler
	WSHandler           *ws.Handler
	MetricsHandler      gin.HandlerFunc
}

// New 创建 Gin 引擎并注册全部路由。
func New(r Router) *gin.Engine {
	engine := gin.New()
	engine.Use(gin.Logger(), middleware.Recovery(), middleware.RequestID())

	// 运维接口（不在 /api/v1 下，GOCHAT_API.md §2.3）
	engine.GET("/health/live", r.HealthHandler.Live)
	engine.GET("/health/ready", r.HealthHandler.Ready)
	if r.MetricsHandler != nil {
		engine.GET("/metrics", r.MetricsHandler)
	}

	// WebSocket（不经过 JWT 中间件，使用一次性 Ticket 鉴权，GOCHAT_API.md §3.1）
	engine.GET("/ws", r.WSHandler.Upgrade)

	api := engine.Group("/api/v1")

	// 公开接口
	api.POST("/auth/register", r.AuthHandler.Register)
	api.POST("/auth/login", r.AuthHandler.Login)

	// 鉴权接口
	authorized := api.Group("")
	authorized.Use(middleware.Auth(r.Tokens))
	authorized.GET("/users/me", r.UserHandler.Me)
	authorized.GET("/users/search", r.UserHandler.Search)

	authorized.POST("/conversations", r.ConversationHandler.Create)
	authorized.GET("/conversations", r.ConversationHandler.List)
	authorized.GET("/conversations/:conversation_id", r.ConversationHandler.Get)
	authorized.GET("/conversations/:conversation_id/messages", r.MessageHandler.List)
	authorized.PUT("/conversations/:conversation_id/read-cursor", r.MessageHandler.MarkRead)

	authorized.GET("/messages/by-client-id/:client_msg_id", r.MessageHandler.GetByClientMessageID)
	authorized.POST("/ws-tickets", r.WSHandler.CreateTicket)

	return engine
}
