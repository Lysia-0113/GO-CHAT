// Package http 是 HTTP 传输层路由注册（GOCHAT_API.md §11.2）。
package http

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/Lysia-0113/GO-CHAT/internal/svc"
	"github.com/Lysia-0113/GO-CHAT/internal/transport/http/handler"
	"github.com/Lysia-0113/GO-CHAT/internal/transport/http/middleware"
	ws "github.com/Lysia-0113/GO-CHAT/internal/transport/websocket"
)

// New 创建 Gin 引擎并注册全部路由。
// Handler 一律从 svcCtx 服务定位器构造（GOCHAT_API.md §11.3）；
// readyFn 是健康检查的额外就绪探针（如 Kafka），由调用方注入。
func New(svcCtx *svc.ServiceContext, wsHandler *ws.Handler, metricsHandler gin.HandlerFunc, readyFn func(ctx context.Context) error) *gin.Engine {
	engine := gin.New()
	engine.Use(gin.Logger(), middleware.Recovery(), middleware.RequestID())

	authHandler := handler.NewAuthHandler(svcCtx)
	userHandler := handler.NewUserHandler(svcCtx)
	convHandler := handler.NewConversationHandler(svcCtx)
	msgHandler := handler.NewMessageHandler(svcCtx)
	healthHandler := handler.NewHealthHandler(svcCtx, readyFn)

	// 运维接口（不在 /api/v1 下，GOCHAT_API.md §2.3）
	engine.GET("/health/live", healthHandler.Live)
	engine.GET("/health/ready", healthHandler.Ready)
	if metricsHandler != nil {
		engine.GET("/metrics", metricsHandler)
	}

	// WebSocket（不经过 JWT 中间件，使用一次性 Ticket 鉴权，GOCHAT_API.md §3.1）
	engine.GET("/ws", wsHandler.Upgrade)

	api := engine.Group("/api/v1")

	// 公开接口
	api.POST("/auth/register", authHandler.Register)
	api.POST("/auth/login", authHandler.Login)

	// 鉴权接口
	authorized := api.Group("")
	authorized.Use(middleware.Auth(svcCtx.Tokens))
	authorized.GET("/users/me", userHandler.Me)
	authorized.GET("/users/search", userHandler.Search)

	authorized.POST("/conversations", convHandler.Create)
	authorized.GET("/conversations", convHandler.List)
	authorized.GET("/conversations/:conversation_id", convHandler.Get)
	authorized.GET("/conversations/:conversation_id/messages", msgHandler.List)
	authorized.PUT("/conversations/:conversation_id/read-cursor", msgHandler.MarkRead)

	authorized.GET("/messages/by-client-id/:client_msg_id", msgHandler.GetByClientMessageID)
	authorized.POST("/ws-tickets", wsHandler.CreateTicket)

	return engine
}
