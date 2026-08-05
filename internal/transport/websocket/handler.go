package websocket

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/Lysia-0113/GO-CHAT/internal/connection"
	"github.com/Lysia-0113/GO-CHAT/internal/conversation"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/redis"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
)

// Handler 是 WebSocket 入口：Ticket 升级、读写循环、心跳与事件分发
// （GOCHAT_API.md §6 / GOCHAT_REDIS.md §6）。
type Handler struct {
	appCtx   context.Context
	tickets  *redis.WSTicketStore
	limiter  *redis.RateLimiter
	manager  *connection.Manager
	presence connection.PresenceRegistry
	messages *message.Service
	convos   *conversation.Service

	readLimit         int64
	heartbeatInterval time.Duration
	missedHeartbeat   int
	writeQueueSize    int
	writeQueueTimeout time.Duration

	inboundRatePerSec      float64
	inboundRateBurst       int
	wsConnectPerMinute     int
	wsConnectPerUserMinute int

	reg *metrics.Registry
	log *slog.Logger
}

// HandlerConfig 是 Handler 配置。
type HandlerConfig struct {
	ReadLimit              int64
	HeartbeatInterval      time.Duration
	MissedHeartbeat        int
	WriteQueueSize         int
	WriteQueueTimeout      time.Duration
	InboundRatePerSec      float64
	InboundRateBurst       int
	WSConnectPerMinute     int
	WSConnectPerUserMinute int
}

// NewHandler 创建 WebSocket Handler。
func NewHandler(
	appCtx context.Context,
	tickets *redis.WSTicketStore,
	limiter *redis.RateLimiter,
	manager *connection.Manager,
	presence connection.PresenceRegistry,
	messages *message.Service,
	convos *conversation.Service,
	cfg HandlerConfig,
	reg *metrics.Registry,
	log *slog.Logger,
) *Handler {
	return &Handler{
		appCtx:                 appCtx,
		tickets:                tickets,
		limiter:                limiter,
		manager:                manager,
		presence:               presence,
		messages:               messages,
		convos:                 convos,
		readLimit:              cfg.ReadLimit,
		heartbeatInterval:      cfg.HeartbeatInterval,
		missedHeartbeat:        cfg.MissedHeartbeat,
		writeQueueSize:         cfg.WriteQueueSize,
		writeQueueTimeout:      cfg.WriteQueueTimeout,
		inboundRatePerSec:      cfg.InboundRatePerSec,
		inboundRateBurst:       cfg.InboundRateBurst,
		wsConnectPerMinute:     cfg.WSConnectPerMinute,
		wsConnectPerUserMinute: cfg.WSConnectPerUserMinute,
		reg:                    reg,
		log:                    log,
	}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	// P0 允许任意 Origin；生产应按域名收紧
	CheckOrigin: func(r *http.Request) bool { return true },
}

// CreateTicket 处理 POST /api/v1/ws-tickets（GOCHAT_API.md §5.12）。
func (h *Handler) CreateTicket(c *gin.Context) {
	var req struct {
		DeviceID string `json:"device_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误"})
		return
	}
	userID := currentUserID(c)
	if userID <= 0 {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "未登录"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 500*time.Millisecond)
	defer cancel()

	token, err := h.tickets.Create(ctx, userID, req.DeviceID)
	if err != nil {
		// Redis 故障：返回 503，不回退为长期 JWT URL（GOCHAT_REDIS.md §6.3）
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "票据服务暂不可用"})
		return
	}
	if h.reg != nil {
		h.reg.Counter(metrics.NameWSTicketCreated, "票据创建数", nil).Inc()
	}
	c.JSON(http.StatusOK, gin.H{
		"request_id": getRequestID(c),
		"data": gin.H{
			"ticket":     token,
			"expires_in": 30,
			"ws_url":     "/ws?ticket=" + token,
		},
	})
}

// Upgrade 处理 GET /ws?ticket=xxx（GOCHAT_API.md §6.4）。
func (h *Handler) Upgrade(c *gin.Context) {
	token := c.Query("ticket")
	ctx, cancel := context.WithTimeout(h.appCtx, 500*time.Millisecond)
	defer cancel()

	// 1. 一次性票据消费（GETDEL 原子读取并删除）
	ticket, err := h.tickets.Consume(ctx, token)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "票据服务暂不可用"})
		return
	}
	if ticket == nil {
		// 票据不存在或已被消费：拒绝连接（重放被拒绝）
		if h.reg != nil {
			h.reg.Counter(metrics.NameWSTicketReplayRejected, "票据重放拒绝数", nil).Inc()
		}
		c.JSON(http.StatusUnauthorized, gin.H{"error": "票据无效或已使用"})
		return
	}

	// 2. 建连限流（IP + 用户维度，GOCHAT_RESILIENCE.md §5.2）
	ip := c.ClientIP()
	if ok, _, _ := h.limiter.AllowWSConnectIP(ctx, ip, h.wsConnectPerMinute); !ok {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "连接过于频繁"})
		return
	}
	if ok, _, _ := h.limiter.AllowWSConnectUser(ctx, ticket.UserID, h.wsConnectPerUserMinute); !ok {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "连接过于频繁"})
		return
	}

	// 3. 升级为 WebSocket（升级成功后连接交给 ConnectionManager）
	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		h.log.Warn("ws upgrade failed", "error", err.Error())
		return
	}
	ws.SetReadLimit(h.readLimit)

	connID := "conn_" + uuid.NewString()
	conn := NewConn(connID, ticket.UserID, ticket.DeviceID, ws, h.writeQueueSize, h.writeQueueTimeout)

	// 4. 注册连接（进程内 + Redis Presence）
	if err := h.manager.Register(ctx, conn); err != nil {
		conn.Close("register failed")
		return
	}
	if h.reg != nil {
		h.reg.Gauge(metrics.NameWSConnectionActive, "在线连接数", nil).Set(h.manager.Count())
	}

	// 5. 建立独立 connCtx（不继承 HTTP 请求 ctx，GOCHAT_API.md §11.3.2）
	connCtx, cancelConn := context.WithCancel(h.appCtx)
	defer func() {
		cancelConn()
		h.manager.Unregister(context.Background(), conn)
		conn.Close("closed")
		if h.reg != nil {
			h.reg.Gauge(metrics.NameWSConnectionActive, "在线连接数", nil).Set(h.manager.Count())
		}
	}()

	// 6. 通知连接就绪
	h.sendReady(conn)

	// 7. 启动读/写/心跳协程
	go h.writerLoop(connCtx, conn)
	go h.heartbeatLoop(connCtx, conn)
	h.readerLoop(connCtx, conn)
}

// sendReady 发送 connection.ready（GOCHAT_API.md §6.4）。
func (h *Handler) sendReady(conn *Conn) {
	event, err := connection.NewEvent(connection.EventConnectionReady, map[string]interface{}{
		"connection_id":              conn.ID(),
		"user_id":                    formatID(conn.UserID()),
		"device_id":                  conn.DeviceID(),
		"server_time":                time.Now().UTC().Format(time.RFC3339Nano),
		"heartbeat_interval_seconds": int(h.heartbeatInterval.Seconds()),
	})
	if err != nil {
		return
	}
	_ = conn.Push(context.Background(), event)
}

// currentUserID 读取鉴权中间件注入的用户 ID。
func currentUserID(c *gin.Context) int64 {
	v, ok := c.Get("user_id")
	if !ok {
		return 0
	}
	if id, ok := v.(int64); ok {
		return id
	}
	return 0
}

func getRequestID(c *gin.Context) string {
	if v, ok := c.Get("request_id"); ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

var _ = json.Marshal
