package handler

import (
	"context"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
	"github.com/Lysia-0113/GO-CHAT/internal/svc"
	"github.com/Lysia-0113/GO-CHAT/internal/transport/http/middleware"
	"github.com/Lysia-0113/GO-CHAT/internal/transport/http/resp"
)

// MessageHandler 处理历史查询、离线补偿、已读与幂等查询。
type MessageHandler struct {
	svcCtx *svc.ServiceContext
}

func NewMessageHandler(svcCtx *svc.ServiceContext) *MessageHandler {
	return &MessageHandler{svcCtx: svcCtx}
}

// List 处理 GET /api/v1/conversations/{conversation_id}/messages
// （GOCHAT_API.md §5.8 历史翻页 / §5.9 离线补偿）。
func (h *MessageHandler) List(c *gin.Context) {
	convID, err := strconv.ParseInt(c.Param("conversation_id"), 10, 64)
	if err != nil {
		resp.Err(c, errs.New(errs.InvalidArgument, "会话 ID 无效"))
		return
	}
	userID := middleware.CurrentUserID(c)

	// 历史查询限流（GOCHAT_RESILIENCE.md §5.2）
	if h.svcCtx.RateLimiter != nil {
		if ok, retryAfter, _ := h.svcCtx.RateLimiter.AllowHistory(c.Request.Context(), userID, convID, 20, 10); !ok {
			resp.Err(c, errs.Retryable(errs.RateLimited, "查询过于频繁", retryAfter))
			return
		}
	}

	// 隔离：历史查询独立并发池，不挤占持久化资源（GOCHAT_RESILIENCE.md §6.1）
	if h.svcCtx.HistoryBulkhead != nil {
		if err := h.svcCtx.HistoryBulkhead.Acquire(c.Request.Context()); err != nil {
			resp.Err(c, err)
			return
		}
		defer h.svcCtx.HistoryBulkhead.Release()
	}

	query := message.HistoryQuery{
		ActorID:        userID,
		ConversationID: convID,
		BeforeSeq:      int64(atoiDefault(c.Query("before_seq"), 0)),
		AfterSeq:       int64(atoiDefault(c.Query("after_seq"), 0)),
		Limit:          atoiDefault(c.Query("limit"), 20),
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 800*time.Millisecond)
	defer cancel()

	page, err := h.svcCtx.MessageService.ListHistory(ctx, query)
	if err != nil {
		resp.Err(c, err)
		return
	}
	items := make([]gin.H, 0, len(page.Items))
	for i := range page.Items {
		items = append(items, messageDTO(&page.Items[i]))
	}
	resp.OK(c, gin.H{
		"items":           items,
		"next_before_seq": page.NextBeforeSeq,
		"next_after_seq":  page.NextAfterSeq,
		"has_more":        page.HasMore,
		// resync_required：客户端本地游标过期，应清空本地副本全量重拉
		"resync_required": page.ResyncRequired,
	})
}

// MarkRead 处理 PUT /api/v1/conversations/{conversation_id}/read-cursor
// （GOCHAT_API.md §5.10）。
func (h *MessageHandler) MarkRead(c *gin.Context) {
	convID, err := strconv.ParseInt(c.Param("conversation_id"), 10, 64)
	if err != nil {
		resp.Err(c, errs.New(errs.InvalidArgument, "会话 ID 无效"))
		return
	}
	var req struct {
		ReadSeq int64 `json:"read_seq"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		resp.Err(c, errs.New(errs.InvalidArgument, "请求体格式错误"))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	if err := h.svcCtx.MessageService.MarkRead(ctx, message.MarkReadCommand{
		UserID:         middleware.CurrentUserID(c),
		ConversationID: convID,
		ReadSeq:        req.ReadSeq,
	}); err != nil {
		resp.Err(c, err)
		return
	}
	resp.OK(c, gin.H{"read_seq": req.ReadSeq})
}

// GetByClientMessageID 处理 GET /api/v1/messages/by-client-id/{client_msg_id}
// （GOCHAT_API.md §5.11：只能查询当前登录用户自己的幂等键）。
func (h *MessageHandler) GetByClientMessageID(c *gin.Context) {
	clientMsgID := c.Param("client_msg_id")
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	m, err := h.svcCtx.MessageService.GetByClientMessageID(ctx, middleware.CurrentUserID(c), clientMsgID)
	if err != nil {
		resp.Err(c, err)
		return
	}
	resp.OK(c, messageDTO(m))
}

// messageDTO 转换消息响应（GOCHAT_API.md §4.3）。
func messageDTO(m *message.Message) gin.H {
	return gin.H{
		"message_id":      formatID(m.MessageID),
		"client_msg_id":   m.ClientMessageID,
		"conversation_id": formatID(m.ConversationID),
		"seq":             m.Seq,
		"sender_id":       formatID(m.SenderID),
		"content_type":    contentTypeName(m.MessageType),
		"content":         m.Content,
		"status":          statusName(m.Status),
		"sent_at":         m.CreatedAt,
	}
}

func contentTypeName(t int8) string {
	switch t {
	case message.TypeText:
		return "text"
	case message.TypeImage:
		return "image"
	case message.TypeFile:
		return "file"
	default:
		return "system"
	}
}

func statusName(s int8) string {
	switch s {
	case message.StatusNormal:
		return "persisted"
	case message.StatusRecalled:
		return "recalled"
	default:
		return "deleted"
	}
}
