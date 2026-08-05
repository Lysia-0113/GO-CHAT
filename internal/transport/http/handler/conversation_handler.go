package handler

import (
	"context"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Lysia-0113/GO-CHAT/internal/conversation"
	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/transport/http/middleware"
	"github.com/Lysia-0113/GO-CHAT/internal/transport/http/resp"
)

// ConversationHandler 处理会话创建与查询。
type ConversationHandler struct {
	convos *conversation.Service
}

func NewConversationHandler(convos *conversation.Service) *ConversationHandler {
	return &ConversationHandler{convos: convos}
}

// Create 处理 POST /api/v1/conversations（GOCHAT_API.md §5.5）。
// 复用已有单聊时返回 200；新创建返回 201。
func (h *ConversationHandler) Create(c *gin.Context) {
	var req struct {
		Type      string   `json:"type"`
		Name      string   `json:"name"`
		MemberIDs []string `json:"member_ids"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		resp.Err(c, errs.New(errs.InvalidArgument, "请求体格式错误"))
		return
	}
	convType, err := parseType(req.Type)
	if err != nil {
		resp.Err(c, err)
		return
	}
	memberIDs, err := parseMemberIDs(req.MemberIDs)
	if err != nil {
		resp.Err(c, err)
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	conv, created, err := h.convos.Create(ctx, conversation.CreateConversationCommand{
		SenderID:  middleware.CurrentUserID(c),
		Type:      convType,
		Name:      req.Name,
		MemberIDs: memberIDs,
	})
	if err != nil {
		resp.Err(c, err)
		return
	}
	body := conversationDTO(conv)
	if created {
		resp.Created(c, body)
		return
	}
	resp.OK(c, body)
}

// List 处理 GET /api/v1/conversations（GOCHAT_API.md §5.6）。
func (h *ConversationHandler) List(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	page, err := h.convos.List(ctx, middleware.CurrentUserID(c), c.Query("cursor"), atoiDefault(c.Query("limit"), 20))
	if err != nil {
		resp.Err(c, err)
		return
	}
	items := make([]gin.H, 0, len(page.Items))
	for i := range page.Items {
		items = append(items, conversationDTO(&page.Items[i]))
	}
	resp.OK(c, gin.H{
		"items":       items,
		"next_cursor": page.NextCursor,
		"has_more":    page.HasMore,
	})
}

// Get 处理 GET /api/v1/conversations/{conversation_id}（GOCHAT_API.md §5.7）。
func (h *ConversationHandler) Get(c *gin.Context) {
	convID, err := strconv.ParseInt(c.Param("conversation_id"), 10, 64)
	if err != nil {
		resp.Err(c, errs.New(errs.InvalidArgument, "会话 ID 无效"))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	conv, err := h.convos.Get(ctx, middleware.CurrentUserID(c), convID)
	if err != nil {
		resp.Err(c, err)
		return
	}
	resp.OK(c, conversationDTO(conv))
}

// conversationDTO 转换会话响应（GOCHAT_API.md §4.2）。
func conversationDTO(c *conversation.Conversation) gin.H {
	typ := "single"
	if c.Type == conversation.TypeGroup {
		typ = "group"
	}
	var lastMessage interface{}
	if c.LastMessageID > 0 {
		lastMessage = gin.H{
			"message_id":   formatID(c.LastMessageID),
			"seq":          c.LastSeq,
			"content_type": "text",
			"content":      gin.H{"text": c.LastMessagePreview},
			"sent_at":      c.LastMessageAt,
		}
	}
	return gin.H{
		"conversation_id": formatID(c.ConversationID),
		"type":            typ,
		"name":            c.Name,
		"avatar_url":      c.AvatarURL,
		"last_seq":        c.LastSeq,
		"read_seq":        c.ReadSeq,
		"unread_count":    c.UnreadCount,
		"last_message":    lastMessage,
		"updated_at":      c.UpdatedAt,
	}
}

func parseType(s string) (int8, error) {
	switch s {
	case "single":
		return conversation.TypeSingle, nil
	case "group":
		return conversation.TypeGroup, nil
	default:
		return 0, errs.New(errs.InvalidArgument, "会话类型无效")
	}
}

func parseMemberIDs(ids []string) ([]int64, error) {
	out := make([]int64, 0, len(ids))
	for _, id := range ids {
		v, err := strconv.ParseInt(id, 10, 64)
		if err != nil || v <= 0 {
			return nil, errs.New(errs.InvalidArgument, "成员 ID 无效")
		}
		out = append(out, v)
	}
	return out, nil
}
