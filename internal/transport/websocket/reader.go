package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/connection"
	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
)

// readerLoop 是单连接读协程：逐条解析事件并分发
// （GOCHAT_API.md §6.2 / §11.3.2：每条入站消息创建短超时 ctx）。
func (h *Handler) readerLoop(connCtx context.Context, conn *Conn) {
	connKey := "im:rate:conn:" + conn.ID()
	for {
		if conn.Closed() {
			return
		}
		// 单连接入站事件限流（GOCHAT_RESILIENCE.md §5.2）
		if h.limiter != nil {
			if ok, _, _ := h.limiter.AllowConnInbound(connCtx, connKey, float64(h.inboundRateBurst), h.inboundRatePerSec); !ok {
				// 持续恶意超额：关闭连接
				h.log.Warn("connection inbound rate limited, closing", "connection_id", conn.ID())
				conn.Close("inbound rate limited")
				return
			}
		}

		_, raw, err := conn.ws.ReadMessage()
		if err != nil {
			// 连接关闭或读超时：结束读循环
			return
		}
		if len(raw) == 0 {
			continue
		}

		// 每条入站消息使用独立短超时 ctx（GOCHAT_API.md §11.3.2）
		msgCtx, cancel := context.WithTimeout(connCtx, 3*time.Second)
		h.dispatch(msgCtx, conn, raw)
		cancel()
	}
}

// dispatch 解析并分发客户端事件。
func (h *Handler) dispatch(ctx context.Context, conn *Conn, raw []byte) {
	var env inbound
	if err := json.Unmarshal(raw, &env); err != nil {
		h.writeError(conn, "", errs.New(errs.InvalidArgument, "协议格式错误"))
		return
	}
	switch env.Event {
	case ClientEventSend:
		h.onSend(ctx, conn, env)
	case ClientEventReceivedAck:
		h.onReceivedAck(ctx, conn, env)
	case ClientEventConversationRead:
		h.onConversationRead(ctx, conn, env)
	case ClientEventHeartbeatPong:
		conn.MarkPong()
		h.refreshPresence(ctx, conn)
	default:
		h.writeError(conn, env.RequestID, errs.New(errs.InvalidArgument, "不支持的事件: "+env.Event))
	}
}

// onSend 处理 message.send（GOCHAT_API.md §6.5）。
func (h *Handler) onSend(ctx context.Context, conn *Conn, env inbound) {
	if h.reg != nil {
		h.reg.Counter(metrics.NameWSIngressReceived, "收到的 message.send 总数", nil).Inc()
	}
	payload, err := parseSend(env.Data)
	if err != nil {
		h.writeError(conn, env.RequestID, err)
		return
	}
	convID, err := ParseID(payload.ConversationID)
	if err != nil {
		h.writeError(conn, env.RequestID, err)
		return
	}
	msgType, err := ContentTypeToCode(payload.ContentType)
	if err != nil {
		h.writeError(conn, env.RequestID, err)
		return
	}

	result, err := h.messages.Send(ctx, message.SendMessageCommand{
		SenderID:        conn.UserID(),
		ClientMessageID: payload.ClientMessageID,
		ConversationID:  convID,
		MessageType:     msgType,
		Content:         payload.Content,
		ClientSentAt:    payload.ClientSentAt,
	})
	if err != nil {
		// 发布失败：计数 + 日志（客户端靠 error 事件 + 幂等重试兜底）
		if h.reg != nil {
			h.reg.Counter(metrics.NamePublishFailed, "Kafka 发布失败数", nil).Inc()
		}
		h.log.Error("publish failed",
			"client_msg_id", payload.ClientMessageID,
			"error", err.Error(),
		)
		h.writeError(conn, env.RequestID, err)
		return
	}
	// Kafka 已可靠接收：返回 message.accepted（仅携带 client_msg_id，无正式 ID）
	event, eerr := connection.NewEvent(connection.EventMessageAccepted, map[string]interface{}{
		"client_msg_id":   result.ClientMessageID,
		"conversation_id": formatID(result.ConversationID),
		"status":          "accepted",
		"accepted_at":     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if eerr == nil {
		event.RequestID = env.RequestID
		if err := conn.Push(ctx, event); err != nil {
			if h.reg != nil {
				h.reg.Counter(metrics.NamePushDropped, "回执推送丢弃数", nil).Inc()
			}
		}
	}
}

// onReceivedAck 处理批量到达确认（GOCHAT_API.md §6.6）。
func (h *Handler) onReceivedAck(ctx context.Context, conn *Conn, env inbound) {
	var payload ackPayload
	if err := json.Unmarshal(env.Data, &payload); err != nil {
		h.writeError(conn, env.RequestID, errs.New(errs.InvalidArgument, "received_ack 数据格式错误"))
		return
	}
	convID, err := ParseID(payload.ConversationID)
	if err != nil {
		h.writeError(conn, env.RequestID, err)
		return
	}
	if err := h.messages.AckReceived(ctx, message.AckReceivedCommand{
		UserID:         conn.UserID(),
		ConversationID: convID,
		ReceivedSeq:    payload.ReceivedSeq,
	}); err != nil {
		h.writeError(conn, env.RequestID, err)
	}
	// 成功无需响应（批量确认，不要求逐条 ACK）
}

// onConversationRead 处理已读上报（与 HTTP read-cursor 共用同一 Service）。
func (h *Handler) onConversationRead(ctx context.Context, conn *Conn, env inbound) {
	var payload readPayload
	if err := json.Unmarshal(env.Data, &payload); err != nil {
		h.writeError(conn, env.RequestID, errs.New(errs.InvalidArgument, "conversation.read 数据格式错误"))
		return
	}
	convID, err := ParseID(payload.ConversationID)
	if err != nil {
		h.writeError(conn, env.RequestID, err)
		return
	}
	if err := h.messages.MarkRead(ctx, message.MarkReadCommand{
		UserID:         conn.UserID(),
		ConversationID: convID,
		ReadSeq:        payload.ReadSeq,
	}); err != nil {
		h.writeError(conn, env.RequestID, err)
		return
	}
	// 异步通知会话其他在线成员（message.read，GOCHAT_API.md §5.10）
	go h.notifyRead(context.Background(), convID, payload.ReadSeq, conn.UserID())
}

// notifyRead 向其他成员推送已读事件（尽力而为）。
func (h *Handler) notifyRead(ctx context.Context, conversationID, readSeq, readerID int64) {
	memberIDs, err := h.convos.ListMemberIDs(ctx, conversationID)
	if err != nil {
		return
	}
	event, err := connection.NewEvent(connection.EventMessageRead, map[string]interface{}{
		"conversation_id": formatID(conversationID),
		"read_seq":        readSeq,
		"reader_id":       formatID(readerID),
	})
	if err != nil {
		return
	}
	for _, memberID := range memberIDs {
		if memberID == readerID {
			continue
		}
		h.manager.PushToUser(ctx, memberID, event)
	}
}

// refreshPresence 心跳续期（GOCHAT_REDIS.md §5.3）。
func (h *Handler) refreshPresence(ctx context.Context, conn *Conn) {
	if h.presence == nil {
		return
	}
	route := connection.ConnectionRoute{
		ConnectionID: conn.ID(),
		UserID:       conn.UserID(),
		DeviceID:     conn.DeviceID(),
		NodeID:       h.manager.NodeID(),
	}
	ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	_ = h.presence.Heartbeat(ctx, route)
}

// writeError 发送 error 事件（GOCHAT_API.md §6.9）。
func (h *Handler) writeError(conn *Conn, requestID string, err error) {
	var e *errs.Error
	if !errors.As(err, &e) {
		e = errs.Internal(err)
	}
	event, eerr := connection.NewEvent(connection.EventError, errorData{
		Code:         string(e.Code),
		Message:      e.Message,
		Retryable:    e.Retryable,
		RetryAfterMS: e.RetryAfterMS,
	})
	if eerr != nil {
		return
	}
	event.RequestID = requestID
	if err := conn.Push(context.Background(), event); err != nil {
		// 错误告知本身也可能被丢弃：客户端靠超时 + 幂等重试兜底
		if h.reg != nil {
			h.reg.Counter(metrics.NamePushDropped, "回执推送丢弃数", nil).Inc()
		}
	}
}
