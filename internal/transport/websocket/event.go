// Package websocket 是 WebSocket 传输层：票据升级、读写循环、心跳与慢连接治理
// （GOCHAT_API.md §6）。
package websocket

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

// inbound 是客户端发来的事件信封（GOCHAT_API.md §6.1）。
type inbound struct {
	Event     string          `json:"event"`
	RequestID string          `json:"request_id,omitempty"`
	Data      json.RawMessage `json:"data"`
}

// 客户端事件名（GOCHAT_API.md §6.2）。
const (
	ClientEventSend             = "message.send"
	ClientEventReceivedAck      = "message.received_ack"
	ClientEventConversationRead = "conversation.read"
	ClientEventHeartbeatPong    = "heartbeat.pong"
)

// sendPayload 是 message.send 的 data（GOCHAT_API.md §6.5）。
type sendPayload struct {
	ClientMessageID string          `json:"client_msg_id"`
	ConversationID  string          `json:"conversation_id"`
	ContentType     string          `json:"content_type"`
	Content         json.RawMessage `json:"content"`
	ClientSentAt    *time.Time      `json:"client_sent_at,omitempty"`
}

// ackPayload 是 message.received_ack 的 data。
type ackPayload struct {
	ConversationID string `json:"conversation_id"`
	ReceivedSeq    int64  `json:"received_seq"`
}

// readPayload 是 conversation.read 的 data。
type readPayload struct {
	ConversationID string `json:"conversation_id"`
	ReadSeq        int64  `json:"read_seq"`
}

// pongPayload 是 heartbeat.pong 的 data。
type pongPayload struct {
	Timestamp int64 `json:"timestamp"`
}

// parseSend 解析 message.send 事件（含 content_type 映射）。
func parseSend(raw json.RawMessage) (*sendPayload, error) {
	var p sendPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, errs.New(errs.InvalidArgument, "message.send 数据格式错误")
	}
	return &p, nil
}

// ContentTypeToCode 把 "text" 等字符串映射为消息类型码。
func ContentTypeToCode(ct string) (int8, error) {
	switch ct {
	case "text":
		return 1, nil
	case "image":
		return 2, nil
	case "file":
		return 3, nil
	default:
		return 0, errs.New(errs.InvalidArgument, "不支持的 content_type")
	}
}

// ParseID 解析十进制字符串 ID（JSON 中 ID 均为字符串，GOCHAT_API.md §3.2）。
func ParseID(s string) (int64, error) {
	if s == "" {
		return 0, errs.New(errs.InvalidArgument, "ID 不能为空")
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, errs.New(errs.InvalidArgument, "ID 格式错误: "+s)
	}
	return v, nil
}

// errorData 是 error 事件的 data（GOCHAT_API.md §6.9）。
type errorData struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	Retryable    bool   `json:"retryable,omitempty"`
	RetryAfterMS int64  `json:"retry_after_ms,omitempty"`
}

// formatID 格式化 int64 为十进制字符串。
func formatID(v int64) string { return fmt.Sprintf("%d", v) }
