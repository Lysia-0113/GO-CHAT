// Package connection 负责在线连接管理：进程内 Map 保存连接，
// 通过 PresenceRegistry 同步到 Redis 供跨节点路由（GOCHAT_API.md §13）。
package connection

import "encoding/json"

// Event 是推送到客户端的 WebSocket 事件信封（GOCHAT_API.md §6.1）。
type Event struct {
	Event     string          `json:"event"`
	RequestID string          `json:"request_id,omitempty"`
	Data      json.RawMessage `json:"data"`
}

// NewEvent 构造事件；data 为 nil 时输出 null。
func NewEvent(event string, data interface{}) (Event, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return Event{}, err
	}
	return Event{Event: event, Data: raw}, nil
}

// 事件名常量（GOCHAT_API.md §6.3）。
const (
	EventConnectionReady  = "connection.ready"
	EventMessageAccepted  = "message.accepted"
	EventMessagePersisted = "message.persisted"
	EventMessageNew       = "message.new"
	EventMessageDelivered = "message.delivered"
	EventMessageRead      = "message.read"
	EventMessageFailed    = "message.failed"
	EventHeartbeatPing    = "heartbeat.ping"
	EventHeartbeatPong    = "heartbeat.pong"
	EventError            = "error"
	EventMessageSend      = "message.send"
	EventReceivedAck      = "message.received_ack"
	EventConversationRead = "conversation.read"
)
