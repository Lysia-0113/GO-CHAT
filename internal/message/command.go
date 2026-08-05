package message

import (
	"encoding/json"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

// 消息体校验错误。
var (
	ErrTypeNotSupported = errs.New(errs.InvalidArgument, "P0 仅支持文本消息")
	ErrEmptyContent     = errs.New(errs.InvalidArgument, "消息内容不能为空")
	ErrBadContent       = errs.New(errs.InvalidArgument, "消息内容格式错误")
	ErrTooLarge         = errs.New(errs.MessageTooLarge, "消息体过大")
)

// SendMessageCommand 是发送消息命令（WebSocket message.send）。
// SenderID 来自连接身份，不接受客户端传入。
type SendMessageCommand struct {
	SenderID        int64
	ClientMessageID string
	ConversationID  int64
	MessageType     int8
	Content         []byte // 原始 JSON
	ClientSentAt    *time.Time
}

// SendMessageResult 是发送入口结果。
// 采用 Kafka 异步持久化：入口成功只代表 accepted（GOCHAT_API.md §6.5）。
type SendMessageResult struct {
	ClientMessageID string
	ConversationID  int64
	Accepted        bool
}

// HistoryQuery 是历史查询与离线补偿参数（GOCHAT_API.md §3.5/§5.8/§5.9）。
// BeforeSeq 与 AfterSeq 不能同时出现。
type HistoryQuery struct {
	ActorID        int64
	ConversationID int64
	BeforeSeq      int64
	AfterSeq       int64
	Limit          int
}

// MessagePage 是历史消息分页结果。
type MessagePage struct {
	Items         []Message
	NextBeforeSeq int64
	NextAfterSeq  int64
	HasMore       bool
}

// AckReceivedCommand 是批量到达确认（message.received_ack）。
type AckReceivedCommand struct {
	UserID         int64
	ConversationID int64
	ReceivedSeq    int64
}

// MarkReadCommand 是已读上报（HTTP read-cursor 与 WS conversation.read 共用）。
type MarkReadCommand struct {
	UserID         int64
	ConversationID int64
	ReadSeq        int64
}

// PersistInput 是持久化 Worker 的事务输入（GOCHAT_DATABASE.md §10）。
// Message 已分配 message_id，seq 由事务内 last_seq+1 计算。
type PersistInput struct {
	Message *Message
}

// MessageIngressEvent 是 im.message.ingress 载荷（GOCHAT_KAFKA.md §5.2）。
type MessageIngressEvent struct {
	SenderID        int64           `json:"sender_id"`
	ClientMessageID string          `json:"client_msg_id"`
	ConversationID  int64           `json:"conversation_id"`
	MessageType     int8            `json:"message_type"`
	Content         json.RawMessage `json:"content"`
	ClientSentAt    *time.Time      `json:"client_sent_at,omitempty"`
}

// MessagePersistedEvent 是 im.message.persisted 载荷（GOCHAT_KAFKA.md §5.3）。
type MessagePersistedEvent struct {
	MessageID       int64           `json:"message_id"`
	Seq             int64           `json:"seq"`
	SenderID        int64           `json:"sender_id"`
	ClientMessageID string          `json:"client_msg_id"`
	ConversationID  int64           `json:"conversation_id"`
	MessageType     int8            `json:"message_type"`
	Content         json.RawMessage `json:"content"`
	ContentPreview  string          `json:"content_preview,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
}

// ReadEvent 是已读推进通知（message.read）。
type ReadEvent struct {
	ConversationID int64 `json:"conversation_id"`
	ReadSeq        int64 `json:"read_seq"`
	ReaderID       int64 `json:"reader_id"`
}
