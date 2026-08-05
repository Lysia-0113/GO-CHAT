// Package message 是消息领域模块：发送入口、历史查询、送达/已读游标与持久化事件。
package message

import (
	"encoding/json"
	"time"
)

// 消息类型与状态（GOCHAT_DATABASE.md §2.3）。
const (
	TypeText   = int8(1)
	TypeImage  = int8(2)
	TypeFile   = int8(3)
	TypeSystem = int8(99)

	StatusNormal   = int8(1)
	StatusRecalled = int8(2)
	StatusDeleted  = int8(3)
)

// MaxTextLen 文本消息最大长度（P0 只支持 text）。
const MaxTextLen = 4000

// Message 是消息领域对象；ID 均为 BIGINT，API 层序列化为十进制字符串。
type Message struct {
	MessageID       int64
	ClientMessageID string
	ConversationID  int64
	Seq             int64
	SenderID        int64
	MessageType     int8
	Content         json.RawMessage // 如 {"text":"你好"}
	ContentPreview  string
	Status          int8
	RecalledBy      *int64
	RecalledAt      *time.Time
	ClientSentAt    *time.Time
	CreatedAt       time.Time
}

// ContentText 读取文本消息内容；非 text 类型返回空串。
func (m *Message) ContentText() string {
	if m.MessageType != TypeText {
		return ""
	}
	var body struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(m.Content, &body); err != nil {
		return ""
	}
	return body.Text
}

// ValidateContent 校验消息体：P0 仅允许非空文本，长度受限（GOCHAT_API.md §6.5）。
func ValidateContent(messageType int8, content json.RawMessage) error {
	if messageType != TypeText {
		return ErrTypeNotSupported
	}
	if len(content) == 0 || string(content) == "null" {
		return ErrEmptyContent
	}
	var body struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(content, &body); err != nil {
		return ErrBadContent
	}
	text := body.Text
	if text == "" {
		return ErrEmptyContent
	}
	if len([]rune(text)) > MaxTextLen {
		return ErrTooLarge
	}
	return nil
}

// Preview 生成会话列表预览：截断 + 去换行。
func Preview(content json.RawMessage, messageType int8) string {
	if messageType == TypeText {
		var body struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(content, &body); err == nil {
			return TruncatePreview(body.Text)
		}
	}
	return ""
}

// TruncatePreview 截断预览文本（GOCHAT_DATABASE.md §7.3：content_preview VARCHAR(255)）。
func TruncatePreview(text string) string {
	const max = 100 // 预留多字节安全余量
	r := []rune(text)
	if len(r) <= max {
		return text
	}
	return string(r[:max]) + "..."
}
