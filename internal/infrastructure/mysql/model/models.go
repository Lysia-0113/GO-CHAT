// Package model 是基础设施层的 GORM 模型。
// 领域对象与 GORM Model 分离：Model 不跨层传递（GOCHAT_DATABASE.md §2.4）。
package model

import "time"

// User 对应用户表（000001_create_users.sql）。
type User struct {
	ID           int64     `gorm:"column:id;primaryKey"`
	Username     string    `gorm:"column:username;size:64"`
	PasswordHash string    `gorm:"column:password_hash;size:255"`
	Nickname     string    `gorm:"column:nickname;size:64"`
	AvatarURL    string    `gorm:"column:avatar_url;size:512"`
	Status       int8      `gorm:"column:status"`
	CreatedAt    time.Time `gorm:"column:created_at"`
	UpdatedAt    time.Time `gorm:"column:updated_at"`
}

func (User) TableName() string { return "users" }

// Conversation 对应会话表（000002_create_conversations.sql）。
type Conversation struct {
	ID                 int64      `gorm:"column:id;primaryKey"`
	Type               int8       `gorm:"column:type"`
	DirectUserLowID    *int64     `gorm:"column:direct_user_low_id"`
	DirectUserHighID   *int64     `gorm:"column:direct_user_high_id"`
	Name               string     `gorm:"column:name;size:128"`
	AvatarURL          string     `gorm:"column:avatar_url;size:512"`
	OwnerID            int64      `gorm:"column:owner_id"`
	Status             int8       `gorm:"column:status"`
	LastSeq            int64      `gorm:"column:last_seq"`
	LastMessageID      *int64     `gorm:"column:last_message_id"`
	LastMessageType    *int8      `gorm:"column:last_message_type"`
	LastMessagePreview string     `gorm:"column:last_message_preview;size:255"`
	LastMessageAt      *time.Time `gorm:"column:last_message_at"`
	CreatedAt          time.Time  `gorm:"column:created_at"`
	UpdatedAt          time.Time  `gorm:"column:updated_at"`
}

func (Conversation) TableName() string { return "conversations" }

// ConversationMember 对应会话成员表（000003_create_conversation_members.sql）。
type ConversationMember struct {
	ConversationID  int64      `gorm:"column:conversation_id;primaryKey"`
	UserID          int64      `gorm:"column:user_id;primaryKey"`
	Role            int8       `gorm:"column:role"`
	Status          int8       `gorm:"column:status"`
	JoinedSeq       int64      `gorm:"column:joined_seq"`
	LastReceivedSeq int64      `gorm:"column:last_received_seq"`
	LastReadSeq     int64      `gorm:"column:last_read_seq"`
	ClearBeforeSeq  int64      `gorm:"column:clear_before_seq"`
	MuteUntil       *time.Time `gorm:"column:mute_until"`
	CreatedAt       time.Time  `gorm:"column:created_at"`
	UpdatedAt       time.Time  `gorm:"column:updated_at"`
}

func (ConversationMember) TableName() string { return "conversation_members" }

// Message 对应消息表（000004_create_messages.sql）。
// 主键为 (conversation_id, seq)，id 为号段分配的全局唯一消息 ID。
type Message struct {
	ID             int64      `gorm:"column:id"`
	ConversationID int64      `gorm:"column:conversation_id;primaryKey"`
	Seq            int64      `gorm:"column:seq;primaryKey"`
	SenderID       int64      `gorm:"column:sender_id"`
	ClientMsgID    []byte     `gorm:"column:client_msg_id;type:binary(16)"`
	MessageType    int8       `gorm:"column:message_type"`
	Content        string     `gorm:"column:content;type:json"`
	ContentPreview string     `gorm:"column:content_preview;size:255"`
	Status         int8       `gorm:"column:status"`
	RecalledBy     *int64     `gorm:"column:recalled_by"`
	RecalledAt     *time.Time `gorm:"column:recalled_at"`
	ClientSentAt   *time.Time `gorm:"column:client_sent_at"`
	CreatedAt      time.Time  `gorm:"column:created_at"`
	UpdatedAt      time.Time  `gorm:"column:updated_at"`
}

func (Message) TableName() string { return "messages" }

// IDGenerator 对应号段表（000005_create_id_generator.sql）。
type IDGenerator struct {
	BizTag     string    `gorm:"column:biz_tag;primaryKey;size:32"`
	MaxID      int64     `gorm:"column:max_id"`
	Step       int       `gorm:"column:step"`
	Version    int64     `gorm:"column:version"`
	UpdateTime time.Time `gorm:"column:update_time"`
}

func (IDGenerator) TableName() string { return "id_generator" }

// MessageOutbox 对应 Outbox 表（000006_create_message_outbox.sql）。
type MessageOutbox struct {
	MessageID   int64      `gorm:"column:message_id;primaryKey"`
	EventType   int8       `gorm:"column:event_type;primaryKey"`
	Payload     string     `gorm:"column:payload;type:json"`
	Status      int8       `gorm:"column:status"`
	RetryCount  int        `gorm:"column:retry_count"`
	NextRetryAt time.Time  `gorm:"column:next_retry_at"`
	LockedBy    *string    `gorm:"column:locked_by;size:64"`
	LockedAt    *time.Time `gorm:"column:locked_at"`
	PublishedAt *time.Time `gorm:"column:published_at"`
	LastError   string     `gorm:"column:last_error;size:1024"`
	CreatedAt   time.Time  `gorm:"column:created_at"`
	UpdatedAt   time.Time  `gorm:"column:updated_at"`
}

func (MessageOutbox) TableName() string { return "message_outbox" }

// Outbox 状态常量（GOCHAT_DATABASE.md §2.3）。
const (
	OutboxPending   int8 = 0
	OutboxPublished int8 = 1
	OutboxRetrying  int8 = 2
	OutboxDead      int8 = 3
)

// Outbox 事件类型。
const (
	OutboxEventPersisted int8 = 1
)
