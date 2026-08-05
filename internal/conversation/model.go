// Package conversation 是会话领域模块：单聊、群聊、成员与游标。
package conversation

import "time"

// 类型与状态常量（GOCHAT_DATABASE.md §2.3）。
const (
	TypeSingle = int8(1)
	TypeGroup  = int8(2)

	StatusNormal    = int8(1)
	StatusDismissed = int8(2)

	RoleMember = int8(1)
	RoleAdmin  = int8(5)
	RoleOwner  = int8(10)

	MemberStatusNormal  = int8(1)
	MemberStatusLeft    = int8(2)
	MemberStatusRemoved = int8(3)
)

// Conversation 是会话领域对象；ReadSeq/ReceivedSeq/UnreadCount 是当前用户视角的游标值。
type Conversation struct {
	ConversationID     int64
	Type               int8
	Name               string
	AvatarURL          string
	OwnerID            int64
	Status             int8
	LastSeq            int64
	LastMessageID      int64
	LastMessageType    int8
	LastMessagePreview string
	LastMessageAt      *time.Time
	// 当前用户游标（来自 conversation_members 行）
	ReadSeq     int64
	ReceivedSeq int64
	UnreadCount int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Member 是会话成员领域对象。
type Member struct {
	ConversationID  int64
	UserID          int64
	Role            int8
	Status          int8
	JoinedSeq       int64
	LastReceivedSeq int64
	LastReadSeq     int64
	ClearBeforeSeq  int64
	MuteUntil       *time.Time
}

// Unread 计算未读数：max(0, last_seq - read_seq)（GOCHAT_DATABASE.md §6.3）。
func (c *Conversation) Unread() int64 {
	n := c.LastSeq - c.ReadSeq
	if n < 0 {
		return 0
	}
	return n
}
