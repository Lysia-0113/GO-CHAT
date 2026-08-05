package conversation

import "context"

// ConversationRepository 是会话存储接口（GOCHAT_API.md §12.2 对应能力）。
type ConversationRepository interface {
	// CreateWithMembers 在事务中创建会话与全部成员。
	// 单聊重复创建时返回 created=false 与已存在会话（复用，GOCHAT_API.md §5.5）。
	CreateWithMembers(ctx context.Context, conv *Conversation, memberIDs []int64) (bool, error)
	// Get 查询会话基本信息（不含成员游标）。
	Get(ctx context.Context, conversationID int64) (*Conversation, error)
	// GetByDirectUsers 按排序后的单聊用户对查询会话。
	GetByDirectUsers(ctx context.Context, lowID, highID int64) (*Conversation, error)
	// ListByUser 分页查询用户会话列表（含各会话的成员游标）。
	// cursorID/cursorTS 是不透明游标解析后的值，排序键为 COALESCE(last_message_at, created_at) DESC, id DESC。
	ListByUser(ctx context.Context, userID, cursorID, cursorTS int64, limit int) ([]Conversation, error)
	// GetMember 查询成员关系；不存在返回 nil, nil。
	GetMember(ctx context.Context, conversationID, userID int64) (*Member, error)
	// ListMemberIDs 返回会话所有正常成员 ID。
	ListMemberIDs(ctx context.Context, conversationID int64) ([]int64, error)
}
