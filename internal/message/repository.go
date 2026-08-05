package message

import "context"

// MessageRepository 是消息存储接口（GOCHAT_API.md §12.3）。
type MessageRepository interface {
	// FindByID 按全局 message_id 查询。
	FindByID(ctx context.Context, messageID int64) (*Message, error)
	// FindByClientMessageID 按 sender_id + client_msg_id 查询（幂等查询，§11.3）。
	FindByClientMessageID(ctx context.Context, senderID int64, clientMessageID string) (*Message, error)
	// ListBefore 向前翻页：seq < beforeSeq，按 seq DESC。
	ListBefore(ctx context.Context, conversationID, beforeSeq int64, limit int) ([]Message, error)
	// ListAfter 离线补偿：seq > afterSeq，按 seq ASC。
	ListAfter(ctx context.Context, conversationID, afterSeq int64, limit int) ([]Message, error)
	// Persist 执行消息持久化事务（GOCHAT_DATABASE.md §10）：
	// 锁 conversations 行 → 计算 next_seq → INSERT messages → UPDATE conversations → INSERT message_outbox。
	// 返回已分配 seq 的消息；幂等冲突视为成功并复用原记录。
	Persist(ctx context.Context, input PersistInput) (*Message, error)
	// AdvanceReceivedCursor 向前推进送达游标；失败返回 false（不回退）。
	AdvanceReceivedCursor(ctx context.Context, conversationID, userID, receivedSeq int64) error
	// AdvanceReadCursor 向前推进已读游标；重复提交相同值必须成功（幂等）。
	AdvanceReadCursor(ctx context.Context, conversationID, userID, readSeq int64) error
}

// RecentMessageCache 是最近消息缓存接口（GOCHAT_API.md §12.4）。
type RecentMessageCache interface {
	// ListBefore 向前翻页读取缓存窗口内消息（descending）。
	// complete=false 表示缓存缺失/不连续/超出窗口，调用方应回源 MySQL。
	ListBefore(ctx context.Context, conversationID, visibleAfterSeq, beforeSeq int64, limit int) (items []Message, complete bool, err error)
	// ListAfter 离线补偿读取（ascending）。
	ListAfter(ctx context.Context, conversationID, visibleAfterSeq, afterSeq int64, limit int) (items []Message, complete bool, err error)
	// Append 追加一条已持久化消息到缓存（ZSET + HASH + Lua 原子写）。
	Append(ctx context.Context, m *Message) error
	// Delete 清空某会话缓存。
	Delete(ctx context.Context, conversationID int64) error
}

// MessagePublisher 是消息事件发布接口（GOCHAT_API.md §12.5）。
type MessagePublisher interface {
	PublishIngress(ctx context.Context, event MessageIngressEvent) error
	PublishPersisted(ctx context.Context, event MessagePersistedEvent) error
}

// RateLimiter 是发送/查询限流接口（L2 Redis 令牌桶 + L1 本地兜底）。
type RateLimiter interface {
	// AllowSend 检查用户与会话发送配额；拒绝时返回 RATE_LIMITED。
	AllowSend(ctx context.Context, userID, conversationID int64) error
	// AllowHistory 检查历史查询配额。
	AllowHistory(ctx context.Context, userID, conversationID int64) error
}

// IdemResult 是快速幂等拦截的结果。
type IdemResult int

const (
	// IdemTaken 本请求获得发送权（可以继续写 Kafka）。
	IdemTaken IdemResult = iota
	// IdemAlreadyAccepted 相同 client_msg_id 已接受，直接复用 accepted 结果。
	IdemAlreadyAccepted
	// IdemProcessing 相同 client_msg_id 正在处理中，客户端应短暂退避后重试。
	IdemProcessing
)

// FastIdempotency 是消息入口快速幂等（Redis SET NX）。
// Redis 不可用时由调用方跳过（MySQL 唯一索引仍是最终兜底）。
type FastIdempotency interface {
	// Acquire 尝试获取发送权，返回 nonce 用于条件更新/删除。
	Acquire(ctx context.Context, senderID int64, clientMessageID string) (IdemResult, string, error)
	// MarkAccepted 仅当值仍等于自己的 nonce 时标记为 accepted 并延长 TTL。
	MarkAccepted(ctx context.Context, senderID int64, clientMessageID, nonce string) error
	// Release 仅当值等于自己的 nonce 时删除（Kafka 失败路径）。
	Release(ctx context.Context, senderID int64, clientMessageID, nonce string) error
}
