package message

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/Lysia-0113/GO-CHAT/internal/conversation"
	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

// MemberChecker 是成员权限校验接口（由 conversation.Service 实现）。
type MemberChecker interface {
	CheckMember(ctx context.Context, conversationID, userID int64) (*conversation.Member, error)
}

// ConversationState 提供会话状态查询（校验会话是否解散，由 conversation.Service 实现）。
type ConversationState interface {
	Get(ctx context.Context, actorID, conversationID int64) (*conversation.Conversation, error)
}

// Dependencies 是 MessageService 的精确依赖（GOCHAT_API.md §11.3）。
type Dependencies struct {
	Messages      MessageRepository
	Members       MemberChecker
	Conversations ConversationState
	// Cursors 是服务端同步游标记录（两层游标设计：客户端本地游标是真相）。
	Cursors     SyncCursorStore
	Publisher   MessagePublisher
	RateLimiter RateLimiter
	FastIdem    FastIdempotency
	// IdemFallback 为 true 时，快速幂等不可用（Redis 故障）跳过拦截，
	// 依赖 MySQL 唯一索引兜底（GOCHAT_REDIS.md §7.3）。
	IdemFallback bool
}

// Service 是消息应用服务：网关发送入口、历史查询、送达与已读。
type Service struct {
	deps Dependencies
}

func NewService(deps Dependencies) *Service {
	return &Service{deps: deps}
}

// Send 处理 WebSocket message.send（GOCHAT_API.md §6.5）。
//
// 链路：限流 → 成员校验 → 快速幂等 → Kafka ingress（acks=all）→ accepted。
// 失败时返回可重试错误，客户端必须复用原 client_msg_id。
func (s *Service) Send(ctx context.Context, cmd SendMessageCommand) (*SendMessageResult, error) {
	if cmd.ClientMessageID == "" {
		return nil, errs.New(errs.InvalidArgument, "client_msg_id 不能为空")
	}
	// 格式前置校验：非 UUID 直接拒绝，否则会 accepted 后无法落库
	// （持久化层按 BINARY(16) 存储，格式错误会卡死分区重试）
	if _, err := uuid.Parse(cmd.ClientMessageID); err != nil {
		return nil, errs.New(errs.InvalidArgument, "client_msg_id 必须是 UUID")
	}
	if err := ValidateContent(cmd.MessageType, cmd.Content); err != nil {
		return nil, err
	}

	// 限流：用户 + 会话（大群配额是 conversation_id 维度，GOCHAT_RESILIENCE.md §5.2）
	if err := s.deps.RateLimiter.AllowSend(ctx, cmd.SenderID, cmd.ConversationID); err != nil {
		return nil, err
	}

	// 成员与会话状态校验（网关不做持久化，只做确定性的轻量校验）
	if _, err := s.deps.Members.CheckMember(ctx, cmd.ConversationID, cmd.SenderID); err != nil {
		return nil, err
	}
	conv, err := s.deps.Conversations.Get(ctx, cmd.SenderID, cmd.ConversationID)
	if err != nil {
		return nil, err
	}
	if conv == nil || conv.Status != conversation.StatusNormal {
		return nil, errs.New(errs.ConversationNotFound, "会话不存在或已解散")
	}

	// 快速幂等拦截（Redis SET NX；Redis 故障时可跳过）
	nonce := ""
	if s.deps.FastIdem != nil {
		result, n, err := s.deps.FastIdem.Acquire(ctx, cmd.SenderID, cmd.ClientMessageID)
		if err != nil && !s.deps.IdemFallback {
			return nil, errs.Wrap(errs.RedisUnavailable, "幂等检查暂时不可用", err)
		}
		if err == nil {
			switch result {
			case IdemAlreadyAccepted:
				return &SendMessageResult{ClientMessageID: cmd.ClientMessageID, ConversationID: cmd.ConversationID, Accepted: true}, nil
			case IdemProcessing:
				return nil, errs.Retryable(errs.SystemBusy, "消息正在处理中，请稍后重试", 500)
			}
			nonce = n
		}
	}

	event := MessageIngressEvent{
		SenderID:        cmd.SenderID,
		ClientMessageID: cmd.ClientMessageID,
		ConversationID:  cmd.ConversationID,
		MessageType:     cmd.MessageType,
		Content:         json.RawMessage(cmd.Content),
		ClientSentAt:    cmd.ClientSentAt,
	}
	if err := s.deps.Publisher.PublishIngress(ctx, event); err != nil {
		if nonce != "" && s.deps.FastIdem != nil {
			_ = s.deps.FastIdem.Release(ctx, cmd.SenderID, cmd.ClientMessageID, nonce)
		}
		return nil, errs.Retryable(errs.KafkaUnavailable, "消息入口暂时不可用，请使用相同 client_msg_id 重试", 1000)
	}
	if nonce != "" && s.deps.FastIdem != nil {
		_ = s.deps.FastIdem.MarkAccepted(ctx, cmd.SenderID, cmd.ClientMessageID, nonce)
	}
	return &SendMessageResult{
		ClientMessageID: cmd.ClientMessageID,
		ConversationID:  cmd.ConversationID,
		Accepted:        true,
	}, nil
}

// GetByClientMessageID 按幂等键查询持久化结果；只能查询当前登录用户的幂等键
// （GOCHAT_API.md §5.11）。
func (s *Service) GetByClientMessageID(ctx context.Context, senderID int64, clientMessageID string) (*Message, error) {
	if clientMessageID == "" {
		return nil, errs.New(errs.InvalidArgument, "client_msg_id 不能为空")
	}
	m, err := s.deps.Messages.FindByClientMessageID(ctx, senderID, clientMessageID)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errs.New(errs.MessageNotFound, "消息尚未持久化")
	}
	return m, nil
}

// ListHistory 处理历史查询（before_seq）与离线补偿（after_seq）。
// 读路径直连 MySQL（会话内 (conversation_id, seq) 复合主键切片），不经过缓存。
func (s *Service) ListHistory(ctx context.Context, query HistoryQuery) (*MessagePage, error) {
	if query.Limit <= 0 {
		query.Limit = 20
	}
	if query.Limit > 100 {
		query.Limit = 100
	}
	if query.BeforeSeq > 0 && query.AfterSeq > 0 {
		return nil, errs.New(errs.InvalidArgument, "before_seq 与 after_seq 不能同时出现")
	}

	member, err := s.deps.Members.CheckMember(ctx, query.ConversationID, query.ActorID)
	if err != nil {
		return nil, err
	}
	// 可见性下界：max(joined_seq, clear_before_seq)（GOCHAT_REDIS.md §4.4）
	visibleAfter := member.JoinedSeq
	if member.ClearBeforeSeq > visibleAfter {
		visibleAfter = member.ClearBeforeSeq
	}

	if query.AfterSeq > 0 {
		return s.listAfter(ctx, query, visibleAfter)
	}
	return s.listBefore(ctx, query, visibleAfter)
}

func (s *Service) listBefore(ctx context.Context, query HistoryQuery, visibleAfter int64) (*MessagePage, error) {
	items, err := s.deps.Messages.ListBefore(ctx, query.ConversationID, query.BeforeSeq, query.Limit)
	if err != nil {
		return nil, err
	}
	// 过滤可见性边界
	filtered := make([]Message, 0, len(items))
	for _, m := range items {
		if m.Seq > visibleAfter {
			filtered = append(filtered, m)
		}
	}
	return &MessagePage{Items: filtered, NextBeforeSeq: nextBefore(filtered), HasMore: len(filtered) == query.Limit}, nil
}

func (s *Service) listAfter(ctx context.Context, query HistoryQuery, visibleAfter int64) (*MessagePage, error) {
	// 游标过期：客户端本地游标低于可见性下界（刚入群/清空聊天记录），
	// 置 resync_required 提示客户端清空本地副本全量重拉。
	// 响应内容仍从可见区开始返回，老客户端忽略该标志时行为与旧版一致。
	resync := query.AfterSeq < visibleAfter
	after := query.AfterSeq
	if visibleAfter > after {
		after = visibleAfter
	}
	items, err := s.deps.Messages.ListAfter(ctx, query.ConversationID, after, query.Limit)
	if err != nil {
		return nil, err
	}
	page := &MessagePage{Items: items, NextAfterSeq: nextAfter(items), HasMore: len(items) == query.Limit, ResyncRequired: resync}
	s.recordCursor(ctx, query, page)
	return page, nil
}

// recordCursor 记录服务端同步游标（两层游标设计：仅记录，查询基准是客户端游标）。
// 推进到本次响应覆盖的最大 seq；无新消息时保持客户端游标（说明已到最新）。
// 写失败静默降级：游标是可重建的服务端记录，不影响本次查询结果。
func (s *Service) recordCursor(ctx context.Context, query HistoryQuery, page *MessagePage) {
	if s.deps.Cursors == nil || page.ResyncRequired {
		// resync 场景不推进：客户端将全量重拉，由重拉后的增量请求重建游标
		return
	}
	synced := page.NextAfterSeq
	if synced == 0 {
		synced = query.AfterSeq
	}
	if synced <= 0 {
		return
	}
	_, _ = s.deps.Cursors.Advance(ctx, query.ConversationID, query.ActorID, synced)
}

func nextBefore(items []Message) int64 {
	if len(items) == 0 {
		return 0
	}
	// 列表为 seq DESC，下一页 before_seq 是当前页最小 seq
	return items[len(items)-1].Seq
}

func nextAfter(items []Message) int64 {
	if len(items) == 0 {
		return 0
	}
	// 列表为 seq ASC，下一页 after_seq 是当前页最大 seq
	return items[len(items)-1].Seq
}

// AckReceived 推进送达游标（message.received_ack，批量确认）。
func (s *Service) AckReceived(ctx context.Context, cmd AckReceivedCommand) error {
	if cmd.ReceivedSeq <= 0 {
		return errs.New(errs.InvalidArgument, "received_seq 无效")
	}
	if _, err := s.deps.Members.CheckMember(ctx, cmd.ConversationID, cmd.UserID); err != nil {
		return err
	}
	return s.deps.Messages.AdvanceReceivedCursor(ctx, cmd.ConversationID, cmd.UserID, cmd.ReceivedSeq)
}

// MarkRead 推进已读位置；HTTP read-cursor 与 WS conversation.read 共用同一规则
// （GOCHAT_API.md §5.10 / §6.7）。
func (s *Service) MarkRead(ctx context.Context, cmd MarkReadCommand) error {
	if cmd.ReadSeq <= 0 {
		return errs.New(errs.InvalidArgument, "read_seq 无效")
	}
	if _, err := s.deps.Members.CheckMember(ctx, cmd.ConversationID, cmd.UserID); err != nil {
		return err
	}
	conv, err := s.deps.Conversations.Get(ctx, cmd.UserID, cmd.ConversationID)
	if err != nil {
		return err
	}
	if conv == nil {
		return errs.New(errs.ConversationNotFound, "会话不存在")
	}
	if cmd.ReadSeq > conv.LastSeq {
		return errs.New(errs.InvalidArgument, "read_seq 不能大于当前会话 last_seq")
	}
	// 只能向前推进；重复提交相同 read_seq 幂等成功（由仓储层保证）
	return s.deps.Messages.AdvanceReadCursor(ctx, cmd.ConversationID, cmd.UserID, cmd.ReadSeq)
}

// NowUTC 统一服务端时间源。
func NowUTC() time.Time { return time.Now().UTC() }
