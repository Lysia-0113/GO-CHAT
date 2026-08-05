package conversation

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/idgen"
)

// Dependencies 是 ConversationService 的精确依赖。
type Dependencies struct {
	Conversations   ConversationRepository
	ConversationIDs idgen.IDGenerator
	MaxGroupMembers int
}

// Service 实现会话创建、查询与成员校验。
type Service struct {
	deps Dependencies
}

func NewService(deps Dependencies) *Service {
	if deps.MaxGroupMembers <= 0 {
		deps.MaxGroupMembers = 500
	}
	return &Service{deps: deps}
}

// CreateConversationCommand 是创建会话命令（GOCHAT_API.md §5.5）。
// SenderID 来自 Token，不能由请求体指定。
type CreateConversationCommand struct {
	SenderID  int64
	Type      int8
	Name      string
	MemberIDs []int64
}

// Create 创建单聊或群聊；当前用户由 SenderID 指定。
// 相同两个用户重复创建单聊时返回已存在的会话（API 层可返回 200）。
func (s *Service) Create(ctx context.Context, cmd CreateConversationCommand) (*Conversation, bool, error) {
	if cmd.Type != TypeSingle && cmd.Type != TypeGroup {
		return nil, false, errs.New(errs.InvalidArgument, "会话类型无效")
	}
	// member_ids 去重（GOCHAT_API.md §5.5）
	members := dedupe(append(cmd.MemberIDs, cmd.SenderID))

	if cmd.Type == TypeSingle {
		// 单聊只能指定一个对方用户
		others := membersWithout(members, cmd.SenderID)
		if len(others) != 1 {
			return nil, false, errs.New(errs.InvalidArgument, "单聊必须且只能指定一个对方用户")
		}
		low, high := sortPair(cmd.SenderID, others[0])
		if existing, err := s.deps.Conversations.GetByDirectUsers(ctx, low, high); err != nil {
			return nil, false, err
		} else if existing != nil {
			existing.ReadSeq, existing.ReceivedSeq = 0, 0
			return existing, false, nil
		}
	}

	convID, err := s.deps.ConversationIDs.Next(ctx)
	if err != nil {
		return nil, false, err
	}

	conv := &Conversation{
		ConversationID: convID,
		Type:           cmd.Type,
		Name:           truncateName(cmd.Name),
		OwnerID:        cmd.SenderID,
		Status:         StatusNormal,
		CreatedAt:      time.Now().UTC(),
	}
	if cmd.Type == TypeSingle {
		// 单聊名称由客户端展示对方昵称，不冗余写入
		conv.Name = ""
	} else if conv.Name == "" {
		return nil, false, errs.New(errs.InvalidArgument, "群聊必须提供名称")
	}
	if cmd.Type == TypeGroup && len(members) > s.deps.MaxGroupMembers {
		return nil, false, errs.New(errs.InvalidArgument, "群成员数量超出上限")
	}

	created, err := s.deps.Conversations.CreateWithMembers(ctx, conv, members)
	if err != nil {
		return nil, false, err
	}
	if !created {
		// 并发下单聊被抢先创建，返回已存在会话
		low, high := sortPair(cmd.SenderID, membersWithout(members, cmd.SenderID)[0])
		existing, err := s.deps.Conversations.GetByDirectUsers(ctx, low, high)
		if err != nil {
			return nil, false, err
		}
		if existing == nil {
			return nil, false, errs.New(errs.InternalError, "会话创建状态不一致")
		}
		return existing, false, nil
	}
	return conv, true, nil
}

// Get 查询会话详情；只有成员可以查询。
func (s *Service) Get(ctx context.Context, actorID, conversationID int64) (*Conversation, error) {
	member, err := s.deps.Conversations.GetMember(ctx, conversationID, actorID)
	if err != nil {
		return nil, err
	}
	if member == nil || member.Status != MemberStatusNormal {
		return nil, errs.New(errs.ConversationForbidden, "无权访问该会话")
	}
	conv, err := s.deps.Conversations.Get(ctx, conversationID)
	if err != nil {
		return nil, err
	}
	if conv == nil || conv.Status != StatusNormal {
		return nil, errs.New(errs.ConversationNotFound, "会话不存在")
	}
	conv.ReadSeq = member.LastReadSeq
	conv.ReceivedSeq = member.LastReceivedSeq
	conv.UnreadCount = maxInt64(0, conv.LastSeq-member.LastReadSeq)
	return conv, nil
}

// List 分页查询当前用户的会话列表。
func (s *Service) List(ctx context.Context, userID int64, cursor string, limit int) (*Page, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	var cursorID, cursorTS int64
	if cursor != "" {
		id, ts, err := parseCursor(cursor)
		if err != nil {
			return nil, errs.New(errs.InvalidArgument, "游标无效")
		}
		cursorID, cursorTS = id, ts
	}
	items, err := s.deps.Conversations.ListByUser(ctx, userID, cursorID, cursorTS, limit+1)
	if err != nil {
		return nil, err
	}
	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	var next string
	if hasMore && len(items) > 0 {
		last := items[len(items)-1]
		next = encodeCursor(last.ConversationID, orderTS(last))
	}
	for i := range items {
		items[i].UnreadCount = maxInt64(0, items[i].LastSeq-items[i].ReadSeq)
	}
	return &Page{Items: items, NextCursor: next, HasMore: hasMore}, nil
}

// CheckMember 校验用户是否为会话正常成员；不是则返回 CONVERSATION_FORBIDDEN。
func (s *Service) CheckMember(ctx context.Context, conversationID, userID int64) (*Member, error) {
	member, err := s.deps.Conversations.GetMember(ctx, conversationID, userID)
	if err != nil {
		return nil, err
	}
	if member == nil || member.Status != MemberStatusNormal {
		return nil, errs.New(errs.ConversationForbidden, "无权访问该会话")
	}
	return member, nil
}

// ListMemberIDs 返回会话全部正常成员 ID。
func (s *Service) ListMemberIDs(ctx context.Context, conversationID int64) ([]int64, error) {
	return s.deps.Conversations.ListMemberIDs(ctx, conversationID)
}

// Page 是会话列表分页结果。
type Page struct {
	Items      []Conversation
	NextCursor string
	HasMore    bool
}

// orderTS 返回会话列表排序值（unix 毫秒）：last_message_at 优先，否则 created_at。
func orderTS(c Conversation) int64 {
	if c.LastMessageAt != nil {
		return c.LastMessageAt.UnixMilli()
	}
	return c.CreatedAt.UnixMilli()
}

// encodeCursor 编码不透明游标：base64("id|timestamp")。
func encodeCursor(id, ts int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%d|%d", id, ts)))
}

func parseCursor(cursor string) (int64, int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(cursor))
	if err != nil {
		return 0, 0, err
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("bad cursor")
	}
	id, err1 := strconv.ParseInt(parts[0], 10, 64)
	ts, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, fmt.Errorf("bad cursor")
	}
	return id, ts, nil
}

func dedupe(ids []int64) []int64 {
	seen := make(map[int64]struct{}, len(ids))
	out := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func membersWithout(ids []int64, exclude int64) []int64 {
	out := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id != exclude {
			out = append(out, id)
		}
	}
	return out
}

func sortPair(a, b int64) (int64, int64) {
	if a < b {
		return a, b
	}
	return b, a
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func truncateName(name string) string {
	r := []rune(name)
	if len(r) > 64 {
		return string(r[:64])
	}
	return name
}
