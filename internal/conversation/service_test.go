package conversation

import (
	"context"
	"errors"
	"testing"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/idgen"
)

// ---- fakes ----

type fakeIDGen struct {
	next int64
}

func (f *fakeIDGen) Next(ctx context.Context) (int64, error) {
	f.next++
	return f.next, nil
}

type fakeConvRepo struct {
	convos  map[int64]*Conversation
	members map[int64][]Member // conversationID → members
	nextID  int64
	dupNext bool // 下一次创建模拟唯一键冲突
}

func newFakeConvRepo() *fakeConvRepo {
	return &fakeConvRepo{convos: make(map[int64]*Conversation), members: make(map[int64][]Member)}
}

func (f *fakeConvRepo) CreateWithMembers(ctx context.Context, conv *Conversation, memberIDs []int64) (bool, error) {
	if f.dupNext {
		f.dupNext = false
		return false, nil
	}
	f.convos[conv.ConversationID] = conv
	for _, uid := range memberIDs {
		f.members[conv.ConversationID] = append(f.members[conv.ConversationID], Member{
			ConversationID: conv.ConversationID, UserID: uid, Status: MemberStatusNormal,
		})
	}
	return true, nil
}

func (f *fakeConvRepo) Get(ctx context.Context, conversationID int64) (*Conversation, error) {
	c, ok := f.convos[conversationID]
	if !ok {
		return nil, nil
	}
	return c, nil
}

func (f *fakeConvRepo) GetByDirectUsers(ctx context.Context, lowID, highID int64) (*Conversation, error) {
	for id, c := range f.convos {
		if c.Type != TypeSingle {
			continue
		}
		var ids []int64
		for _, m := range f.members[id] {
			ids = append(ids, m.UserID)
		}
		if len(ids) == 2 && ((ids[0] == lowID && ids[1] == highID) || (ids[0] == highID && ids[1] == lowID)) {
			return c, nil
		}
	}
	return nil, nil
}

func (f *fakeConvRepo) ListByUser(ctx context.Context, userID, cursorID, cursorTS int64, limit int) ([]Conversation, error) {
	return nil, nil
}

func (f *fakeConvRepo) GetMember(ctx context.Context, conversationID, userID int64) (*Member, error) {
	for _, m := range f.members[conversationID] {
		if m.UserID == userID {
			return &m, nil
		}
	}
	return nil, nil
}

func (f *fakeConvRepo) ListMemberIDs(ctx context.Context, conversationID int64) ([]int64, error) {
	ids := make([]int64, 0, len(f.members[conversationID]))
	for _, m := range f.members[conversationID] {
		ids = append(ids, m.UserID)
	}
	return ids, nil
}

func newTestService() *Service {
	return NewService(Dependencies{
		Conversations:   newFakeConvRepo(),
		ConversationIDs: &fakeIDGen{},
	})
}

func TestCreateSingleChat(t *testing.T) {
	s := newTestService()
	conv, created, err := s.Create(context.Background(), CreateConversationCommand{
		SenderID: 1, Type: TypeSingle, MemberIDs: []int64{2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("expected created")
	}
	if conv.Type != TypeSingle || conv.OwnerID != 1 {
		t.Fatalf("unexpected conv: %+v", conv)
	}
	// 单聊不允许指定多个对方
	_, _, err = s.Create(context.Background(), CreateConversationCommand{
		SenderID: 1, Type: TypeSingle, MemberIDs: []int64{2, 3},
	})
	if !errs.IsCode(err, errs.InvalidArgument) {
		t.Fatalf("expected INVALID_ARGUMENT, got %v", err)
	}
}

func TestCreateGroupMemberCap(t *testing.T) {
	s := newTestService()
	// 上限内（默认 1024，含创建者）允许创建
	ids := make([]int64, 0, 1023)
	for i := int64(2); i <= 1024; i++ {
		ids = append(ids, i)
	}
	_, created, err := s.Create(context.Background(), CreateConversationCommand{
		SenderID: 1, Type: TypeGroup, Name: "cap-ok", MemberIDs: ids,
	})
	if err != nil || !created {
		t.Fatalf("expected created within cap, err=%v", err)
	}
	// 超出上限拒绝（1023 成员 + 创建者 = 1024 是上限，再多一个拒绝）
	over := append(ids, 1025)
	_, _, err = s.Create(context.Background(), CreateConversationCommand{
		SenderID: 1, Type: TypeGroup, Name: "cap-over", MemberIDs: over,
	})
	if !errs.IsCode(err, errs.InvalidArgument) {
		t.Fatalf("expected INVALID_ARGUMENT over cap, got %v", err)
	}
}

func TestCreateGroupRequiresName(t *testing.T) {
	s := newTestService()
	_, _, err := s.Create(context.Background(), CreateConversationCommand{
		SenderID: 1, Type: TypeGroup, MemberIDs: []int64{2},
	})
	if !errs.IsCode(err, errs.InvalidArgument) {
		t.Fatalf("expected INVALID_ARGUMENT for empty group name, got %v", err)
	}
	conv, created, err := s.Create(context.Background(), CreateConversationCommand{
		SenderID: 1, Type: TypeGroup, Name: "Go 后端交流群", MemberIDs: []int64{2, 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !created || conv.Type != TypeGroup {
		t.Fatalf("unexpected: %+v", conv)
	}
}

func TestCreateSingleDupReusesExisting(t *testing.T) {
	s := newTestService()
	// 第一次创建
	if _, created, err := s.Create(context.Background(), CreateConversationCommand{
		SenderID: 1, Type: TypeSingle, MemberIDs: []int64{2},
	}); err != nil || !created {
		t.Fatalf("first create failed: %v", err)
	}
	// 模拟唯一键冲突 → 返回已存在会话（GOCHAT_API.md §5.5：复用返回 200）
	repo := s.deps.Conversations.(*fakeConvRepo)
	repo.dupNext = true
	conv, created, err := s.Create(context.Background(), CreateConversationCommand{
		SenderID: 1, Type: TypeSingle, MemberIDs: []int64{2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("expected reuse, not created")
	}
	if conv == nil {
		t.Fatal("expected existing conversation")
	}
}

func TestMemberDedupe(t *testing.T) {
	s := newTestService()
	conv, _, err := s.Create(context.Background(), CreateConversationCommand{
		SenderID: 1, Type: TypeGroup, Name: "群", MemberIDs: []int64{2, 2, 3, 1}, // 含重复和创建者
	})
	if err != nil {
		t.Fatal(err)
	}
	repo := s.deps.Conversations.(*fakeConvRepo)
	ids, _ := repo.ListMemberIDs(context.Background(), conv.ConversationID)
	seen := map[int64]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate member: %d", id)
		}
		seen[id] = true
	}
	if !seen[1] || !seen[2] || !seen[3] || len(ids) != 3 {
		t.Fatalf("unexpected members: %v", ids)
	}
}

func TestGetForbiddenNonMember(t *testing.T) {
	s := newTestService()
	conv, _, err := s.Create(context.Background(), CreateConversationCommand{
		SenderID: 1, Type: TypeSingle, MemberIDs: []int64{2},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 成员可以查询
	if _, err := s.Get(context.Background(), 2, conv.ConversationID); err != nil {
		t.Fatal(err)
	}
	// 非成员查询被拒绝
	_, err = s.Get(context.Background(), 999, conv.ConversationID)
	if !errs.IsCode(err, errs.ConversationForbidden) {
		t.Fatalf("expected CONVERSATION_FORBIDDEN, got %v", err)
	}
}

func TestCheckMember(t *testing.T) {
	s := newTestService()
	conv, _, _ := s.Create(context.Background(), CreateConversationCommand{
		SenderID: 1, Type: TypeSingle, MemberIDs: []int64{2},
	})
	m, err := s.CheckMember(context.Background(), conv.ConversationID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if m.UserID != 2 {
		t.Fatalf("unexpected member: %+v", m)
	}
	_, err = s.CheckMember(context.Background(), conv.ConversationID, 999)
	if !errs.IsCode(err, errs.ConversationForbidden) {
		t.Fatalf("expected CONVERSATION_FORBIDDEN, got %v", err)
	}
}

var _ = errors.New
var _ idgen.IDGenerator = (*fakeIDGen)(nil)
