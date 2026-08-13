package message

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/conversation"
	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

// ---- fakes ----

type fakePublisher struct {
	mu          sync.Mutex
	ingress     []MessageIngressEvent
	persisted   []MessagePersistedEvent
	failPublish bool
}

func (f *fakePublisher) PublishIngress(ctx context.Context, e MessageIngressEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failPublish {
		return errs.New(errs.KafkaUnavailable, "kafka down")
	}
	f.ingress = append(f.ingress, e)
	return nil
}

func (f *fakePublisher) PublishPersisted(ctx context.Context, e MessagePersistedEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.persisted = append(f.persisted, e)
	return nil
}

func (f *fakePublisher) ingressCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.ingress)
}

type fakeMemberChecker struct {
	member *conversation.Member
	err    error
}

func (f *fakeMemberChecker) CheckMember(ctx context.Context, conversationID, userID int64) (*conversation.Member, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.member == nil {
		return nil, errs.New(errs.ConversationForbidden, "无权访问该会话")
	}
	return f.member, nil
}

type fakeConvState struct {
	conv *conversation.Conversation
}

func (f *fakeConvState) Get(ctx context.Context, actorID, conversationID int64) (*conversation.Conversation, error) {
	if f.conv == nil {
		return nil, nil
	}
	return f.conv, nil
}

type fakeRateLimiter struct {
	allowSend bool
}

func (f *fakeRateLimiter) AllowSend(ctx context.Context, userID, conversationID int64) error {
	if !f.allowSend {
		return errs.Retryable(errs.RateLimited, "发送过于频繁", 500)
	}
	return nil
}

func (f *fakeRateLimiter) AllowHistory(ctx context.Context, userID, conversationID int64) error {
	return nil
}

type fakeIdem struct {
	mu       sync.Mutex
	state    map[string]IdemResult // key: client_msg_id
	released []string
	marked   []string
}

func newFakeIdem() *fakeIdem {
	return &fakeIdem{state: make(map[string]IdemResult)}
}

// fakeSyncCursor 是 SyncCursorStore 的内存实现：只增不减，记录推进历史。
type fakeSyncCursor struct {
	mu       sync.Mutex
	cursors  map[[2]int64]int64
	advances [][3]int64 // conversationID, userID, seq
	fail     bool
}

func newFakeSyncCursor() *fakeSyncCursor {
	return &fakeSyncCursor{cursors: make(map[[2]int64]int64)}
}

func (f *fakeSyncCursor) Advance(ctx context.Context, conversationID, userID, seq int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return 0, errs.New(errs.RedisUnavailable, "cursor down")
	}
	f.advances = append(f.advances, [3]int64{conversationID, userID, seq})
	key := [2]int64{conversationID, userID}
	if seq > f.cursors[key] {
		f.cursors[key] = seq
	}
	return f.cursors[key], nil
}

func (f *fakeSyncCursor) Get(ctx context.Context, conversationID, userID int64) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	seq, ok := f.cursors[[2]int64{conversationID, userID}]
	return seq, ok, nil
}

func (f *fakeIdem) Acquire(ctx context.Context, senderID int64, clientMessageID string) (IdemResult, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	res, ok := f.state[clientMessageID]
	if !ok {
		f.state[clientMessageID] = IdemTaken
		return IdemTaken, "nonce-" + clientMessageID, nil
	}
	return res, "", nil
}

func (f *fakeIdem) MarkAccepted(ctx context.Context, senderID int64, clientMessageID, nonce string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marked = append(f.marked, clientMessageID)
	f.state[clientMessageID] = IdemAlreadyAccepted
	return nil
}

func (f *fakeIdem) Release(ctx context.Context, senderID int64, clientMessageID, nonce string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released = append(f.released, clientMessageID)
	delete(f.state, clientMessageID)
	return nil
}

type fakeMsgRepo struct {
	msgs []Message
}

func (f *fakeMsgRepo) FindByID(ctx context.Context, messageID int64) (*Message, error) {
	return nil, nil
}
func (f *fakeMsgRepo) FindByClientMessageID(ctx context.Context, senderID int64, clientMessageID string) (*Message, error) {
	for i := range f.msgs {
		if f.msgs[i].SenderID == senderID && f.msgs[i].ClientMessageID == clientMessageID {
			return &f.msgs[i], nil
		}
	}
	return nil, nil
}
func (f *fakeMsgRepo) ListBefore(ctx context.Context, conversationID, beforeSeq int64, limit int) ([]Message, error) {
	var out []Message
	for _, m := range f.msgs {
		if m.ConversationID == conversationID && (beforeSeq == 0 || m.Seq < beforeSeq) && m.Seq > 0 {
			out = append(out, m)
		}
	}
	return out, nil
}
func (f *fakeMsgRepo) ListAfter(ctx context.Context, conversationID, afterSeq int64, limit int) ([]Message, error) {
	var out []Message
	for _, m := range f.msgs {
		if m.ConversationID == conversationID && m.Seq > afterSeq {
			out = append(out, m)
		}
	}
	return out, nil
}
func (f *fakeMsgRepo) Persist(ctx context.Context, input PersistInput) (*Message, error) {
	m := input.Message
	m.Seq = 1
	f.msgs = append(f.msgs, *m)
	return m, nil
}
func (f *fakeMsgRepo) AdvanceReceivedCursor(ctx context.Context, conversationID, userID, receivedSeq int64) error {
	return nil
}
func (f *fakeMsgRepo) AdvanceReadCursor(ctx context.Context, conversationID, userID, readSeq int64) error {
	return nil
}

// ---- helpers ----

func newTestService(overrides map[string]interface{}) *Service {
	pub := &fakePublisher{}
	checker := &fakeMemberChecker{member: &conversation.Member{UserID: 1, ConversationID: 10, Status: conversation.MemberStatusNormal, JoinedSeq: 0}}
	conv := &conversation.Conversation{ConversationID: 10, Status: conversation.StatusNormal, LastSeq: 100}
	deps := Dependencies{
		Messages:      &fakeMsgRepo{},
		Members:       checker,
		Conversations: &fakeConvState{conv: conv},
		Cursors:       newFakeSyncCursor(),
		Publisher:     pub,
		RateLimiter:   &fakeRateLimiter{allowSend: true},
		FastIdem:      newFakeIdem(),
		IdemFallback:  false,
	}
	return NewService(deps)
}

func textContent(t *testing.T, text string) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]string{"text": text})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func baseSendCmd() SendMessageCommand {
	content, _ := json.Marshal(map[string]string{"text": "你好"})
	return SendMessageCommand{
		SenderID:        1,
		ClientMessageID: "019fd1c3-8b25-7ba0-a49d-001122334455",
		ConversationID:  10,
		MessageType:     TypeText,
		Content:         content,
	}
}

// ---- tests ----

func TestSendValidation(t *testing.T) {
	s := newTestService(nil)

	// 空 client_msg_id
	_, err := s.Send(context.Background(), SendMessageCommand{SenderID: 1, ConversationID: 10, MessageType: TypeText, Content: textContent(t, "hi")})
	if !errs.IsCode(err, errs.InvalidArgument) {
		t.Fatalf("expected INVALID_ARGUMENT, got %v", err)
	}
	// 空文本
	_, err = s.Send(context.Background(), SendMessageCommand{SenderID: 1, ClientMessageID: "x", ConversationID: 10, MessageType: TypeText, Content: textContent(t, "")})
	if !errs.IsCode(err, errs.InvalidArgument) {
		t.Fatalf("expected INVALID_ARGUMENT for empty text, got %v", err)
	}
	// 超长文本
	long := make([]rune, MaxTextLen+1)
	for i := range long {
		long[i] = 'a'
	}
	_, err = s.Send(context.Background(), SendMessageCommand{SenderID: 1, ClientMessageID: "x", ConversationID: 10, MessageType: TypeText, Content: textContent(t, string(long))})
	if !errs.IsCode(err, errs.MessageTooLarge) {
		t.Fatalf("expected MESSAGE_TOO_LARGE, got %v", err)
	}
	// 非文本类型（P0 只支持 text）
	_, err = s.Send(context.Background(), SendMessageCommand{SenderID: 1, ClientMessageID: "x", ConversationID: 10, MessageType: TypeImage, Content: textContent(t, "hi")})
	if !errs.IsCode(err, errs.InvalidArgument) {
		t.Fatalf("expected INVALID_ARGUMENT for image in P0, got %v", err)
	}
}

func TestSendRateLimited(t *testing.T) {
	s := newTestService(nil)
	rl := s.deps.RateLimiter.(*fakeRateLimiter)
	rl.allowSend = false
	_, err := s.Send(context.Background(), baseSendCmd())
	if !errs.IsCode(err, errs.RateLimited) {
		t.Fatalf("expected RATE_LIMITED, got %v", err)
	}
}

func TestSendForbiddenNonMember(t *testing.T) {
	s := newTestService(nil)
	s.deps.Members = &fakeMemberChecker{err: errs.New(errs.ConversationForbidden, "无权访问该会话")}
	_, err := s.Send(context.Background(), baseSendCmd())
	if !errs.IsCode(err, errs.ConversationForbidden) {
		t.Fatalf("expected CONVERSATION_FORBIDDEN, got %v", err)
	}
}

func TestSendSuccessPublishesIngress(t *testing.T) {
	s := newTestService(nil)
	cmd := baseSendCmd()
	res, err := s.Send(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Accepted || res.ClientMessageID != cmd.ClientMessageID {
		t.Fatalf("unexpected result: %+v", res)
	}
	pub := s.deps.Publisher.(*fakePublisher)
	if pub.ingressCount() != 1 {
		t.Fatalf("expected 1 ingress publish, got %d", pub.ingressCount())
	}
	if pub.ingress[0].SenderID != 1 || pub.ingress[0].ConversationID != 10 {
		t.Fatalf("wrong ingress event: %+v", pub.ingress[0])
	}
	idem := s.deps.FastIdem.(*fakeIdem)
	if len(idem.marked) != 1 || idem.marked[0] != cmd.ClientMessageID {
		t.Fatalf("expected MarkAccepted, got %v", idem.marked)
	}
}

func TestSendIdempotentAcceptedReused(t *testing.T) {
	s := newTestService(nil)
	cmd := baseSendCmd()
	if _, err := s.Send(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}
	// 相同 client_msg_id 重试：直接复用 accepted，不再次写 Kafka（GOCHAT_API.md §6.5）
	idem := s.deps.FastIdem.(*fakeIdem)
	idem.state[cmd.ClientMessageID] = IdemAlreadyAccepted
	res, err := s.Send(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Accepted {
		t.Fatal("expected accepted reuse")
	}
	pub := s.deps.Publisher.(*fakePublisher)
	if pub.ingressCount() != 1 {
		t.Fatalf("expected no new ingress publish, got %d", pub.ingressCount())
	}
}

func TestSendKafkaFailureReleasesIdem(t *testing.T) {
	s := newTestService(nil)
	pub := s.deps.Publisher.(*fakePublisher)
	pub.failPublish = true
	cmd := baseSendCmd()
	_, err := s.Send(context.Background(), cmd)
	if !errs.IsCode(err, errs.KafkaUnavailable) {
		t.Fatalf("expected KAFKA_UNAVAILABLE, got %v", err)
	}
	if !errs.As(err).Retryable {
		t.Fatal("expected retryable error")
	}
	idem := s.deps.FastIdem.(*fakeIdem)
	if len(idem.released) != 1 {
		t.Fatalf("expected idem release on kafka failure, got %v", idem.released)
	}
}

func TestGetByClientMessageIDSenderScoped(t *testing.T) {
	s := newTestService(nil)
	repo := s.deps.Messages.(*fakeMsgRepo)
	repo.msgs = append(repo.msgs, Message{
		MessageID: 100, SenderID: 1, ClientMessageID: "cid-1", ConversationID: 10, Seq: 1,
	})
	m, err := s.GetByClientMessageID(context.Background(), 1, "cid-1")
	if err != nil {
		t.Fatal(err)
	}
	if m.MessageID != 100 {
		t.Fatalf("unexpected message: %+v", m)
	}
	// 其他发送者查不到
	_, err = s.GetByClientMessageID(context.Background(), 2, "cid-1")
	if !errs.IsCode(err, errs.MessageNotFound) {
		t.Fatalf("expected MESSAGE_NOT_FOUND for other sender, got %v", err)
	}
}

func TestListHistoryBeforeAfterMutuallyExclusive(t *testing.T) {
	s := newTestService(nil)
	_, err := s.ListHistory(context.Background(), HistoryQuery{
		ActorID: 1, ConversationID: 10, BeforeSeq: 50, AfterSeq: 10, Limit: 20,
	})
	if !errs.IsCode(err, errs.InvalidArgument) {
		t.Fatalf("expected INVALID_ARGUMENT, got %v", err)
	}
}

func TestListHistoryBeforeMySQL(t *testing.T) {
	s := newTestService(nil)
	repo := s.deps.Messages.(*fakeMsgRepo)
	repo.msgs = append(repo.msgs, Message{MessageID: 9, Seq: 60, ConversationID: 10, SenderID: 1})

	// 读路径直连 MySQL（已移除缓存层）
	page, err := s.ListHistory(context.Background(), HistoryQuery{ActorID: 1, ConversationID: 10, BeforeSeq: 100, Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Seq != 60 {
		t.Fatalf("mysql query failed: %+v", page.Items)
	}
}

func TestListHistoryVisibilityBound(t *testing.T) {
	s := newTestService(nil)
	// joined_seq=50：seq<=50 的旧消息不可见
	s.deps.Members = &fakeMemberChecker{member: &conversation.Member{
		UserID: 1, ConversationID: 10, Status: conversation.MemberStatusNormal, JoinedSeq: 50,
	}}
	repo := s.deps.Messages.(*fakeMsgRepo)
	repo.msgs = []Message{
		{MessageID: 1, Seq: 30, ConversationID: 10, SenderID: 1},
		{MessageID: 2, Seq: 60, ConversationID: 10, SenderID: 1},
	}
	page, err := s.ListHistory(context.Background(), HistoryQuery{ActorID: 1, ConversationID: 10, BeforeSeq: 100, Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Seq != 60 {
		t.Fatalf("visibility bound not applied: %+v", page.Items)
	}
}

func TestMarkReadRules(t *testing.T) {
	s := newTestService(nil)
	conv := s.deps.Conversations.(*fakeConvState).conv
	conv.LastSeq = 100

	// read_seq > last_seq → INVALID_ARGUMENT（GOCHAT_API.md §5.10）
	err := s.MarkRead(context.Background(), MarkReadCommand{UserID: 1, ConversationID: 10, ReadSeq: 101})
	if !errs.IsCode(err, errs.InvalidArgument) {
		t.Fatalf("expected INVALID_ARGUMENT, got %v", err)
	}
	// 有效推进
	err = s.MarkRead(context.Background(), MarkReadCommand{UserID: 1, ConversationID: 10, ReadSeq: 50})
	if err != nil {
		t.Fatal(err)
	}
	// 非成员
	s.deps.Members = &fakeMemberChecker{err: errs.New(errs.ConversationForbidden, "无权访问该会话")}
	err = s.MarkRead(context.Background(), MarkReadCommand{UserID: 1, ConversationID: 10, ReadSeq: 50})
	if !errs.IsCode(err, errs.ConversationForbidden) {
		t.Fatalf("expected CONVERSATION_FORBIDDEN, got %v", err)
	}
}

func TestAckReceived(t *testing.T) {
	s := newTestService(nil)
	if err := s.AckReceived(context.Background(), AckReceivedCommand{UserID: 1, ConversationID: 10, ReceivedSeq: 0}); !errs.IsCode(err, errs.InvalidArgument) {
		t.Fatalf("expected INVALID_ARGUMENT for seq 0, got %v", err)
	}
	if err := s.AckReceived(context.Background(), AckReceivedCommand{UserID: 1, ConversationID: 10, ReceivedSeq: 80}); err != nil {
		t.Fatal(err)
	}
}

func TestSendProcessingBackoff(t *testing.T) {
	s := newTestService(nil)
	idem := s.deps.FastIdem.(*fakeIdem)
	idem.state["cid-processing"] = IdemProcessing
	cmd := baseSendCmd()
	cmd.ClientMessageID = "cid-processing"
	_, err := s.Send(context.Background(), cmd)
	if !errs.IsCode(err, errs.SystemBusy) || !errs.As(err).Retryable {
		t.Fatalf("expected retryable SYSTEM_BUSY, got %v", err)
	}
}

// ---- 同步游标（GOCHAT_REDIS.md §10）----

func TestListHistoryAfterAdvancesServerCursor(t *testing.T) {
	s := newTestService(nil)
	repo := s.deps.Messages.(*fakeMsgRepo)
	repo.msgs = []Message{
		{MessageID: 1, Seq: 101, ConversationID: 10, SenderID: 1},
		{MessageID: 2, Seq: 102, ConversationID: 10, SenderID: 1},
	}
	cursor := s.deps.Cursors.(*fakeSyncCursor)

	page, err := s.ListHistory(context.Background(), HistoryQuery{ActorID: 1, ConversationID: 10, AfterSeq: 100, Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 || page.ResyncRequired {
		t.Fatalf("unexpected page: %+v", page)
	}
	if page.NextAfterSeq != 102 {
		t.Fatalf("expected next_after_seq 102, got %d", page.NextAfterSeq)
	}
	// 服务端游标推进到本次响应覆盖的最大 seq
	seq, ok, _ := cursor.Get(context.Background(), 10, 1)
	if !ok || seq != 102 {
		t.Fatalf("expected server cursor 102, got %d exists=%v", seq, ok)
	}
}

func TestListHistoryAfterNoNewMessagesKeepsCursor(t *testing.T) {
	s := newTestService(nil)
	cursor := s.deps.Cursors.(*fakeSyncCursor)

	// after_seq 已到最新（无更晚消息）：游标保持在客户端位置
	page, err := s.ListHistory(context.Background(), HistoryQuery{ActorID: 1, ConversationID: 10, AfterSeq: 100, Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 0 || page.ResyncRequired {
		t.Fatalf("unexpected page: %+v", page)
	}
	seq, ok, _ := cursor.Get(context.Background(), 10, 1)
	if !ok || seq != 100 {
		t.Fatalf("expected cursor kept at 100, got %d exists=%v", seq, ok)
	}
}

func TestListHistoryAfterExpiredCursorSignalsResync(t *testing.T) {
	s := newTestService(nil)
	// joined_seq=50：客户端游标 30 低于可见性下界
	s.deps.Members = &fakeMemberChecker{member: &conversation.Member{
		UserID: 1, ConversationID: 10, Status: conversation.MemberStatusNormal, JoinedSeq: 50,
	}}
	repo := s.deps.Messages.(*fakeMsgRepo)
	repo.msgs = []Message{{MessageID: 1, Seq: 60, ConversationID: 10, SenderID: 1}}
	cursor := s.deps.Cursors.(*fakeSyncCursor)

	page, err := s.ListHistory(context.Background(), HistoryQuery{ActorID: 1, ConversationID: 10, AfterSeq: 30, Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if !page.ResyncRequired {
		t.Fatal("expected resync_required when cursor below visibility bound")
	}
	// 响应仍从可见区返回（老客户端忽略标志时行为不变）
	if len(page.Items) != 1 || page.Items[0].Seq != 60 {
		t.Fatalf("expected visible items, got %+v", page.Items)
	}
	// resync 场景不推进服务端游标（客户端全量重拉后重建）
	if len(cursor.advances) != 0 {
		t.Fatalf("cursor must not advance on resync, got %v", cursor.advances)
	}
}

func TestListHistoryCursorWriteFailureDegrades(t *testing.T) {
	s := newTestService(nil)
	repo := s.deps.Messages.(*fakeMsgRepo)
	repo.msgs = []Message{{MessageID: 1, Seq: 101, ConversationID: 10, SenderID: 1}}
	cursor := s.deps.Cursors.(*fakeSyncCursor)
	cursor.fail = true

	// 游标写失败不影响查询结果（两层游标设计：记录可重建，查询基准是客户端游标）
	page, err := s.ListHistory(context.Background(), HistoryQuery{ActorID: 1, ConversationID: 10, AfterSeq: 100, Limit: 20})
	if err != nil {
		t.Fatalf("query must succeed despite cursor failure, got %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].Seq != 101 {
		t.Fatalf("unexpected page: %+v", page)
	}
}

var _ = time.Now
