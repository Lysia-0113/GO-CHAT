package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/Lysia-0113/GO-CHAT/internal/connection"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
)

func connectionRoute(userID int64, connID, deviceID, nodeID string) connection.ConnectionRoute {
	return connection.ConnectionRoute{ConnectionID: connID, UserID: userID, DeviceID: deviceID, NodeID: nodeID}
}

// newTestRedis 启动 miniredis 并加载脚本。
func newTestRedis(t *testing.T) (*goredis.Client, *Scripts) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	scripts, err := LoadScripts()
	if err != nil {
		t.Fatal(err)
	}
	return client, scripts
}

func testOpts() Options {
	return Options{ReadTimeout: time.Second, WriteTimeout: time.Second}
}

// TestWSTicketOneTime 票据只能消费一次，重放被拒绝（GOCHAT_REDIS.md §6.2 / §13.2）。
func TestWSTicketOneTime(t *testing.T) {
	client, scripts := newTestRedis(t)
	store := NewWSTicketStore(client, scripts, 30*time.Second, testOpts())

	token, err := store.Create(context.Background(), 42, "web-chrome-a1")
	if err != nil {
		t.Fatal(err)
	}
	data, err := store.Consume(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil || data.UserID != 42 || data.DeviceID != "web-chrome-a1" {
		t.Fatalf("unexpected ticket data: %+v", data)
	}
	// 重放：第二次消费必须拒绝
	again, err := store.Consume(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	if again != nil {
		t.Fatal("ticket replay must be rejected")
	}
	// 无效 token
	bad, err := store.Consume(context.Background(), "not-a-ticket")
	if err != nil {
		t.Fatal(err)
	}
	if bad != nil {
		t.Fatal("invalid ticket must be rejected")
	}
}

// TestRecentCacheAppendAndRead 最近消息缓存：追加、读取、裁剪（GOCHAT_REDIS.md §4.3）。
func TestRecentCacheAppendAndRead(t *testing.T) {
	client, scripts := newTestRedis(t)
	cache := NewRecentMessageCache(client, scripts, 24*time.Hour, 3, testOpts())

	for seq := int64(1); seq <= 5; seq++ {
		if err := cache.Append(context.Background(), &message.Message{
			MessageID: 1000 + seq, Seq: seq, ConversationID: 9001, SenderID: 1,
			MessageType: 1, Content: json.RawMessage(`{"text":"hi"}`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	// max=3：应只保留 seq 3,4,5
	items, complete, err := cache.ListBefore(context.Background(), 9001, 0, 100, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || len(items) != 3 {
		t.Fatalf("expected 3 items complete, got %d complete=%v", len(items), complete)
	}
	if items[0].Seq != 5 || items[2].Seq != 3 {
		t.Fatalf("unexpected order: %+v", items)
	}
}

// TestRecentCacheMissingFieldFallsBack 快照缺失时 complete=false（回源 MySQL）。
func TestRecentCacheMissingFieldFallsBack(t *testing.T) {
	client, scripts := newTestRedis(t)
	cache := NewRecentMessageCache(client, scripts, 24*time.Hour, 10, testOpts())

	if err := cache.Append(context.Background(), &message.Message{
		MessageID: 1, Seq: 1, ConversationID: 9001, SenderID: 1, MessageType: 1,
	}); err != nil {
		t.Fatal(err)
	}
	// 手工删除 data HASH 字段模拟不一致
	if err := client.HDel(context.Background(), RecentDataKey(9001), "1").Err(); err != nil {
		t.Fatal(err)
	}
	_, complete, err := cache.ListBefore(context.Background(), 9001, 0, 100, 10)
	if err != nil {
		t.Fatal(err)
	}
	if complete {
		t.Fatal("expected incomplete when snapshot missing")
	}
}

// TestRecentCacheVisibleBound 可见性下界过滤。
func TestRecentCacheVisibleBound(t *testing.T) {
	client, scripts := newTestRedis(t)
	cache := NewRecentMessageCache(client, scripts, 24*time.Hour, 10, testOpts())
	for seq := int64(1); seq <= 5; seq++ {
		_ = cache.Append(context.Background(), &message.Message{
			MessageID: seq, Seq: seq, ConversationID: 9001, SenderID: 1, MessageType: 1,
		})
	}
	// visibleAfterSeq=3 → 只返回 seq 4,5
	items, complete, err := cache.ListBefore(context.Background(), 9001, 3, 100, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || len(items) != 2 || items[0].Seq != 5 {
		t.Fatalf("unexpected: %+v complete=%v", items, complete)
	}
}

// TestIdempotencyLifecycle 幂等：获得发送权 → accepted → 重复请求命中 accepted
// （GOCHAT_REDIS.md §7.2）。
func TestIdempotencyLifecycle(t *testing.T) {
	client, scripts := newTestRedis(t)
	store := NewIdempotencyStore(client, scripts, 5*time.Second, 10*time.Minute, testOpts())
	ctx := context.Background()
	const cid = "019fd1c3-8b25-7ba0-a49d-001122334455"

	result, nonce, err := store.Acquire(ctx, 7, cid)
	if err != nil || result != message.IdemTaken || nonce == "" {
		t.Fatalf("acquire failed: %v %v", result, err)
	}
	// 另一个并发请求：处理中
	result2, _, err := store.Acquire(ctx, 7, cid)
	if err != nil || result2 != message.IdemProcessing {
		t.Fatalf("expected processing, got %v %v", result2, err)
	}
	// Kafka 成功后标记 accepted
	if err := store.MarkAccepted(ctx, 7, cid, nonce); err != nil {
		t.Fatal(err)
	}
	// 重试：命中 accepted
	result3, _, err := store.Acquire(ctx, 7, cid)
	if err != nil || result3 != message.IdemAlreadyAccepted {
		t.Fatalf("expected already accepted, got %v %v", result3, err)
	}
	// 使用错误 nonce 无法释放
	if err := store.Release(ctx, 7, cid, "wrong-nonce"); err != nil {
		t.Fatal(err)
	}
	result4, _, err := store.Acquire(ctx, 7, cid)
	if err != nil || result4 != message.IdemAlreadyAccepted {
		t.Fatalf("release with wrong nonce must not remove state, got %v", result4)
	}
}

// TestIdempotencyRelease 失败路径：仅自己的 nonce 可删除。
func TestIdempotencyRelease(t *testing.T) {
	client, scripts := newTestRedis(t)
	store := NewIdempotencyStore(client, scripts, 5*time.Second, 10*time.Minute, testOpts())
	ctx := context.Background()
	const cid = "cid-rel-1"

	_, nonce, err := store.Acquire(ctx, 1, cid)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Release(ctx, 1, cid, nonce); err != nil {
		t.Fatal(err)
	}
	result, _, err := store.Acquire(ctx, 1, cid)
	if err != nil || result != message.IdemTaken {
		t.Fatalf("expected taken after release, got %v %v", result, err)
	}
}

// TestTokenBucket 令牌桶：允许 → 拒绝 → 恢复（GOCHAT_REDIS.md §8.2）。
func TestTokenBucket(t *testing.T) {
	client, scripts := newTestRedis(t)
	limiter := NewRateLimiter(client, scripts, testOpts())
	ctx := context.Background()

	// 容量 3，速率 10/s：前 3 次允许
	for i := 0; i < 3; i++ {
		ok, _, err := limiter.Allow(ctx, "im:test:bucket", 3, 10, time.Minute)
		if err != nil || !ok {
			t.Fatalf("attempt %d: expected allow, got %v %v", i, ok, err)
		}
	}
	ok, wait, err := limiter.Allow(ctx, "im:test:bucket", 3, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected reject when bucket empty")
	}
	if wait <= 0 {
		t.Fatal("expected retry_after > 0")
	}
	// 补充后恢复（10/s → 100ms 一个 token）
	time.Sleep(150 * time.Millisecond)
	ok, _, err = limiter.Allow(ctx, "im:test:bucket", 3, 10, time.Minute)
	if err != nil || !ok {
		t.Fatalf("expected allow after refill, got %v %v", ok, err)
	}
}

// TestPresenceLifecycle 在线状态：注册 → 查询 → 心跳 → 移除（GOCHAT_REDIS.md §5）。
func TestPresenceLifecycle(t *testing.T) {
	client, _ := newTestRedis(t)
	presence := NewPresenceRegistry(client, 90*time.Second, 3*time.Minute, testOpts())
	ctx := context.Background()

	route := connectionRoute(100, "conn-a", "web", "node-1")
	if err := presence.Register(ctx, route); err != nil {
		t.Fatal(err)
	}
	routes, err := presence.OnlineConnections(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 1 || routes[0].ConnectionID != "conn-a" {
		t.Fatalf("unexpected routes: %+v", routes)
	}
	if err := presence.Heartbeat(ctx, route); err != nil {
		t.Fatal(err)
	}
	if err := presence.Remove(ctx, "conn-a", 100); err != nil {
		t.Fatal(err)
	}
	routes, err = presence.OnlineConnections(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 0 {
		t.Fatalf("expected empty routes, got %+v", routes)
	}
}

// TestPresenceStaleCleanup 过期连接自动清理。
func TestPresenceStaleCleanup(t *testing.T) {
	client, _ := newTestRedis(t)
	presence := NewPresenceRegistry(client, 90*time.Second, 3*time.Minute, testOpts())
	ctx := context.Background()

	_ = presence.Register(ctx, connectionRoute(200, "conn-old", "web", "node-1"))
	// 手工把过期时间改到过去
	_ = client.ZAdd(ctx, PresenceUserKey(200), goredis.Z{Score: float64(time.Now().Add(-time.Minute).UnixMilli()), Member: "conn-old"}).Err()
	_ = client.Expire(ctx, PresenceConnKey("conn-old"), 90*time.Second).Err()

	cleaned := false
	presence.OnStaleCleanup = func(n int) { cleaned = n > 0 }
	routes, err := presence.OnlineConnections(ctx, 200)
	if err != nil {
		t.Fatal(err)
	}
	if !cleaned {
		t.Fatal("expected stale cleanup")
	}
	if len(routes) != 0 {
		t.Fatalf("expected empty after cleanup, got %+v", routes)
	}
}
