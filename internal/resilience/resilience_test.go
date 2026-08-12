package resilience

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

// TestBreakerOnlyCountsTechnicalFailures 只有技术失败计入熔断统计
// （GOCHAT_RESILIENCE.md §3.1/§3.2）。
func TestBreakerOnlyCountsTechnicalFailures(t *testing.T) {
	// 场景 A：大量业务错误不应触发熔断
	biz := NewBreakers([]BreakerConfig{
		{Name: "redis:recent_get", Interval: time.Second, MinRequests: 3, FailureRatio: 0.5, OpenTimeout: 50 * time.Millisecond, HalfOpenMax: 1},
	})
	for i := 0; i < 100; i++ {
		err := biz.ExecuteByName(context.Background(), "redis:recent_get", func() error {
			return errs.New(errs.ConversationForbidden, "无权")
		})
		if err != nil {
			t.Fatalf("business error should pass through: %v", err)
		}
	}
	if st := biz.States()["redis:recent_get"]; st.String() != "closed" {
		t.Fatalf("breaker must stay closed for business errors, got %v", st)
	}

	// 场景 B：技术失败达到阈值 → Open，且 Open 期间不再调用依赖
	tech := NewBreakers([]BreakerConfig{
		{Name: "kafka:ingress_publish", Interval: time.Second, MinRequests: 3, FailureRatio: 0.5, OpenTimeout: 50 * time.Millisecond, HalfOpenMax: 1},
	})
	for i := 0; i < 5; i++ {
		_ = tech.ExecuteByName(context.Background(), "kafka:ingress_publish", func() error {
			return errs.New(errs.KafkaUnavailable, "kafka down")
		})
	}
	err := tech.ExecuteByName(context.Background(), "kafka:ingress_publish", func() error {
		t.Fatal("fn must not run when breaker open")
		return nil
	})
	if err == nil || !errs.IsCode(err, errs.SystemBusy) {
		t.Fatalf("expected SYSTEM_BUSY when open, got %v", err)
	}
}

// TestBreakerHalfOpenRecovery Half-Open 探测成功后恢复（GOCHAT_RESILIENCE.md §7.4）。
func TestBreakerHalfOpenRecovery(t *testing.T) {
	b := NewBreakers([]BreakerConfig{
		{Name: "mysql:history_query", Interval: time.Second, MinRequests: 3, FailureRatio: 0.6, OpenTimeout: 100 * time.Millisecond, HalfOpenMax: 2},
	})

	// 打满失败
	for i := 0; i < 5; i++ {
		_ = b.ExecuteByName(context.Background(), "mysql:history_query", func() error {
			return errors.New("conn refused")
		})
	}
	// 等待 Open 超时进入 Half-Open
	time.Sleep(150 * time.Millisecond)
	// Half-Open 期间探测成功 → 恢复 Closed
	for i := 0; i < 2; i++ {
		if err := b.ExecuteByName(context.Background(), "mysql:history_query", func() error {
			return nil
		}); err != nil {
			t.Fatalf("half-open probe failed: %v", err)
		}
	}
	if st := b.States()["mysql:history_query"]; st.String() != "closed" {
		t.Fatalf("expected closed after recovery, got %v", st)
	}
}

// TestBulkheadRejectsWhenFull 隔离舱满时快速失败（GOCHAT_RESILIENCE.md §6.1）。
func TestBulkheadRejectsWhenFull(t *testing.T) {
	b := NewBulkhead("test", 2)
	ctx := context.Background()

	if err := b.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	rejected := make(chan error, 1)
	go func() {
		rejected <- b.Acquire(ctx)
	}()
	select {
	case err := <-rejected:
		if err == nil || !errs.IsCode(err, errs.SystemBusy) {
			t.Fatalf("expected SYSTEM_BUSY, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("acquire did not fail fast")
	}

	b.Release()
	b.Release()
	if err := b.Acquire(ctx); err != nil {
		t.Fatalf("acquire after release failed: %v", err)
	}
	b.Release()
}

// TestBulkheadConcurrent 并发获取/释放不泄漏槽位。
func TestBulkheadConcurrent(t *testing.T) {
	b := NewBulkhead("test", 8)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if err := b.Acquire(context.Background()); err != nil {
					continue // 满时跳过，不算失败
				}
				b.Release()
			}
		}()
	}
	wg.Wait()
	if b.Active() != 0 {
		t.Fatalf("leaked slots: %d", b.Active())
	}
}
