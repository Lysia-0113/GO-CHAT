package segment

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

// fakeRepo 模拟共享 MySQL 的号段表：max_id/step/version + CAS 语义。
type fakeRepo struct {
	mu   sync.Mutex
	rows map[string]*fakeRow
	// conflictForced 前 N 次申请强制返回 CAS 冲突（测试重试）
	conflictForced atomic.Int64
	// maxIDCap 模拟号段耗尽（MySQL 不可用）
	maxIDCap atomic.Int64
}

type fakeRow struct {
	maxID   int64
	step    int64
	version int64
}

func newFakeRepo(bizTag string, step int64) *fakeRepo {
	return &fakeRepo{rows: map[string]*fakeRow{bizTag: {maxID: 0, step: step, version: 0}}}
}

func (f *fakeRepo) Allocate(ctx context.Context, bizTag string) (Segment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if n := f.conflictForced.Load(); n > 0 {
		f.conflictForced.Add(-1)
		return Segment{}, ErrSegmentConflict
	}

	row := f.rows[bizTag]
	if row == nil {
		return Segment{}, errors.New("biz_tag not initialized")
	}
	if cap := f.maxIDCap.Load(); cap > 0 && row.maxID >= cap {
		return Segment{}, errors.New("mysql unavailable")
	}
	seg := Segment{Min: row.maxID + 1, Max: row.maxID + row.step, Step: row.step}
	row.maxID = seg.Max
	row.version++
	return seg, nil
}

// testOptions 返回快速切换的小号段配置。
func testOptions() []Option {
	return []Option{
		WithPrefetchRatio(0.5),
		WithAllocateTimeout(50 * time.Millisecond),
		WithNextWaitTimeout(20 * time.Millisecond),
		WithMaxRetries(5),
	}
}

// TestSingleGeneratorConcurrentNoDuplicates 单实例高并发无重复、连续无空洞。
// 注意：并发下各协程的接收顺序不等于发号顺序，因此先收集再排序断言
// 唯一性 + [min, min+count) 连续性（发号器本身在互斥锁内严格递增）。
func TestSingleGeneratorConcurrentNoDuplicates(t *testing.T) {
	repo := newFakeRepo("im_message", 1000)
	gen, err := NewGenerator(context.Background(), "im_message", repo, testOptions()...)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 16
	const perGoroutine = 500
	ids := make(chan int64, goroutines*perGoroutine)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				id, err := gen.Next(context.Background())
				if err != nil {
					t.Errorf("Next error: %v", err)
					return
				}
				ids <- id
			}
		}()
	}
	wg.Wait()
	close(ids)

	all := make([]int64, 0, goroutines*perGoroutine)
	for id := range ids {
		all = append(all, id)
	}
	if len(all) != goroutines*perGoroutine {
		t.Fatalf("expected %d ids, got %d", goroutines*perGoroutine, len(all))
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	for i, id := range all {
		if i > 0 && id == all[i-1] {
			t.Fatalf("duplicate id: %d", id)
		}
		if i > 0 && id != all[i-1]+1 {
			t.Fatalf("gap at %d: %d after %d", i, id, all[i-1])
		}
	}
}

// TestMultipleGeneratorsNoOverlap 多实例共享同一 MySQL 时号段互不重叠。
func TestMultipleGeneratorsNoOverlap(t *testing.T) {
	repo := newFakeRepo("im_message", 500)
	genA, err := NewGenerator(context.Background(), "im_message", repo, testOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	genB, err := NewGenerator(context.Background(), "im_message", repo, testOptions()...)
	if err != nil {
		t.Fatal(err)
	}

	collect := func(gen *Generator, n int) map[int64]struct{} {
		out := make(map[int64]struct{}, n)
		for i := 0; i < n; i++ {
			id, err := gen.Next(context.Background())
			if err != nil {
				t.Fatalf("Next error: %v", err)
			}
			out[id] = struct{}{}
		}
		return out
	}
	a := collect(genA, 700)
	b := collect(genB, 700)
	if len(a)+len(b) != 1400 {
		t.Fatalf("lost ids: %d + %d", len(a), len(b))
	}
	for id := range a {
		if _, dup := b[id]; dup {
			t.Fatalf("overlapping id across generators: %d", id)
		}
	}
}

// TestCASConflictRetry CAS 冲突后能够有限重试并最终成功。
func TestCASConflictRetry(t *testing.T) {
	repo := newFakeRepo("im_message", 100)
	repo.conflictForced.Store(3) // 前 3 次申请冲突
	gen, err := NewGenerator(context.Background(), "im_message", repo, testOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	// 第一次申请（NewGenerator 内）被强制冲突 3 次后成功，或后续申请触发
	id1, err := gen.Next(context.Background())
	if err != nil {
		t.Fatalf("expected retry success, got %v", err)
	}
	if id1 <= 0 {
		t.Fatal("invalid id")
	}
}

// TestSwitchBoundaryNoGapNoReuse current/next 切换边界不重复、不回退。
func TestSwitchBoundaryNoGapNoReuse(t *testing.T) {
	repo := newFakeRepo("im_message", 50) // 小号段，快速切换
	gen, err := NewGenerator(context.Background(), "im_message", repo, testOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	const total = 500
	var prev int64
	for i := 0; i < total; i++ {
		id, err := gen.Next(context.Background())
		if err != nil {
			t.Fatalf("Next error at %d: %v", i, err)
		}
		if i > 0 && id != prev+1 {
			t.Fatalf("gap or reuse at %d: %d after %d", i, id, prev)
		}
		prev = id
	}
}

// TestPrefetchBackground 预加载在阈值触发后异步准备 next。
func TestPrefetchBackground(t *testing.T) {
	repo := newFakeRepo("im_message", 100)
	gen, err := NewGenerator(context.Background(), "im_message", repo, testOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	// 消耗超过 50%（ratio=0.5）触发预加载
	for i := 0; i < 55; i++ {
		if _, err := gen.Next(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if gen.State().NextReady {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("next segment was not prefetched")
}

// TestPrefetchFailureThenRecovery 预加载失败后可重试成功。
func TestPrefetchFailureThenRecovery(t *testing.T) {
	repo := newFakeRepo("im_message", 100)
	gen, err := NewGenerator(context.Background(), "im_message", repo, testOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	// 模拟 MySQL 短暂不可用：耗尽当前段时申请失败
	repo.maxIDCap.Store(100) // 只允许申请到第一段
	for i := 0; i < 100; i++ {
		if _, err := gen.Next(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	// 此刻申请 next 失败（库存耗尽）；恢复 MySQL 后应能自愈
	repo.maxIDCap.Store(0)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, err := gen.Next(context.Background())
		if err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("generator did not recover after mysql restored")
}

// TestExhaustedReturnsError 库存耗尽且无法申请时快速失败，不永久阻塞。
func TestExhaustedReturnsError(t *testing.T) {
	repo := newFakeRepo("im_message", 100)
	gen, err := NewGenerator(context.Background(), "im_message", repo, testOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	repo.maxIDCap.Store(100) // 只能申请第一段，无 next
	start := time.Now()
	for i := 0; i < 100; i++ {
		if _, err := gen.Next(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	_, err = gen.Next(context.Background())
	if err == nil {
		t.Fatal("expected error when exhausted")
	}
	if !errs.IsCode(err, errs.IDGeneratorUnavailable) {
		t.Fatalf("expected ID_GENERATOR_UNAVAILABLE, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("exhaustion did not fail fast: %v", elapsed)
	}
}

// TestRestartAllowsHoleNoReuse 重启后允许空洞，但不能重复使用旧号段。
func TestRestartAllowsHoleNoReuse(t *testing.T) {
	repo := newFakeRepo("im_message", 100)
	gen1, err := NewGenerator(context.Background(), "im_message", repo, testOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		if _, err := gen1.Next(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	// 模拟重启：新生成器重新申请号段（原实例丢弃未用库存 → 允许空洞）
	gen2, err := NewGenerator(context.Background(), "im_message", repo, testOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	id, err := gen2.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// 第二段起点应大于第一段终点（不重复）
	if id <= 100 {
		t.Fatalf("reused old segment: got %d", id)
	}
}
