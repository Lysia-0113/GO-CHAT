// Package segment 实现 MySQL 号段模式的双 Buffer 发号器
// （GOCHAT_API.md §7.4/§7.5/§7.6/§7.7）。
//
// 核心约束：
//   - 单实例高并发无重复 ID；
//   - 多实例共享 MySQL 时号段互不重叠（version CAS）；
//   - current/next 切换边界不重复、不回退；
//   - 服务重启后允许空洞，但不能重复使用旧号段。
package segment

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

// Segment 是一次申请得到的连续号段 [Min, Max]。
type Segment struct {
	Min  int64
	Max  int64
	Step int64
}

// SegmentRepository 负责向 MySQL 申请号段（实现见 mysql_repository.go）。
type SegmentRepository interface {
	Allocate(ctx context.Context, bizTag string) (Segment, error)
}

// ErrSegmentConflict 表示 CAS 冲突，需要重试。
var ErrSegmentConflict = errors.New("segment allocation conflict")

// ErrUnavailable 表示号段库存耗尽且暂时无法申请新号段。
var ErrUnavailable = errors.New("id generator unavailable")

// Buffer 是单 biz_tag 的双 Buffer 结构（GOCHAT_API.md §7.7）。
//
//	current：正在发号
//	next：预加载号段
//	loading：是否正在申请 next
//	nextReady：next 是否可切换
type Buffer struct {
	min, max, cursor int64

	loading   atomic.Bool
	nextReady atomic.Bool
	nextMin   atomic.Int64
	nextMax   atomic.Int64
}

// State 是 Buffer 可观测状态，供指标与测试。
type State struct {
	Min       int64
	Max       int64
	Remaining int64
	NextReady bool
	Loading   bool
}

// options 是生成器配置。
type options struct {
	prefetchRatio   float64
	allocateTimeout time.Duration
	nextWaitTimeout time.Duration
	maxRetries      int
}

// Option 配置生成器行为。
type Option func(*options)

// WithPrefetchRatio 设置预加载阈值（0 < ratio < 1），默认 0.70。
// 该值不是固定法则，应根据压测调整（GOCHAT_API.md §7.7）。
func WithPrefetchRatio(ratio float64) Option {
	return func(o *options) { o.prefetchRatio = ratio }
}

// WithAllocateTimeout 设置申请新号段的超时。
func WithAllocateTimeout(d time.Duration) Option {
	return func(o *options) { o.allocateTimeout = d }
}

// WithNextWaitTimeout 设置 current 耗尽后等待 next 的时长上限。
func WithNextWaitTimeout(d time.Duration) Option {
	return func(o *options) { o.nextWaitTimeout = d }
}

// WithMaxRetries 设置 CAS 冲突重试次数。
func WithMaxRetries(n int) Option {
	return func(o *options) { o.maxRetries = n }
}

// Generator 是双 Buffer 发号器，一个实例绑定一个 biz_tag。
type Generator struct {
	bizTag string
	repo   SegmentRepository
	opts   options

	mu  sync.Mutex
	cur Buffer
	// retries 记录当前申请 next 的 CAS 重试次数
	retries int
}

// NewGenerator 创建绑定 bizTag 的发号器，并立即申请第一个号段。
func NewGenerator(ctx context.Context, bizTag string, repo SegmentRepository, opts ...Option) (*Generator, error) {
	o := options{
		prefetchRatio:   0.70,
		allocateTimeout: 100 * time.Millisecond,
		nextWaitTimeout: 50 * time.Millisecond,
		maxRetries:      10,
	}
	for _, opt := range opts {
		opt(&o)
	}
	g := &Generator{bizTag: bizTag, repo: repo, opts: o}
	if _, err := g.loadNext(ctx); err != nil {
		return nil, err
	}
	g.swapToNext()
	return g, nil
}

// Next 返回下一个 ID。并发安全。
func (g *Generator) Next(ctx context.Context) (int64, error) {
	for {
		if id, ok := g.tryNext(); ok {
			return id, nil
		}
		if err := g.switchSegment(ctx); err != nil {
			return 0, err
		}
	}
}

// tryNext 尝试从 current 原子取出一个 ID；返回 false 表示 current 已耗尽。
func (g *Generator) tryNext() (int64, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	cur := &g.cur
	if cur.cursor <= cur.max {
		id := cur.cursor
		cur.cursor++
		g.maybePrefetchLocked()
		return id, true
	}
	return 0, false
}

// maybePrefetchLocked 当前号段使用达到阈值且 next 未就绪时触发预加载。
// 必须持有 g.mu；后台加载只允许一个协程执行。
func (g *Generator) maybePrefetchLocked() {
	cur := &g.cur
	if cur.loading.Load() || cur.nextReady.Load() {
		return
	}
	total := cur.max - cur.min + 1
	if total <= 0 {
		return
	}
	used := cur.cursor - cur.min
	if float64(used)/float64(total) >= g.opts.prefetchRatio {
		if cur.loading.CompareAndSwap(false, true) {
			go g.loadNextBackground()
		}
	}
}

// loadNextBackground 后台申请 next 号段；失败时复位 loading，允许后续重试。
func (g *Generator) loadNextBackground() {
	defer g.cur.loading.Store(false)

	ctx, cancel := context.WithTimeout(context.Background(), g.opts.allocateTimeout)
	defer cancel()

	seg, err := g.loadNext(ctx)
	if err != nil {
		// 预加载失败：保留 loading=false，下次触发时重试；指标由上层观测。
		return
	}
	_ = seg
}

// loadNext 申请并装载 next 号段（带 CAS 重试）。
func (g *Generator) loadNext(ctx context.Context) (Segment, error) {
	var lastErr error
	for attempt := 0; attempt <= g.opts.maxRetries; attempt++ {
		seg, err := g.repo.Allocate(ctx, g.bizTag)
		if err == nil {
			g.cur.nextMin.Store(seg.Min)
			g.cur.nextMax.Store(seg.Max)
			g.cur.nextReady.Store(true)
			return seg, nil
		}
		lastErr = err
		if !errors.Is(err, ErrSegmentConflict) {
			return Segment{}, errs.Wrap(errs.IDGeneratorUnavailable, "号段申请失败", err)
		}
		// CAS 冲突：短暂退避后重试
		select {
		case <-ctx.Done():
			return Segment{}, errs.Wrap(errs.IDGeneratorUnavailable, "号段申请超时", ctx.Err())
		case <-time.After(time.Duration(attempt+1) * 5 * time.Millisecond):
		}
	}
	return Segment{}, errs.Wrap(errs.IDGeneratorUnavailable, "号段申请冲突次数过多", lastErr)
}

// switchSegment 把 next 切换到 current；next 未就绪时有限等待。
//
// 检查与交换必须在同一临界区内完成，且交换前必须确认 current 确实已耗尽
// （cursor > max）。否则两个协程同时看到 current 耗尽时，第二个协程会把
// 刚换入的新段再次换出，丢弃仍有库存的号段（GOCHAT_API.md §12.8：
// current 与 next 切换边界不重复、不回退）。
func (g *Generator) switchSegment(ctx context.Context) error {
	deadline := time.Now().Add(g.opts.nextWaitTimeout)
	for {
		g.mu.Lock()
		// 其他协程已换入新段：当前有库存，返回让 Next 重试 tryNext
		if g.cur.cursor <= g.cur.max {
			g.mu.Unlock()
			return nil
		}
		if g.cur.nextReady.Load() {
			g.swapToNext()
			g.mu.Unlock()
			return nil
		}
		g.mu.Unlock()

		// next 未就绪：若尚未开始加载，主动申请一次
		if !g.cur.loading.Load() && !g.cur.nextReady.Load() {
			if g.cur.loading.CompareAndSwap(false, true) {
				go g.loadNextBackground()
			}
		}
		if time.Now().After(deadline) {
			return errs.Retryable(errs.IDGeneratorUnavailable,
				"号段库存耗尽且无法申请新号段", 100)
		}
		select {
		case <-ctx.Done():
			return errs.Wrap(errs.IDGeneratorUnavailable, "号段申请被取消", ctx.Err())
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// swapToNext 将 next 装为 current。必须持有 g.mu。
func (g *Generator) swapToNext() {
	g.cur.min = g.cur.nextMin.Load()
	g.cur.max = g.cur.nextMax.Load()
	g.cur.cursor = g.cur.min
	g.cur.nextReady.Store(false)
}

// State 返回当前观测状态（供指标与测试）。
func (g *Generator) State() State {
	g.mu.Lock()
	defer g.mu.Unlock()
	remaining := g.cur.max - g.cur.cursor + 1
	if remaining < 0 {
		remaining = 0
	}
	return State{
		Min:       g.cur.min,
		Max:       g.cur.max,
		Remaining: remaining,
		NextReady: g.cur.nextReady.Load(),
		Loading:   g.cur.loading.Load(),
	}
}
