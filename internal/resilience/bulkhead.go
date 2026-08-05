package resilience

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

// Bulkhead 是有界并发槽位（GOCHAT_RESILIENCE.md §6.1）。
// 队列满时快速失败返回 SYSTEM_BUSY，不挤占其他资源域。
type Bulkhead struct {
	name   string
	sem    chan struct{}
	active atomic.Int64
	// OnRejected 拒绝回调（指标）
	OnRejected func(name string)
}

// NewBulkhead 创建并发上限为 limit 的隔离舱；limit <= 0 表示不限制。
func NewBulkhead(name string, limit int) *Bulkhead {
	if limit <= 0 {
		limit = 1 << 30
	}
	return &Bulkhead{name: name, sem: make(chan struct{}, limit)}
}

// Acquire 尝试获取槽位；ctx 取消或超时时返回错误。
// 调用方必须 defer Release()。
func (b *Bulkhead) Acquire(ctx context.Context) error {
	select {
	case b.sem <- struct{}{}:
		b.active.Add(1)
		return nil
	default:
		// 槽位耗尽：尝试带超时等待
		select {
		case b.sem <- struct{}{}:
			b.active.Add(1)
			return nil
		case <-ctx.Done():
			if b.OnRejected != nil {
				b.OnRejected(b.name)
			}
			return errs.Retryable(errs.SystemBusy, "系统繁忙，请稍后重试", 200)
		case <-time.After(100 * time.Millisecond):
			if b.OnRejected != nil {
				b.OnRejected(b.name)
			}
			return errs.Retryable(errs.SystemBusy, "系统繁忙，请稍后重试", 200)
		}
	}
}

// Release 释放槽位。
func (b *Bulkhead) Release() {
	select {
	case <-b.sem:
		b.active.Add(-1)
	default:
	}
}

// Active 返回当前占用槽位数。
func (b *Bulkhead) Active() int64 { return b.active.Load() }

// WithTimeout 为调用绑定统一超时预算（GOCHAT_RESILIENCE.md §4.2）。
// 内层调用超时必须小于外层（由装配层保证）。
func WithTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, d)
}
