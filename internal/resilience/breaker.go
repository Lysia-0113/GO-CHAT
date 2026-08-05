// Package resilience 实现熔断、隔离、超时等韧性机制
// （GOCHAT_RESILIENCE.md §7：sony/gobreaker，按 依赖:操作 维度）。
package resilience

import (
	"context"
	"sync"
	"time"

	"github.com/sony/gobreaker"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
)

// BreakerConfig 是单个熔断器配置（GOCHAT_RESILIENCE.md §7.3）。
type BreakerConfig struct {
	Name         string        // 形如 "redis:recent_get"
	Interval     time.Duration // 统计周期
	MinRequests  uint32        // 最小请求数
	FailureRatio float64       // 失败率阈值
	OpenTimeout  time.Duration // Open 时长
	HalfOpenMax  uint32        // Half-Open 探测请求上限
}

// Breakers 是进程内全部熔断器的注册表。
type Breakers struct {
	mu  sync.RWMutex
	m   map[string]*Breaker
	reg *metrics.Registry
}

// NewBreakers 按配置创建熔断器集合。
func NewBreakers(configs []BreakerConfig, reg *metrics.Registry) *Breakers {
	b := &Breakers{
		m:   make(map[string]*Breaker, len(configs)),
		reg: reg,
	}
	for _, c := range configs {
		b.m[c.Name] = newBreaker(c)
	}
	return b
}

// Breaker 封装 gobreaker 并暴露状态观测。
type Breaker struct {
	name string
	cb   *gobreaker.CircuitBreaker
}

func newBreaker(c BreakerConfig) *Breaker {
	cfg := gobreaker.Settings{
		Name:        c.Name,
		Interval:    c.Interval,
		Timeout:     c.OpenTimeout,
		MaxRequests: c.HalfOpenMax,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// 未达到最小请求数时不打开（GOCHAT_RESILIENCE.md §7.3：低流量依赖保护）
			if counts.Requests < c.MinRequests {
				return false
			}
			return float64(counts.TotalFailures)/float64(counts.Requests) >= c.FailureRatio
		},
	}
	return &Breaker{name: c.Name, cb: gobreaker.NewCircuitBreaker(cfg)}
}

// Execute 执行受保护调用。
// 只把"技术失败"（errs.IsTechnical）计入熔断统计；业务错误、4xx、用户取消不计入。
// 熔断打开时直接返回 SYSTEM_BUSY 类错误，不再调用依赖。
func (b *Breaker) Execute(ctx context.Context, fn func() error) error {
	_, err := b.cb.Execute(func() (interface{}, error) {
		err := fn()
		if err != nil && !errs.IsTechnical(err) {
			// 业务错误不破坏熔断统计
			return nil, nil
		}
		return nil, err
	})
	if err != nil {
		if err == gobreaker.ErrOpenState {
			return errs.Retryable(errs.SystemBusy, "依赖暂时不可用（熔断）", 100)
		}
	}
	return err
}

// ExecuteByName 按名称执行受保护调用；未注册的依赖直接执行（不做熔断）。
func (b *Breakers) ExecuteByName(ctx context.Context, name string, fn func() error) error {
	b.mu.RLock()
	br, ok := b.m[name]
	b.mu.RUnlock()
	if !ok {
		return fn()
	}
	return br.Execute(ctx, fn)
}

// States 返回全部熔断器当前状态（供 /metrics 输出）。
func (b *Breakers) States() map[string]gobreaker.State {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make(map[string]gobreaker.State, len(b.m))
	for name, br := range b.m {
		out[name] = br.cb.State()
	}
	return out
}
