package redis

import (
	"context"
	"strconv"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

// BucketConfig 是令牌桶参数。
type BucketConfig struct {
	Capacity  float64 // 容量
	PerSecond float64 // 补充速率
	Key       func() string
	TTL       time.Duration
}

// RateLimiter 实现 L2 分布式令牌桶（Redis Lua）+ L1 本地兜底
// （GOCHAT_RESILIENCE.md §5.3）。
type RateLimiter struct {
	client  *goredis.Client
	scripts *Scripts
	opts    Options
	// local 是 L1 本地限流器（Redis 故障时启用，更严格）
	local *localRateStore
	// metrics 回调
	OnReject   func(key string)
	OnFallback func(key string)
}

func NewRateLimiter(client *goredis.Client, scripts *Scripts, opts Options) *RateLimiter {
	return &RateLimiter{
		client:  client,
		scripts: scripts,
		opts:    opts,
		local:   newLocalRateStore(),
	}
}

// Allow 对 key 执行令牌桶检查（L2；失败时降级 L1 本地限流）。
// 返回 (允许, 建议重试毫秒数, 错误)。
func (r *RateLimiter) Allow(ctx context.Context, key string, capacity, perSecond float64, ttl time.Duration) (bool, int64, error) {
	allowed, retryAfter, err := r.allowL2(ctx, key, capacity, perSecond, ttl)
	if err == nil {
		return allowed, retryAfter, nil
	}
	// Redis 故障：切 L1 本地更严格限流，不能无限放行（GOCHAT_RESILIENCE.md §5.3）
	if r.OnFallback != nil {
		r.OnFallback(key)
	}
	return r.allowL1(key, capacity, perSecond)
}

// allowL2 执行 Redis Lua 令牌桶。
func (r *RateLimiter) allowL2(ctx context.Context, key string, capacity, perSecond float64, ttl time.Duration) (bool, int64, error) {
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	ctx, cancel := withTimeout(ctx, r.opts.WriteTimeout)
	defer cancel()
	res, err := r.scripts.TokenBucket.Run(ctx, r.client, []string{key},
		f(capacity), f(perSecond), "1", IDString(int64(ttl.Seconds())),
	).Int()
	if err != nil {
		return false, 0, errs.Wrap(errs.RedisUnavailable, "限流器不可用", err)
	}
	if res == 1 {
		return true, 0, nil
	}
	// 拒绝：估算补充 1 个 token 所需毫秒数
	waitMS := int64(0)
	if perSecond > 0 {
		waitMS = int64(1000.0 / perSecond)
	}
	return false, waitMS, nil
}

// allowL1 本地令牌桶兜底（Redis 故障时）。
func (r *RateLimiter) allowL1(key string, capacity, perSecond float64) (bool, int64, error) {
	return r.local.Allow(key, capacity, perSecond)
}

// SetObservers 注入拒绝/降级回调（供指标统计）。
func (r *RateLimiter) SetObservers(onReject, onFallback func(key string)) {
	r.OnReject = onReject
	r.OnFallback = onFallback
}

// localRateStore 是带清理的本地限流器表。
type localRateStore struct {
	mu    sync.Mutex
	items map[string]*localBucket
}

type localBucket struct {
	limiter *rate.Limiter
	last    time.Time
}

func newLocalRateStore() *localRateStore {
	return &localRateStore{items: make(map[string]*localBucket)}
}

// Allow 使用 x/time/rate 令牌桶；桶超过 maxIdle 未访问会被清理。
func (s *localRateStore) Allow(key string, capacity, perSecond float64) (bool, int64, error) {
	const maxIdle = 10 * time.Minute
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.items) > 10000 {
		now := time.Now()
		for k, b := range s.items {
			if now.Sub(b.last) > maxIdle {
				delete(s.items, k)
			}
		}
	}
	b, ok := s.items[key]
	if !ok {
		b = &localBucket{
			limiter: rate.NewLimiter(rate.Limit(perSecond), int(capacity)),
			last:    time.Now(),
		}
		s.items[key] = b
	}
	b.last = time.Now()
	if b.limiter.Allow() {
		return true, 0, nil
	}
	waitMS := int64(0)
	if perSecond > 0 {
		waitMS = int64(1000.0 / perSecond)
	}
	return false, waitMS, nil
}

// f 格式化浮点数为 Lua 参数。
func f(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// ---- 各场景类型化方法（GOCHAT_RESILIENCE.md §5.2） ----

// AllowSendUser 单用户发送配额。
func (r *RateLimiter) AllowSendUser(ctx context.Context, userID int64, capacity, perSecond float64) (bool, int64, error) {
	key := RateSendKey(userID)
	ok, wait, err := r.Allow(ctx, key, capacity, perSecond, 2*time.Minute)
	return ok, wait, err
}

// AllowSendConversation 单会话发送配额（大群 fanout 保护）。
func (r *RateLimiter) AllowSendConversation(ctx context.Context, conversationID int64, capacity, perSecond float64) (bool, int64, error) {
	key := RateSendConvKey(conversationID)
	ok, wait, err := r.Allow(ctx, key, capacity, perSecond, 2*time.Minute)
	return ok, wait, err
}

// AllowHistory 历史查询配额（user_id + conversation_id）。
func (r *RateLimiter) AllowHistory(ctx context.Context, userID, conversationID int64, capacity, perSecond float64) (bool, int64, error) {
	key := RateHistoryKey(userID, conversationID)
	ok, wait, err := r.Allow(ctx, key, capacity, perSecond, 2*time.Minute)
	return ok, wait, err
}

// AllowLogin 登录配额（IP + 规范化用户名）。
func (r *RateLimiter) AllowLogin(ctx context.Context, ip, username string, perMinute int) (bool, int64, error) {
	key := RateLoginKey(ip, username)
	return r.Allow(ctx, key, float64(perMinute), float64(perMinute)/60.0, time.Minute)
}

// AllowWSConnectIP 建连配额（IP）。
func (r *RateLimiter) AllowWSConnectIP(ctx context.Context, ip string, perMinute int) (bool, int64, error) {
	key := RateWSConnectIPKey(ip)
	return r.Allow(ctx, key, float64(perMinute), float64(perMinute)/60.0, time.Minute)
}

// AllowWSConnectUser 建连配额（用户）。
func (r *RateLimiter) AllowWSConnectUser(ctx context.Context, userID int64, perMinute int) (bool, int64, error) {
	key := RateWSConnectUserKey(userID)
	return r.Allow(ctx, key, float64(perMinute), float64(perMinute)/60.0, time.Minute)
}

// AllowConnInbound 单连接入站事件配额（connection_id 维度由调用方传 key）。
func (r *RateLimiter) AllowConnInbound(ctx context.Context, key string, capacity, perSecond float64) (bool, int64, error) {
	return r.Allow(ctx, key, capacity, perSecond, time.Minute)
}
