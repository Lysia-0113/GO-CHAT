package redis

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
)

// IdempotencyStore 实现 message.FastIdempotency：
// 网关快速拦截（Redis SET NX），MySQL 唯一索引仍是最终兜底（GOCHAT_REDIS.md §7）。
type IdempotencyStore struct {
	client  *goredis.Client
	scripts *Scripts
	// processingTTL 处理中状态 TTL（默认 5s）
	processingTTL int64 // 毫秒
	// acceptedTTL 已接受状态 TTL（默认 10 分钟）
	acceptedTTL int64 // 毫秒
	opts        Options
}

func NewIdempotencyStore(client *goredis.Client, scripts *Scripts, processingTTL, acceptedTTL time.Duration, opts Options) *IdempotencyStore {
	return &IdempotencyStore{
		client:        client,
		scripts:       scripts,
		processingTTL: int64(processingTTL / time.Millisecond),
		acceptedTTL:   int64(acceptedTTL / time.Millisecond),
		opts:          opts,
	}
}

const idemAcceptedValue = "accepted"

func nonceValue(nonce string) string { return "processing:" + nonce }

// Acquire 尝试获得发送权（GOCHAT_REDIS.md §7.2 流程 1-4）。
func (s *IdempotencyStore) Acquire(ctx context.Context, senderID int64, clientMessageID string) (message.IdemResult, string, error) {
	nonce := newNonce()
	ctx, cancel := withTimeout(ctx, s.opts.WriteTimeout)
	defer cancel()
	res, err := s.scripts.AcquireMessageIdem.Run(ctx, s.client,
		[]string{IdemKey(senderID, clientMessageID)},
		nonceValue(nonce), IDString(s.processingTTL), idemAcceptedValue,
	).Int()
	if err != nil {
		return message.IdemTaken, "", errs.Wrap(errs.RedisUnavailable, "幂等检查失败", err)
	}
	switch res {
	case 1:
		return message.IdemTaken, nonce, nil
	case 2:
		return message.IdemAlreadyAccepted, "", nil
	default:
		return message.IdemProcessing, "", nil
	}
}

// MarkAccepted 仅当值仍等于自己的 nonce 时标记 accepted（流程 5）。
func (s *IdempotencyStore) MarkAccepted(ctx context.Context, senderID int64, clientMessageID, nonce string) error {
	ctx, cancel := withTimeout(ctx, s.opts.WriteTimeout)
	defer cancel()
	err := s.scripts.ReleaseMessageIdem.Run(ctx, s.client,
		[]string{IdemKey(senderID, clientMessageID)},
		nonceValue(nonce), idemAcceptedValue, IDString(s.acceptedTTL),
	).Err()
	if err != nil {
		return errs.Wrap(errs.RedisUnavailable, "幂等状态更新失败", err)
	}
	return nil
}

// Release 仅当值等于自己的 nonce 时删除（流程 6：Kafka 失败路径）。
func (s *IdempotencyStore) Release(ctx context.Context, senderID int64, clientMessageID, nonce string) error {
	ctx, cancel := withTimeout(ctx, s.opts.WriteTimeout)
	defer cancel()
	err := s.scripts.ReleaseMessageIdem.Run(ctx, s.client,
		[]string{IdemKey(senderID, clientMessageID)},
		nonceValue(nonce), "", "0",
	).Err()
	if err != nil {
		return errs.Wrap(errs.RedisUnavailable, "幂等状态释放失败", err)
	}
	return nil
}

var nonceSeq atomic.Uint64

// newNonce 生成请求内 nonce（进程内唯一即可）。
func newNonce() string {
	n := nonceSeq.Add(1)
	return fmt.Sprintf("n%d", n)
}
