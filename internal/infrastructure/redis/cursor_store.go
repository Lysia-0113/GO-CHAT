package redis

import (
	"context"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
)

// CursorStore 是会话同步游标存储：im:cursor:{conversation_id} HASH，
// field=user_id，value=该用户已同步到的最新 seq（GOCHAT_REDIS.md §10）。
//
// 两层游标设计：
//   - 客户端本地游标是真相：查询基准、写入本地成功才推进；
//   - 本组件只是服务端记录，供多设备同步/缺口诊断，写入失败不影响查询结果。
//
// 不设 TTL：游标是数据而非易失状态，过期丢失会迫使客户端全量重拉；
// 持久性由 Redis 持久化策略（AOF/RDB）承担，即使丢失也可由客户端游标重建。
type CursorStore struct {
	client  *goredis.Client
	scripts *Scripts
	opts    Options
}

func NewCursorStore(client *goredis.Client, scripts *Scripts, opts Options) *CursorStore {
	return &CursorStore{client: client, scripts: scripts, opts: opts}
}

// Get 读取用户在某会话的同步游标；不存在时返回 (0, false, nil)。
func (s *CursorStore) Get(ctx context.Context, conversationID, userID int64) (int64, bool, error) {
	start := time.Now()
	defer func() {
		metrics.DependencyDuration.WithLabelValues("redis_cursor_read").Observe(time.Since(start).Seconds())
	}()
	ctx, cancel := withTimeout(ctx, s.opts.ReadTimeout)
	defer cancel()

	raw, err := s.client.HGet(ctx, CursorKey(conversationID), IDString(userID)).Result()
	if err == goredis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, errs.Wrap(errs.RedisUnavailable, "同步游标读取失败", err)
	}
	seq, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false, errs.Wrap(errs.RedisUnavailable, "同步游标数据损坏", err)
	}
	return seq, true, nil
}

// Advance 推进游标（Lua 保证只增不减），返回推进后的值。
func (s *CursorStore) Advance(ctx context.Context, conversationID, userID, seq int64) (int64, error) {
	start := time.Now()
	defer func() {
		metrics.DependencyDuration.WithLabelValues("redis_cursor_write").Observe(time.Since(start).Seconds())
	}()
	ctx, cancel := withTimeout(ctx, s.opts.WriteTimeout)
	defer cancel()

	res, err := s.scripts.AdvanceCursor.Run(ctx, s.client,
		[]string{CursorKey(conversationID)},
		IDString(userID), IDString(seq),
	).Int64()
	if err != nil {
		return 0, errs.Wrap(errs.RedisUnavailable, "同步游标推进失败", err)
	}
	return res, nil
}
