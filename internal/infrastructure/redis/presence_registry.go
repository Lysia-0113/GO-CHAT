package redis

import (
	"context"
	"encoding/json"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/Lysia-0113/GO-CHAT/internal/connection"
	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

// PresenceRegistry 实现 connection.PresenceRegistry：
// 连接路由 HASH（90s TTL）+ 用户在线 ZSET（3 分钟 TTL）（GOCHAT_REDIS.md §5）。
type PresenceRegistry struct {
	client *goredis.Client
	// connHashTTL 连接路由 HASH TTL
	connHashTTL time.Duration
	// userSetTTL 用户 ZSET TTL
	userSetTTL time.Duration
	opts       Options
	// OnStaleCleanup 清理过期路由时的回调（指标）
	OnStaleCleanup func(n int)
}

func NewPresenceRegistry(client *goredis.Client, connHashTTL, userSetTTL time.Duration, opts Options) *PresenceRegistry {
	return &PresenceRegistry{
		client:      client,
		connHashTTL: connHashTTL,
		userSetTTL:  userSetTTL,
		opts:        opts,
	}
}

// Register 写入连接路由并加入用户在线 ZSET（GOCHAT_REDIS.md §5.3 连接建立）。
func (p *PresenceRegistry) Register(ctx context.Context, route connection.ConnectionRoute) error {
	ctx, cancel := withTimeout(ctx, p.opts.WriteTimeout)
	defer cancel()

	connKey := PresenceConnKey(route.ConnectionID)
	expireAt := time.Now().Add(p.connHashTTL).UnixMilli()
	data, err := json.Marshal(route)
	if err != nil {
		return errs.Internal(err)
	}
	pipe := p.client.Pipeline()
	pipe.Set(ctx, connKey, data, p.connHashTTL)
	pipe.ZAdd(ctx, PresenceUserKey(route.UserID), goredis.Z{
		Score:  float64(expireAt),
		Member: route.ConnectionID,
	})
	pipe.Expire(ctx, PresenceUserKey(route.UserID), p.userSetTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return errs.Wrap(errs.RedisUnavailable, "在线状态注册失败", err)
	}
	return nil
}

// Heartbeat 刷新连接与用户在线状态（GOCHAT_REDIS.md §5.3 心跳）。
func (p *PresenceRegistry) Heartbeat(ctx context.Context, route connection.ConnectionRoute) error {
	ctx, cancel := withTimeout(ctx, p.opts.WriteTimeout)
	defer cancel()

	connKey := PresenceConnKey(route.ConnectionID)
	expireAt := time.Now().Add(p.connHashTTL).UnixMilli()
	pipe := p.client.Pipeline()
	pipe.Expire(ctx, connKey, p.connHashTTL)
	pipe.ZAdd(ctx, PresenceUserKey(route.UserID), goredis.Z{
		Score:  float64(expireAt),
		Member: route.ConnectionID,
	})
	pipe.Expire(ctx, PresenceUserKey(route.UserID), p.userSetTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return errs.Wrap(errs.RedisUnavailable, "心跳续期失败", err)
	}
	return nil
}

// Remove 删除连接路由（正常断开，GOCHAT_REDIS.md §5.3）。
func (p *PresenceRegistry) Remove(ctx context.Context, connectionID string, userID int64) error {
	ctx, cancel := withTimeout(ctx, p.opts.WriteTimeout)
	defer cancel()

	pipe := p.client.Pipeline()
	pipe.Del(ctx, PresenceConnKey(connectionID))
	pipe.ZRem(ctx, PresenceUserKey(userID), connectionID)
	if _, err := pipe.Exec(ctx); err != nil {
		return errs.Wrap(errs.RedisUnavailable, "在线状态清理失败", err)
	}
	return nil
}

// OnlineConnections 返回用户当前仍可能存活的连接路由
// （GOCHAT_REDIS.md §5.3 查询：先清理过期成员再读取）。
func (p *PresenceRegistry) OnlineConnections(ctx context.Context, userID int64) ([]connection.ConnectionRoute, error) {
	ctx, cancel := withTimeout(ctx, p.opts.ReadTimeout)
	defer cancel()

	userKey := PresenceUserKey(userID)
	now := time.Now().UnixMilli()

	// 1. 清理已过期 connection_id
	removed, err := p.client.ZRemRangeByScore(ctx, userKey, "-inf", IDString(now)).Result()
	if err != nil {
		return nil, errs.Wrap(errs.RedisUnavailable, "在线状态查询失败", err)
	}
	if removed > 0 && p.OnStaleCleanup != nil {
		p.OnStaleCleanup(int(removed))
	}

	// 2. 获取剩余连接
	ids, err := p.client.ZRange(ctx, userKey, 0, -1).Result()
	if err != nil {
		return nil, errs.Wrap(errs.RedisUnavailable, "在线状态查询失败", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}

	// 3. Pipeline 读取每个连接路由
	routes := make([]connection.ConnectionRoute, 0, len(ids))
	pipe := p.client.Pipeline()
	cmds := make([]*goredis.StringCmd, 0, len(ids))
	for _, id := range ids {
		cmds = append(cmds, pipe.Get(ctx, PresenceConnKey(id)))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, errs.Wrap(errs.RedisUnavailable, "在线状态查询失败", err)
	}
	for i, id := range ids {
		raw, err := cmds[i].Result()
		if err != nil {
			// 路由 HASH 缺失（TTL 已过期）：从 ZSET 清理
			_ = p.client.ZRem(ctx, userKey, id).Err()
			continue
		}
		var route connection.ConnectionRoute
		if err := json.Unmarshal([]byte(raw), &route); err != nil {
			continue
		}
		routes = append(routes, route)
	}
	return routes, nil
}
