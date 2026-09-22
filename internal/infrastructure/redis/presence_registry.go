package redis

import (
	"context"
	"encoding/json"
	"errors"
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
	all, err := p.OnlineConnectionsBatch(ctx, []int64{userID})
	if err != nil {
		return nil, err
	}
	return all[userID], nil
}

// OnlineConnectionsBatch 批量返回多个用户当前仍可能存活的连接路由。
//
// Outbox Publisher 每次处理的是一条会话消息，成员数可能达到上千；如果
// 对每个成员单独执行 ZRANGE/GET，会把 Redis RTT 放大到成员数级别。这里
// 把过期清理、ZSET 读取和连接路由 GET 分成两个 pipeline，保持查询次数
// 与批次数量相关，而不是与成员数相关。
func (p *PresenceRegistry) OnlineConnectionsBatch(ctx context.Context, userIDs []int64) (map[int64][]connection.ConnectionRoute, error) {
	ctx, cancel := withTimeout(ctx, p.opts.ReadTimeout)
	defer cancel()

	uniqueUserIDs := make([]int64, 0, len(userIDs))
	seenUsers := make(map[int64]struct{}, len(userIDs))
	for _, userID := range userIDs {
		if userID <= 0 {
			continue
		}
		if _, ok := seenUsers[userID]; ok {
			continue
		}
		seenUsers[userID] = struct{}{}
		uniqueUserIDs = append(uniqueUserIDs, userID)
	}
	routesByUser := make(map[int64][]connection.ConnectionRoute, len(uniqueUserIDs))
	if len(uniqueUserIDs) == 0 {
		return routesByUser, nil
	}

	now := time.Now().UnixMilli()

	// 1. 对所有用户清理并读取已过期 connection_id。
	pipe := p.client.Pipeline()
	removeCmds := make([]*goredis.IntCmd, 0, len(uniqueUserIDs))
	rangeCmds := make([]*goredis.StringSliceCmd, 0, len(uniqueUserIDs))
	for _, userID := range uniqueUserIDs {
		userKey := PresenceUserKey(userID)
		removeCmds = append(removeCmds, pipe.ZRemRangeByScore(ctx, userKey, "-inf", IDString(now)))
		rangeCmds = append(rangeCmds, pipe.ZRange(ctx, userKey, 0, -1))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, errs.Wrap(errs.RedisUnavailable, "在线状态批量查询失败", err)
	}

	staleCount := int64(0)
	connectionIDsByUser := make(map[int64][]string)
	for i, userID := range uniqueUserIDs {
		removed, err := removeCmds[i].Result()
		if err != nil {
			return nil, errs.Wrap(errs.RedisUnavailable, "在线状态批量查询失败", err)
		}
		staleCount += removed
		ids, err := rangeCmds[i].Result()
		if err != nil {
			return nil, errs.Wrap(errs.RedisUnavailable, "在线状态批量查询失败", err)
		}
		if len(ids) > 0 {
			connectionIDsByUser[userID] = ids
		}
	}
	if staleCount > 0 && p.OnStaleCleanup != nil {
		p.OnStaleCleanup(int(staleCount))
	}

	// 2. 去重后一次性读取所有连接路由。
	type routeLookup struct {
		userID       int64
		connectionID string
		cmd          *goredis.StringCmd
	}
	lookups := make([]routeLookup, 0)
	seenConnections := make(map[string]struct{})
	pipe = p.client.Pipeline()
	for userID, ids := range connectionIDsByUser {
		for _, connectionID := range ids {
			if _, ok := seenConnections[connectionID]; ok {
				continue
			}
			seenConnections[connectionID] = struct{}{}
			lookups = append(lookups, routeLookup{
				userID:       userID,
				connectionID: connectionID,
				cmd:          pipe.Get(ctx, PresenceConnKey(connectionID)),
			})
		}
	}
	if len(lookups) == 0 {
		return routesByUser, nil
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		return nil, errs.Wrap(errs.RedisUnavailable, "在线状态批量查询失败", err)
	}

	// 3. 路由 HASH 可能在 ZSET 之后过期；缺失或损坏的成员只清理并忽略，
	// 不影响同一批次中其他用户的在线投递。
	cleanup := p.client.Pipeline()
	cleanupCount := 0
	for _, lookup := range lookups {
		raw, err := lookup.cmd.Result()
		if err != nil {
			if errors.Is(err, goredis.Nil) {
				cleanup.ZRem(ctx, PresenceUserKey(lookup.userID), lookup.connectionID)
				cleanupCount++
				continue
			}
			return nil, errs.Wrap(errs.RedisUnavailable, "在线状态批量查询失败", err)
		}
		var route connection.ConnectionRoute
		if err := json.Unmarshal([]byte(raw), &route); err != nil || route.UserID != lookup.userID || route.ConnectionID != lookup.connectionID {
			cleanup.ZRem(ctx, PresenceUserKey(lookup.userID), lookup.connectionID)
			cleanupCount++
			continue
		}
		routesByUser[lookup.userID] = append(routesByUser[lookup.userID], route)
	}
	if cleanupCount > 0 {
		if _, err := cleanup.Exec(ctx); err == nil && p.OnStaleCleanup != nil {
			p.OnStaleCleanup(cleanupCount)
		}
	}
	return routesByUser, nil
}
