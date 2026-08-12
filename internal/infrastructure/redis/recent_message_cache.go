package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
)

// maxExactSeq 是 ZSET double score 可精确表示的上界（2^53，GOCHAT_REDIS.md §4.2）。
const maxExactSeq = int64(1) << 53

// snapshot 是缓存中的消息快照（GOCHAT_REDIS.md §4.2 示例格式）。
type snapshot struct {
	ID       string          `json:"id"`
	Seq      string          `json:"seq"`
	SenderID string          `json:"sender_id"`
	Type     int8            `json:"type"`
	Content  json.RawMessage `json:"content"`
	Status   int8            `json:"status,omitempty"`
}

// RecentMessageCache 实现 message.RecentMessageCache：
// ZSET 索引（score=seq）+ HASH 快照，Lua 原子写入，配对 TTL。
type RecentMessageCache struct {
	client  *goredis.Client
	scripts *Scripts
	ttl     time.Duration
	max     int64
	opts    Options
}

func NewRecentMessageCache(client *goredis.Client, scripts *Scripts, ttl time.Duration, max int, opts Options) *RecentMessageCache {
	if max <= 0 {
		max = 200
	}
	return &RecentMessageCache{
		client:  client,
		scripts: scripts,
		ttl:     ttl,
		max:     int64(max),
		opts:    opts,
	}
}

// Append 追加一条已持久化消息（MySQL 提交后才允许调用，GOCHAT_REDIS.md §4.3）。
func (c *RecentMessageCache) Append(ctx context.Context, m *message.Message) error {
	if m.Seq <= 0 || m.Seq > maxExactSeq {
		return nil // 超出精确范围，放弃缓存（MySQL 是最终真相）
	}
	data, err := json.Marshal(snapshot{
		ID:       IDString(m.MessageID),
		Seq:      IDString(m.Seq),
		SenderID: IDString(m.SenderID),
		Type:     m.MessageType,
		Content:  m.Content,
		Status:   m.Status,
	})
	if err != nil {
		return errs.Internal(err)
	}
	ttlSec := int64(c.ttl.Seconds())
	if ttlSec <= 0 {
		ttlSec = 24 * 3600
	}
	ctx, cancel := withTimeout(ctx, c.opts.WriteTimeout)
	defer cancel()
	err = c.scripts.AppendRecentMessage.Run(ctx, c.client,
		[]string{RecentIdxKey(m.ConversationID), RecentDataKey(m.ConversationID)},
		IDString(m.Seq), IDString(m.MessageID), string(data), IDString(c.max), IDString(ttlSec),
	).Err()
	if err != nil {
		return errs.Wrap(errs.RedisUnavailable, "最近消息缓存写入失败", err)
	}
	return nil
}

// ListBefore 向前翻页读取缓存；complete=false 时调用方回源 MySQL
// （GOCHAT_REDIS.md §4.4：缺失、窗口不足、序号不连续均回退）。
func (c *RecentMessageCache) ListBefore(ctx context.Context, conversationID, visibleAfterSeq, beforeSeq int64, limit int) ([]message.Message, bool, error) {
	start := time.Now()
	defer func() {
		metrics.DependencyDuration.WithLabelValues("redis_recent_read").Observe(time.Since(start).Seconds())
	}()
	ctx, cancel := withTimeout(ctx, c.opts.ReadTimeout)
	defer cancel()

	idxKey := RecentIdxKey(conversationID)
	max := "-inf"
	if beforeSeq > 0 {
		max = IDString(beforeSeq - 1)
	}
	min := IDString(visibleAfterSeq + 1)

	// 多取一条用于判断"缓存窗口是否覆盖完整请求范围"
	ids, err := c.client.ZRevRangeByScore(ctx, idxKey, &goredis.ZRangeBy{
		Min: min, Max: max, Offset: 0, Count: int64(limit) + 1,
	}).Result()
	if err != nil {
		return nil, false, errs.Wrap(errs.RedisUnavailable, "最近消息缓存读取失败", err)
	}
	if len(ids) == 0 {
		// 窗口内无消息：需要确认请求范围内确实没有更早消息（窗口边界检查）
		return nil, false, nil
	}
	if len(ids) > limit {
		// 请求范围超出缓存窗口（更早的消息已被裁剪），回退 MySQL
		return nil, false, nil
	}
	return c.hmgetMessages(ctx, conversationID, ids)
}

// ListAfter 离线补偿读取（ascending）。
func (c *RecentMessageCache) ListAfter(ctx context.Context, conversationID, visibleAfterSeq, afterSeq int64, limit int) ([]message.Message, bool, error) {
	start := time.Now()
	defer func() {
		metrics.DependencyDuration.WithLabelValues("redis_recent_read").Observe(time.Since(start).Seconds())
	}()
	ctx, cancel := withTimeout(ctx, c.opts.ReadTimeout)
	defer cancel()

	idxKey := RecentIdxKey(conversationID)
	min := IDString(visibleAfterSeq + 1)
	max := "+inf"

	ids, err := c.client.ZRangeByScore(ctx, idxKey, &goredis.ZRangeBy{
		Min: min, Max: max, Offset: 0, Count: int64(limit) + 1,
	}).Result()
	if err != nil {
		return nil, false, errs.Wrap(errs.RedisUnavailable, "最近消息缓存读取失败", err)
	}
	if len(ids) > limit {
		// 缓存未覆盖完整补偿范围
		return nil, false, nil
	}
	if len(ids) == 0 {
		return nil, true, nil
	}
	return c.hmgetMessages(ctx, conversationID, ids)
}

// hmgetMessages 批量读取快照并转换为领域消息。
// 任一 field 缺失或解析失败 → complete=false。
func (c *RecentMessageCache) hmgetMessages(ctx context.Context, conversationID int64, ids []string) ([]message.Message, bool, error) {
	dataKey := RecentDataKey(conversationID)
	values, err := c.client.HMGet(ctx, dataKey, ids...).Result()
	if err != nil {
		return nil, false, errs.Wrap(errs.RedisUnavailable, "最近消息缓存读取失败", err)
	}
	items := make([]message.Message, 0, len(ids))
	for _, v := range values {
		if v == nil {
			return nil, false, nil // 快照缺失
		}
		raw, ok := v.(string)
		if !ok {
			return nil, false, nil
		}
		var snap snapshot
		if err := json.Unmarshal([]byte(raw), &snap); err != nil {
			return nil, false, nil
		}
		var msgID, seq, senderID int64
		if _, err := fmt.Sscanf(snap.ID, "%d", &msgID); err != nil {
			return nil, false, nil
		}
		if _, err := fmt.Sscanf(snap.Seq, "%d", &seq); err != nil {
			return nil, false, nil
		}
		if _, err := fmt.Sscanf(snap.SenderID, "%d", &senderID); err != nil {
			return nil, false, nil
		}
		items = append(items, message.Message{
			MessageID:       msgID,
			ClientMessageID: "",
			ConversationID:  conversationID,
			Seq:             seq,
			SenderID:        senderID,
			MessageType:     snap.Type,
			Content:         snap.Content,
			Status:          snap.Status,
		})
	}
	return items, true, nil
}

// Delete 清空某会话缓存。
func (c *RecentMessageCache) Delete(ctx context.Context, conversationID int64) error {
	ctx, cancel := withTimeout(ctx, c.opts.WriteTimeout)
	defer cancel()
	err := c.client.Del(ctx, RecentIdxKey(conversationID), RecentDataKey(conversationID)).Err()
	if err != nil {
		return errs.Wrap(errs.RedisUnavailable, "最近消息缓存删除失败", err)
	}
	return nil
}
