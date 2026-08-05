package redis

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

// TicketData 是 WebSocket 票据载荷（GOCHAT_REDIS.md §6.1）。
type TicketData struct {
	UserID   int64     `json:"user_id"`
	DeviceID string    `json:"device_id"`
	IssuedAt time.Time `json:"issued_at"`
}

// WSTicketStore 管理一次性 WebSocket 票据：创建（SET NX EX）与消费（GETDEL）。
type WSTicketStore struct {
	client  *goredis.Client
	scripts *Scripts
	ttl     time.Duration
	opts    Options
}

func NewWSTicketStore(client *goredis.Client, scripts *Scripts, ttl time.Duration, opts Options) *WSTicketStore {
	return &WSTicketStore{client: client, scripts: scripts, ttl: ttl, opts: opts}
}

// NewToken 生成至少 256 位密码学随机 Token（GOCHAT_REDIS.md §6.1）。
func NewToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", errs.Internal(err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// Create 创建一次性票据并返回 Token。
func (s *WSTicketStore) Create(ctx context.Context, userID int64, deviceID string) (string, error) {
	token, err := NewToken()
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(TicketData{
		UserID:   userID,
		DeviceID: deviceID,
		IssuedAt: time.Now().UTC(),
	})
	if err != nil {
		return "", errs.Internal(err)
	}
	ctx, cancel := withTimeout(ctx, s.opts.WriteTimeout)
	defer cancel()
	ok, err := s.client.SetNX(ctx, WSTicketKey(token), data, s.ttl).Result()
	if err != nil {
		return "", errs.Wrap(errs.RedisUnavailable, "票据创建失败", err)
	}
	if !ok {
		// 碰撞概率可忽略；重试一次即可
		return "", errs.Wrap(errs.InternalError, "票据创建冲突", nil)
	}
	return token, nil
}

// Consume 一次性消费票据；不存在或已消费返回 nil（重放被拒绝，GOCHAT_REDIS.md §6.2）。
func (s *WSTicketStore) Consume(ctx context.Context, token string) (*TicketData, error) {
	if token == "" {
		return nil, nil
	}
	ctx, cancel := withTimeout(ctx, s.opts.ReadTimeout)
	defer cancel()
	val, err := s.scripts.ConsumeWSTicket.Run(ctx, s.client,
		[]string{WSTicketKey(token)}).Result()
	if err != nil {
		// 脚本返回 nil（票据不存在/已消费）在客户端表现为 redis.Nil，不是故障
		if errors.Is(err, goredis.Nil) {
			return nil, nil
		}
		return nil, errs.Wrap(errs.RedisUnavailable, "票据校验失败", err)
	}
	if val == nil {
		return nil, nil
	}
	raw, ok := val.(string)
	if !ok {
		return nil, nil
	}
	var data TicketData
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		return nil, errs.Wrap(errs.InvalidTicket, "票据数据损坏", err)
	}
	return &data, nil
}
