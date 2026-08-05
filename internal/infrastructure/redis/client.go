package redis

import (
	"context"
	"embed"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

//go:embed lua/*.lua
var luaFS embed.FS

// Scripts 集中加载 Lua 脚本（GOCHAT_REDIS.md §2.1）。
type Scripts struct {
	AppendRecentMessage *goredis.Script
	ConsumeWSTicket     *goredis.Script
	AcquireMessageIdem  *goredis.Script
	ReleaseMessageIdem  *goredis.Script
	TokenBucket         *goredis.Script
}

// LoadScripts 从嵌入文件加载全部脚本；加载失败返回错误。
func LoadScripts() (*Scripts, error) {
	load := func(name string) (*goredis.Script, error) {
		b, err := luaFS.ReadFile("lua/" + name)
		if err != nil {
			return nil, err
		}
		return goredis.NewScript(string(b)), nil
	}
	s := &Scripts{}
	var err error
	if s.AppendRecentMessage, err = load("append_recent_message.lua"); err != nil {
		return nil, err
	}
	if s.ConsumeWSTicket, err = load("consume_ws_ticket.lua"); err != nil {
		return nil, err
	}
	if s.AcquireMessageIdem, err = load("acquire_message_idem.lua"); err != nil {
		return nil, err
	}
	if s.ReleaseMessageIdem, err = load("release_message_idem.lua"); err != nil {
		return nil, err
	}
	if s.TokenBucket, err = load("token_bucket.lua"); err != nil {
		return nil, err
	}
	return s, nil
}

// Options 是 Redis 组件共享配置。
type Options struct {
	// 命令超时（GOCHAT_RESILIENCE.md §4.2：Redis 读 50ms / 写 80ms）
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// DefaultOptions 返回推荐初值。
func DefaultOptions() Options {
	return Options{
		ReadTimeout:  50 * time.Millisecond,
		WriteTimeout: 80 * time.Millisecond,
	}
}

// withTimeout 为一次命令设置整体超时。
func withTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, d)
}
