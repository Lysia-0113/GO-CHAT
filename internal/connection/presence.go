package connection

import "context"

// ConnectionRoute 是 Redis Presence 中的连接路由（GOCHAT_REDIS.md §5.2）。
type ConnectionRoute struct {
	ConnectionID string `json:"connection_id"`
	UserID       int64  `json:"user_id"`
	DeviceID     string `json:"device_id"`
	NodeID       string `json:"node_id"`
}

// PresenceRegistry 是在线状态注册表接口（GOCHAT_API.md §12.7）。
// 实现见 internal/infrastructure/redis/presence_registry.go。
type PresenceRegistry interface {
	// Register 记录连接路由（HSET + ZADD，带 TTL）。
	Register(ctx context.Context, route ConnectionRoute) error
	// Heartbeat 刷新连接与用户在线状态。
	Heartbeat(ctx context.Context, route ConnectionRoute) error
	// Remove 删除连接路由。
	Remove(ctx context.Context, connectionID string, userID int64) error
	// OnlineConnections 返回用户当前仍可能存活的连接路由。
	OnlineConnections(ctx context.Context, userID int64) ([]ConnectionRoute, error)
}
