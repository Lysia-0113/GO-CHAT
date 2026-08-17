package connection

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
)

// Connection 是网关持有的单个 WebSocket 连接抽象。
type Connection interface {
	// ID 返回连接唯一标识。
	ID() string
	// UserID 返回连接归属用户。
	UserID() int64
	// DeviceID 返回客户端设备标识。
	DeviceID() string
	// Push 将事件写入有界发送队列（非阻塞语义：队列满返回错误）。
	Push(ctx context.Context, event Event) error
	// Close 关闭连接（幂等）。
	Close(reason string)
	// Closed 返回连接是否已关闭。
	Closed() bool
}

// Manager 是进程内 ConnectionManager（V1 单节点实现，GOCHAT_API.md §13.1）。
type Manager struct {
	nodeID   string
	presence PresenceRegistry

	mu     sync.RWMutex
	conns  map[string]Connection
	byUser map[int64]map[string]struct{}

	active atomic.Int64
}

// NewManager 创建连接管理器。
func NewManager(nodeID string, presence PresenceRegistry) *Manager {
	return &Manager{
		nodeID:   nodeID,
		presence: presence,
		conns:    make(map[string]Connection),
		byUser:   make(map[int64]map[string]struct{}),
	}
}

// Register 注册新连接并写入 Presence。
func (m *Manager) Register(ctx context.Context, conn Connection) error {
	m.mu.Lock()
	m.conns[conn.ID()] = conn
	if _, ok := m.byUser[conn.UserID()]; !ok {
		m.byUser[conn.UserID()] = make(map[string]struct{})
	}
	m.byUser[conn.UserID()][conn.ID()] = struct{}{}
	m.mu.Unlock()
	m.active.Add(1)

	if m.presence != nil {
		// 连接已在本机 map 注册，Presence 仅是为未来的在线状态功能维护的副本：
		// 广播模型下投递链路不查询 Presence，写入失败不影响连接可用性与在线投递
		_ = m.presence.Register(ctx, ConnectionRoute{
			ConnectionID: conn.ID(),
			UserID:       conn.UserID(),
			DeviceID:     conn.DeviceID(),
			NodeID:       m.nodeID,
		})
	}
	return nil
}

// Unregister 移除连接并清理 Presence。
func (m *Manager) Unregister(ctx context.Context, conn Connection) {
	m.mu.Lock()
	if _, ok := m.conns[conn.ID()]; ok {
		delete(m.conns, conn.ID())
		if set, ok := m.byUser[conn.UserID()]; ok {
			delete(set, conn.ID())
			if len(set) == 0 {
				delete(m.byUser, conn.UserID())
			}
		}
		m.active.Add(-1)
	}
	m.mu.Unlock()

	if m.presence != nil {
		_ = m.presence.Remove(ctx, conn.ID(), conn.UserID())
	}
}

// PushToConnection 向指定连接推送事件。
func (m *Manager) PushToConnection(ctx context.Context, connectionID string, event Event) error {
	m.mu.RLock()
	conn, ok := m.conns[connectionID]
	m.mu.RUnlock()
	if !ok {
		return ErrConnectionNotFound
	}
	return conn.Push(ctx, event)
}

// PushToUser 向用户全部在线连接推送事件；推送失败不阻塞其他连接。
// 返回实际送达的连接数。
func (m *Manager) PushToUser(ctx context.Context, userID int64, event Event) int {
	m.mu.RLock()
	conns := make([]Connection, 0, len(m.byUser[userID]))
	for id := range m.byUser[userID] {
		if c, ok := m.conns[id]; ok {
			conns = append(conns, c)
		}
	}
	m.mu.RUnlock()

	delivered := 0
	for _, c := range conns {
		if err := c.Push(ctx, event); err == nil {
			delivered++
		} else {
			// 队列满/连接关闭：按事件类型分桶计数（GOCHAT_RESILIENCE.md §11.1）
			metrics.PushDropped.WithLabelValues(event.Event).Inc()
		}
	}
	return delivered
}

// Count 返回当前在线连接数。
func (m *Manager) Count() int64 { return m.active.Load() }

// NodeID 返回本网关节点标识。
func (m *Manager) NodeID() string { return m.nodeID }
