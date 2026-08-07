package websocket

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Lysia-0113/GO-CHAT/internal/connection"
	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

// Conn 是 WebSocket 连接封装：有界出站队列 + 单写协程 + 心跳状态
// （GOCHAT_API.md §6.8：每个连接一个读协程一个写协程，禁止并发写 Socket）。
type Conn struct {
	id                string
	userID            int64
	deviceID          string
	ws                *websocket.Conn
	send              chan []byte
	writeQueueTimeout time.Duration
	writeMu           sync.Mutex // gorilla 禁止并发写：writer/heartbeat 两个协程必须串行

	lastPong  atomic.Int64 // 毫秒时间戳
	closed    atomic.Bool
	closeOnce sync.Once

	// OnSlowConnection 队列持续满时的回调（指标与日志）
	OnSlowConnection func(connID string)
	// queueStalled 记录队列持续未排空的时间起点（毫秒）
	queueStalled atomic.Int64
}

// NewConn 创建连接封装。
func NewConn(id string, userID int64, deviceID string, ws *websocket.Conn, queueSize int, queueTimeout time.Duration) *Conn {
	if queueSize <= 0 {
		queueSize = 256
	}
	if queueTimeout <= 0 {
		queueTimeout = 30 * time.Second
	}
	c := &Conn{
		id:                id,
		userID:            userID,
		deviceID:          deviceID,
		ws:                ws,
		send:              make(chan []byte, queueSize),
		writeQueueTimeout: queueTimeout,
	}
	c.lastPong.Store(time.Now().UnixMilli())
	return c
}

// ID 返回连接标识。
func (c *Conn) ID() string { return c.id }

// UserID 返回归属用户。
func (c *Conn) UserID() int64 { return c.userID }

// DeviceID 返回设备标识。
func (c *Conn) DeviceID() string { return c.deviceID }

// Push 将事件序列化后写入有界队列（非阻塞语义）。
// 队列满时记录慢连接状态并返回 ErrQueueFull（由调用方决定是否断开）。
func (c *Conn) Push(ctx context.Context, event connection.Event) error {
	if c.closed.Load() {
		return connection.ErrClosed
	}
	raw, err := marshalEvent(event)
	if err != nil {
		return errs.Internal(err)
	}
	select {
	case c.send <- raw:
		// 队列恢复排空
		c.queueStalled.Store(0)
		return nil
	default:
		if c.queueStalled.Load() == 0 {
			c.queueStalled.Store(time.Now().UnixMilli())
		}
		return connection.ErrQueueFull
	}
}

// pushBlocking 阻塞写入队列（供 writer 内部使用；带取消）。
func (c *Conn) pushBlocking(ctx context.Context, raw []byte) error {
	select {
	case c.send <- raw:
		c.queueStalled.Store(0)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// StallDuration 返回队列持续满的时长；0 表示未停滞。
func (c *Conn) StallDuration() time.Duration {
	start := c.queueStalled.Load()
	if start == 0 {
		return 0
	}
	return time.Since(time.UnixMilli(start))
}

// QueueLen 返回当前队列长度。
func (c *Conn) QueueLen() int { return len(c.send) }

// MarkPong 记录心跳响应。
func (c *Conn) MarkPong() { c.lastPong.Store(time.Now().UnixMilli()) }

// LastPongAt 返回最近心跳响应时间。
func (c *Conn) LastPongAt() time.Time { return time.UnixMilli(c.lastPong.Load()) }

// Closed 返回是否已关闭。
func (c *Conn) Closed() bool { return c.closed.Load() }

// WriteMessage 串行写 Socket：writerLoop 与 heartbeatLoop 两个协程共用，
// gorilla/websocket 禁止并发写（GOCHAT_API.md §6.8），必须串行化。
func (c *Conn) WriteMessage(raw []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.ws.WriteMessage(websocket.TextMessage, raw)
}

// Close 关闭连接（幂等）：关闭队列并发送关闭帧。
func (c *Conn) Close(reason string) {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.send)
		_ = c.ws.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseGoingAway, reason),
			time.Now().Add(time.Second))
	})
}

// marshalEvent 序列化连接事件。
func marshalEvent(event connection.Event) ([]byte, error) {
	type envelope struct {
		Event     string          `json:"event"`
		RequestID string          `json:"request_id,omitempty"`
		Data      json.RawMessage `json:"data"`
	}
	return json.Marshal(envelope{Event: event.Event, RequestID: event.RequestID, Data: event.Data})
}
