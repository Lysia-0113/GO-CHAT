package websocket

import (
	"context"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/connection"
)

// writerLoop 是单连接写协程：从有界队列取事件写入 Socket
// （GOCHAT_API.md §6.8：限制单个 WebSocket Frame 大小、慢客户端治理）。
//
// 慢连接治理：出站队列持续满超过 writeQueueTimeout 时主动断开，
// 由客户端重连并通过 after_seq 补拉（GOCHAT_RESILIENCE.md §6.3）。
func (h *Handler) writerLoop(ctx context.Context, conn *Conn) {
	// 双保险：单个连接的意外 panic 只关掉自己，不炸整个进程
	defer func() {
		if r := recover(); r != nil {
			h.log.Error("writer goroutine panic recovered", "err", r)
			conn.Close("writer panic recovered")
		}
	}()

	writeTimeout := 10 * time.Second
	for {
		select {
		case <-ctx.Done():
			return
		case raw, ok := <-conn.send:
			if !ok {
				return // 队列已关闭
			}
			if err := conn.ws.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
				return
			}
			if err := conn.WriteMessage(raw); err != nil {
				conn.Close("write failed")
				return
			}
		default:
			// 队列为空：检查慢连接状态
			if conn.StallDuration() > h.writeQueueTimeout {
				h.log.Warn("slow connection closed",
					"connection_id", conn.ID(),
					"stall_ms", conn.StallDuration().Milliseconds(),
					"queue_len", conn.QueueLen(),
				)
				if h.reg != nil {
					h.reg.Counter("websocket_slow_connection_closed_total", "慢连接断开数", nil).Inc()
				}
				conn.Close("slow consumer")
				return
			}
			// 小睡避免忙循环
			select {
			case <-ctx.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
}

// heartbeatLoop 是心跳协程：每 interval 发送 heartbeat.ping；
// 连续两个周期未收到 pong 则关闭连接（GOCHAT_API.md §6.8）。
func (h *Handler) heartbeatLoop(ctx context.Context, conn *Conn) {
	// 双保险：心跳协程的意外 panic 只关掉自己，不炸整个进程
	defer func() {
		if r := recover(); r != nil {
			h.log.Error("heartbeat goroutine panic recovered", "err", r)
			conn.Close("heartbeat panic recovered")
		}
	}()

	ticker := time.NewTicker(h.heartbeatInterval)
	defer ticker.Stop()

	missedThreshold := time.Duration(h.missedHeartbeat) * h.heartbeatInterval
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if time.Since(conn.LastPongAt()) > missedThreshold {
				h.log.Warn("heartbeat timeout, closing connection", "connection_id", conn.ID())
				conn.Close("heartbeat timeout")
				return
			}
			event, err := connection.NewEvent(connection.EventHeartbeatPing,
				map[string]int64{"timestamp": time.Now().UnixMilli()})
			if err != nil {
				return
			}
			raw, err := marshalEvent(event)
			if err != nil {
				return
			}
			// 心跳直写 Socket，不走有界队列（避免慢队列干扰保活）；
			// 经 Conn.WriteMessage 串行化，与 writerLoop 共用写锁
			_ = conn.ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := conn.WriteMessage(raw); err != nil {
				conn.Close("heartbeat write failed")
				return
			}
		}
	}
}
