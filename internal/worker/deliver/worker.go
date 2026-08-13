// Package deliver 是在线投递 Worker：消费 im.message.persisted，
// 更新最近消息缓存、查询在线路由并推送给持有连接的网关节点
// （GOCHAT_KAFKA.md §9）。
package deliver

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/connection"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/kafka"
	redisinfra "github.com/Lysia-0113/GO-CHAT/internal/infrastructure/redis"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
	"github.com/Lysia-0113/GO-CHAT/internal/svc"
)

// Worker 是投递 Worker。依赖经 svcCtx 服务定位器取用（GOCHAT_API.md §11.3）。
type Worker struct {
	svcCtx *svc.ServiceContext
	dedup  *dedupe
}

// New 创建投递 Worker。
func New(svcCtx *svc.ServiceContext) *Worker {
	return &Worker{
		svcCtx: svcCtx,
		dedup:  newDedupe(5 * time.Minute),
	}
}

// Run 消费 im.message.persisted 直至 ctx 取消。
func (w *Worker) Run(appCtx context.Context) error {
	for {
		msg, err := w.svcCtx.DeliverConsumer.FetchMessage(appCtx)
		if err != nil {
			if appCtx.Err() != nil {
				return nil
			}
			w.svcCtx.Log.Error("deliver fetch failed", "error", err.Error())
			select {
			case <-appCtx.Done():
				return nil
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		if err := w.handle(appCtx, msg); err != nil {
			// 投递失败不提交 Offset：消息已在 MySQL，重复投递由 message_id 去重
			w.svcCtx.Log.Warn("deliver handle failed",
				"partition", msg.Partition, "offset", msg.Offset, "error", err.Error())
		}
	}
}

// handle 处理一条 persisted 事件（GOCHAT_KAFKA.md §9.2 投递规则）。
func (w *Worker) handle(ctx context.Context, msg kafka.Message) error {
	var env kafka.Envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		return w.svcCtx.DeliverConsumer.CommitMessages(ctx, msg)
	}
	var event message.MessagePersistedEvent
	if err := json.Unmarshal(env.Data, &event); err != nil {
		return w.svcCtx.DeliverConsumer.CommitMessages(ctx, msg)
	}

	// 1. 按 message_id 短期幂等（Outbox 可能重复投递）
	if w.dedup.Seen(event.MessageID) {
		return w.svcCtx.DeliverConsumer.CommitMessages(ctx, msg)
	}
	w.dedup.Mark(event.MessageID)

	// 2. 成员 fanout：发送者收 message.persisted，接收者收 message.new
	memberIDs, err := w.svcCtx.ConvRepo.ListMemberIDs(ctx, event.ConversationID)
	if err != nil {
		return err
	}
	for _, memberID := range memberIDs {
		w.pushToMember(ctx, event, memberID)
	}

	// 3. 提交 Offset
	return w.svcCtx.DeliverConsumer.CommitMessages(ctx, msg)
}

// pushToMember 向成员的全部在线连接投递（本机直推或跨节点 Pub/Sub）。
func (w *Worker) pushToMember(ctx context.Context, event message.MessagePersistedEvent, memberID int64) {
	routes, err := w.svcCtx.Presence.OnlineConnections(ctx, memberID)
	if err != nil {
		// 在线查询失败：投递缺口主嫌疑（之前被静默吞掉）
		metrics.OnlineQueryFailed.Inc()
		return
	}
	if len(routes) == 0 {
		return // 离线：由 after_seq 补偿，不需要 Kafka 副本（GOCHAT_KAFKA.md §9.2）
	}

	ev, err := eventForMember(event, memberID)
	if err != nil {
		w.svcCtx.Log.Error("build push event failed", "error", err.Error())
		return
	}

	for _, route := range routes {
		if route.NodeID == w.svcCtx.ConnManager.NodeID() {
			// 本机连接直推
			if err := w.svcCtx.ConnManager.PushToConnection(ctx, route.ConnectionID, ev); err != nil {
				metrics.PushToConnFailed.Inc()
			}
			continue
		}
		if w.svcCtx.Pubsub != nil {
			// 跨节点：发布到目标节点频道（丢失后客户端补拉）
			raw, _ := json.Marshal(ev)
			_ = w.svcCtx.Pubsub.PublishToNode(ctx, route.NodeID, redisinfra.DeliveryEvent{
				EventName:     ev.Event,
				Data:          raw,
				ConnectionIDs: []string{route.ConnectionID},
			})
		}
	}
}

// eventForMember 构造成员视角的事件（GOCHAT_API.md §6.5）：
// 发送者收到 message.persisted；接收者收到 message.new。
func eventForMember(event message.MessagePersistedEvent, memberID int64) (connection.Event, error) {
	if memberID == event.SenderID {
		return connection.NewEvent(connection.EventMessagePersisted, map[string]interface{}{
			"client_msg_id":   event.ClientMessageID,
			"message_id":      itoa(event.MessageID),
			"conversation_id": itoa(event.ConversationID),
			"seq":             event.Seq,
			"status":          "persisted",
			"sent_at":         event.CreatedAt,
		})
	}
	return connection.NewEvent(connection.EventMessageNew, map[string]interface{}{
		"message_id":      itoa(event.MessageID),
		"client_msg_id":   event.ClientMessageID,
		"conversation_id": itoa(event.ConversationID),
		"seq":             event.Seq,
		"sender_id":       itoa(event.SenderID),
		"content_type":    contentType(event.MessageType),
		"content":         json.RawMessage(event.Content),
		"sent_at":         event.CreatedAt,
	})
}

func contentType(t int8) string {
	switch t {
	case message.TypeText:
		return "text"
	case message.TypeImage:
		return "image"
	case message.TypeFile:
		return "file"
	default:
		return "text"
	}
}

func itoa(v int64) string {
	return strconv.FormatInt(v, 10)
}

// dedupe 是按 message_id 的短期投递去重（GOCHAT_KAFKA.md §9.2 规则 2）。
type dedupe struct {
	mu   sync.Mutex
	seen map[int64]time.Time
	ttl  time.Duration
}

func newDedupe(ttl time.Duration) *dedupe {
	return &dedupe{seen: make(map[int64]time.Time), ttl: ttl}
}

func (d *dedupe) Seen(id int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	ts, ok := d.seen[id]
	if !ok {
		return false
	}
	return time.Since(ts) < d.ttl
}

func (d *dedupe) Mark(id int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.seen) > 10000 {
		now := time.Now()
		for k, ts := range d.seen {
			if now.Sub(ts) > d.ttl {
				delete(d.seen, k)
			}
		}
	}
	d.seen[id] = time.Now()
}
