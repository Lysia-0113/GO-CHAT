// Package deliver 是 Gateway 在线推送 Worker：固定消费当前实例绑定的
// im.message.push partition，并只向事件携带的 user_id 推送。
//
// Outbox Publisher 已经完成跨节点路由和 partition 选择；本 Worker 不再
// 查询成员、不再广播全量事件。本机没有目标用户连接时直接提交 offset，
// 客户端通过 Pull 补齐。
package deliver

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/connection"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/kafka"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
	"github.com/Lysia-0113/GO-CHAT/internal/svc"
)

// Worker 是固定 partition 的 Gateway 推送 Worker。
type Worker struct {
	svcCtx        *svc.ServiceContext
	dedup         *dedupe
	commitMessage func(context.Context, kafka.Message) error
	publishDLQ    func(context.Context, kafka.Envelope) error
}

// Config 保留为空结构，便于未来增加 Gateway 消费参数。
type Config struct{}

// New 创建 Gateway 推送 Worker。
func New(svcCtx *svc.ServiceContext, _ Config) *Worker {
	worker := &Worker{
		svcCtx: svcCtx,
		dedup:  newDedupe(5 * time.Minute),
	}
	worker.commitMessage = func(ctx context.Context, msg kafka.Message) error {
		return svcCtx.GatewayConsumer.CommitMessages(ctx, msg)
	}
	worker.publishDLQ = svcCtx.Kafka.PublishDLQ
	return worker
}

// Run 消费当前 Gateway 固定绑定的 push partition 直至 ctx 取消。
func (w *Worker) Run(appCtx context.Context) error {
	for {
		msg, err := w.svcCtx.GatewayConsumer.FetchMessage(appCtx)
		if err != nil {
			if appCtx.Err() != nil {
				return nil
			}
			w.svcCtx.Log.Error("gateway fetch failed", "partition", w.svcCtx.GatewayConsumer.Partition(), "error", err.Error())
			select {
			case <-appCtx.Done():
				return nil
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		if err := w.handleWithRetry(appCtx, msg); err != nil {
			return err
		}
	}
}

// handleWithRetry 串行处理一条消息；基础设施失败（包括 DLQ 写入失败）时
// 退避重试，不能越过当前 partition 的较早 offset。
func (w *Worker) handleWithRetry(appCtx context.Context, msg kafka.Message) error {
	for {
		if err := w.handle(appCtx, msg); err == nil {
			return nil
		} else {
			w.svcCtx.Log.Error("gateway handle failed",
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset,
				"error", err.Error())
		}
		select {
		case <-appCtx.Done():
			return appCtx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// handle 处理一条 push 事件。
func (w *Worker) handle(ctx context.Context, msg kafka.Message) error {
	var env kafka.Envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		return w.commitMessage(ctx, msg)
	}
	if env.EventType != kafka.EventPush {
		return w.commitMessage(ctx, msg)
	}
	var event message.MessagePushEvent
	if err := json.Unmarshal(env.Data, &event); err != nil {
		return w.commitMessage(ctx, msg)
	}

	// Outbox 重试可能向同一 partition 重复写入；Gateway 以 message_id 做短期幂等。
	if w.dedup.Seen(event.MessageID) {
		return w.commitMessage(ctx, msg)
	}

	var pushFailures []connection.PushFailure
	for _, userID := range event.TargetUserIDs {
		if userID <= 0 {
			continue
		}
		ev, err := eventForUser(event, userID)
		if err != nil {
			pushFailures = append(pushFailures, connection.PushFailure{UserID: userID, Reason: err.Error()})
			continue
		}
		// 本机没有该 user_id 的连接是正常情况，PushToUserWithFailures
		// 返回空失败集合，直接丢弃并推进 offset。
		_, failures := w.svcCtx.ConnManager.PushToUserWithFailures(ctx, userID, ev)
		pushFailures = append(pushFailures, failures...)
	}
	if len(pushFailures) != 0 {
		if err := w.pushFailuresToDLQ(ctx, msg, event, pushFailures); err != nil {
			return err
		}
	}

	w.dedup.Mark(event.MessageID)
	return w.commitMessage(ctx, msg)
}

func (w *Worker) pushFailuresToDLQ(ctx context.Context, msg kafka.Message, event message.MessagePushEvent, failures []connection.PushFailure) error {
	payload := kafka.DLQPayload{
		FailedTopic:     msg.Topic,
		FailedPartition: msg.Partition,
		FailedOffset:    msg.Offset,
		RetryCount:      0,
		ErrorCode:       "WEBSOCKET_PUSH_FAILED",
		ErrorMessage:    pushFailureDescription(failures),
		FailedAt:        time.Now().UTC(),
		OriginalEvent:   json.RawMessage(append([]byte(nil), msg.Value...)),
	}
	env, err := kafka.NewEnvelope(kafka.EventDLQ, "gateway-worker", event.ConversationID, payload)
	if err != nil {
		return err
	}
	if err := w.publishDLQ(ctx, env); err != nil {
		w.svcCtx.Log.Error("gateway dlq publish failed", "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err.Error())
		return err
	}
	metrics.KafkaDLQ.WithLabelValues(msg.Topic, payload.ErrorCode).Inc()
	return nil
}

func pushFailureDescription(failures []connection.PushFailure) string {
	parts := make([]string, 0, len(failures))
	for _, failure := range failures {
		part := fmt.Sprintf("user_id=%d", failure.UserID)
		if failure.ConnectionID != "" {
			part += " connection_id=" + failure.ConnectionID
		}
		if failure.Reason != "" {
			part += " reason=" + strings.ReplaceAll(failure.Reason, "\n", " ")
		}
		parts = append(parts, part)
	}
	description := fmt.Sprintf("%d websocket push failure(s): %s", len(failures), strings.Join(parts, "; "))
	if len(description) > 1024 {
		return description[:1024]
	}
	return description
}

// eventForUser 构造成员视角的客户端事件：发送者收到 message.persisted，
// 其他目标用户收到 message.new。
func eventForUser(event message.MessagePushEvent, userID int64) (connection.Event, error) {
	if userID == event.SenderID {
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

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

// dedupe 是按 message_id 的短期投递去重。
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
