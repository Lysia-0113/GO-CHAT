// Package deliver 是在线投递 Worker：消费 im.message.persisted，
// 按事件携带的成员快照推送给本机连接（广播模型，GOCHAT_KAFKA.md §9）。
//
// 广播模型：每个节点使用独立的消费者组消费全部分区（组名 = delivery_group + node_id），
// 每条消息在每个节点都被处理，但只推送本机持有的连接——不需要 Presence 路由、
// 不需要跨节点 Pub/Sub。离线用户由 after_seq 补偿。
//
// 顺序保证：与 persist Worker 相同的分区串行模型——1 个分发 goroutine 按分区号
// 投递，每分区 1 个处理 goroutine 串行推送。同一会话经 conversation_id 哈希进入
// 同一 Kafka 分区，因此同分区串行 = 同会话推送有序。
package deliver

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/connection"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/kafka"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
	"github.com/Lysia-0113/GO-CHAT/internal/svc"
)

// partChannelSize 每分区处理队列容量；满时分发层阻塞形成 Kafka 背压。
const partChannelSize = 128

// Worker 是投递 Worker。依赖经 svcCtx 服务定位器取用（GOCHAT_API.md §11.3）。
type Worker struct {
	svcCtx        *svc.ServiceContext
	dedup         *dedupe
	numPartitions int
}

// Config 是 Worker 配置。
type Config struct {
	NumPartitions int
}

// New 创建投递 Worker。
func New(svcCtx *svc.ServiceContext, cfg Config) *Worker {
	if cfg.NumPartitions <= 0 {
		cfg.NumPartitions = 3
	}
	return &Worker{
		svcCtx:        svcCtx,
		dedup:         newDedupe(5 * time.Minute),
		numPartitions: cfg.NumPartitions,
	}
}

// Run 消费 im.message.persisted 直至 ctx 取消。
//
// 并发模型：1 个分发 goroutine 拉取消息并按分区号投递到 per-partition 队列，
// 每个分区 1 个处理 goroutine 串行推送。分区内失败原地重试、不跳过，
// 从根上避免并发消费导致的 offset 跳跃提交与推送乱序。
func (w *Worker) Run(appCtx context.Context) error {
	partChans := make([]chan kafka.Message, w.numPartitions)
	for p := range partChans {
		partChans[p] = make(chan kafka.Message, partChannelSize)
	}

	// 分发层：单 goroutine 拉取，按分区号投递（队列满时阻塞 = Kafka 背压）
	go func() {
		for {
			msg, err := w.svcCtx.DeliverConsumer.FetchMessage(appCtx)
			if err != nil {
				if appCtx.Err() != nil {
					return // 优雅退出
				}
				w.svcCtx.Log.Error("deliver fetch failed", "error", err.Error())
				select {
				case <-appCtx.Done():
					return
				case <-time.After(500 * time.Millisecond):
				}
				continue
			}
			// 路由键 = 分区号；配置与实际不一致时按取模兜底（同分区仍同队列，顺序保持）
			idx := int(msg.Partition) % w.numPartitions
			if int(msg.Partition) >= w.numPartitions {
				w.svcCtx.Log.Warn("partition exceeds configured count",
					"partition", msg.Partition, "configured", w.numPartitions)
			}
			select {
			case partChans[idx] <- msg:
			case <-appCtx.Done():
				return
			}
		}
	}()

	// 处理层：每分区一个 goroutine，串行 handle（失败重试，不跳过）
	for p := range partChans {
		go func(p int) {
			for {
				select {
				case <-appCtx.Done():
					return
				case msg := <-partChans[p]:
					if err := w.handleWithRetry(appCtx, msg); err != nil {
						w.svcCtx.Log.Error("partition worker stopped",
							"partition", p, "error", err.Error())
						return
					}
				}
			}
		}(p)
	}

	<-appCtx.Done()
	return nil
}

// handleWithRetry 串行处理一条消息；基础设施失败时退避重试直到成功或 ctx 取消。
// 不能跳过失败消息：跳过会导致 offset 被后续消息提交覆盖，在线推送静默丢失。
// 连接级推送失败不在此列——那是尽力而为（PushToUser 内部计数），不阻塞分区。
func (w *Worker) handleWithRetry(appCtx context.Context, msg kafka.Message) error {
	for {
		if err := w.handle(appCtx, msg); err == nil {
			return nil
		} else {
			w.svcCtx.Log.Error("deliver handle failed",
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

// handle 处理一条 persisted 事件（GOCHAT_KAFKA.md §9.2 投递规则）。
//
// 错误分类：
//   - 解析失败：消息已在 MySQL，只丢推送（客户端补拉兜底），提交跳过；
//   - 成员快照缺失（旧事件）时回退查库：查询失败返回错误，整体重试；
//   - 连接级失败（本机连接不可达/队列满）：尽力而为，只计数不阻塞分区。
func (w *Worker) handle(ctx context.Context, msg kafka.Message) error {
	var env kafka.Envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		return w.svcCtx.DeliverConsumer.CommitMessages(ctx, msg)
	}
	var event message.MessagePersistedEvent
	if err := json.Unmarshal(env.Data, &event); err != nil {
		return w.svcCtx.DeliverConsumer.CommitMessages(ctx, msg)
	}

	// 按 message_id 短期幂等（Outbox 可能重复投递）
	if w.dedup.Seen(event.MessageID) {
		return w.svcCtx.DeliverConsumer.CommitMessages(ctx, msg)
	}

	// 成员列表：优先用事件携带的快照（投递侧零 DB 查询）；旧事件回退查库
	memberIDs := event.MemberIDs
	if len(memberIDs) == 0 {
		var err error
		memberIDs, err = w.svcCtx.ConvRepo.ListMemberIDs(ctx, event.ConversationID)
		if err != nil {
			return err // 基础设施失败：不提交 offset，由 handleWithRetry 重试
		}
	}

	// 广播投递：发送者收 message.persisted，接收者收 message.new；
	// 只推本机连接（本机无该用户连接时 PushToUser 是 O(1) 空操作）
	for _, memberID := range memberIDs {
		ev, err := eventForMember(event, memberID)
		if err != nil {
			w.svcCtx.Log.Error("build push event failed", "error", err.Error())
			continue
		}
		w.svcCtx.ConnManager.PushToUser(ctx, memberID, ev)
	}

	// fanout 成功后才标记去重：处理失败重试时不会因 Seen 命中跳过投递；
	// 提交失败重试时直接命中 Seen，只补提交、不重复推送
	w.dedup.Mark(event.MessageID)

	return w.svcCtx.DeliverConsumer.CommitMessages(ctx, msg)
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
