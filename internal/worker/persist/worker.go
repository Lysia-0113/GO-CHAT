// Package persist 是 Kafka 到 MySQL 的持久化 Worker
// （GOCHAT_KAFKA.md §7）：按会话顺序落库、sender_id+client_msg_id 幂等、
// MySQL 事务提交后才提交 Offset。
//
// 顺序保证：生产者按 conversation_id 哈希分区，同一会话进入同一分区；
// 本 Worker 按分区号路由到独立的处理 goroutine，同分区消息串行落库，
// 从而保证 seq 分配顺序 = 发送顺序（并发抢锁会导致 seq 颠倒，见 §7.2）。
package persist

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	kafkainfra "github.com/Lysia-0113/GO-CHAT/internal/infrastructure/kafka"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
	"github.com/Lysia-0113/GO-CHAT/internal/svc"
)

// partChannelSize 每分区处理队列容量；满时分发层阻塞形成 Kafka 背压。
const partChannelSize = 128

// Worker 持久化 Worker。依赖经 svcCtx 服务定位器取用（GOCHAT_API.md §11.3）。
type Worker struct {
	svcCtx        *svc.ServiceContext
	publisher     message.MessagePublisher // 本 Worker 专用的 persisted 发布器
	maxRetries    int
	backoff       time.Duration
	txTimeout     time.Duration
	numPartitions int
}

// Config 是 Worker 配置。
type Config struct {
	MaxRetries    int
	Backoff       time.Duration
	TxTimeout     time.Duration
	NumPartitions int
}

// New 创建持久化 Worker。
func New(svcCtx *svc.ServiceContext, cfg Config) *Worker {
	if cfg.NumPartitions <= 0 {
		cfg.NumPartitions = 3
	}
	return &Worker{
		svcCtx:        svcCtx,
		publisher:     kafkainfra.NewPublisher(svcCtx.Kafka, "persist-worker"),
		maxRetries:    cfg.MaxRetries,
		backoff:       cfg.Backoff,
		txTimeout:     cfg.TxTimeout,
		numPartitions: cfg.NumPartitions,
	}
}

// Run 消费 im.message.ingress 直至 ctx 取消。
// 返回 nil 表示优雅退出。
//
// 并发模型：1 个分发 goroutine 拉取消息并按分区号投递到 per-partition 队列，
// 每个分区 1 个处理 goroutine 串行落库。同一分区（同一会话）的消息严格串行，
// 从根上避免并发抢锁导致的 seq 颠倒与跳跃提交丢消息。
func (w *Worker) Run(appCtx context.Context) error {
	partChans := make([]chan kafkainfra.Message, w.numPartitions)
	for p := range partChans {
		partChans[p] = make(chan kafkainfra.Message, partChannelSize)
	}

	// 分发层：单 goroutine 拉取，按分区号投递（队列满时阻塞 = Kafka 背压）
	go func() {
		for {
			msg, err := w.svcCtx.PersistConsumer.FetchMessage(appCtx)
			if err != nil {
				if appCtx.Err() != nil {
					return // 优雅退出
				}
				w.svcCtx.Log.Error("persist fetch failed", "error", err.Error())
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
						// 仅当 ctx 取消时返回；其余失败在 handleWithRetry 内退避重试
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

// handleWithRetry 串行处理一条消息；失败时退避重试直到成功或 ctx 取消。
// 不能跳过失败消息：跳过会导致 offset 跳跃提交，失败消息静默丢失。
// 持续失败时该分区积压，由 LAG 告警兜底（宁可卡住，不可丢）。
func (w *Worker) handleWithRetry(appCtx context.Context, msg kafkainfra.Message) error {
	for {
		handled, hErr := w.handle(appCtx, msg)
		if hErr == nil {
			return nil
		}
		w.svcCtx.Log.Error("persist handle failed",
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset,
			"error", hErr.Error(), "committed", handled)
		select {
		case <-appCtx.Done():
			return appCtx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// handle 处理单条 ingress 事件。
// 返回 (是否已提交 Offset, 错误)。
func (w *Worker) handle(appCtx context.Context, msg kafkainfra.Message) (bool, error) {
	// 1. 解析 Envelope（GOCHAT_KAFKA.md §7.3）
	var env kafkainfra.Envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		return w.dlqAndCommit(appCtx, msg, env, "SCHEMA_ERROR", "envelope 解析失败", 0)
	}
	if env.SchemaVersion != kafkainfra.SchemaVersion {
		return w.dlqAndCommit(appCtx, msg, env, "SCHEMA_ERROR", "不支持的 schema_version", 0)
	}
	var ingress message.MessageIngressEvent
	if err := json.Unmarshal(env.Data, &ingress); err != nil {
		return w.dlqAndCommit(appCtx, msg, env, "SCHEMA_ERROR", "ingress 载荷解析失败", 0)
	}
	if ingress.SenderID <= 0 || ingress.ClientMessageID == "" || ingress.ConversationID <= 0 {
		return w.dlqAndCommit(appCtx, msg, env, "INVALID_ARGUMENT", "ingress 缺少必要字段", 0)
	}
	// 格式兜底校验：历史坏消息/绕过网关的发布方可能携带非 UUID，
	// 归类永久错误进 DLQ，避免 handleWithRetry 无限重试卡死分区
	if _, err := uuid.Parse(ingress.ClientMessageID); err != nil {
		return w.dlqAndCommit(appCtx, msg, env, "INVALID_ARGUMENT", "client_msg_id 不是合法 UUID", 0)
	}
	if err := message.ValidateContent(ingress.MessageType, ingress.Content); err != nil {
		return w.dlqAndCommit(appCtx, msg, env, string(errs.As(err).Code), errs.As(err).Message, 0)
	}

	// 2. 幂等检查：已存在则复用原结果并重新发布 persisted（GOCHAT_DATABASE.md §10）
	existing, err := w.svcCtx.MsgRepo.FindByClientMessageID(appCtx, ingress.SenderID, ingress.ClientMessageID)
	if err != nil {
		return false, err
	}
	if existing != nil {
		if err := w.publisher.PublishPersisted(appCtx, toPersistedEvent(existing)); err != nil {
			return false, err
		}
		metrics.PersistIdempotent.Inc()
		if err := w.svcCtx.PersistConsumer.CommitMessages(appCtx, msg); err != nil {
			return false, err
		}
		return true, nil
	}

	// 3. 分配 message_id（Kafka 消费者内生成，避免入口重复发号）
	messageID, err := w.svcCtx.MessageIDs.Next(appCtx)
	if err != nil {
		return false, err
	}

	// 4. MySQL 持久化事务（带超时；事务失败不提交 Offset）
	msgDomain := &message.Message{
		MessageID:       messageID,
		ClientMessageID: ingress.ClientMessageID,
		ConversationID:  ingress.ConversationID,
		SenderID:        ingress.SenderID,
		MessageType:     ingress.MessageType,
		Content:         ingress.Content,
		ContentPreview:  message.Preview(ingress.Content, ingress.MessageType),
		ClientSentAt:    ingress.ClientSentAt,
		Status:          message.StatusNormal,
	}

	var lastErr error
	for attempt := 0; attempt <= w.maxRetries; attempt++ {
		if attempt > 0 {
			w.svcCtx.Log.Warn("persist retry", "attempt", attempt, "client_msg_id", ingress.ClientMessageID)
			metrics.PersistRetry.Inc()
			select {
			case <-appCtx.Done():
				return false, appCtx.Err()
			case <-time.After(w.backoff * time.Duration(attempt+1)):
			}
		}

		persistCtx, cancel := context.WithTimeout(appCtx, w.txTimeout)
		persisted, perr := w.svcCtx.MsgRepo.Persist(persistCtx, message.PersistInput{Message: msgDomain})
		cancel()

		if perr == nil {
			// 5. MySQL 提交成功后才提交 Offset（GOCHAT_KAFKA.md §7.3）
			metrics.PersistSuccess.Inc()
			if err := w.svcCtx.PersistConsumer.CommitMessages(appCtx, msg); err != nil {
				// Offset 提交失败：允许重复消费，唯一索引去重
				return false, err
			}
			w.svcCtx.Log.Info("message persisted",
				"message_id", persisted.MessageID,
				"seq", persisted.Seq,
				"conversation_id", persisted.ConversationID,
				"client_msg_id", persisted.ClientMessageID,
				"offset", msg.Offset,
			)
			return true, nil
		}

		lastErr = perr
		// 永久业务错误不重试，直接进 DLQ
		if !isTransient(perr) {
			break
		}
	}

	// 6. 超过重试次数或永久错误：进入 DLQ 并提交 Offset，不能无限阻塞 Partition
	code := string(errs.InternalError)
	if e := errs.As(lastErr); e != nil {
		code = string(e.Code)
	}
	return w.dlqAndCommit(appCtx, msg, env, code, safeErrorMessage(lastErr), w.maxRetries)
}

// dlqAndCommit 发布 DLQ 事件并提交 Offset。
func (w *Worker) dlqAndCommit(ctx context.Context, msg kafkainfra.Message, env kafkainfra.Envelope, code, errMsg string, retries int) (bool, error) {
	original, _ := json.Marshal(env)
	dlqPayload := kafkainfra.DLQPayload{
		FailedTopic:     msg.Topic,
		FailedPartition: msg.Partition,
		FailedOffset:    msg.Offset,
		RetryCount:      retries,
		ErrorCode:       code,
		ErrorMessage:    errMsg,
		FailedAt:        time.Now().UTC(),
		OriginalEvent:   original,
	}
	dlqEnv, err := kafkainfra.NewEnvelope(kafkainfra.EventDLQ, "persist-worker", parseConvID(env.ConversationID), dlqPayload)
	if err != nil {
		return false, err
	}
	if err := w.svcCtx.Kafka.PublishDLQ(ctx, dlqEnv); err != nil {
		w.svcCtx.Log.Error("dlq publish failed", "error", err.Error())
	}
	metrics.KafkaDLQ.WithLabelValues(msg.Topic, code).Inc()
	if err := w.svcCtx.PersistConsumer.CommitMessages(ctx, msg); err != nil {
		return false, err
	}
	return true, nil
}

// isTransient 判断错误是否可重试（GOCHAT_KAFKA.md §7.4 失败分类）。
func isTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	if e := errs.As(err); e != nil {
		switch e.Code {
		case errs.ConversationNotFound, errs.InvalidArgument, errs.ConversationForbidden:
			return false // 永久业务错误
		}
		return true // 数据库临时错误等
	}
	return true
}

// safeErrorMessage 只暴露安全摘要，不携带敏感内容。
func safeErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func parseConvID(s string) int64 {
	var v int64
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0
		}
		v = v*10 + int64(r-'0')
	}
	return v
}

// toPersistedEvent 把已持久化消息转换为 persisted 事件。
func toPersistedEvent(m *message.Message) message.MessagePersistedEvent {
	return message.MessagePersistedEvent{
		MessageID:       m.MessageID,
		Seq:             m.Seq,
		SenderID:        m.SenderID,
		ClientMessageID: m.ClientMessageID,
		ConversationID:  m.ConversationID,
		MessageType:     m.MessageType,
		Content:         m.Content,
		ContentPreview:  m.ContentPreview,
		CreatedAt:       m.CreatedAt,
	}
}
