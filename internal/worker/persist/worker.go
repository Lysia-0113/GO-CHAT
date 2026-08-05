// Package persist 是 Kafka 到 MySQL 的持久化 Worker
// （GOCHAT_KAFKA.md §7）：按会话顺序落库、sender_id+client_msg_id 幂等、
// MySQL 事务提交后才提交 Offset。
package persist

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/idgen"
	kafkainfra "github.com/Lysia-0113/GO-CHAT/internal/infrastructure/kafka"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
)

// Worker 持久化 Worker。
type Worker struct {
	consumer   *kafkainfra.Consumer
	messages   message.MessageRepository
	messageIDs idgen.IDGenerator
	publisher  message.MessagePublisher
	producer   *kafkainfra.Producer // DLQ 发布
	topics     kafkainfra.Topics

	maxRetries int
	backoff    time.Duration
	txTimeout  time.Duration
	reg        *metrics.Registry
	log        *slog.Logger
}

// Config 是 Worker 配置。
type Config struct {
	MaxRetries int
	Backoff    time.Duration
	TxTimeout  time.Duration
}

// New 创建持久化 Worker。
func New(consumer *kafkainfra.Consumer,
	messages message.MessageRepository,
	messageIDs idgen.IDGenerator,
	publisher message.MessagePublisher,
	producer *kafkainfra.Producer,
	topics kafkainfra.Topics,
	cfg Config,
	reg *metrics.Registry,
	log *slog.Logger,
) *Worker {
	return &Worker{
		consumer:   consumer,
		messages:   messages,
		messageIDs: messageIDs,
		publisher:  publisher,
		producer:   producer,
		topics:     topics,
		maxRetries: cfg.MaxRetries,
		backoff:    cfg.Backoff,
		txTimeout:  cfg.TxTimeout,
		reg:        reg,
		log:        log,
	}
}

// Run 消费 im.message.ingress 直至 ctx 取消。
// 返回 nil 表示优雅退出。
func (w *Worker) Run(appCtx context.Context) error {
	for {
		msg, err := w.consumer.FetchMessage(appCtx)
		if err != nil {
			if appCtx.Err() != nil {
				return nil // 优雅退出
			}
			w.log.Error("persist fetch failed", "error", err.Error())
			select {
			case <-appCtx.Done():
				return nil
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		handled, hErr := w.handle(appCtx, msg)
		if hErr != nil {
			// 处理失败：不提交 Offset（Kafka 会重新投递，数据库幂等去重）
			w.log.Error("persist handle failed",
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset,
				"error", hErr.Error(),
				"committed", handled,
			)
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
	if err := message.ValidateContent(ingress.MessageType, ingress.Content); err != nil {
		return w.dlqAndCommit(appCtx, msg, env, string(errs.As(err).Code), errs.As(err).Message, 0)
	}

	// 2. 幂等检查：已存在则复用原结果并重新发布 persisted（GOCHAT_DATABASE.md §10）
	existing, err := w.messages.FindByClientMessageID(appCtx, ingress.SenderID, ingress.ClientMessageID)
	if err != nil {
		return false, err
	}
	if existing != nil {
		if err := w.publisher.PublishPersisted(appCtx, toPersistedEvent(existing)); err != nil {
			return false, err
		}
		if w.reg != nil {
			w.reg.Counter(metrics.NamePersistIdempotent, "重复消费命中", nil).Inc()
		}
		if err := w.consumer.CommitMessages(appCtx, msg); err != nil {
			return false, err
		}
		return true, nil
	}

	// 3. 分配 message_id（Kafka 消费者内生成，避免入口重复发号）
	messageID, err := w.messageIDs.Next(appCtx)
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
			w.log.Warn("persist retry", "attempt", attempt, "client_msg_id", ingress.ClientMessageID)
			if w.reg != nil {
				w.reg.Counter(metrics.NamePersistRetry, "持久化重试次数", nil).Inc()
			}
			select {
			case <-appCtx.Done():
				return false, appCtx.Err()
			case <-time.After(w.backoff * time.Duration(attempt+1)):
			}
		}

		persistCtx, cancel := context.WithTimeout(appCtx, w.txTimeout)
		persisted, perr := w.messages.Persist(persistCtx, message.PersistInput{Message: msgDomain})
		cancel()

		if perr == nil {
			// 5. MySQL 提交成功后才提交 Offset（GOCHAT_KAFKA.md §7.3）
			if w.reg != nil {
				w.reg.Counter(metrics.NamePersistSuccess, "持久化成功数", nil).Inc()
			}
			if err := w.consumer.CommitMessages(appCtx, msg); err != nil {
				// Offset 提交失败：允许重复消费，唯一索引去重
				return false, err
			}
			w.log.Info("message persisted",
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
	if err := w.producer.PublishDLQ(ctx, dlqEnv); err != nil {
		w.log.Error("dlq publish failed", "error", err.Error())
	}
	if w.reg != nil {
		w.reg.Counter(metrics.NameKafkaDLQ, "进入死信的事件数", map[string]string{"topic": msg.Topic}).Inc()
	}
	if err := w.consumer.CommitMessages(ctx, msg); err != nil {
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
