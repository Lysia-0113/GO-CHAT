package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
	"github.com/Lysia-0113/GO-CHAT/internal/resilience"
)

// Producer 是 Kafka 生产者：acks=all + 幂等 + 按 conversation_id 哈希分区
// （GOCHAT_KAFKA.md §6.2）。
//
// 发送失败时不得返回 message.accepted；调用方让客户端复用 client_msg_id 重试。
type Producer struct {
	topics   Topics
	writer   *kafkago.Writer
	breakers *resilience.Breakers
	timeout  time.Duration
	reg      *metrics.Registry

	producerName string
}

// ProducerConfig 是生产者配置。
type ProducerConfig struct {
	Brokers     []string
	Timeout     time.Duration // 等待 acks=all 的超时上限
	AcksAll     bool
	TopicSuffix string
	Logger      kafkago.Logger // 可选；nil 时静默（nopLogger）
}

// NewProducer 创建生产者。
// 单 Writer 按 Topic 复用连接；分区平衡使用 Hash（同 Key 同 Partition，保证会话顺序）。
func NewProducer(cfg ProducerConfig, breakers *resilience.Breakers, reg *metrics.Registry, producerName string) (*Producer, error) {
	acks := kafkago.RequireAll
	if !cfg.AcksAll {
		acks = kafkago.RequireOne
	}
	logger := cfg.Logger
	if logger == nil {
		logger = nopLogger{}
	}
	writer := &kafkago.Writer{
		Addr:                   kafkago.TCP(cfg.Brokers...),
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           acks,
		MaxAttempts:            10,
		BatchTimeout:           5 * time.Millisecond,
		BatchSize:              100,
		WriteTimeout:           cfg.Timeout,
		AllowAutoTopicCreation: true,
		Logger:                 logger,
	}
	return &Producer{
		topics:       NewTopics(cfg.TopicSuffix),
		writer:       writer,
		breakers:     breakers,
		timeout:      cfg.Timeout,
		reg:          reg,
		producerName: producerName,
	}, nil
}

// Publish 发送一条事件；Key 使用 conversation_id。
func (p *Producer) Publish(ctx context.Context, topic string, conversationID int64, env Envelope) error {
	payload, err := env.Marshal()
	if err != nil {
		return errs.Internal(err)
	}
	msg := kafkago.Message{
		Topic: topic,
		Key:   []byte(KeyOf(conversationID)),
		Value: payload,
		Time:  time.Now().UTC(),
	}
	if p.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
	}
	return p.writer.WriteMessages(ctx, msg)
}

// PublishIngress 发布待持久化事件（message.ingress）。
func (p *Producer) PublishIngress(ctx context.Context, env Envelope) error {
	return p.publish(ctx, p.topics.Ingress(), env)
}

// PublishPersisted 发布持久化事件（message.persisted）。
func (p *Producer) PublishPersisted(ctx context.Context, env Envelope) error {
	return p.publish(ctx, p.topics.Persisted(), env)
}

// PublishDLQ 发布死信事件（message.dlq）；DLQ 发布失败只告警不重试。
func (p *Producer) PublishDLQ(ctx context.Context, env Envelope) error {
	return p.publish(ctx, p.topics.DLQ(), env)
}

func (p *Producer) publish(ctx context.Context, topic string, env Envelope) error {
	convID := parseID(env.ConversationID)
	// 熔断保护：按 依赖:操作 维度（GOCHAT_RESILIENCE.md §7.3）
	breakerName := p.breakerName(topic)
	err := p.breakers.ExecuteByName(ctx, breakerName, func() error {
		return p.Publish(ctx, topic, convID, env)
	})
	if err != nil {
		if p.reg != nil {
			p.reg.Counter("kafka_producer_error_total", "生产者失败数", map[string]string{"topic": topic}).Inc()
		}
		return errs.Wrap(errs.KafkaUnavailable, "Kafka 发布失败", err)
	}
	if p.reg != nil {
		p.reg.Counter("kafka_producer_send_total", "生产者发送总数", map[string]string{"topic": topic}).Inc()
	}
	return nil
}

func (p *Producer) breakerName(topic string) string {
	switch topic {
	case p.topics.Ingress():
		return "kafka:ingress_publish"
	case p.topics.Persisted():
		return "kafka:persisted_publish"
	default:
		return "kafka:dlq_publish"
	}
}

// Close 关闭生产者。
func (p *Producer) Close() error { return p.writer.Close() }

func parseID(s string) int64 {
	var v int64
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0
		}
		v = v*10 + int64(r-'0')
	}
	return v
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...interface{}) {}

// slogLogger 把应用日志接入 kafka-go 的 Printf 接口（替代 nopLogger，生产可观测）。
type slogLogger struct{ log *slog.Logger }

func (l slogLogger) Printf(format string, v ...interface{}) {
	l.log.Warn(fmt.Sprintf(format, v...))
}

// SlogLogger 构造 kafka-go 日志适配器；log 为 nil 时回退 nopLogger。
func SlogLogger(log *slog.Logger) kafkago.Logger {
	if log == nil {
		return nopLogger{}
	}
	return slogLogger{log: log}
}
