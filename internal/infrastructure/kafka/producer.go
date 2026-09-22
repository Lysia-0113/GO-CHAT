package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
	"github.com/Lysia-0113/GO-CHAT/internal/resilience"
)

// Producer 是 Kafka 生产者。
//
// Inbox/DLQ 使用 conversation_id 的 Kafka 默认兼容哈希，保证同一会话
// 进入同一 partition；Push 使用 ManualPartitioner，由 Outbox 显式指定
// 目标 Gateway partition。
type Producer struct {
	topics   Topics
	inbox    *kgo.Client
	push     *kgo.Client
	breakers *resilience.Breakers
	timeout  time.Duration
}

// ProducerConfig 是生产者配置。
type ProducerConfig struct {
	Brokers     []string
	Timeout     time.Duration // 等待 acks=all 的超时上限
	AcksAll     bool
	TopicSuffix string
	Logger      *slog.Logger
}

// NewProducer 创建 franz-go 生产者。
func NewProducer(cfg ProducerConfig, breakers *resilience.Breakers) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka brokers must not be empty")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 3 * time.Second
	}
	topics := NewTopics(cfg.TopicSuffix)
	base := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5 * time.Millisecond),
		kgo.ProducerBatchMaxBytes(1 << 20),
		kgo.ProduceRequestTimeout(cfg.Timeout),
		kgo.RecordDeliveryTimeout(cfg.Timeout),
	}
	if !cfg.AcksAll {
		base[1] = kgo.RequiredAcks(kgo.LeaderAck())
	}
	if cfg.Logger != nil {
		base = append(base, kgo.WithLogger(newFranzLogger(cfg.Logger)))
	}

	inboxOpts := append([]kgo.Opt{}, base...)
	inboxOpts = append(inboxOpts, kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
	inboxClient, err := kgo.NewClient(inboxOpts...)
	if err != nil {
		return nil, fmt.Errorf("create inbox producer: %w", err)
	}

	pushOpts := append([]kgo.Opt{}, base...)
	pushOpts = append(pushOpts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	pushClient, err := kgo.NewClient(pushOpts...)
	if err != nil {
		inboxClient.Close()
		return nil, fmt.Errorf("create push producer: %w", err)
	}

	return &Producer{
		topics:   topics,
		inbox:    inboxClient,
		push:     pushClient,
		breakers: breakers,
		timeout:  cfg.Timeout,
	}, nil
}

// PublishIngress 发布待持久化事件。方法名保留 ingress 语义，Topic 已改为 inbox。
func (p *Producer) PublishIngress(ctx context.Context, env Envelope) error {
	return p.publish(ctx, p.inbox, p.topics.Inbox(), -1, env, "kafka:inbox_publish")
}

// PublishPush 将事件显式写入指定 Gateway partition。
func (p *Producer) PublishPush(ctx context.Context, partition int, env Envelope) error {
	if partition < 0 {
		return errs.New(errs.InvalidArgument, "push partition 不能为负数")
	}
	return p.publish(ctx, p.push, p.topics.Push(), partition, env, "kafka:push_publish")
}

// PublishDLQ 发布死信事件；DLQ 发布失败由调用方决定是否继续重试。
func (p *Producer) PublishDLQ(ctx context.Context, env Envelope) error {
	return p.publish(ctx, p.inbox, p.topics.DLQ(), -1, env, "kafka:dlq_publish")
}

func (p *Producer) publish(ctx context.Context, client *kgo.Client, topic string, partition int, env Envelope, breakerName string) error {
	payload, err := env.Marshal()
	if err != nil {
		return errs.Internal(err)
	}
	if p.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
	}
	record := &kgo.Record{
		Topic:     topic,
		Key:       []byte(KeyOf(parseID(env.ConversationID))),
		Value:     payload,
		Timestamp: time.Now().UTC(),
	}
	if partition >= 0 {
		record.Partition = int32(partition)
	}
	start := time.Now()
	defer func() {
		metrics.DependencyDuration.WithLabelValues(breakerName).Observe(time.Since(start).Seconds())
	}()
	produce := func() error {
		return client.ProduceSync(ctx, record).FirstErr()
	}
	if p.breakers != nil {
		err = p.breakers.ExecuteByName(ctx, breakerName, produce)
	} else {
		err = produce()
	}
	if err != nil {
		metrics.KafkaProducerError.WithLabelValues(topic).Inc()
		return errs.Wrap(errs.KafkaUnavailable, "Kafka 发布失败", err)
	}
	metrics.KafkaProducerSend.WithLabelValues(topic).Inc()
	return nil
}

// Ping 检查 Kafka Broker 是否可达。
func (p *Producer) Ping(ctx context.Context) error {
	if err := p.inbox.Ping(ctx); err != nil {
		return err
	}
	return p.push.Ping(ctx)
}

// Close 关闭生产者。
func (p *Producer) Close() error {
	if p.inbox != nil {
		p.inbox.Close()
	}
	if p.push != nil {
		p.push.Close()
	}
	return nil
}

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

type franzLogger struct{ log *slog.Logger }

func newFranzLogger(log *slog.Logger) kgo.Logger { return franzLogger{log: log} }

func (l franzLogger) Level() kgo.LogLevel { return kgo.LogLevelInfo }

func (l franzLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	if l.log == nil {
		return
	}
	args := make([]any, 0, len(keyvals))
	args = append(args, keyvals...)
	switch level {
	case kgo.LogLevelError:
		l.log.Error(msg, args...)
	case kgo.LogLevelWarn:
		l.log.Warn(msg, args...)
	case kgo.LogLevelDebug:
		l.log.Debug(msg, args...)
	default:
		l.log.Info(msg, args...)
	}
}
