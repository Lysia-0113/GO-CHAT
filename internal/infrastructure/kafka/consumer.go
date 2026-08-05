package kafka

import (
	"context"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

// Consumer 是手动提交 Offset 的消费者封装（GOCHAT_KAFKA.md §7.2）。
// 业务成功（MySQL 事务提交）后才调用 CommitMessages。
type Consumer struct {
	reader *kafkago.Reader
	topic  string
	group  string
}

// ConsumerConfig 是消费者配置。
type ConsumerConfig struct {
	Brokers          []string
	Topic            string
	Group            string
	StartOffset      string // earliest / latest
	MaxBytes         int
	SessionTimeout   time.Duration
	RebalanceTimeout time.Duration
}

// NewConsumer 创建消费者。
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	offset := kafkago.FirstOffset
	if cfg.StartOffset == "latest" {
		offset = kafkago.LastOffset
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:          cfg.Brokers,
		Topic:            cfg.Topic,
		GroupID:          cfg.Group,
		StartOffset:      offset,
		MaxBytes:         cfg.MaxBytes,
		SessionTimeout:   cfg.SessionTimeout,
		RebalanceTimeout: cfg.RebalanceTimeout,
		CommitInterval:   0, // 手动提交：业务成功后才提交
		Logger:           nopLogger{},
	})
	return &Consumer{reader: reader, topic: cfg.Topic, group: cfg.Group}, nil
}

// Topic 返回消费者订阅的 Topic。
func (c *Consumer) Topic() string { return c.topic }

// Group 返回消费者组。
func (c *Consumer) Group() string { return c.group }

// FetchMessage 阻塞获取下一条消息（ctx 取消时返回）。
func (c *Consumer) FetchMessage(ctx context.Context) (Message, error) {
	m, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return Message{}, err
	}
	return Message{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
		Time:      m.Time,
	}, nil
}

// CommitMessages 在业务成功后提交偏移（必须晚于 MySQL 事务提交）。
func (c *Consumer) CommitMessages(ctx context.Context, msgs ...Message) error {
	km := make([]kafkago.Message, 0, len(msgs))
	for i := range msgs {
		km = append(km, kafkago.Message{
			Topic:     msgs[i].Topic,
			Partition: msgs[i].Partition,
			Offset:    msgs[i].Offset,
		})
	}
	if err := c.reader.CommitMessages(ctx, km...); err != nil {
		return errs.Wrap(errs.KafkaUnavailable, "Offset 提交失败", err)
	}
	return nil
}

// Close 关闭消费者。
func (c *Consumer) Close() error { return c.reader.Close() }

// Message 是消费到的一条消息。
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Time      time.Time
}
