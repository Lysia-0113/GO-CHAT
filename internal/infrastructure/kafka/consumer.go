package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

// Consumer 是手动提交 Offset 的 franz-go 消费者封装。
//
// 普通 Consumer 使用 Kafka Consumer Group，适用于 Inbox/Persist 和 DLQ。
// Partition Consumer 使用 ConsumePartitions 直接绑定一个 partition，适用于
// Gateway；它通过 kadm 在 Kafka 中保存 offset，但不加入 Consumer Group 再平衡。
type Consumer struct {
	client         *kgo.Client
	admin          *kadm.Client
	topic          string
	group          string
	maxPollRecords int

	direct    bool
	partition int32

	mu      sync.Mutex
	pending []*kgo.Record
}

// ConsumerConfig 是消费者配置。
type ConsumerConfig struct {
	Brokers          []string
	Topic            string
	Group            string
	Partition        int // 仅 NewPartitionConsumer 使用；0 是合法 partition
	StartOffset      string
	MaxBytes         int
	MaxPollRecords   int
	SessionTimeout   time.Duration
	RebalanceTimeout time.Duration
	Logger           *slog.Logger
}

// NewConsumer 创建共享 Consumer Group 消费者。
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if err := validateConsumerConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.Group == "" {
		return nil, errors.New("kafka consumer group must not be empty")
	}
	opts := consumerBaseOptions(cfg)
	opts = append(opts,
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumerGroup(cfg.Group),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(resetOffset(cfg.StartOffset)),
	)
	if cfg.SessionTimeout > 0 {
		opts = append(opts, kgo.SessionTimeout(cfg.SessionTimeout))
	}
	if cfg.RebalanceTimeout > 0 {
		opts = append(opts, kgo.RebalanceTimeout(cfg.RebalanceTimeout))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka group consumer: %w", err)
	}
	return &Consumer{client: client, topic: cfg.Topic, group: cfg.Group, maxPollRecords: cfg.MaxPollRecords}, nil
}

// NewPartitionConsumer 创建固定绑定单 partition 的消费者。
// offsetGroup 只是 Kafka offset 的命名空间，不会触发分区再平衡。
func NewPartitionConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if err := validateConsumerConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.Group == "" {
		return nil, errors.New("kafka partition offset group must not be empty")
	}
	if cfg.Partition < 0 {
		return nil, errors.New("kafka partition must not be negative")
	}

	start, err := loadCommittedOffset(cfg)
	if err != nil {
		return nil, err
	}
	opts := consumerBaseOptions(cfg)
	opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		cfg.Topic: {int32(cfg.Partition): start},
	}))
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka partition consumer: %w", err)
	}
	return &Consumer{
		client:         client,
		admin:          kadm.NewClient(client),
		topic:          cfg.Topic,
		group:          cfg.Group,
		maxPollRecords: cfg.MaxPollRecords,
		direct:         true,
		partition:      int32(cfg.Partition),
	}, nil
}

func validateConsumerConfig(cfg ConsumerConfig) error {
	if len(cfg.Brokers) == 0 {
		return errors.New("kafka brokers must not be empty")
	}
	if cfg.Topic == "" {
		return errors.New("kafka topic must not be empty")
	}
	return nil
}

func consumerBaseOptions(cfg ConsumerConfig) []kgo.Opt {
	opts := []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...)}
	if cfg.MaxBytes > 0 {
		opts = append(opts, kgo.FetchMaxBytes(int32(cfg.MaxBytes)))
	}
	if cfg.Logger != nil {
		opts = append(opts, kgo.WithLogger(newFranzLogger(cfg.Logger)))
	}
	return opts
}

func resetOffset(value string) kgo.Offset {
	if value == "latest" {
		return kgo.NewOffset().AtEnd()
	}
	return kgo.NewOffset().AtStart()
}

// loadCommittedOffset 从 Kafka offset store 读取固定 partition 的下一个 offset。
// 首次启动没有 group offset 时回退到 auto_offset_reset。
func loadCommittedOffset(cfg ConsumerConfig) (kgo.Offset, error) {
	adminClient, err := kgo.NewClient(consumerBaseOptions(cfg)...)
	if err != nil {
		return kgo.Offset{}, fmt.Errorf("create kafka offset client: %w", err)
	}
	defer adminClient.Close()

	timeout := cfg.SessionTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	offsetCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	responses, err := kadm.NewClient(adminClient).FetchOffsets(offsetCtx, cfg.Group)
	if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
		return kgo.Offset{}, fmt.Errorf("fetch kafka offset: %w", err)
	}
	if err == nil {
		if response, ok := responses.Lookup(cfg.Topic, int32(cfg.Partition)); ok {
			if response.Err != nil {
				return kgo.Offset{}, fmt.Errorf("fetch kafka offset for %s[%d]: %w", cfg.Topic, cfg.Partition, response.Err)
			}
			if response.At >= 0 {
				return kgo.NewOffset().At(response.At).WithEpoch(response.LeaderEpoch), nil
			}
		}
	}
	return resetOffset(cfg.StartOffset), nil
}

// Topic 返回消费者订阅的 Topic。
func (c *Consumer) Topic() string { return c.topic }

// Group 返回消费者组或固定 partition 的 offset group。
func (c *Consumer) Group() string { return c.group }

// Partition 返回固定消费者绑定的 partition；共享组消费者返回 -1。
func (c *Consumer) Partition() int {
	if !c.direct {
		return -1
	}
	return int(c.partition)
}

// FetchMessage 阻塞获取下一条消息（ctx 取消时返回）。
func (c *Consumer) FetchMessage(ctx context.Context) (Message, error) {
	for {
		if record := c.popPending(); record != nil {
			return toMessage(record), nil
		}
		fetches := c.client.PollRecords(ctx, c.maxPollRecords)
		if fetches.IsClientClosed() {
			return Message{}, kgo.ErrClientClosed
		}
		if fetchErrors := fetches.Errors(); len(fetchErrors) > 0 {
			return Message{}, fetchErrors[0].Err
		}
		fetches.EachRecord(func(record *kgo.Record) {
			c.mu.Lock()
			c.pending = append(c.pending, record)
			c.mu.Unlock()
		})
	}
}

func (c *Consumer) popPending() *kgo.Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) == 0 {
		return nil
	}
	record := c.pending[0]
	copy(c.pending, c.pending[1:])
	c.pending[len(c.pending)-1] = nil
	c.pending = c.pending[:len(c.pending)-1]
	return record
}

// CommitMessages 在业务成功后提交偏移。
func (c *Consumer) CommitMessages(ctx context.Context, msgs ...Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if c.direct {
		return c.commitDirect(ctx, msgs...)
	}
	records := make([]*kgo.Record, 0, len(msgs))
	for i := range msgs {
		if msgs[i].record != nil {
			records = append(records, msgs[i].record)
			continue
		}
		records = append(records, &kgo.Record{
			Topic:     msgs[i].Topic,
			Partition: int32(msgs[i].Partition),
			Offset:    msgs[i].Offset,
		})
	}
	if err := c.client.CommitRecords(ctx, records...); err != nil {
		return errs.Wrap(errs.KafkaUnavailable, "Offset 提交失败", err)
	}
	return nil
}

func (c *Consumer) commitDirect(ctx context.Context, msgs ...Message) error {
	offsets := make(kadm.Offsets)
	for _, msg := range msgs {
		partition := int32(msg.Partition)
		next := msg.Offset + 1
		if existing, ok := offsets[msg.Topic][partition]; ok && existing.At >= next {
			continue
		}
		if offsets[msg.Topic] == nil {
			offsets[msg.Topic] = make(map[int32]kadm.Offset)
		}
		offsets[msg.Topic][partition] = kadm.Offset{
			Topic:     msg.Topic,
			Partition: partition,
			At:        next,
		}
	}
	responses, err := c.admin.CommitOffsets(ctx, c.group, offsets)
	if err != nil {
		return errs.Wrap(errs.KafkaUnavailable, "固定 partition offset 提交失败", err)
	}
	if err := responses.Error(); err != nil {
		return errs.Wrap(errs.KafkaUnavailable, "固定 partition offset 提交失败", err)
	}
	return nil
}

func toMessage(record *kgo.Record) Message {
	return Message{
		Topic:     record.Topic,
		Partition: int(record.Partition),
		Offset:    record.Offset,
		Key:       record.Key,
		Value:     record.Value,
		Time:      record.Timestamp,
		record:    record,
	}
}

// Close 关闭消费者。
func (c *Consumer) Close() error {
	if c.client != nil {
		c.client.Close()
	}
	return nil
}

// Message 是消费到的一条消息。
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Time      time.Time

	record *kgo.Record
}
