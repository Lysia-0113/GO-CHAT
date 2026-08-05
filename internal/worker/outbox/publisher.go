// Package outbox 是 Outbox Publisher：把 message_outbox 待投递记录发布到
// im.message.persisted（GOCHAT_KAFKA.md §8）。多实例通过 FOR UPDATE SKIP LOCKED 领取。
package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/kafka"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/mysql/repository"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
)

// Publisher 是 Outbox Publisher。
type Publisher struct {
	repo         *repository.OutboxRepository
	producer     *kafka.Producer
	topics       kafka.Topics
	maxRetries   int
	backoff      time.Duration
	pollInterval time.Duration
	batchSize    int
	instanceID   string
	reg          *metrics.Registry
	log          *slog.Logger
}

// Config 是 Publisher 配置。
type Config struct {
	MaxRetries   int
	Backoff      time.Duration
	PollInterval time.Duration
	BatchSize    int
	InstanceID   string
}

// New 创建 Outbox Publisher。
func New(repo *repository.OutboxRepository, producer *kafka.Producer, topics kafka.Topics, cfg Config, reg *metrics.Registry, log *slog.Logger) *Publisher {
	return &Publisher{
		repo:         repo,
		producer:     producer,
		topics:       topics,
		maxRetries:   cfg.MaxRetries,
		backoff:      cfg.Backoff,
		pollInterval: cfg.PollInterval,
		batchSize:    cfg.BatchSize,
		instanceID:   cfg.InstanceID,
		reg:          reg,
		log:          log,
	}
}

// Run 周期领取并发布 persisted 事件，直至 ctx 取消。
func (p *Publisher) Run(appCtx context.Context) error {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-appCtx.Done():
			return nil
		case <-ticker.C:
			p.dispatch(appCtx)
		}
	}
}

// dispatch 领取一批任务并逐个发布。
func (p *Publisher) dispatch(ctx context.Context) {
	records, err := p.repo.Claim(ctx, p.batchSize, p.instanceID)
	if err != nil {
		p.log.Error("outbox claim failed", "error", err.Error())
		return
	}
	for _, rec := range records {
		p.publishOne(ctx, rec)
	}
	// 指标：待投递积压与最老年龄（GOCHAT_KAFKA.md §11.2）
	if p.reg != nil {
		if n, err := p.repo.PendingCount(ctx); err == nil {
			p.reg.Gauge(metrics.NameOutboxPending, "待投递 Outbox 数量", map[string]string{"event_type": "message_persisted"}).Set(n)
		}
		if age, err := p.repo.OldestPendingAge(ctx); err == nil {
			p.reg.Gauge(metrics.NameOutboxOldestAge, "最老待投递记录年龄", map[string]string{"event_type": "message_persisted"}).Set(int64(age))
		}
	}
}

func (p *Publisher) publishOne(ctx context.Context, rec repository.OutboxRecord) {
	event := rec.Payload
	env, err := kafka.NewEnvelope(kafka.EventPersisted, "outbox-publisher", event.ConversationID, event)
	if err != nil {
		_ = p.repo.MarkFailed(ctx, rec.MessageID, rec.EventType, "envelope 构造失败", p.maxRetries, p.backoff)
		return
	}
	if err := p.producer.PublishPersisted(ctx, env); err != nil {
		p.log.Warn("outbox publish failed", "message_id", rec.MessageID, "error", err.Error())
		if p.reg != nil {
			p.reg.Counter(metrics.NameOutboxPublishError, "Outbox 发布失败数", nil).Inc()
		}
		// 留在 Outbox 退避重试；超过上限进入死信（GOCHAT_DATABASE.md §9.3）
		_ = p.repo.MarkFailed(ctx, rec.MessageID, rec.EventType, err.Error(), p.maxRetries, p.backoff)
		return
	}
	if err := p.repo.MarkPublished(ctx, rec.MessageID, rec.EventType); err != nil {
		// 发布成功但状态更新失败：下游按 message_id 幂等，允许重复投递
		p.log.Warn("outbox mark published failed", "message_id", rec.MessageID, "error", err.Error())
	}
}
