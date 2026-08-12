// Package outbox 是 Outbox Publisher：把 message_outbox 待投递记录发布到
// im.message.persisted（GOCHAT_KAFKA.md §8）。多实例通过 FOR UPDATE SKIP LOCKED 领取。
package outbox

import (
	"context"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/kafka"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/mysql/repository"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
	"github.com/Lysia-0113/GO-CHAT/internal/svc"
)

// Publisher 是 Outbox Publisher。依赖经 svcCtx 服务定位器取用（GOCHAT_API.md §11.3）。
type Publisher struct {
	svcCtx       *svc.ServiceContext
	maxRetries   int
	backoff      time.Duration
	pollInterval time.Duration
	batchSize    int
	instanceID   string
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
func New(svcCtx *svc.ServiceContext, cfg Config) *Publisher {
	return &Publisher{
		svcCtx:       svcCtx,
		maxRetries:   cfg.MaxRetries,
		backoff:      cfg.Backoff,
		pollInterval: cfg.PollInterval,
		batchSize:    cfg.BatchSize,
		instanceID:   cfg.InstanceID,
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
	records, err := p.svcCtx.OutboxRepo.Claim(ctx, p.batchSize, p.instanceID)
	if err != nil {
		p.svcCtx.Log.Error("outbox claim failed", "error", err.Error())
		return
	}
	for _, rec := range records {
		p.publishOne(ctx, rec)
	}
	// 指标：待投递积压与最老年龄（GOCHAT_KAFKA.md §11.2）
	if n, err := p.svcCtx.OutboxRepo.PendingCount(ctx); err == nil {
		metrics.OutboxPending.WithLabelValues("message_persisted").Set(float64(n))
	}
	if age, err := p.svcCtx.OutboxRepo.OldestPendingAge(ctx); err == nil {
		metrics.OutboxOldestAge.WithLabelValues("message_persisted").Set(float64(age))
	}
}

func (p *Publisher) publishOne(ctx context.Context, rec repository.OutboxRecord) {
	event := rec.Payload
	env, err := kafka.NewEnvelope(kafka.EventPersisted, "outbox-publisher", event.ConversationID, event)
	if err != nil {
		_ = p.svcCtx.OutboxRepo.MarkFailed(ctx, rec.MessageID, rec.EventType, "envelope 构造失败", p.maxRetries, p.backoff)
		return
	}
	if err := p.svcCtx.Kafka.PublishPersisted(ctx, env); err != nil {
		p.svcCtx.Log.Warn("outbox publish failed", "message_id", rec.MessageID, "error", err.Error())
		metrics.OutboxPublishError.Inc()
		// 留在 Outbox 退避重试；超过上限进入死信（GOCHAT_DATABASE.md §9.3）
		_ = p.svcCtx.OutboxRepo.MarkFailed(ctx, rec.MessageID, rec.EventType, err.Error(), p.maxRetries, p.backoff)
		return
	}
	if err := p.svcCtx.OutboxRepo.MarkPublished(ctx, rec.MessageID, rec.EventType); err != nil {
		// 发布成功但状态更新失败：下游按 message_id 幂等，允许重复投递
		p.svcCtx.Log.Warn("outbox mark published failed", "message_id", rec.MessageID, "error", err.Error())
	}
}
