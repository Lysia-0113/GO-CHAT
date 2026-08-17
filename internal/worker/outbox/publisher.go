// Package outbox 是 Outbox Publisher：把 message_outbox 待投递记录发布到
// im.message.persisted（GOCHAT_KAFKA.md §8）。多实例通过 FOR UPDATE SKIP LOCKED
// 领取 + 发布租约（next_retry_at）保证同一行不被重复发布。
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
	claimLease   time.Duration
}

// Config 是 Publisher 配置。
// ClaimLease 是领取租约：Claim 后行在该时长内对其他 Publisher 不可见。
// 必须大于 Kafka 发布超时（ProducerTimeout），默认 5s。
type Config struct {
	MaxRetries   int
	Backoff      time.Duration
	PollInterval time.Duration
	BatchSize    int
	InstanceID   string
	ClaimLease   time.Duration
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
		claimLease:   cfg.ClaimLease,
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
	records, err := p.svcCtx.OutboxRepo.Claim(ctx, p.batchSize, p.instanceID, p.claimLease)
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
		p.markFailed(ctx, rec, "envelope 构造失败", err)
		return
	}
	if pubErr := p.svcCtx.Kafka.PublishPersisted(ctx, env); pubErr != nil {
		p.svcCtx.Log.Warn("outbox publish failed", "message_id", rec.MessageID, "error", pubErr.Error())
		metrics.OutboxPublishError.Inc()
		// 留在 Outbox 退避重试；超过上限进入死信（GOCHAT_DATABASE.md §9.3）
		p.markFailed(ctx, rec, pubErr.Error(), pubErr)
		return
	}
	if applied, mErr := p.svcCtx.OutboxRepo.MarkPublished(ctx, rec.MessageID, rec.EventType, p.instanceID); mErr != nil {
		// 发布成功但状态更新失败：下游按 message_id 幂等，允许重复投递
		p.svcCtx.Log.Warn("outbox mark published failed", "message_id", rec.MessageID, "error", mErr.Error())
	} else if !applied {
		// 所有权已转移（租约过期后行被其他 Publisher 重新领取）：新持有者会收尾，
		// 本次发布成功的副本由下游按 message_id 去重。
		p.svcCtx.Log.Debug("outbox ownership lost after publish", "message_id", rec.MessageID)
	}
}

// markFailed 记录发布失败：退避重试或超过上限进死信（GOCHAT_DATABASE.md §9.3）。
// 所有权已转移（租约过期被重新领取）时不处理，由新持有者负责重试。
func (p *Publisher) markFailed(ctx context.Context, rec repository.OutboxRecord, errMsg string, cause error) {
	applied, err := p.svcCtx.OutboxRepo.MarkFailed(ctx, rec.MessageID, rec.EventType, errMsg, p.maxRetries, p.backoff, p.instanceID)
	if err != nil {
		p.svcCtx.Log.Error("outbox mark failed error", "message_id", rec.MessageID, "error", err.Error(), "cause", cause.Error())
		return
	}
	if !applied {
		p.svcCtx.Log.Debug("outbox ownership lost on failure", "message_id", rec.MessageID, "cause", cause.Error())
	}
}
