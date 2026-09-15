// Package outbox 是 Outbox Publisher：把 message_outbox 待投递记录发布到
// im.message.persisted。MySQL 命名锁负责跨实例唯一分配全局 worker slot。
package outbox

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/kafka"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/mysql/model"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/mysql/repository"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
	"github.com/Lysia-0113/GO-CHAT/internal/svc"
)

var errNoWorkerSlot = errors.New("no outbox worker slot available")

// Publisher 是 Outbox Publisher。依赖经 svcCtx 服务定位器取用（GOCHAT_API.md §11.3）。
type Publisher struct {
	svcCtx             *svc.ServiceContext
	maxRetries         int
	backoff            time.Duration
	pollInterval       time.Duration
	batchSize          int
	workerCount        int
	publishConcurrency int
	claimLease         time.Duration
}

// Config 是 Publisher 配置。
// WorkerCount 是全局 worker slot 数，各实例必须设置成相同值；worker 通过
// MySQL GET_LOCK 自动获取空闲 slot，slot 映射到固定的会话 hash shards。
type Config struct {
	MaxRetries         int
	Backoff            time.Duration
	PollInterval       time.Duration
	BatchSize          int
	WorkerCount        int
	PublishConcurrency int
	ClaimLease         time.Duration
}

// New 创建 Outbox Publisher。
func New(svcCtx *svc.ServiceContext, cfg Config) *Publisher {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 10
	}
	if cfg.Backoff <= 0 {
		cfg.Backoff = 2 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 1
	}
	if cfg.PublishConcurrency <= 0 {
		cfg.PublishConcurrency = 8
	}
	if cfg.PublishConcurrency > cfg.BatchSize {
		cfg.PublishConcurrency = cfg.BatchSize
	}
	if cfg.ClaimLease <= 0 {
		cfg.ClaimLease = 5 * time.Second
	}
	return &Publisher{
		svcCtx:             svcCtx,
		maxRetries:         cfg.MaxRetries,
		backoff:            cfg.Backoff,
		pollInterval:       cfg.PollInterval,
		batchSize:          cfg.BatchSize,
		workerCount:        cfg.WorkerCount,
		publishConcurrency: cfg.PublishConcurrency,
		claimLease:         cfg.ClaimLease,
	}
}

// Run 周期领取并发布 persisted 事件，直至 ctx 取消。MySQL 命名锁保证同一
// worker slot 在所有进程中最多只有一个活跃持有者；进程退出后其他实例可接管。
func (p *Publisher) Run(appCtx context.Context) error {
	for appCtx.Err() == nil {
		conn, workerID, lockName, err := p.acquireWorkerSlot(appCtx)
		if err != nil {
			if appCtx.Err() != nil {
				return nil
			}
			if !errors.Is(err, errNoWorkerSlot) {
				p.svcCtx.Log.Error("outbox worker slot acquisition failed", "error", err.Error())
			}
			if !waitContext(appCtx, p.pollInterval) {
				return nil
			}
			continue
		}

		ownerID := fmt.Sprintf("obx-%d-%s", workerID, uuid.NewString())
		shards := message.OutboxShardsForWorker(workerID, p.workerCount)
		p.svcCtx.Log.Info("outbox worker slot acquired", "worker_id", workerID, "shards", len(shards))
		p.runSlot(appCtx, ownerID, shards, conn, lockName)
		p.releaseWorkerSlot(conn, lockName)
	}
	return nil
}

func (p *Publisher) acquireWorkerSlot(ctx context.Context) (*sql.Conn, int, string, error) {
	sqlDB, err := p.svcCtx.DB.DB()
	if err != nil {
		return nil, -1, "", err
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return nil, -1, "", err
	}

	var databaseName string
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&databaseName); err != nil {
		_ = conn.Close()
		return nil, -1, "", err
	}
	// Named lock 长度限制为 64；使用 schema hash 避免不同库冲突，也避免长库名超限。
	schemaHash := sha256.Sum256([]byte(databaseName))
	namespace := hex.EncodeToString(schemaHash[:8])
	for workerID := 0; workerID < p.workerCount; workerID++ {
		lockName := fmt.Sprintf("gochat:outbox:%s:%d", namespace, workerID)
		var acquired sql.NullInt64
		if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", lockName).Scan(&acquired); err != nil {
			_ = conn.Close()
			return nil, -1, "", err
		}
		if acquired.Valid && acquired.Int64 == 1 {
			return conn, workerID, lockName, nil
		}
	}
	_ = conn.Close()
	return nil, -1, "", errNoWorkerSlot
}

func (p *Publisher) releaseWorkerSlot(conn *sql.Conn, lockName string) {
	if conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var released sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", lockName).Scan(&released); err != nil {
		p.svcCtx.Log.Warn("outbox worker slot release failed", "error", err.Error())
	}
	if err := conn.Close(); err != nil {
		p.svcCtx.Log.Warn("outbox worker slot connection close failed", "error", err.Error())
	}
}

func (p *Publisher) runSlot(parent context.Context, ownerID string, shardIDs []int, lockConn *sql.Conn, lockName string) {
	if err := checkWorkerLock(parent, lockConn, lockName); err != nil {
		p.svcCtx.Log.Error("outbox worker slot lock is not held", "error", err.Error())
		return
	}
	ctx, cancel := context.WithCancel(parent)
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		interval := p.pollInterval
		if interval > time.Second {
			interval = time.Second
		}
		if interval < 200*time.Millisecond {
			interval = 200 * time.Millisecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := checkWorkerLock(ctx, lockConn, lockName); err != nil {
					if ctx.Err() == nil {
						p.svcCtx.Log.Error("outbox worker slot lock lost", "error", err.Error())
					}
					cancel()
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		<-monitorDone
	}()

	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.dispatch(ctx, ownerID, shardIDs)
		}
	}
}

func checkWorkerLock(ctx context.Context, conn *sql.Conn, lockName string) error {
	var owned sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?) = CONNECTION_ID()", lockName).Scan(&owned); err != nil {
		return err
	}
	if !owned.Valid || owned.Int64 != 1 {
		return errors.New("MySQL worker slot lock is no longer owned by this connection")
	}
	return nil
}

// dispatch 领取一批不同会话的队头消息，并在固定大小的并发池内发布。
func (p *Publisher) dispatch(ctx context.Context, ownerID string, shardIDs []int) {
	claimSize := p.batchSize
	if claimSize > p.publishConcurrency {
		claimSize = p.publishConcurrency
	}
	records, err := p.svcCtx.OutboxRepo.Claim(ctx, claimSize, shardIDs, ownerID, p.claimLease)
	if err != nil {
		p.svcCtx.Log.Error("outbox claim failed", "error", err.Error())
		return
	}
	var wg sync.WaitGroup
	for _, rec := range records {
		rec := rec
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.publishOne(ctx, rec, ownerID)
		}()
	}
	wg.Wait()

	if n, err := p.svcCtx.OutboxRepo.PendingCount(ctx); err == nil {
		metrics.OutboxPending.WithLabelValues("message_persisted").Set(float64(n))
	}
	if age, err := p.svcCtx.OutboxRepo.OldestPendingAge(ctx); err == nil {
		metrics.OutboxOldestAge.WithLabelValues("message_persisted").Set(float64(age))
	}
}

func (p *Publisher) publishOne(ctx context.Context, rec repository.OutboxRecord, ownerID string) {
	if rec.Status == model.OutboxDLQPending {
		p.publishDLQ(ctx, rec, ownerID, nil)
		return
	}

	event := rec.Payload
	env, err := kafka.NewEnvelope(kafka.EventPersisted, "outbox-publisher", event.ConversationID, event)
	if err != nil {
		p.markFailed(ctx, rec, ownerID, "persisted envelope 构造失败: "+err.Error(), err, rec.RawPayload)
		return
	}
	original, _ := env.Marshal()
	if pubErr := p.svcCtx.Kafka.PublishPersisted(ctx, env); pubErr != nil {
		p.svcCtx.Log.Warn("outbox publish failed", "message_id", rec.MessageID, "error", pubErr.Error())
		metrics.OutboxPublishError.Inc()
		p.markFailed(ctx, rec, ownerID, pubErr.Error(), pubErr, original)
		return
	}
	if applied, mErr := p.svcCtx.OutboxRepo.MarkPublished(ctx, rec.MessageID, rec.EventType, ownerID); mErr != nil {
		p.svcCtx.Log.Warn("outbox mark published failed", "message_id", rec.MessageID, "error", mErr.Error())
	} else if !applied {
		p.svcCtx.Log.Debug("outbox ownership lost after publish", "message_id", rec.MessageID)
	}
}

func (p *Publisher) markFailed(ctx context.Context, rec repository.OutboxRecord, ownerID, errMsg string, cause error, original []byte) {
	applied, needsDLQ, err := p.svcCtx.OutboxRepo.MarkFailed(ctx, rec.MessageID, rec.EventType, errMsg,
		p.maxRetries, p.backoff, p.claimLease, ownerID)
	if err != nil {
		p.svcCtx.Log.Error("outbox mark failed error", "message_id", rec.MessageID, "error", err.Error(), "cause", cause.Error())
		return
	}
	if !applied {
		p.svcCtx.Log.Debug("outbox ownership lost on failure", "message_id", rec.MessageID, "cause", cause.Error())
		return
	}
	if needsDLQ {
		rec.Status = model.OutboxDLQPending
		rec.RetryCount++
		rec.LastError = errMsg
		p.publishDLQ(ctx, rec, ownerID, original)
	}
}

func (p *Publisher) publishDLQ(ctx context.Context, rec repository.OutboxRecord, ownerID string, original []byte) {
	if len(original) == 0 {
		if rec.Payload.ConversationID != 0 {
			if env, err := kafka.NewEnvelope(kafka.EventPersisted, "outbox-publisher", rec.ConversationID, rec.Payload); err == nil {
				original, _ = env.Marshal()
			}
		}
		if len(original) == 0 {
			original = append([]byte(nil), rec.RawPayload...)
		}
	}
	conversationID := rec.ConversationID
	if conversationID == 0 {
		conversationID = rec.Payload.ConversationID
	}
	payload := kafka.DLQPayload{
		FailedTopic:     p.svcCtx.Topics.Persisted(),
		FailedPartition: -1,
		FailedOffset:    -1,
		RetryCount:      rec.RetryCount,
		ErrorCode:       "OUTBOX_PUBLISH_FAILED",
		ErrorMessage:    rec.LastError,
		FailedAt:        time.Now().UTC(),
		OriginalEvent:   json.RawMessage(original),
	}
	dlqEnv, err := kafka.NewEnvelope(kafka.EventDLQ, "outbox-publisher", conversationID, payload)
	if err == nil {
		err = p.svcCtx.Kafka.PublishDLQ(ctx, dlqEnv)
	}
	if err != nil {
		p.svcCtx.Log.Error("outbox dlq publish failed", "message_id", rec.MessageID, "error", err.Error())
		if _, retryErr := p.svcCtx.OutboxRepo.RetryDLQ(ctx, rec.MessageID, rec.EventType, p.backoff, ownerID); retryErr != nil {
			p.svcCtx.Log.Error("outbox dlq retry scheduling failed", "message_id", rec.MessageID, "error", retryErr.Error())
		}
		return
	}
	metrics.KafkaDLQ.WithLabelValues(payload.FailedTopic, payload.ErrorCode).Inc()
	if applied, markErr := p.svcCtx.OutboxRepo.MarkDead(ctx, rec.MessageID, rec.EventType, ownerID); markErr != nil {
		p.svcCtx.Log.Error("outbox mark dead failed after dlq publish", "message_id", rec.MessageID, "error", markErr.Error())
	} else if !applied {
		p.svcCtx.Log.Debug("outbox ownership lost after dlq publish", "message_id", rec.MessageID)
	}
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
