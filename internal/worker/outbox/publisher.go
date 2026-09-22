// Package outbox 是 Outbox Publisher：把 message_outbox 待投递记录查询 Presence
// 后直接发布到 im.message.push 的目标 Gateway partition。MySQL 命名锁负责
// 跨实例唯一分配全局 worker slot。
package outbox

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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

// Run 周期领取并发布 push 事件，直至 ctx 取消。MySQL 命名锁保证同一
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
		metrics.OutboxPending.WithLabelValues("message_push").Set(float64(n))
	}
	if age, err := p.svcCtx.OutboxRepo.OldestPendingAge(ctx); err == nil {
		metrics.OutboxOldestAge.WithLabelValues("message_push").Set(float64(age))
	}
}

func (p *Publisher) publishOne(ctx context.Context, rec repository.OutboxRecord, ownerID string) {
	if rec.Status == model.OutboxDLQPending {
		p.publishDLQ(ctx, rec, ownerID, nil)
		return
	}

	event := rec.Payload
	targets, err := p.routeTargets(ctx, event)
	if err != nil {
		p.svcCtx.Log.Warn("outbox route lookup failed", "message_id", rec.MessageID, "error", err.Error())
		metrics.OutboxPublishError.Inc()
		p.markFailed(ctx, rec, ownerID, err.Error(), err, pushEnvelopeFallback(event, rec.RawPayload))
		return
	}

	// 没有在线用户时无需向 Kafka 写空事件；消息已持久化，客户端重连后
	// 通过 Pull 补齐。存在多个 Gateway 时，逐 partition 发布同一条消息的
	// 用户子集；任一 partition 发布失败都会重试整条 Outbox，Gateway 以
	// message_id 做短期幂等。
	var original []byte
	for partition, userIDs := range targets {
		pushEvent := toPushEvent(event, userIDs)
		env, envelopeErr := kafka.NewEnvelope(kafka.EventPush, "outbox-publisher", event.ConversationID, pushEvent)
		if envelopeErr != nil {
			p.markFailed(ctx, rec, ownerID, "push envelope 构造失败: "+envelopeErr.Error(), envelopeErr, original)
			return
		}
		// 记录当前 partition 的完整 Envelope，部分 partition 成功而后续
		// 失败时，DLQ 至少保留最后一次实际尝试的目标用户集合。
		original, _ = env.Marshal()
		if pubErr := p.svcCtx.Kafka.PublishPush(ctx, partition, env); pubErr != nil {
			p.svcCtx.Log.Warn("outbox push publish failed", "message_id", rec.MessageID, "partition", partition, "error", pubErr.Error())
			metrics.OutboxPublishError.Inc()
			p.markFailed(ctx, rec, ownerID, pubErr.Error(), pubErr, original)
			return
		}
	}
	if applied, mErr := p.svcCtx.OutboxRepo.MarkPublished(ctx, rec.MessageID, rec.EventType, ownerID); mErr != nil {
		p.svcCtx.Log.Warn("outbox mark published failed", "message_id", rec.MessageID, "error", mErr.Error())
	} else if !applied {
		p.svcCtx.Log.Debug("outbox ownership lost after publish", "message_id", rec.MessageID)
	}
}

// routeTargets 查询当前 Presence，把成员用户按固定 Gateway partition 聚合。
// Presence 不一致时允许产生空投递或过期路由，Gateway 侧没有本地连接即丢弃。
func (p *Publisher) routeTargets(ctx context.Context, event message.MessagePersistedEvent) (map[int][]int64, error) {
	if p.svcCtx.Presence == nil {
		return nil, errors.New("presence registry is not configured")
	}
	memberIDs := event.MemberIDs
	if len(memberIDs) == 0 {
		if p.svcCtx.ConvRepo == nil {
			return nil, errors.New("outbox event has no member snapshot")
		}
		var err error
		memberIDs, err = p.svcCtx.ConvRepo.ListMemberIDs(ctx, event.ConversationID)
		if err != nil {
			return nil, err
		}
	}
	partitionCount := p.svcCtx.Config.Kafka.PushPartitions
	if partitionCount <= 0 {
		partitionCount = 1
	}
	routesByUser, err := p.svcCtx.Presence.OnlineConnectionsBatch(ctx, memberIDs)
	if err != nil {
		return nil, err
	}
	grouped := make(map[int]map[int64]struct{})
	for userID, routes := range routesByUser {
		for _, route := range routes {
			if route.PartitionID < 0 || route.PartitionID >= partitionCount {
				// Presence 可能保留旧连接的短暂快照。无效 partition 没有
				// 可投递目标，按离线处理并依赖 after_seq 补偿；不要因为一条
				// 脏路由阻塞整个会话的 Outbox 队头。
				p.svcCtx.Log.Warn("ignore invalid presence partition",
					"user_id", userID, "partition", route.PartitionID, "partition_count", partitionCount)
				continue
			}
			if grouped[route.PartitionID] == nil {
				grouped[route.PartitionID] = make(map[int64]struct{})
			}
			grouped[route.PartitionID][userID] = struct{}{}
		}
	}
	result := make(map[int][]int64, len(grouped))
	for partition, users := range grouped {
		ids := make([]int64, 0, len(users))
		for userID := range users {
			ids = append(ids, userID)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		result[partition] = ids
	}
	return result, nil
}

func toPushEvent(event message.MessagePersistedEvent, userIDs []int64) message.MessagePushEvent {
	return message.MessagePushEvent{
		MessageID:       event.MessageID,
		Seq:             event.Seq,
		SenderID:        event.SenderID,
		ClientMessageID: event.ClientMessageID,
		ConversationID:  event.ConversationID,
		MessageType:     event.MessageType,
		Content:         event.Content,
		ContentPreview:  event.ContentPreview,
		CreatedAt:       event.CreatedAt,
		TargetUserIDs:   userIDs,
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
			pushEvent := toPushEvent(rec.Payload, nil)
			if env, err := kafka.NewEnvelope(kafka.EventPush, "outbox-publisher", rec.ConversationID, pushEvent); err == nil {
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
		FailedTopic:     p.svcCtx.Topics.Push(),
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

func pushEnvelopeFallback(event message.MessagePersistedEvent, fallback []byte) []byte {
	pushEvent := toPushEvent(event, nil)
	if env, err := kafka.NewEnvelope(kafka.EventPush, "outbox-publisher", event.ConversationID, pushEvent); err == nil {
		if raw, marshalErr := env.Marshal(); marshalErr == nil {
			return raw
		}
	}
	return append([]byte(nil), fallback...)
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
