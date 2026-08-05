package repository

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"

	"gorm.io/gorm"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/mysql/model"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
)

// OutboxRecord 是领取到的一条待投递记录。
type OutboxRecord struct {
	MessageID int64
	EventType int8
	Payload   message.MessagePersistedEvent
}

// OutboxRepository 负责 Outbox 领取、发布成功/失败状态推进
// （GOCHAT_DATABASE.md §9.3 / GOCHAT_KAFKA.md §8）。
type OutboxRepository struct {
	db *gorm.DB
}

func NewOutboxRepository(db *gorm.DB) *OutboxRepository {
	return &OutboxRepository{db: db}
}

// Claim 领取一批到期待投递任务：SELECT ... FOR UPDATE SKIP LOCKED 后
// 立即写入 locked_by/locked_at 并提交领取事务；实际 Kafka 发布在事务外执行。
func (r *OutboxRepository) Claim(ctx context.Context, batchSize int, instanceID string) ([]OutboxRecord, error) {
	var records []OutboxRecord
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rows []model.MessageOutbox
		if err := tx.Raw(
			"SELECT message_id, event_type, payload "+
				"FROM message_outbox "+
				"WHERE status IN (?, ?) AND next_retry_at <= UTC_TIMESTAMP(3) "+
				"ORDER BY created_at, message_id "+
				"LIMIT ? "+
				"FOR UPDATE SKIP LOCKED",
			model.OutboxPending, model.OutboxRetrying, batchSize,
		).Scan(&rows).Error; err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		now := time.Now().UTC()
		for _, row := range rows {
			var payload message.MessagePersistedEvent
			if err := json.Unmarshal([]byte(row.Payload), &payload); err != nil {
				// payload 损坏：直接标记死信，避免阻塞队列
				if err := tx.Model(&model.MessageOutbox{}).
					Where("message_id = ? AND event_type = ?", row.MessageID, row.EventType).
					Updates(map[string]interface{}{
						"status":     model.OutboxDead,
						"last_error": "invalid payload: " + err.Error(),
						"locked_by":  nil,
					}).Error; err != nil {
					return err
				}
				continue
			}
			if err := tx.Model(&model.MessageOutbox{}).
				Where("message_id = ? AND event_type = ?", row.MessageID, row.EventType).
				Updates(map[string]interface{}{
					"status":    model.OutboxRetrying,
					"locked_by": instanceID,
					"locked_at": now,
				}).Error; err != nil {
				return err
			}
			records = append(records, OutboxRecord{
				MessageID: row.MessageID,
				EventType: row.EventType,
				Payload:   payload,
			})
		}
		return nil
	})
	if err != nil {
		return nil, errs.Internal(err)
	}
	return records, nil
}

// MarkPublished 发布成功后更新状态。
func (r *OutboxRepository) MarkPublished(ctx context.Context, messageID int64, eventType int8) error {
	err := r.db.WithContext(ctx).Model(&model.MessageOutbox{}).
		Where("message_id = ? AND event_type = ?", messageID, eventType).
		Updates(map[string]interface{}{
			"status":       model.OutboxPublished,
			"published_at": time.Now().UTC(),
			"locked_by":    nil,
			"locked_at":    nil,
			"last_error":   "",
		}).Error
	if err != nil {
		return errs.Internal(err)
	}
	return nil
}

// MarkFailed 发布失败：重试次数 +1；超过上限进入死信（GOCHAT_DATABASE.md §9.3）。
func (r *OutboxRepository) MarkFailed(ctx context.Context, messageID int64, eventType int8, errMsg string, maxRetries int, backoff time.Duration) error {
	now := time.Now().UTC()

	// 读取当前重试次数，计算指数退避 + 随机抖动（GOCHAT_RESILIENCE.md §10.2）
	var cur model.MessageOutbox
	if err := r.db.WithContext(ctx).
		Where("message_id = ? AND event_type = ?", messageID, eventType).
		Select("retry_count").Take(&cur).Error; err != nil {
		return errs.Internal(err)
	}
	nextRetries := cur.RetryCount + 1
	delay := backoff * time.Duration(1<<minInt(nextRetries, 6))
	jitter := time.Duration(rand.Int63n(int64(delay / 4)))
	next := now.Add(delay + jitter)

	newStatus := model.OutboxRetrying
	nextRetryAt := next
	if nextRetries >= maxRetries {
		newStatus = model.OutboxDead
		nextRetryAt = now
	}

	err := r.db.WithContext(ctx).Model(&model.MessageOutbox{}).
		Where("message_id = ? AND event_type = ?", messageID, eventType).
		Updates(map[string]interface{}{
			"status":        newStatus,
			"retry_count":   nextRetries,
			"next_retry_at": nextRetryAt,
			"last_error":    truncateErr(errMsg),
			"locked_by":     nil,
			"locked_at":     nil,
		}).Error
	if err != nil {
		return errs.Internal(err)
	}
	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func truncateErr(s string) string {
	if len(s) > 1024 {
		return s[:1024]
	}
	return s
}

// PendingCount 返回待投递/重试中的记录数（供指标）。
func (r *OutboxRepository) PendingCount(ctx context.Context) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).Model(&model.MessageOutbox{}).
		Where("status IN (?, ?)", model.OutboxPending, model.OutboxRetrying).
		Count(&n).Error
	if err != nil {
		return 0, errs.Internal(err)
	}
	return n, nil
}

// OldestPendingAge 返回最老待投递记录的年龄（秒，供指标）。
func (r *OutboxRepository) OldestPendingAge(ctx context.Context) (float64, error) {
	var createdAt time.Time
	err := r.db.WithContext(ctx).Model(&model.MessageOutbox{}).
		Where("status IN (?, ?)", model.OutboxPending, model.OutboxRetrying).
		Order("created_at ASC").Limit(1).
		Pluck("created_at", &createdAt).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return 0, nil
		}
		return 0, errs.Internal(err)
	}
	return time.Since(createdAt).Seconds(), nil
}
