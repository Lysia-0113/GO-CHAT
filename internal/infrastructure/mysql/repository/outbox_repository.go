package repository

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/mysql/model"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
)

// OutboxRecord 是领取到的一条待投递记录。
type OutboxRecord struct {
	MessageID      int64
	EventType      int8
	Status         int8
	RetryCount     int
	LastError      string
	ShardID        int
	ConversationID int64
	Payload        message.MessagePersistedEvent
	RawPayload     json.RawMessage
}

type outboxCandidate struct {
	ShardID        int   `gorm:"column:shard_id"`
	ConversationID int64 `gorm:"column:conversation_id"`
}

// OutboxRepository 负责 Outbox 领取、发布成功/失败状态推进。
// 候选消息按固定 shard 分派；领取时再锁 conversation 行和该会话最早的
// 非终态 Outbox 行，因此同会话严格按 seq 推进，其他会话可并行。
type OutboxRepository struct {
	db *gorm.DB
}

func NewOutboxRepository(db *gorm.DB) *OutboxRepository {
	return &OutboxRepository{db: db}
}

// Claim 领取一批不同会话的队头任务。只有 Pending、Retrying 和
// DLQPending 属于非终态；DLQPending 在 DLQ 写入成功前仍挡住后续 seq。
func (r *OutboxRepository) Claim(ctx context.Context, batchSize int, shardIDs []int, ownerID string, lease time.Duration) ([]OutboxRecord, error) {
	if batchSize <= 0 || len(shardIDs) == 0 {
		return nil, nil
	}
	if lease <= 0 {
		lease = 5 * time.Second
	}

	var records []OutboxRecord
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		candidates, err := dueOutboxCandidates(tx, shardIDs, batchSize)
		if err != nil {
			return err
		}
		if len(candidates) == 0 {
			return nil
		}

		now := time.Now().UTC()
		for _, candidate := range candidates {
			// Persist 持有同一 conversation 行锁来分配 seq。锁住它可避免
			// Claim 检查队头时与一笔正在提交的新消息事务交错。
			var conv model.Conversation
			err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
				Where("id = ?", candidate.ConversationID).Take(&conv).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				continue
			}
			if err != nil {
				return err
			}

			// 当前读 + 行锁：候选查询只是缩小范围，真正的队头必须在锁内
			// 按 payload.seq 重读，避免并发发布者或 Persist 造成跳 seq。
			var row model.MessageOutbox
			err = tx.Model(&model.MessageOutbox{}).
				Select("message_id, event_type, shard_id, payload, status, retry_count, next_retry_at, last_error").
				Where("shard_id = ? AND CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, '$.conversation_id')) AS UNSIGNED) = ?", candidate.ShardID, candidate.ConversationID).
				Where("status IN (?, ?, ?)", model.OutboxPending, model.OutboxRetrying, model.OutboxDLQPending).
				Order("CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, '$.seq')) AS UNSIGNED) ASC").
				Clauses(clause.Locking{Strength: "UPDATE"}).
				Limit(1).Take(&row).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if row.NextRetryAt.After(now) {
				continue
			}

			status := row.Status
			var payload message.MessagePersistedEvent
			unmarshalErr := json.Unmarshal([]byte(row.Payload), &payload)
			if unmarshalErr == nil && (payload.MessageID != row.MessageID || payload.ConversationID != candidate.ConversationID || payload.Seq <= 0) {
				unmarshalErr = errors.New("outbox payload identity does not match its row")
			}
			lastError := row.LastError
			if unmarshalErr != nil {
				// 保留原始 JSON 并将损坏事件送 DLQ；只有 DLQ 成功后才放行
				// 该会话的下一条消息。
				status = model.OutboxDLQPending
				lastError = truncateErr("invalid outbox payload: " + unmarshalErr.Error())
			}
			if status != model.OutboxDLQPending {
				status = model.OutboxRetrying
			}
			updates := map[string]interface{}{
				"status":        status,
				"locked_by":     ownerID,
				"locked_at":     now,
				"next_retry_at": now.Add(lease),
			}
			if unmarshalErr != nil {
				updates["last_error"] = lastError
			}
			if err := tx.Model(&model.MessageOutbox{}).
				Where("message_id = ? AND event_type = ?", row.MessageID, row.EventType).
				Updates(updates).Error; err != nil {
				return err
			}

			records = append(records, OutboxRecord{
				MessageID:      row.MessageID,
				EventType:      row.EventType,
				Status:         status,
				RetryCount:     row.RetryCount,
				LastError:      lastError,
				ShardID:        row.ShardID,
				ConversationID: candidate.ConversationID,
				Payload:        payload,
				RawPayload:     json.RawMessage(append([]byte(nil), row.Payload...)),
			})
		}
		return nil
	})
	if err != nil {
		return nil, errs.Internal(err)
	}
	return records, nil
}

func dueOutboxCandidates(tx *gorm.DB, shardIDs []int, limit int) ([]outboxCandidate, error) {
	marks := strings.TrimRight(strings.Repeat("?,", len(shardIDs)), ",")
	query := "SELECT o.shard_id, " +
		"CAST(JSON_UNQUOTE(JSON_EXTRACT(o.payload, '$.conversation_id')) AS UNSIGNED) AS conversation_id " +
		"FROM message_outbox AS o " +
		"WHERE o.shard_id IN (" + marks + ") " +
		"AND o.status IN (?, ?, ?) AND o.next_retry_at <= UTC_TIMESTAMP(3) " +
		"AND NOT EXISTS (" +
		"SELECT 1 FROM message_outbox AS prev " +
		"WHERE prev.shard_id = o.shard_id " +
		"AND JSON_UNQUOTE(JSON_EXTRACT(prev.payload, '$.conversation_id')) = JSON_UNQUOTE(JSON_EXTRACT(o.payload, '$.conversation_id')) " +
		"AND CAST(JSON_UNQUOTE(JSON_EXTRACT(prev.payload, '$.seq')) AS UNSIGNED) < CAST(JSON_UNQUOTE(JSON_EXTRACT(o.payload, '$.seq')) AS UNSIGNED) " +
		"AND prev.status IN (?, ?, ?)) " +
		"GROUP BY o.shard_id, conversation_id " +
		"ORDER BY MIN(o.created_at), conversation_id LIMIT ?"

	args := make([]interface{}, 0, len(shardIDs)+7)
	for _, shardID := range shardIDs {
		args = append(args, shardID)
	}
	args = append(args,
		model.OutboxPending, model.OutboxRetrying, model.OutboxDLQPending,
		model.OutboxPending, model.OutboxRetrying, model.OutboxDLQPending,
		limit,
	)
	var candidates []outboxCandidate
	if err := tx.Raw(query, args...).Scan(&candidates).Error; err != nil {
		return nil, err
	}
	return candidates, nil
}

// MarkPublished 发布成功后更新状态。lockedBy 是带随机运行 token 的 fencing token。
func (r *OutboxRepository) MarkPublished(ctx context.Context, messageID int64, eventType int8, lockedBy string) (bool, error) {
	res := r.db.WithContext(ctx).Model(&model.MessageOutbox{}).
		Where("message_id = ? AND event_type = ? AND locked_by = ?", messageID, eventType, lockedBy).
		Updates(map[string]interface{}{
			"status":       model.OutboxPublished,
			"published_at": time.Now().UTC(),
			"locked_by":    nil,
			"locked_at":    nil,
			"last_error":   "",
		})
	if res.Error != nil {
		return false, errs.Internal(res.Error)
	}
	return res.RowsAffected > 0, nil
}

// MarkFailed 将普通发布失败改为退避重试；达到上限后改成 DLQPending，
// 保留当前 owner 和租约，让当前 worker 立即尝试写 DLQ，防止后续 seq 越过。
func (r *OutboxRepository) MarkFailed(ctx context.Context, messageID int64, eventType int8, errMsg string, maxRetries int, backoff, lease time.Duration, lockedBy string) (bool, bool, error) {
	now := time.Now().UTC()
	var cur model.MessageOutbox
	if err := r.db.WithContext(ctx).
		Where("message_id = ? AND event_type = ? AND locked_by = ?", messageID, eventType, lockedBy).
		Select("retry_count").Take(&cur).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, false, nil
		}
		return false, false, errs.Internal(err)
	}

	nextRetries := cur.RetryCount + 1
	newStatus := model.OutboxRetrying
	updates := map[string]interface{}{
		"status":      newStatus,
		"retry_count": nextRetries,
		"last_error":  truncateErr(errMsg),
		"locked_by":   nil,
		"locked_at":   nil,
	}
	needsDLQ := nextRetries >= maxRetries
	if needsDLQ {
		if lease <= 0 {
			lease = 5 * time.Second
		}
		updates["status"] = model.OutboxDLQPending
		updates["next_retry_at"] = now.Add(lease)
		updates["locked_by"] = lockedBy
		updates["locked_at"] = now
	} else {
		delay := backoff * time.Duration(1<<minInt(nextRetries, 6))
		var jitter time.Duration
		if maxJitter := int64(delay / 4); maxJitter > 0 {
			jitter = time.Duration(rand.Int63n(maxJitter))
		}
		updates["next_retry_at"] = now.Add(delay + jitter)
	}

	res := r.db.WithContext(ctx).Model(&model.MessageOutbox{}).
		Where("message_id = ? AND event_type = ? AND locked_by = ?", messageID, eventType, lockedBy).
		Updates(updates)
	if res.Error != nil {
		return false, false, errs.Internal(res.Error)
	}
	applied := res.RowsAffected > 0
	return applied, applied && needsDLQ, nil
}

// RetryDLQ 在 DLQ 写入失败后保留队头，退避后只重试 DLQ，不再重复发布原事件。
func (r *OutboxRepository) RetryDLQ(ctx context.Context, messageID int64, eventType int8, backoff time.Duration, lockedBy string) (bool, error) {
	if backoff <= 0 {
		backoff = time.Second
	}
	res := r.db.WithContext(ctx).Model(&model.MessageOutbox{}).
		Where("message_id = ? AND event_type = ? AND status = ? AND locked_by = ?", messageID, eventType, model.OutboxDLQPending, lockedBy).
		Updates(map[string]interface{}{
			"next_retry_at": time.Now().UTC().Add(backoff),
			"locked_by":     nil,
			"locked_at":     nil,
		})
	if res.Error != nil {
		return false, errs.Internal(res.Error)
	}
	return res.RowsAffected > 0, nil
}

// MarkDead 将已成功写入 DLQ 的记录标为终态；此后同会话后续 seq 可被领取。
func (r *OutboxRepository) MarkDead(ctx context.Context, messageID int64, eventType int8, lockedBy string) (bool, error) {
	res := r.db.WithContext(ctx).Model(&model.MessageOutbox{}).
		Where("message_id = ? AND event_type = ? AND status = ? AND locked_by = ?", messageID, eventType, model.OutboxDLQPending, lockedBy).
		Updates(map[string]interface{}{
			"status":    model.OutboxDead,
			"locked_by": nil,
			"locked_at": nil,
		})
	if res.Error != nil {
		return false, errs.Internal(res.Error)
	}
	return res.RowsAffected > 0, nil
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

// PendingCount 返回仍需完成持久化 Topic 或 DLQ 发布的记录数。
func (r *OutboxRepository) PendingCount(ctx context.Context) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).Model(&model.MessageOutbox{}).
		Where("status IN (?, ?, ?)", model.OutboxPending, model.OutboxRetrying, model.OutboxDLQPending).
		Count(&n).Error
	if err != nil {
		return 0, errs.Internal(err)
	}
	return n, nil
}

// OldestPendingAge 返回最老未完成记录的年龄（秒，供指标）。
func (r *OutboxRepository) OldestPendingAge(ctx context.Context) (float64, error) {
	var createdAt time.Time
	err := r.db.WithContext(ctx).Model(&model.MessageOutbox{}).
		Where("status IN (?, ?, ?)", model.OutboxPending, model.OutboxRetrying, model.OutboxDLQPending).
		Order("created_at ASC").Limit(1).
		Pluck("created_at", &createdAt).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		return 0, errs.Internal(err)
	}
	return time.Since(createdAt).Seconds(), nil
}
