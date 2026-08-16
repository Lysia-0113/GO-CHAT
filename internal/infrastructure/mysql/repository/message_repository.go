package repository

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/mysql/model"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
)

// MessageRepository 实现 message.MessageRepository。
type MessageRepository struct {
	db *gorm.DB
}

func NewMessageRepository(db *gorm.DB) *MessageRepository {
	return &MessageRepository{db: db}
}

func (r *MessageRepository) FindByID(ctx context.Context, messageID int64) (*message.Message, error) {
	var m model.Message
	err := r.db.WithContext(ctx).Where("id = ?", messageID).Take(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errs.Internal(err)
	}
	return toDomainMessage(&m), nil
}

func (r *MessageRepository) FindByClientMessageID(ctx context.Context, senderID int64, clientMessageID string) (*message.Message, error) {
	raw, err := uuidToBytes(clientMessageID)
	if err != nil {
		return nil, errs.New(errs.InvalidArgument, "client_msg_id 格式无效")
	}
	var m model.Message
	err = r.db.WithContext(ctx).
		Where("sender_id = ? AND client_msg_id = ?", senderID, raw).
		Take(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errs.Internal(err)
	}
	return toDomainMessage(&m), nil
}

func (r *MessageRepository) ListBefore(ctx context.Context, conversationID, beforeSeq int64, limit int) ([]message.Message, error) {
	start := time.Now()
	defer func() {
		metrics.DependencyDuration.WithLabelValues("mysql_history_query").Observe(time.Since(start).Seconds())
	}()
	q := r.db.WithContext(ctx).Where("conversation_id = ?", conversationID)
	if beforeSeq > 0 {
		q = q.Where("seq < ?", beforeSeq)
	}
	var rows []model.Message
	err := q.Order("seq DESC").Limit(limit).Find(&rows).Error
	if err != nil {
		return nil, errs.Internal(err)
	}
	return toDomainMessages(rows), nil
}

func (r *MessageRepository) ListAfter(ctx context.Context, conversationID, afterSeq int64, limit int) ([]message.Message, error) {
	start := time.Now()
	defer func() {
		metrics.DependencyDuration.WithLabelValues("mysql_history_query").Observe(time.Since(start).Seconds())
	}()
	var rows []model.Message
	err := r.db.WithContext(ctx).
		Where("conversation_id = ? AND seq > ?", conversationID, afterSeq).
		Order("seq ASC").Limit(limit).Find(&rows).Error
	if err != nil {
		return nil, errs.Internal(err)
	}
	return toDomainMessages(rows), nil
}

// Persist 执行消息持久化事务（GOCHAT_DATABASE.md §10）：
//
//	BEGIN
//	1. 幂等检查：sender_id + client_msg_id 已存在 → 复用原记录
//	2. SELECT conversations.last_seq FOR UPDATE
//	3. next_seq = last_seq + 1
//	4. INSERT messages
//	5. UPDATE conversations（last_seq、最新消息摘要）
//	6. INSERT message_outbox（message_persisted 事件）
//	COMMIT
//
// 任一错误整体回滚，不允许留下"已递增但无消息"的 last_seq。
// 事务内唯一索引冲突同样回滚后复用已存在消息。
func (r *MessageRepository) Persist(ctx context.Context, input message.PersistInput) (*message.Message, error) {
	start := time.Now()
	defer func() {
		metrics.DependencyDuration.WithLabelValues("mysql_persist_tx").Observe(time.Since(start).Seconds())
	}()
	m := input.Message
	rawID, err := uuidToBytes(m.ClientMessageID)
	if err != nil {
		return nil, errs.New(errs.InvalidArgument, "client_msg_id 格式无效")
	}

	var result *message.Message
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. 幂等检查（事务内再做一次，防重复消费窗口）
		var existing model.Message
		queryErr := tx.Where("sender_id = ? AND client_msg_id = ?", m.SenderID, rawID).Take(&existing).Error
		if queryErr == nil {
			result = toDomainMessage(&existing)
			return nil
		}
		if !errors.Is(queryErr, gorm.ErrRecordNotFound) {
			return queryErr
		}

		// 2. 锁 conversations 行，串行化同一会话的 seq 分配
		var conv model.Conversation
		if err := tx.Clauses(lockForUpdateClause()).
			Where("id = ? AND status = ?", m.ConversationID, 1).
			Take(&conv).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errs.New(errs.ConversationNotFound, "会话不存在或已解散")
			}
			return err
		}

		// 3. 计算 next_seq
		nextSeq := conv.LastSeq + 1
		now := time.Now().UTC()

		// 4. INSERT messages
		row := &model.Message{
			ID:             m.MessageID,
			ConversationID: m.ConversationID,
			Seq:            nextSeq,
			SenderID:       m.SenderID,
			ClientMsgID:    rawID,
			MessageType:    m.MessageType,
			Content:        string(m.Content),
			ContentPreview: m.ContentPreview,
			Status:         message.StatusNormal,
			ClientSentAt:   m.ClientSentAt,
			CreatedAt:      now,
		}
		if err := tx.Create(row).Error; err != nil {
			if isDuplicateKey(err) {
				// 唯一键冲突：回滚本次 seq 递增，查询并复用已存在消息
				return errPersistDuplicate
			}
			return err
		}

		// 5. UPDATE conversations 摘要
		msgType := m.MessageType
		if err := tx.Model(&model.Conversation{}).
			Where("id = ?", m.ConversationID).
			Updates(map[string]interface{}{
				"last_seq":             nextSeq,
				"last_message_id":      m.MessageID,
				"last_message_type":    msgType,
				"last_message_preview": m.ContentPreview,
				"last_message_at":      now,
			}).Error; err != nil {
			return err
		}

		// 6. INSERT message_outbox：与消息落库同一事务（GOCHAT_DATABASE.md §9）
		payload, err := json.Marshal(message.MessagePersistedEvent{
			MessageID:       m.MessageID,
			Seq:             nextSeq,
			SenderID:        m.SenderID,
			ClientMessageID: m.ClientMessageID,
			ConversationID:  m.ConversationID,
			MessageType:     m.MessageType,
			Content:         m.Content,
			ContentPreview:  m.ContentPreview,
			CreatedAt:       now,
			MemberIDs:       input.MemberIDs,
		})
		if err != nil {
			return err
		}
		outbox := &model.MessageOutbox{
			MessageID:   m.MessageID,
			EventType:   model.OutboxEventPersisted,
			Payload:     string(payload),
			Status:      model.OutboxPending,
			NextRetryAt: now,
			CreatedAt:   now,
		}
		if err := tx.Create(outbox).Error; err != nil {
			return err
		}

		// 返回带 seq 的消息
		m.Seq = nextSeq
		m.CreatedAt = now
		result = m
		return nil
	})
	if err != nil {
		if errors.Is(err, errPersistDuplicate) {
			// 事务回滚后查询已存在记录复用
			dup, qerr := r.FindByClientMessageID(ctx, m.SenderID, m.ClientMessageID)
			if qerr != nil {
				return nil, qerr
			}
			if dup == nil {
				return nil, errs.Internal(errors.New("幂等冲突但未查询到原消息"))
			}
			return dup, nil
		}
		return nil, errs.Internal(err)
	}
	return result, nil
}

var errPersistDuplicate = errors.New("persist duplicate message")

// AdvanceReceivedCursor 向前推进送达游标：只能前进，不能大于会话 last_seq；
// 小于当前值或超过 last_seq 时不更新（成功返回）。
func (r *MessageRepository) AdvanceReceivedCursor(ctx context.Context, conversationID, userID, receivedSeq int64) error {
	res := r.db.WithContext(ctx).Exec(
		"UPDATE conversation_members AS cm "+
			"JOIN conversations AS c ON c.id = cm.conversation_id "+
			"SET cm.last_received_seq = ? "+
			"WHERE cm.conversation_id = ? AND cm.user_id = ? AND cm.status = 1 "+
			"AND ? <= c.last_seq AND cm.last_received_seq < ?",
		receivedSeq, conversationID, userID, receivedSeq, receivedSeq,
	)
	if res.Error != nil {
		return errs.Internal(res.Error)
	}
	return nil
}

// AdvanceReadCursor 向前推进已读游标：只能前进；重复提交相同值幂等成功
// （GOCHAT_API.md §5.10）。
func (r *MessageRepository) AdvanceReadCursor(ctx context.Context, conversationID, userID, readSeq int64) error {
	res := r.db.WithContext(ctx).Exec(
		"UPDATE conversation_members AS cm "+
			"JOIN conversations AS c ON c.id = cm.conversation_id "+
			"SET cm.last_read_seq = ? "+
			"WHERE cm.conversation_id = ? AND cm.user_id = ? AND cm.status = 1 "+
			"AND ? <= c.last_seq AND cm.last_read_seq < ?",
		readSeq, conversationID, userID, readSeq, readSeq,
	)
	if res.Error != nil {
		return errs.Internal(res.Error)
	}
	return nil
}

// ---- 转换辅助 ----

func toDomainMessage(m *model.Message) *message.Message {
	return &message.Message{
		MessageID:       m.ID,
		ClientMessageID: bytesToUUID(m.ClientMsgID),
		ConversationID:  m.ConversationID,
		Seq:             m.Seq,
		SenderID:        m.SenderID,
		MessageType:     m.MessageType,
		Content:         json.RawMessage(m.Content),
		ContentPreview:  m.ContentPreview,
		Status:          m.Status,
		RecalledBy:      m.RecalledBy,
		RecalledAt:      m.RecalledAt,
		ClientSentAt:    m.ClientSentAt,
		CreatedAt:       m.CreatedAt,
	}
}

func toDomainMessages(rows []model.Message) []message.Message {
	out := make([]message.Message, 0, len(rows))
	for i := range rows {
		out = append(out, *toDomainMessage(&rows[i]))
	}
	return out
}
