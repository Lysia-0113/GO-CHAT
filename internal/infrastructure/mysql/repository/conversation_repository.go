package repository

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"

	"github.com/Lysia-0113/GO-CHAT/internal/conversation"
	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/mysql/model"
)

// ConversationRepository 实现 conversation.ConversationRepository。
type ConversationRepository struct {
	db *gorm.DB
}

func NewConversationRepository(db *gorm.DB) *ConversationRepository {
	return &ConversationRepository{db: db}
}

// CreateWithMembers 在事务中创建会话与全部成员。
// 单聊重复时依赖 uk_direct_conversation 唯一索引：返回 created=false，由调用方复用已有会话。
func (r *ConversationRepository) CreateWithMembers(ctx context.Context, conv *conversation.Conversation, memberIDs []int64) (bool, error) {
	created := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		m := &model.Conversation{
			ID:        conv.ConversationID,
			Type:      conv.Type,
			Name:      conv.Name,
			AvatarURL: conv.AvatarURL,
			OwnerID:   conv.OwnerID,
			Status:    conv.Status,
			CreatedAt: conv.CreatedAt,
		}
		if conv.Type == conversation.TypeSingle {
			low, high := sortPair(conv.OwnerID, memberIDs[0])
			m.DirectUserLowID = &low
			m.DirectUserHighID = &high
		}
		if err := tx.Create(m).Error; err != nil {
			if isDuplicateKey(err) {
				return errDupConversation
			}
			return err
		}

		now := time.Now().UTC()
		for _, uid := range memberIDs {
			member := &model.ConversationMember{
				ConversationID: conv.ConversationID,
				UserID:         uid,
				Role:           conversation.RoleMember,
				Status:         conversation.MemberStatusNormal,
				CreatedAt:      now,
			}
			// 群聊创建者自动成为群主（GOCHAT_API.md §5.5）
			if conv.Type == conversation.TypeGroup && uid == conv.OwnerID {
				member.Role = conversation.RoleOwner
			}
			if err := tx.Create(member).Error; err != nil {
				return err
			}
		}
		created = true
		return nil
	})
	if err != nil {
		if errors.Is(err, errDupConversation) {
			return false, nil
		}
		return false, errs.Internal(err)
	}
	return created, nil
}

var errDupConversation = errors.New("duplicate direct conversation")

func (r *ConversationRepository) Get(ctx context.Context, conversationID int64) (*conversation.Conversation, error) {
	var m model.Conversation
	err := r.db.WithContext(ctx).Where("id = ?", conversationID).Take(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errs.Internal(err)
	}
	return toDomainConversation(&m), nil
}

func (r *ConversationRepository) GetByDirectUsers(ctx context.Context, lowID, highID int64) (*conversation.Conversation, error) {
	var m model.Conversation
	err := r.db.WithContext(ctx).
		Where("type = ? AND direct_user_low_id = ? AND direct_user_high_id = ? AND status = ?",
			conversation.TypeSingle, lowID, highID, conversation.StatusNormal).
		Take(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errs.Internal(err)
	}
	return toDomainConversation(&m), nil
}

// ListByUser 查询用户会话列表；排序键 COALESCE(last_message_at, created_at) DESC, id DESC
// （GOCHAT_DATABASE.md §11.4），游标条件为 (ts < ?) OR (ts = ? AND c.id < ?)。
func (r *ConversationRepository) ListByUser(ctx context.Context, userID, cursorID, cursorTS int64, limit int) ([]conversation.Conversation, error) {
	q := r.db.WithContext(ctx).
		Table("conversation_members AS cm").
		Joins("JOIN conversations AS c ON c.id = cm.conversation_id").
		Select("c.*, cm.last_read_seq AS read_seq, cm.last_received_seq AS received_seq, cm.clear_before_seq AS clear_before_seq").
		Where("cm.user_id = ? AND cm.status = ? AND c.status = ?", userID, conversation.MemberStatusNormal, conversation.StatusNormal)
	if cursorID > 0 {
		q = q.Where("(COALESCE(c.last_message_at, c.created_at) < ?) OR "+
			"(COALESCE(c.last_message_at, c.created_at) = ? AND c.id < ?)",
			time.UnixMilli(cursorTS).UTC(), time.UnixMilli(cursorTS).UTC(), cursorID)
	}
	q = q.Order("COALESCE(c.last_message_at, c.created_at) DESC, c.id DESC").Limit(limit)

	var rows []conversationRow
	if err := q.Scan(&rows).Error; err != nil {
		return nil, errs.Internal(err)
	}
	out := make([]conversation.Conversation, 0, len(rows))
	for i := range rows {
		out = append(out, conversationRowToDomain(&rows[i]))
	}
	return out, nil
}

func (r *ConversationRepository) GetMember(ctx context.Context, conversationID, userID int64) (*conversation.Member, error) {
	var m model.ConversationMember
	err := r.db.WithContext(ctx).
		Where("conversation_id = ? AND user_id = ?", conversationID, userID).
		Take(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errs.Internal(err)
	}
	return &conversation.Member{
		ConversationID:  m.ConversationID,
		UserID:          m.UserID,
		Role:            m.Role,
		Status:          m.Status,
		JoinedSeq:       m.JoinedSeq,
		LastReceivedSeq: m.LastReceivedSeq,
		LastReadSeq:     m.LastReadSeq,
		ClearBeforeSeq:  m.ClearBeforeSeq,
		MuteUntil:       m.MuteUntil,
	}, nil
}

func (r *ConversationRepository) ListMemberIDs(ctx context.Context, conversationID int64) ([]int64, error) {
	var ids []int64
	err := r.db.WithContext(ctx).
		Table("conversation_members").
		Where("conversation_id = ? AND status = ?", conversationID, conversation.MemberStatusNormal).
		Pluck("user_id", &ids).Error
	if err != nil {
		return nil, errs.Internal(err)
	}
	return ids, nil
}

// ---- 内部转换 ----

func toDomainConversation(m *model.Conversation) *conversation.Conversation {
	return &conversation.Conversation{
		ConversationID:     m.ID,
		Type:               m.Type,
		Name:               m.Name,
		AvatarURL:          m.AvatarURL,
		OwnerID:            m.OwnerID,
		Status:             m.Status,
		LastSeq:            m.LastSeq,
		LastMessageID:      derefID(m.LastMessageID),
		LastMessageType:    derefType(m.LastMessageType),
		LastMessagePreview: m.LastMessagePreview,
		LastMessageAt:      m.LastMessageAt,
		CreatedAt:          m.CreatedAt,
		UpdatedAt:          m.UpdatedAt,
	}
}

type conversationRow struct {
	model.Conversation
	ReadSeq        int64 `gorm:"column:read_seq"`
	ReceivedSeq    int64 `gorm:"column:received_seq"`
	ClearBeforeSeq int64 `gorm:"column:clear_before_seq"`
}

func conversationRowToDomain(r *conversationRow) conversation.Conversation {
	c := *toDomainConversation(&r.Conversation)
	c.ReadSeq = r.ReadSeq
	c.ReceivedSeq = r.ReceivedSeq
	return c
}

func derefID(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

func derefType(p *int8) int8 {
	if p == nil {
		return 0
	}
	return *p
}

func sortPair(a, b int64) (int64, int64) {
	if a < b {
		return a, b
	}
	return b, a
}
