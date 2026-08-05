package repository

import (
	"context"
	"errors"

	"gorm.io/gorm"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/idgen/segment"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/mysql/model"
)

// SegmentRepository 实现 segment.SegmentRepository：version CAS 申请号段
// （GOCHAT_API.md §7.6 / GOCHAT_DATABASE.md §8.2）。
type SegmentRepository struct {
	db *gorm.DB
}

func NewSegmentRepository(db *gorm.DB) *SegmentRepository {
	return &SegmentRepository{db: db}
}

// Allocate 读取 max_id/step/version 后执行条件更新；受影响行数为 1 时获得
// [old_max_id+1, old_max_id+step] 号段。CAS 冲突返回 segment.ErrSegmentConflict。
func (r *SegmentRepository) Allocate(ctx context.Context, bizTag string) (segment.Segment, error) {
	var m model.IDGenerator
	err := r.db.WithContext(ctx).Where("biz_tag = ?", bizTag).Take(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return segment.Segment{}, errs.New(errs.InternalError, "号段 biz_tag 未初始化: "+bizTag)
	}
	if err != nil {
		return segment.Segment{}, errs.Internal(err)
	}

	newMax := m.MaxID + int64(m.Step)
	res := r.db.WithContext(ctx).Exec(
		"UPDATE id_generator "+
			"SET max_id = ?, version = version + 1, update_time = CURRENT_TIMESTAMP(3) "+
			"WHERE biz_tag = ? AND max_id = ? AND version = ?",
		newMax, bizTag, m.MaxID, m.Version,
	)
	if res.Error != nil {
		return segment.Segment{}, errs.Internal(res.Error)
	}
	if res.RowsAffected == 0 {
		// 被其他实例抢先更新，由生成器有限重试
		return segment.Segment{}, segment.ErrSegmentConflict
	}
	return segment.Segment{
		Min:  m.MaxID + 1,
		Max:  newMax,
		Step: int64(m.Step),
	}, nil
}
