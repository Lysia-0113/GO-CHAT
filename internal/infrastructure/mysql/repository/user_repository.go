// Package repository 是 MySQL 仓储实现。
// 持有私有 *gorm.DB，禁止 Handler/Service 直接使用全局 DB（GOCHAT_DATABASE.md §2.4）。
package repository

import (
	"context"
	"errors"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/mysql/model"
	"github.com/Lysia-0113/GO-CHAT/internal/user"
)

// isDuplicateKey 判断是否 MySQL 1062 唯一键冲突。
func isDuplicateKey(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}

// UserRepository 实现 user.UserRepository。
type UserRepository struct {
	db *gorm.DB
}

func NewUserRepository(db *gorm.DB) *UserRepository {
	return &UserRepository{db: db}
}

func (r *UserRepository) Create(ctx context.Context, u *user.User, passwordHash string) error {
	m := &model.User{
		ID:           u.UserID,
		Username:     u.Username,
		PasswordHash: passwordHash,
		Nickname:     u.Nickname,
		AvatarURL:    u.AvatarURL,
		Status:       u.Status,
		CreatedAt:    u.CreatedAt,
	}
	err := r.db.WithContext(ctx).Create(m).Error
	if err != nil {
		if isDuplicateKey(err) {
			return errs.New(errs.UsernameConflict, "用户名已存在")
		}
		return errs.Internal(err)
	}
	return nil
}

func (r *UserRepository) FindByUsername(ctx context.Context, username string) (*user.User, string, error) {
	var m model.User
	err := r.db.WithContext(ctx).Where("username = ?", username).Take(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", errs.Internal(err)
	}
	return toDomainUser(&m), m.PasswordHash, nil
}

func (r *UserRepository) FindByID(ctx context.Context, userID int64) (*user.User, error) {
	var m model.User
	err := r.db.WithContext(ctx).Where("id = ?", userID).Take(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errs.Internal(err)
	}
	return toDomainUser(&m), nil
}

func (r *UserRepository) Search(ctx context.Context, keyword string, cursor int64, limit int) ([]user.User, error) {
	var rows []model.User
	q := r.db.WithContext(ctx).
		Where("(username LIKE ? OR nickname LIKE ?)", "%"+keyword+"%", "%"+keyword+"%").
		Where("status = ?", user.StatusNormal)
	if cursor > 0 {
		q = q.Where("id > ?", cursor)
	}
	err := q.Order("id ASC").Limit(limit).Find(&rows).Error
	if err != nil {
		return nil, errs.Internal(err)
	}
	out := make([]user.User, 0, len(rows))
	for i := range rows {
		out = append(out, *toDomainUser(&rows[i]))
	}
	return out, nil
}

func toDomainUser(m *model.User) *user.User {
	return &user.User{
		UserID:    m.ID,
		Username:  m.Username,
		Nickname:  m.Nickname,
		AvatarURL: m.AvatarURL,
		Status:    m.Status,
		CreatedAt: m.CreatedAt,
	}
}
