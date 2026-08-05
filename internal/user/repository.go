package user

import (
	"context"
)

// UserRepository 是用户存储接口，定义在使用方（GOCHAT_API.md §12）。
type UserRepository interface {
	// Create 写入用户；username 冲突返回 errs.UsernameConflict。
	Create(ctx context.Context, u *User, passwordHash string) error
	// FindByUsername 按登录名查询；不存在返回 nil, nil。
	FindByUsername(ctx context.Context, username string) (*User, string, error)
	// FindByID 按 ID 查询；不存在返回 nil, nil。
	FindByID(ctx context.Context, userID int64) (*User, error)
	// Search 按关键字模糊匹配用户名/昵称，使用不透明 cursor（上一个 user_id 的十进制字符串）。
	Search(ctx context.Context, keyword string, cursor int64, limit int) ([]User, error)
}
