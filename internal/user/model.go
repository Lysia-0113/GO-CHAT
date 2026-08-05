// Package user 是用户领域模块：注册、登录、查询。
package user

import (
	"strconv"
	"time"
)

// Status 常量（与 GOCHAT_DATABASE.md §2.3 一致）。
const (
	StatusNormal   int8 = 1
	StatusDisabled int8 = 2
	StatusDeleted  int8 = 3
)

// User 是用户领域对象；ID 为 im_user 号段分配的 BIGINT，API 层序列化为十进制字符串。
type User struct {
	UserID    int64
	Username  string
	Nickname  string
	AvatarURL string
	Status    int8
	CreatedAt time.Time
}

// Public 是用户对外可见信息（不包含密码哈希等敏感字段）。
type Public struct {
	UserID    string    `json:"user_id"`
	Username  string    `json:"username"`
	Nickname  string    `json:"nickname"`
	AvatarURL string    `json:"avatar_url,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// ToPublic 转换为对外响应。
func (u *User) ToPublic() Public {
	return Public{
		UserID:    u.IDString(),
		Username:  u.Username,
		Nickname:  u.Nickname,
		AvatarURL: u.AvatarURL,
		CreatedAt: u.CreatedAt,
	}
}

// IDString 返回十进制字符串形式的用户 ID。
func (u *User) IDString() string { return FormatID(u.UserID) }

// FormatID 把 BIGINT ID 序列化为十进制字符串（GOCHAT_API.md §3.2）。
func FormatID(id int64) string {
	return strconv.FormatInt(id, 10)
}
