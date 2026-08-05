package user

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/auth"
	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/idgen"
)

// 用户名规则：小写字母、数字、下划线，3-32 位（GOCHAT_DATABASE.md §4.2）。
var usernamePattern = regexp.MustCompile(`^[a-z0-9_]{3,32}$`)

// MaxNicknameLen 昵称最大长度。
const MaxNicknameLen = 32

// Dependencies 是 UserService 的精确依赖（GOCHAT_API.md §11.3）。
type Dependencies struct {
	Users   UserRepository
	UserIDs idgen.IDGenerator
	Tokens  *auth.TokenManager
	// Argon2 密码哈希参数
	Argon2 auth.Argon2Params
}

// Service 实现用户注册、登录、查询。
type Service struct {
	deps Dependencies
}

func NewService(deps Dependencies) *Service {
	return &Service{deps: deps}
}

// RegisterCommand 是注册命令。
type RegisterCommand struct {
	Username string
	Password string
	Nickname string
}

// RegisterResult 是注册结果。
type RegisterResult struct {
	User *User
}

// Register 注册新用户。username 唯一，密码只保存 Argon2id 哈希。
func (s *Service) Register(ctx context.Context, cmd RegisterCommand) (*RegisterResult, error) {
	username := strings.ToLower(strings.TrimSpace(cmd.Username))
	if !usernamePattern.MatchString(username) {
		return nil, errs.New(errs.InvalidArgument,
			"用户名只能包含小写字母、数字、下划线，长度 3-32")
	}
	if len(cmd.Password) < 8 {
		return nil, errs.New(errs.InvalidArgument, "密码长度至少 8 位")
	}
	nickname := strings.TrimSpace(cmd.Nickname)
	if nickname == "" {
		nickname = username
	}
	if len([]rune(nickname)) > MaxNicknameLen {
		return nil, errs.New(errs.InvalidArgument, "昵称过长")
	}

	userID, err := s.deps.UserIDs.Next(ctx)
	if err != nil {
		return nil, err
	}
	hash, err := auth.HashPassword(cmd.Password, s.deps.Argon2)
	if err != nil {
		return nil, err
	}
	u := &User{
		UserID:    userID,
		Username:  username,
		Nickname:  nickname,
		Status:    StatusNormal,
		CreatedAt: time.Now().UTC(),
	}
	if err := s.deps.Users.Create(ctx, u, hash); err != nil {
		return nil, err
	}
	return &RegisterResult{User: u}, nil
}

// LoginCommand 是登录命令。
type LoginCommand struct {
	Username string
	Password string
	DeviceID string
}

// LoginResult 是登录结果。
type LoginResult struct {
	AccessToken string
	ExpiresIn   int64 // 秒
	User        *User
}

// Login 校验用户名密码，签发访问令牌。
func (s *Service) Login(ctx context.Context, cmd LoginCommand) (*LoginResult, error) {
	username := strings.ToLower(strings.TrimSpace(cmd.Username))
	u, passwordHash, err := s.deps.Users.FindByUsername(ctx, username)
	if err != nil {
		return nil, err
	}
	if u == nil {
		return nil, errs.New(errs.InvalidArgument, "用户名或密码错误")
	}
	if u.Status != StatusNormal {
		return nil, errs.Wrap(errs.AuthRequired, "账号不可用", nil)
	}
	// 密码校验失败统一返回"用户名或密码错误"，避免用户名枚举。
	ok, err := auth.VerifyPassword(cmd.Password, passwordHash)
	if err != nil {
		return nil, errs.Wrap(errs.InternalError, "密码校验失败", err)
	}
	if !ok {
		return nil, errs.New(errs.InvalidArgument, "用户名或密码错误")
	}

	token, err := s.deps.Tokens.Issue(ctx, u.UserID, cmd.DeviceID)
	if err != nil {
		return nil, err
	}
	ttl := s.deps.Tokens.TTL()
	return &LoginResult{
		AccessToken: token,
		ExpiresIn:   int64(ttl.Seconds()),
		User:        u,
	}, nil
}

// Get 查询用户详情；不存在返回 USER_NOT_FOUND。
func (s *Service) Get(ctx context.Context, userID int64) (*User, error) {
	u, err := s.deps.Users.FindByID(ctx, userID)
	if err != nil {
		return nil, err
	}
	if u == nil {
		return nil, errs.New(errs.UserNotFound, "用户不存在")
	}
	return u, nil
}

// Search 按关键字搜索用户；cursor 是不透明分页游标（上一页末尾 user_id 的十进制字符串）。
func (s *Service) Search(ctx context.Context, keyword string, cursor string, limit int) (*SearchPage, error) {
	keyword = strings.TrimSpace(keyword)
	if keyword == "" {
		return nil, errs.New(errs.InvalidArgument, "搜索关键字不能为空")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	var cursorID int64
	if cursor != "" {
		id, err := parseOpaqueCursor(cursor)
		if err != nil {
			return nil, errs.New(errs.InvalidArgument, "游标无效")
		}
		cursorID = id
	}
	users, err := s.deps.Users.Search(ctx, keyword, cursorID, limit+1)
	if err != nil {
		return nil, err
	}
	hasMore := len(users) > limit
	if hasMore {
		users = users[:limit]
	}
	var nextCursor string
	if hasMore && len(users) > 0 {
		nextCursor = encodeOpaqueCursor(users[len(users)-1].UserID)
	}
	items := make([]*User, 0, len(users))
	for i := range users {
		items = append(items, &users[i])
	}
	return &SearchPage{Items: items, NextCursor: nextCursor, HasMore: hasMore}, nil
}

// SearchPage 是用户搜索分页结果。
type SearchPage struct {
	Items      []*User `json:"-"`
	NextCursor string
	HasMore    bool
}
