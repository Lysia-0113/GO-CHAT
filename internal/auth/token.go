// Package auth 提供 JWT 签发/解析与 Argon2id 密码哈希。
package auth

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/argon2"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

// Claims 是访问令牌的声明。
type Claims struct {
	UserID   int64  `json:"uid"`
	DeviceID string `json:"dev,omitempty"`
	jwt.RegisteredClaims
}

// TokenManager 负责 JWT 的签发与校验。
type TokenManager struct {
	secret []byte
	ttl    time.Duration
}

func NewTokenManager(secret string, ttl time.Duration) *TokenManager {
	return &TokenManager{secret: []byte(secret), ttl: ttl}
}

// TTL 返回访问令牌有效期，供登录响应中的 expires_in 使用。
func (t *TokenManager) TTL() time.Duration { return t.ttl }

// Issue 为用户签发访问令牌。
func (t *TokenManager) Issue(ctx context.Context, userID int64, deviceID string) (string, error) {
	now := time.Now()
	claims := Claims{
		UserID:   userID,
		DeviceID: deviceID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "gochat",
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(t.ttl)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := token.SignedString(t.secret)
	if err != nil {
		return "", errs.Internal(err)
	}
	return s, nil
}

// Parse 校验令牌并返回 Claims。
func (t *TokenManager) Parse(tokenString string) (*Claims, error) {
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return t.secret, nil
	})
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, errs.New(errs.TokenExpired, "令牌已过期")
		}
		return nil, errs.Wrap(errs.AuthRequired, "令牌无效", err)
	}
	if !token.Valid {
		return nil, errs.New(errs.AuthRequired, "令牌无效")
	}
	if claims.UserID <= 0 {
		return nil, errs.New(errs.AuthRequired, "令牌缺少用户信息")
	}
	return claims, nil
}

// ---- Argon2id 密码哈希（GOCHAT_DATABASE.md §4.1：优先 Argon2id） ----

type Argon2Params struct {
	Time    uint32
	Memory  uint32 // KiB
	Threads uint8
	KeyLen  uint32
}

// DefaultArgon2Params 是推荐初值；生产应结合硬件压测调整。
var DefaultArgon2Params = Argon2Params{Time: 1, Memory: 64 * 1024, Threads: 4, KeyLen: 32}

const argon2idVersion = 19

// HashPassword 生成编码后的 Argon2id 哈希。
// 格式：$argon2id$v=19$m=65536,t=1,p=4$<salt_b64>$<hash_b64>
func HashPassword(password string, params Argon2Params) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", errs.Internal(err)
	}
	hash := argon2.IDKey([]byte(password), salt, params.Time, params.Memory, params.Threads, params.KeyLen)
	encoded := fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2idVersion, params.Memory, params.Time, params.Threads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(hash),
	)
	return encoded, nil
}

var ErrInvalidHash = errors.New("invalid password hash format")

// VerifyPassword 校验明文密码与编码哈希是否匹配。
func VerifyPassword(password, encodedHash string) (bool, error) {
	parts := strings.Split(encodedHash, "$")
	if len(parts) != 6 || parts[1] != "argon2id" {
		return false, errs.Wrap(errs.InternalError, "密码哈希格式错误", ErrInvalidHash)
	}
	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return false, errs.Wrap(errs.InternalError, "密码哈希格式错误", ErrInvalidHash)
	}
	if version != argon2idVersion {
		return false, errs.Wrap(errs.InternalError, "不支持的 Argon2 版本", ErrInvalidHash)
	}
	var m, t, p uint32
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &m, &t, &p); err != nil {
		return false, errs.Wrap(errs.InternalError, "密码哈希格式错误", ErrInvalidHash)
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, errs.Wrap(errs.InternalError, "密码哈希格式错误", ErrInvalidHash)
	}
	wantHash, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, errs.Wrap(errs.InternalError, "密码哈希格式错误", ErrInvalidHash)
	}
	gotHash := argon2.IDKey([]byte(password), salt, t, m, uint8(p), uint32(len(wantHash)))
	if subtle.ConstantTimeCompare(gotHash, wantHash) != 1 {
		return false, nil
	}
	return true, nil
}
