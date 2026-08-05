package auth

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

func TestTokenRoundTrip(t *testing.T) {
	tm := NewTokenManager("test-secret", time.Hour)
	token, err := tm.Issue(context.Background(), 1001, "web-chrome-a1")
	if err != nil {
		t.Fatal(err)
	}
	claims, err := tm.Parse(token)
	if err != nil {
		t.Fatal(err)
	}
	if claims.UserID != 1001 || claims.DeviceID != "web-chrome-a1" {
		t.Fatalf("unexpected claims: %+v", claims)
	}
}

func TestTokenExpired(t *testing.T) {
	tm := NewTokenManager("test-secret", -time.Minute)
	token, err := tm.Issue(context.Background(), 1, "d")
	if err != nil {
		t.Fatal(err)
	}
	_, err = tm.Parse(token)
	if !errs.IsCode(err, errs.TokenExpired) {
		t.Fatalf("expected TOKEN_EXPIRED, got %v", err)
	}
}

func TestTokenInvalid(t *testing.T) {
	tm := NewTokenManager("test-secret", time.Hour)
	if _, err := tm.Parse("not-a-jwt"); !errs.IsCode(err, errs.AuthRequired) {
		t.Fatalf("expected AUTH_REQUIRED, got %v", err)
	}
	// 错误密钥签发的令牌
	tm2 := NewTokenManager("other-secret", time.Hour)
	token, _ := tm2.Issue(context.Background(), 1, "d")
	if _, err := tm.Parse(token); !errs.IsCode(err, errs.AuthRequired) {
		t.Fatalf("expected AUTH_REQUIRED for wrong secret, got %v", err)
	}
}

func TestArgon2HashVerify(t *testing.T) {
	hash, err := HashPassword("change-me", DefaultArgon2Params)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(hash, "$argon2id$v=19$") {
		t.Fatalf("unexpected hash format: %s", hash)
	}
	ok, err := VerifyPassword("change-me", hash)
	if err != nil || !ok {
		t.Fatalf("verify failed: ok=%v err=%v", ok, err)
	}
	ok, err = VerifyPassword("wrong-password", hash)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("wrong password must not verify")
	}
	// 损坏的哈希
	if _, err := VerifyPassword("x", "garbage"); err == nil {
		t.Fatal("expected error for garbage hash")
	}
}
