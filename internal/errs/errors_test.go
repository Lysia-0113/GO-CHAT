package errs

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestHTTPStatusMapping(t *testing.T) {
	cases := []struct {
		err  *Error
		want int
	}{
		{New(InvalidArgument, ""), http.StatusBadRequest},
		{New(AuthRequired, ""), http.StatusUnauthorized},
		{New(TokenExpired, ""), http.StatusUnauthorized},
		{New(UserNotFound, ""), http.StatusNotFound},
		{New(UsernameConflict, ""), http.StatusConflict},
		{New(ConversationForbidden, ""), http.StatusForbidden},
		{New(MessageRateLimited, ""), http.StatusTooManyRequests},
		{New(KafkaUnavailable, ""), http.StatusServiceUnavailable},
	}
	for _, c := range cases {
		if got := c.err.HTTPStatus(); got != c.want {
			t.Fatalf("%s: got %d want %d", c.err.Code, got, c.want)
		}
	}
}

func TestIsCodeAndWrap(t *testing.T) {
	inner := New(KafkaUnavailable, "kafka down")
	wrapped := Wrap(InternalError, "oops", inner)
	// IsCode 检查最外层错误码；底层原因通过 As 获取
	if !IsCode(wrapped, InternalError) {
		t.Fatal("IsCode must match outermost code")
	}
	if IsCode(wrapped, KafkaUnavailable) {
		t.Fatal("wrong code match")
	}
	if e := As(wrapped); e == nil || e.Code != InternalError {
		t.Fatal("As must unwrap to outermost")
	}
}

func TestIsTechnicalClassification(t *testing.T) {
	// 业务/权限/限流/用户取消：不计入熔断（GOCHAT_RESILIENCE.md §3.2）
	business := []error{
		New(InvalidArgument, ""),
		New(ConversationForbidden, ""),
		New(RateLimited, ""),
		context.Canceled,
		context.DeadlineExceeded,
	}
	for _, e := range business {
		if IsTechnical(e) {
			t.Fatalf("%v must not be technical", e)
		}
	}
	// 技术失败：计入熔断
	technical := []error{
		New(KafkaUnavailable, ""),
		New(RedisUnavailable, ""),
		errors.New("connection refused"),
	}
	for _, e := range technical {
		if !IsTechnical(e) {
			t.Fatalf("%v must be technical", e)
		}
	}
}

func TestRetryableError(t *testing.T) {
	e := Retryable(SystemBusy, "busy", 500)
	if !e.Retryable || e.RetryAfterMS != 500 {
		t.Fatalf("unexpected retryable error: %+v", e)
	}
	if e.HTTPStatus() != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: %d", e.HTTPStatus())
	}
}
