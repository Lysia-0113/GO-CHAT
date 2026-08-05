// Package errs 定义 P0 统一错误码与错误类型。
//
// 错误码清单见 GOCHAT_API.md §14 与 GOCHAT_RESILIENCE.md §3.3。
// 每个错误携带：错误码、用户可读消息、HTTP 状态码、是否可重试、建议重试间隔。
package errs

import (
	"errors"
	"net/http"
)

// Code 是稳定的业务错误码，通过 HTTP/WebSocket 暴露给客户端。
type Code string

const (
	InvalidArgument        Code = "INVALID_ARGUMENT"
	AuthRequired           Code = "AUTH_REQUIRED"
	TokenExpired           Code = "TOKEN_EXPIRED"
	UserNotFound           Code = "USER_NOT_FOUND"
	UsernameConflict       Code = "USERNAME_CONFLICT"
	ConversationNotFound   Code = "CONVERSATION_NOT_FOUND"
	ConversationForbidden  Code = "CONVERSATION_FORBIDDEN"
	MessageNotFound        Code = "MESSAGE_NOT_FOUND"
	MessageDuplicate       Code = "MESSAGE_DUPLICATE"
	MessageRateLimited     Code = "MESSAGE_RATE_LIMITED"
	MessageTooLarge        Code = "MESSAGE_TOO_LARGE"
	KafkaUnavailable       Code = "KAFKA_UNAVAILABLE"
	IDGeneratorUnavailable Code = "ID_GENERATOR_UNAVAILABLE"
	InternalError          Code = "INTERNAL_ERROR"

	// 韧性相关（GOCHAT_RESILIENCE.md §3.3）
	RateLimited           Code = "RATE_LIMITED"
	SystemBusy            Code = "SYSTEM_BUSY"
	HistoryUnavailable    Code = "HISTORY_UNAVAILABLE"
	ConnectionSlow        Code = "CONNECTION_SLOW"
	ConnectionRateLimited Code = "CONNECTION_RATE_LIMITED"

	// 基础设施
	RedisUnavailable Code = "REDIS_UNAVAILABLE"
	InvalidTicket    Code = "INVALID_TICKET"
)

// httpStatus 是错误码到 HTTP 状态码的映射（GOCHAT_API.md §3.4）。
var httpStatus = map[Code]int{
	InvalidArgument:        http.StatusBadRequest,
	AuthRequired:           http.StatusUnauthorized,
	TokenExpired:           http.StatusUnauthorized,
	UserNotFound:           http.StatusNotFound,
	UsernameConflict:       http.StatusConflict,
	ConversationNotFound:   http.StatusNotFound,
	ConversationForbidden:  http.StatusForbidden,
	MessageNotFound:        http.StatusNotFound,
	MessageDuplicate:       http.StatusConflict,
	MessageRateLimited:     http.StatusTooManyRequests,
	MessageTooLarge:        http.StatusUnprocessableEntity,
	KafkaUnavailable:       http.StatusServiceUnavailable,
	IDGeneratorUnavailable: http.StatusServiceUnavailable,
	InternalError:          http.StatusInternalServerError,
	RateLimited:            http.StatusTooManyRequests,
	SystemBusy:             http.StatusServiceUnavailable,
	HistoryUnavailable:     http.StatusServiceUnavailable,
	ConnectionSlow:         http.StatusServiceUnavailable,
	ConnectionRateLimited:  http.StatusTooManyRequests,
	RedisUnavailable:       http.StatusServiceUnavailable,
	InvalidTicket:          http.StatusUnauthorized,
}

// Error 是统一的业务错误。
type Error struct {
	Code         Code
	Message      string
	Retryable    bool
	RetryAfterMS int64 // 限流/退避建议等待时间，0 表示不适用
	Cause        error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return string(e.Code) + ": " + e.Message + ": " + e.Cause.Error()
	}
	return string(e.Code) + ": " + e.Message
}

func (e *Error) Unwrap() error { return e.Cause }

// HTTPStatus 返回错误对应的 HTTP 状态码。
func (e *Error) HTTPStatus() int {
	if s, ok := httpStatus[e.Code]; ok {
		return s
	}
	return http.StatusInternalServerError
}

// IsCode 判断错误是否属于指定业务错误码（支持 wrapped error）。
func IsCode(err error, code Code) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Code == code
	}
	return false
}

// As 取出 *Error；非业务错误返回 nil。
func As(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return nil
}

// New 构造业务错误。
func New(code Code, msg string) *Error {
	return &Error{Code: code, Message: msg}
}

// Wrap 构造带底层原因的业务错误；对外只暴露 Code 与 Message。
func Wrap(code Code, msg string, cause error) *Error {
	return &Error{Code: code, Message: msg, Cause: cause}
}

// Retryable 构造可重试的业务错误，附带建议等待毫秒数。
func Retryable(code Code, msg string, retryAfterMS int64) *Error {
	return &Error{Code: code, Message: msg, Retryable: true, RetryAfterMS: retryAfterMS}
}

// Internal 包装未预期错误，统一转为 INTERNAL_ERROR。
func Internal(cause error) *Error {
	return Wrap(InternalError, "服务内部错误", cause)
}

// IsTechnical 判断错误是否属于"技术失败"（计入熔断统计）。
// 业务校验、权限、限流、用户取消不算技术失败（GOCHAT_RESILIENCE.md §3.2）。
func IsTechnical(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, contextCanceled) || errors.Is(err, contextDeadlineExceeded) {
		// ctx.Canceled 由客户端断连或主动取消引起，不计入熔断。
		return false
	}
	var e *Error
	if errors.As(err, &e) {
		switch e.Code {
		case InvalidArgument, AuthRequired, TokenExpired, UserNotFound,
			UsernameConflict, ConversationNotFound, ConversationForbidden,
			MessageNotFound, MessageDuplicate, MessageRateLimited, MessageTooLarge,
			RateLimited, ConnectionRateLimited:
			return false
		}
	}
	return true
}
