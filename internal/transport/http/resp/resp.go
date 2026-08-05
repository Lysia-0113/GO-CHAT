// Package resp 提供统一 HTTP 响应格式（GOCHAT_API.md §3.3 / §3.4）。
package resp

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

// Body 是成功响应体：{ "request_id": ..., "data": ... }
type Body struct {
	RequestID string      `json:"request_id"`
	Data      interface{} `json:"data"`
}

// ErrorBody 是失败响应体：{ "request_id": ..., "error": { "code", "message", "details" } }
type ErrorBody struct {
	RequestID string     `json:"request_id"`
	Error     ErrorField `json:"error"`
}

type ErrorField struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	Details      string `json:"details,omitempty"`
	Retryable    bool   `json:"retryable,omitempty"`
	RetryAfterMS int64  `json:"retry_after_ms,omitempty"`
}

// RequestID 从 gin.Context 读取链路 ID；未注入时使用 "unknown"。
func RequestID(c *gin.Context) string {
	if v, ok := c.Get("request_id"); ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return "unknown"
}

// OK 写出 200 成功响应。
func OK(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, Body{RequestID: RequestID(c), Data: data})
}

// Created 写出 201 成功响应。
func Created(c *gin.Context, data interface{}) {
	c.JSON(http.StatusCreated, Body{RequestID: RequestID(c), Data: data})
}

// Err 将业务错误转换为对应的 HTTP 状态码与响应体。
func Err(c *gin.Context, err error) {
	var e *errs.Error
	if !errors.As(err, &e) {
		e = errs.Internal(err)
	}
	body := ErrorBody{
		RequestID: RequestID(c),
		Error: ErrorField{
			Code:         string(e.Code),
			Message:      e.Message,
			Details:      detailOf(e),
			Retryable:    e.Retryable,
			RetryAfterMS: e.RetryAfterMS,
		},
	}
	c.JSON(e.HTTPStatus(), body)
}

// detailOf 只暴露底层错误摘要；敏感信息（SQL、密码、Token、完整消息内容）不得写入。
func detailOf(e *errs.Error) string {
	if e.Cause == nil {
		return ""
	}
	return e.Cause.Error()
}
