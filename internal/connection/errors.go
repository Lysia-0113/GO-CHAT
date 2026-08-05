package connection

import "errors"

// ErrConnectionNotFound 连接不存在或已关闭。
var ErrConnectionNotFound = errors.New("connection not found")

// ErrQueueFull 出站队列已满（慢客户端背压）。
var ErrQueueFull = errors.New("write queue full")

// ErrClosed 连接已关闭。
var ErrClosed = errors.New("connection closed")
