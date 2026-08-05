package kafka

import (
	"strconv"
	"strings"
)

// Topics 是固定 Topic 配置（GOCHAT_KAFKA.md §3.1）。
// 禁止按会话动态创建 Topic；环境后缀由装配层统一附加。
type Topics struct {
	prefix string
}

// NewTopics 创建 Topic 集合；prefix 为空或 "." 时使用无后缀名。
// 示例：prefix="dev" → "im.message.ingress.dev"。
func NewTopics(prefix string) Topics {
	prefix = strings.Trim(prefix, ".")
	return Topics{prefix: prefix}
}

func (t Topics) withSuffix(name string) string {
	if t.prefix == "" {
		return name
	}
	return name + "." + t.prefix
}

// Ingress 待持久化消息入口 Topic。
func (t Topics) Ingress() string { return t.withSuffix("im.message.ingress") }

// Persisted 消息落库后的事件总线 Topic。
func (t Topics) Persisted() string { return t.withSuffix("im.message.persisted") }

// DLQ 超过重试次数的失败消息 Topic。
func (t Topics) DLQ() string { return t.withSuffix("im.message.dlq") }

// KeyOf 返回 Kafka Message Key（conversation_id 十进制字符串）。
func KeyOf(conversationID int64) string { return strconv.FormatInt(conversationID, 10) }
