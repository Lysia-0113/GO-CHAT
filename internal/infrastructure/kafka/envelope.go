// Package kafka 是 Kafka 基础设施：Envelope、Producer、Consumer
// （GOCHAT_KAFKA.md §5/§6/§7）。
package kafka

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// Envelope 是全部 Topic 的统一事件信封（GOCHAT_KAFKA.md §5.1）。
type Envelope struct {
	SchemaVersion  int             `json:"schema_version"`
	EventID        string          `json:"event_id"`
	EventType      string          `json:"event_type"`
	OccurredAt     time.Time       `json:"occurred_at"`
	Producer       string          `json:"producer"`
	ConversationID string          `json:"conversation_id"`
	Data           json.RawMessage `json:"data"`
}

// 事件类型常量。
const (
	EventIngress   = "message.ingress"
	EventPersisted = "message.persisted"
	EventDLQ       = "message.dlq"
)

// SchemaVersion 是当前 Envelope/Data 版本（禁止静默修改字段语义）。
const SchemaVersion = 1

// NewEnvelope 构造信封；event_id 使用 UUIDv4（日志关联与短期去重）。
func NewEnvelope(eventType string, producer string, conversationID int64, data interface{}) (Envelope, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{
		SchemaVersion:  SchemaVersion,
		EventID:        "evt_" + uuid.NewString(),
		EventType:      eventType,
		OccurredAt:     time.Now().UTC(),
		Producer:       producer,
		ConversationID: itoa(conversationID),
		Data:           raw,
	}, nil
}

// Marshal 序列化为传输字节。
func (e Envelope) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

// DLQPayload 是死信事件载荷（GOCHAT_KAFKA.md §5.4）。
// 必须保留原始 Envelope；error_message 不得携带密码、Token 或完整敏感内容。
type DLQPayload struct {
	FailedTopic     string          `json:"failed_topic"`
	FailedPartition int             `json:"failed_partition"`
	FailedOffset    int64           `json:"failed_offset"`
	RetryCount      int             `json:"retry_count"`
	ErrorCode       string          `json:"error_code"`
	ErrorMessage    string          `json:"error_message"`
	FailedAt        time.Time       `json:"failed_at"`
	OriginalEvent   json.RawMessage `json:"original_event"`
}

func itoa(v int64) string {
	return strconv.FormatInt(v, 10)
}
