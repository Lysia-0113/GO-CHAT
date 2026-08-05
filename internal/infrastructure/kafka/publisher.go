package kafka

import (
	"context"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
)

func errsInternal(err error) error { return errs.Internal(err) }

// Publisher 实现 message.MessagePublisher（GOCHAT_API.md §12.5）：
// 把领域事件封装为统一 Envelope 后由 Producer 发送。
type Publisher struct {
	producer *Producer
	// producerName 是事件来源标识（如 "ws-gateway" / "outbox-publisher"）
	producerName string
}

// NewPublisher 创建消息事件发布适配器。
func NewPublisher(producer *Producer, producerName string) *Publisher {
	return &Publisher{producer: producer, producerName: producerName}
}

// PublishIngress 发布待持久化事件（Key=conversation_id）。
func (p *Publisher) PublishIngress(ctx context.Context, event message.MessageIngressEvent) error {
	env, err := NewEnvelope(EventIngress, p.producerName, event.ConversationID, event)
	if err != nil {
		return errsInternal(err)
	}
	return p.producer.PublishIngress(ctx, env)
}

// PublishPersisted 发布持久化事件。
func (p *Publisher) PublishPersisted(ctx context.Context, event message.MessagePersistedEvent) error {
	env, err := NewEnvelope(EventPersisted, p.producerName, event.ConversationID, event)
	if err != nil {
		return errsInternal(err)
	}
	return p.producer.PublishPersisted(ctx, env)
}
