package redis

import (
	"context"
	"encoding/json"

	goredis "github.com/redis/go-redis/v9"

	"github.com/Lysia-0113/GO-CHAT/internal/errs"
)

// DeliveryEvent 是跨节点在线投递通知（GOCHAT_REDIS.md §9.1）。
type DeliveryEvent struct {
	// MessageJSON 是推送事件的 data 部分（message.new / message.persisted）
	EventName     string          `json:"event_name"`
	Data          json.RawMessage `json:"data"`
	ConnectionIDs []string        `json:"connection_ids,omitempty"`
}

// PubsubGateway 按 node_id 划分的 Redis Pub/Sub 通道：
// Dispatcher 发布到目标节点频道，网关订阅本节点频道后本机推送
// （GOCHAT_API.md §13.2 / GOCHAT_REDIS.md §9）。
type PubsubGateway struct {
	client *goredis.Client
	nodeID string
	opts   Options
}

func NewPubsubGateway(client *goredis.Client, nodeID string, opts Options) *PubsubGateway {
	return &PubsubGateway{client: client, nodeID: nodeID, opts: opts}
}

// PublishToNode 发布在线投递通知到指定节点频道。
// Pub/Sub 只是临时在线通知，丢失后由客户端 after_seq 补偿（GOCHAT_REDIS.md §9.2）。
func (g *PubsubGateway) PublishToNode(ctx context.Context, nodeID string, event DeliveryEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return errs.Internal(err)
	}
	ctx, cancel := withTimeout(ctx, g.opts.WriteTimeout)
	defer cancel()
	if err := g.client.Publish(ctx, GatewayChannel(nodeID), payload).Err(); err != nil {
		return errs.Wrap(errs.RedisUnavailable, "网关通知发布失败", err)
	}
	return nil
}

// Subscribe 订阅本节点频道；返回的通道在 ctx 取消时关闭。
// 注意：Pub/Sub 断开期间的投递会丢失（客户端补拉兜底），不做持久化。
func (g *PubsubGateway) Subscribe(ctx context.Context) (<-chan DeliveryEvent, error) {
	sub := g.client.Subscribe(ctx, GatewayChannel(g.nodeID))
	ch := make(chan DeliveryEvent, 128)
	go func() {
		defer close(ch)
		defer sub.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-sub.Channel():
				if !ok {
					return
				}
				var event DeliveryEvent
				if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
					continue
				}
				select {
				case ch <- event:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return ch, nil
}
