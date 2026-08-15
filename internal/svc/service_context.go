// Package svc 提供应用级服务定位器 ServiceContext（GOCHAT_API.md §11.3）。
//
// 借鉴 go-zero 的 svc.ServiceContext 风格：启动时一次性装配全部依赖，
// 传输层（HTTP handler / WebSocket handler）与 Worker 一律持有
// `svcCtx *svc.ServiceContext`，方法内 `h.svcCtx.Xxx` 随处可用。
//
// 边界（Go 循环导入约束决定，参见 GOCHAT_API.md §11.3）：
//   - svcCtx 持有 3 个领域服务，因此领域服务不能再持有 svcCtx——
//     user / conversation / message 保持精确构造注入，单测只传 Fake；
//   - svc 只 import 叶子包（config / infrastructure / 领域服务等），
//     被 svcCtx 持有的包一律不得反向 import svc；
//   - 禁止存放任何请求级状态（用户、会话、消息、请求 ctx）。
package svc

import (
	"log/slog"

	goredis "github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"github.com/Lysia-0113/GO-CHAT/internal/auth"
	"github.com/Lysia-0113/GO-CHAT/internal/config"
	"github.com/Lysia-0113/GO-CHAT/internal/connection"
	"github.com/Lysia-0113/GO-CHAT/internal/conversation"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/idgen/segment"
	kafkainfra "github.com/Lysia-0113/GO-CHAT/internal/infrastructure/kafka"
	mysqlrepo "github.com/Lysia-0113/GO-CHAT/internal/infrastructure/mysql/repository"
	redisinfra "github.com/Lysia-0113/GO-CHAT/internal/infrastructure/redis"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
	"github.com/Lysia-0113/GO-CHAT/internal/resilience"
	"github.com/Lysia-0113/GO-CHAT/internal/user"
)

// ServiceContext 是应用装配容器（公司 data-importer 的 svc.ServiceContext 风格）。
type ServiceContext struct {
	Config config.Config
	Log    *slog.Logger

	// 基础设施
	DB    *gorm.DB
	Redis goredis.UniversalClient
	Kafka *kafkainfra.Producer
	// Topics 按环境后缀生成的 Topic 名集合
	Topics kafkainfra.Topics

	// Redis 组件
	Presence    *redisinfra.PresenceRegistry
	Pubsub      *redisinfra.PubsubGateway
	WSTickets   *redisinfra.WSTicketStore
	Cursors     *redisinfra.CursorStore
	RateLimiter *redisinfra.RateLimiter
	IdemStore   *redisinfra.IdempotencyStore

	// 连接与会话
	ConnManager *connection.Manager

	// 韧性组件
	Breakers        *resilience.Breakers
	HistoryBulkhead *resilience.Bulkhead
	IngressBulkhead *resilience.Bulkhead

	// 鉴权
	Tokens *auth.TokenManager

	// 领域服务
	UserService         *user.Service
	ConversationService *conversation.Service
	MessageService      *message.Service

	// 仓储（Worker 使用）
	UserRepo   *mysqlrepo.UserRepository
	ConvRepo   *mysqlrepo.ConversationRepository
	MsgRepo    *mysqlrepo.MessageRepository
	OutboxRepo *mysqlrepo.OutboxRepository
	UserIDs    *segment.Generator
	ConvIDs    *segment.Generator
	MessageIDs *segment.Generator

	// 消费者（生命周期归 svcCtx，优雅退出时统一关闭）
	PersistConsumer *kafkainfra.Consumer
	DeliverConsumer *kafkainfra.Consumer
	DLQConsumer     *kafkainfra.Consumer
}
