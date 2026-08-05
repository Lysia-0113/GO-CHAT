package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
	kafkago "github.com/segmentio/kafka-go"
	"gorm.io/gorm"

	"github.com/Lysia-0113/GO-CHAT/internal/auth"
	"github.com/Lysia-0113/GO-CHAT/internal/connection"
	"github.com/Lysia-0113/GO-CHAT/internal/conversation"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/idgen/segment"
	kafkainfra "github.com/Lysia-0113/GO-CHAT/internal/infrastructure/kafka"
	mysqlrepo "github.com/Lysia-0113/GO-CHAT/internal/infrastructure/mysql/repository"
	redisinfra "github.com/Lysia-0113/GO-CHAT/internal/infrastructure/redis"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
	"github.com/Lysia-0113/GO-CHAT/internal/resilience"
	httptransport "github.com/Lysia-0113/GO-CHAT/internal/transport/http"
	"github.com/Lysia-0113/GO-CHAT/internal/transport/http/handler"
	"github.com/Lysia-0113/GO-CHAT/internal/transport/websocket"
	"github.com/Lysia-0113/GO-CHAT/internal/user"
	"github.com/Lysia-0113/GO-CHAT/internal/worker/deliver"
	"github.com/Lysia-0113/GO-CHAT/internal/worker/outbox"
	"github.com/Lysia-0113/GO-CHAT/internal/worker/persist"
)

// App 是装配完成的应用。
type App struct {
	cfg    *Config
	log    *slog.Logger
	reg    *metrics.Registry
	svcCtx *ServiceContext

	db    *gorm.DB
	redis *goredis.Client
	kafka *kafkainfra.Producer

	presence *redisinfra.PresenceRegistry
	pubsub   *redisinfra.PubsubGateway

	persistConsumer *kafkainfra.Consumer
	deliverConsumer *kafkainfra.Consumer
	outboxRepo      *mysqlrepo.OutboxRepository
	topics          kafkainfra.Topics

	userRepo        *mysqlrepo.UserRepository
	convRepo        *mysqlrepo.ConversationRepository
	msgRepo         *mysqlrepo.MessageRepository
	msgIDs          *segment.Generator
	userIDs         *segment.Generator
	convIDs         *segment.Generator
	users           *user.Service
	convos          *conversation.Service
	messages        *message.Service
	connManager     *connection.Manager
	wsTickets       *redisinfra.WSTicketStore
	recentCache     *redisinfra.RecentMessageCache
	rateLimiter     *redisinfra.RateLimiter
	idemStore       *redisinfra.IdempotencyStore
	breakers        *resilience.Breakers
	tokens          *auth.TokenManager
	historyBulkhead *resilience.Bulkhead
	ingressBulkhead *resilience.Bulkhead
}

// Run 启动 HTTP 服务与全部 Worker，阻塞直到 ctx 取消后优雅退出。
func (a *App) Run(appCtx context.Context) error {
	// ---- Worker 启动 ----
	persistWorker := persist.New(a.persistConsumer, a.msgRepo, a.msgIDs,
		kafkainfra.NewPublisher(a.kafka, "persist-worker"), a.kafka, a.topics,
		persist.Config{
			MaxRetries: a.cfg.Kafka.PersistMaxRetries,
			Backoff:    a.cfg.Kafka.PersistBackoff,
			TxTimeout:  a.cfg.Resilience.PersistTxTimeout,
		}, a.reg, a.log)
	outboxPublisher := outbox.New(a.outboxRepo, a.kafka, a.topics, outbox.Config{
		MaxRetries:   a.cfg.Kafka.OutboxMaxRetries,
		Backoff:      a.cfg.Kafka.OutboxBackoff,
		PollInterval: a.cfg.Kafka.OutboxPollInterval,
		BatchSize:    a.cfg.Kafka.OutboxBatchSize,
		InstanceID:   a.cfg.Server.NodeID,
	}, a.reg, a.log)
	deliverWorker := deliver.New(a.deliverConsumer, a.convRepo, a.recentCache, a.presence, a.connManager, a.pubsub, a.reg, a.log)

	// 多实例取同一个 Consumer Group 时，每个实例各启动一份 Worker
	for i := 0; i < a.cfg.Resilience.PersistWorkers; i++ {
		go func() {
			if err := persistWorker.Run(appCtx); err != nil {
				a.log.Error("persist worker exited", "error", err.Error())
			}
		}()
	}
	for i := 0; i < a.cfg.Resilience.DeliveryWorkers; i++ {
		go func() {
			if err := deliverWorker.Run(appCtx); err != nil {
				a.log.Error("deliver worker exited", "error", err.Error())
			}
		}()
	}
	for i := 0; i < a.cfg.Resilience.OutboxWorkers; i++ {
		go func() {
			if err := outboxPublisher.Run(appCtx); err != nil {
				a.log.Error("outbox publisher exited", "error", err.Error())
			}
		}()
	}

	// ---- 跨节点 Pub/Sub 投递订阅（本节点网关） ----
	if a.pubsub != nil {
		deliveryCh, err := a.pubsub.Subscribe(appCtx)
		if err == nil {
			go func() {
				for ev := range deliveryCh {
					for _, connID := range ev.ConnectionIDs {
						_ = a.connManager.PushToConnection(appCtx, connID, connection.Event{
							Event: ev.EventName,
							Data:  ev.Data,
						})
					}
				}
			}()
		} else {
			a.log.Warn("pubsub subscribe failed", "error", err.Error())
		}
	}

	// ---- 指标循环（号段库存、熔断状态、Outbox 积压） ----
	go a.metricsLoop(appCtx)

	// ---- HTTP / WebSocket ----
	wsHandler := websocket.NewHandler(appCtx, a.wsTickets, a.rateLimiter, a.connManager, a.presence,
		a.messages, a.convos,
		websocket.HandlerConfig{
			ReadLimit:              a.cfg.Server.WSReadLimit,
			HeartbeatInterval:      a.cfg.Server.WSHeartbeatInterval,
			MissedHeartbeat:        a.cfg.Server.WSMissedHeartbeat,
			WriteQueueSize:         a.cfg.Server.WSWriteQueueSize,
			WriteQueueTimeout:      a.cfg.Server.WSWriteQueueTimeout,
			InboundRatePerSec:      a.cfg.Resilience.ConnInboundPerSecond,
			InboundRateBurst:       a.cfg.Resilience.ConnInboundBurst,
			WSConnectPerMinute:     a.cfg.Resilience.WSConnectPerMinute,
			WSConnectPerUserMinute: a.cfg.Resilience.WSConnectPerUserPerMinute,
		}, a.reg, a.log)

	router := httptransport.New(httptransport.Router{
		Tokens:              a.tokens,
		AuthHandler:         handler.NewAuthHandler(a.users, a.tokens, a.rateLimiter),
		UserHandler:         handler.NewUserHandler(a.users),
		ConversationHandler: handler.NewConversationHandler(a.convos),
		MessageHandler:      handler.NewMessageHandler(a.messages, a.rateLimiter, a.historyBulkhead),
		HealthHandler:       handler.NewHealthHandler(a.db, a.redis, a.kafkaReady),
		WSHandler:           wsHandler,
		MetricsHandler:      a.metricsHandler(),
	})

	gin.SetMode(gin.ReleaseMode)
	srv := &http.Server{
		Addr:              a.cfg.Server.Addr,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		a.log.Info("server started", "addr", a.cfg.Server.Addr, "node_id", a.cfg.Server.NodeID)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-appCtx.Done():
		a.log.Info("shutting down")
	case err := <-errCh:
		return err
	}

	// ---- 优雅退出 ----
	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.cfg.Server.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		a.log.Warn("http shutdown", "error", err.Error())
	}
	a.persistConsumer.Close()
	a.deliverConsumer.Close()
	a.kafka.Close()
	if sqlDB, err := a.db.DB(); err == nil {
		_ = sqlDB.Close()
	}
	_ = a.redis.Close()
	a.log.Info("server stopped")
	return nil
}

// kafkaReady 就绪探针：能读取 Topic 元数据即视为可用。
func (a *App) kafkaReady(ctx context.Context) error {
	client := &kafkago.Client{
		Addr:    kafkago.TCP(a.cfg.Kafka.Brokers...),
		Timeout: 2 * time.Second,
	}
	_, err := client.Metadata(ctx, &kafkago.MetadataRequest{})
	// Client 无显式 Close：请求完成后由 Transport 连接池回收
	return err
}

// metricsLoop 周期刷新仪表盘指标。
func (a *App) metricsLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 号段剩余库存（GOCHAT_RESILIENCE.md §11.1）
			for name, gen := range map[string]*segment.Generator{
				"im_user": a.userIDs, "im_conversation": a.convIDs, "im_message": a.msgIDs,
			} {
				st := gen.State()
				a.reg.Gauge(metrics.NameIDSegmentRemaining, "号段剩余库存",
					map[string]string{"biz_tag": name, "node": a.cfg.Server.NodeID}).Set(st.Remaining)
			}
			// 熔断器状态
			for name, st := range a.breakers.States() {
				a.reg.Gauge(metrics.NameBreakerState, "熔断器状态 0=closed 1=open 2=half-open",
					map[string]string{"dependency": name}).Set(int64(st))
			}
			// 连接数
			a.reg.Gauge(metrics.NameWSConnectionActive, "在线连接数", nil).Set(a.connManager.Count())
			// 隔离舱占用
			a.reg.Gauge(metrics.NameBulkheadQueueLength, "隔离舱占用", map[string]string{"worker": "history_query"}).Set(a.historyBulkhead.Active())
			a.reg.Gauge(metrics.NameBulkheadQueueLength, "隔离舱占用", map[string]string{"worker": "ws_ingress"}).Set(a.ingressBulkhead.Active())
		}
	}
}

// metricsHandler 输出 Prometheus 文本格式指标。
func (a *App) metricsHandler() gin.HandlerFunc {
	if !a.cfg.Metrics.Enabled {
		return nil
	}
	return func(c *gin.Context) {
		c.Data(http.StatusOK, "text/plain; version=0.0.4", []byte(a.reg.Render()))
	}
}

// ---- 日志辅助 ----

// newStdoutWriter 返回标准输出写入器。
func newStdoutWriter() io.Writer { return os.Stdout }

// slogWriter 适配 GORM 日志到 slog。
type slogWriter struct {
	log *slog.Logger
}

func (w *slogWriter) Printf(format string, args ...interface{}) {
	w.log.Warn(fmt.Sprintf(format, args...))
}
