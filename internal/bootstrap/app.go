// Package bootstrap 提供应用装配（GOCHAT_API.md §11.3 构造顺序）：
//
//	Config → DB / Redis / Kafka / IDGenerator → Repository → Service → ServiceContext → Handler / Worker
//
// 配置类型在 internal/config（svc.ServiceContext 需要持有 Config，
// 独立成包避免 bootstrap↔svc 循环导入）。
package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/Lysia-0113/GO-CHAT/internal/auth"
	"github.com/Lysia-0113/GO-CHAT/internal/config"
	"github.com/Lysia-0113/GO-CHAT/internal/connection"
	"github.com/Lysia-0113/GO-CHAT/internal/conversation"
	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/idgen/segment"
	kafkainfra "github.com/Lysia-0113/GO-CHAT/internal/infrastructure/kafka"
	mysqlrepo "github.com/Lysia-0113/GO-CHAT/internal/infrastructure/mysql/repository"
	redisinfra "github.com/Lysia-0113/GO-CHAT/internal/infrastructure/redis"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
	"github.com/Lysia-0113/GO-CHAT/internal/resilience"
	"github.com/Lysia-0113/GO-CHAT/internal/svc"
	"github.com/Lysia-0113/GO-CHAT/internal/user"
)

// NewLogger 创建结构化日志器。
func NewLogger(cfg config.LogConfig) *slog.Logger {
	level := slog.LevelInfo
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: level}
	if cfg.Pretty {
		return slog.New(slog.NewTextHandler(newStdoutWriter(), opts))
	}
	return slog.New(slog.NewJSONHandler(newStdoutWriter(), opts))
}

// Build 装配完整应用：基础设施 → 仓储 → 服务 → 传输层 → Worker。
// 返回 App 便于 Run 与优雅退出。
func Build(ctx context.Context, cfg *config.Config, log *slog.Logger) (*App, error) {
	cfg.Normalize()

	// ---- 指标注册表 ----
	reg := metrics.New()

	// ---- MySQL ----
	db, err := openMySQL(cfg.MySQL, log)
	if err != nil {
		return nil, fmt.Errorf("mysql: %w", err)
	}

	// ---- Redis ----
	rdb, scripts, err := openRedis(cfg.Redis)
	if err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}
	redisOpts := redisinfra.Options{
		ReadTimeout:  cfg.Resilience.RedisReadTimeout,
		WriteTimeout: cfg.Resilience.RedisWriteTimeout,
	}

	// ---- Kafka ----
	producer, err := kafkainfra.NewProducer(kafkainfra.ProducerConfig{
		Brokers:     cfg.Kafka.Brokers,
		Timeout:     cfg.Kafka.ProducerTimeout,
		AcksAll:     cfg.Kafka.ProducerAcksAll,
		TopicSuffix: cfg.Kafka.TopicPrefix,
		Logger:      kafkainfra.SlogLogger(log),
	}, newBreakers(cfg), "gateway")
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	topics := kafkainfra.NewTopics(cfg.Kafka.TopicPrefix)

	// ---- Repository ----
	userRepo := mysqlrepo.NewUserRepository(db)
	convRepo := mysqlrepo.NewConversationRepository(db)
	msgRepo := mysqlrepo.NewMessageRepository(db)
	segRepo := mysqlrepo.NewSegmentRepository(db)
	outboxRepo := mysqlrepo.NewOutboxRepository(db)

	// ---- IDGenerator（启动时立即申请第一段） ----
	userIDs, err := newSegmentGenerator(ctx, "im_user", segRepo, cfg)
	if err != nil {
		return nil, err
	}
	convIDs, err := newSegmentGenerator(ctx, "im_conversation", segRepo, cfg)
	if err != nil {
		return nil, err
	}
	msgIDs, err := newSegmentGenerator(ctx, "im_message", segRepo, cfg)
	if err != nil {
		return nil, err
	}

	// ---- 鉴权 ----
	tokens := auth.NewTokenManager(cfg.Auth.JWTSecret, cfg.Auth.AccessTokenTTL)

	// ---- Redis 组件 ----
	wsTickets := redisinfra.NewWSTicketStore(rdb, scripts, cfg.Auth.TicketTTL, redisOpts)
	cursors := redisinfra.NewCursorStore(rdb, scripts, redisOpts)
	presence := redisinfra.NewPresenceRegistry(rdb, cfg.Presence.ConnHashTTL, cfg.Presence.UserSetTTL, redisOpts)
	rateLimiter := redisinfra.NewRateLimiter(rdb, scripts, redisOpts)
	rateLimiter.SetObservers(
		func(key string) {
			metrics.RateLimitRejected.WithLabelValues("send").Inc()
		},
		func(key string) { metrics.RateLimitL1Fallback.Inc() },
	)
	idemStore := redisinfra.NewIdempotencyStore(rdb, scripts, cfg.Presence.IdemProcessingTTL, cfg.Presence.IdemAcceptedTTL, redisOpts)

	// ---- Service ----
	users := user.NewService(user.Dependencies{
		Users:   userRepo,
		UserIDs: userIDs,
		Tokens:  tokens,
		Argon2: auth.Argon2Params{
			Time:    cfg.Auth.Argon2Time,
			Memory:  cfg.Auth.Argon2Memory,
			Threads: cfg.Auth.Argon2Threads,
			KeyLen:  cfg.Auth.Argon2KeyLen,
		},
	})
	convos := conversation.NewService(conversation.Dependencies{
		Conversations:   convRepo,
		ConversationIDs: convIDs,
	})
	connManager := connection.NewManager(cfg.Server.NodeID, presence)

	breakers := newBreakers(cfg)

	messages := message.NewService(message.Dependencies{
		Messages:      msgRepo,
		Members:       convos,
		Conversations: convos,
		Cursors:       cursors,
		Publisher:     kafkainfra.NewPublisher(producer, "ws-gateway"),
		RateLimiter:   &rateLimiterAdapter{r: rateLimiter, cfg: cfg.Resilience},
		FastIdem:      idemStore,
		IdemFallback:  true, // Redis 故障时跳过快速幂等，MySQL 唯一索引兜底
	})

	// ---- Worker 消费者 ----
	persistConsumer, err := kafkainfra.NewConsumer(kafkainfra.ConsumerConfig{
		Brokers:          cfg.Kafka.Brokers,
		Topic:            topics.Ingress(),
		Group:            cfg.Kafka.PersistGroup,
		StartOffset:      cfg.Kafka.AutoOffsetReset,
		MaxBytes:         8 * 1024 * 1024,
		SessionTimeout:   10 * time.Second,
		RebalanceTimeout: 10 * time.Second,
		Logger:           kafkainfra.SlogLogger(log),
	})
	if err != nil {
		return nil, fmt.Errorf("kafka persist consumer: %w", err)
	}
	deliverConsumer, err := kafkainfra.NewConsumer(kafkainfra.ConsumerConfig{
		Brokers: cfg.Kafka.Brokers,
		Topic:   topics.Persisted(),
		// 广播模型：每节点独立消费者组（组名带 node_id），各自消费全部分区
		Group:            cfg.Kafka.DeliveryGroup + "-" + cfg.Server.NodeID,
		StartOffset:      cfg.Kafka.AutoOffsetReset,
		MaxBytes:         8 * 1024 * 1024,
		SessionTimeout:   10 * time.Second,
		RebalanceTimeout: 10 * time.Second,
		Logger:           kafkainfra.SlogLogger(log),
	})
	if err != nil {
		return nil, fmt.Errorf("kafka deliver consumer: %w", err)
	}
	dlqConsumer, err := kafkainfra.NewConsumer(kafkainfra.ConsumerConfig{
		Brokers:          cfg.Kafka.Brokers,
		Topic:            topics.DLQ(),
		Group:            cfg.Kafka.DLQGroup,
		StartOffset:      cfg.Kafka.AutoOffsetReset,
		MaxBytes:         8 * 1024 * 1024,
		SessionTimeout:   10 * time.Second,
		RebalanceTimeout: 10 * time.Second,
		Logger:           kafkainfra.SlogLogger(log),
	})
	if err != nil {
		return nil, fmt.Errorf("kafka dlq consumer: %w", err)
	}

	// ---- ServiceContext（服务定位器，GOCHAT_API.md §11.3） ----
	// 全量依赖在此装配一次；传输层 / Worker 经 svcCtx 取用。
	svcCtx := &svc.ServiceContext{
		Config: *cfg,
		Log:    log,

		DB:     db,
		Redis:  rdb,
		Kafka:  producer,
		Topics: topics,

		Presence:    presence,
		WSTickets:   wsTickets,
		Cursors:     cursors,
		RateLimiter: rateLimiter,
		IdemStore:   idemStore,

		ConnManager: connManager,

		Breakers:        breakers,
		HistoryBulkhead: resilience.NewBulkhead("history_query", cfg.Resilience.HistoryQueryConcurrency),
		IngressBulkhead: resilience.NewBulkhead("ws_ingress", cfg.Resilience.WSIngressConcurrency),

		Tokens: tokens,

		UserService:         users,
		ConversationService: convos,
		MessageService:      messages,

		UserRepo:   userRepo,
		ConvRepo:   convRepo,
		MsgRepo:    msgRepo,
		OutboxRepo: outboxRepo,
		UserIDs:    userIDs,
		ConvIDs:    convIDs,
		MessageIDs: msgIDs,

		PersistConsumer: persistConsumer,
		DeliverConsumer: deliverConsumer,
		DLQConsumer:     dlqConsumer,
	}

	return &App{
		svcCtx: svcCtx,
		reg:    reg,
	}, nil
}

// rateLimiterAdapter 适配 message.RateLimiter 接口。
type rateLimiterAdapter struct {
	r   *redisinfra.RateLimiter
	cfg config.ResilienceConfig
}

func (a *rateLimiterAdapter) AllowSend(ctx context.Context, userID, conversationID int64) error {
	if a.r == nil {
		return nil
	}
	if ok, wait, _ := a.r.AllowSendUser(ctx, userID, float64(a.cfg.SendBurst), a.cfg.SendPerSecond); !ok {
		return errs.Retryable(errs.RateLimited, "发送过于频繁", wait)
	}
	if ok, wait, _ := a.r.AllowSendConversation(ctx, conversationID, float64(a.cfg.ConversationBurst), a.cfg.ConversationPerSecond); !ok {
		return errs.Retryable(errs.RateLimited, "会话发送过于频繁", wait)
	}
	return nil
}

func (a *rateLimiterAdapter) AllowHistory(ctx context.Context, userID, conversationID int64) error {
	if a.r == nil {
		return nil
	}
	if ok, wait, _ := a.r.AllowHistory(ctx, userID, conversationID, float64(a.cfg.HistoryBurst), a.cfg.HistoryPerSecond); !ok {
		return errs.Retryable(errs.RateLimited, "查询过于频繁", wait)
	}
	return nil
}

// openMySQL 打开 GORM 连接并配置连接池。
func openMySQL(cfg config.MySQLConfig, log *slog.Logger) (*gorm.DB, error) {
	gormLog := logger.New(&slogWriter{log: log}, logger.Config{
		SlowThreshold:             cfg.SlowThreshold,
		LogLevel:                  logger.Warn,
		IgnoreRecordNotFoundError: true,
		ParameterizedQueries:      true, // 生产不记录参数（脱敏，GOCHAT_DATABASE.md §2.4）
	})
	db, err := gorm.Open(mysql.Open(cfg.DSN), &gorm.Config{Logger: gormLog})
	if err != nil {
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	return db, nil
}

// openRedis 创建 Redis 客户端并加载 Lua 脚本。
func openRedis(cfg config.RedisConfig) (*goredis.Client, *redisinfra.Scripts, error) {
	client := goredis.NewClient(&goredis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})
	scripts, err := redisinfra.LoadScripts()
	if err != nil {
		return nil, nil, err
	}
	return client, scripts, nil
}

// newSegmentGenerator 创建绑定 biz_tag 的双 Buffer 发号器。
func newSegmentGenerator(ctx context.Context, bizTag string, repo *mysqlrepo.SegmentRepository, cfg *config.Config) (*segment.Generator, error) {
	return segment.NewGenerator(ctx, bizTag, repo,
		segment.WithPrefetchRatio(cfg.IDGen.PrefetchRatio),
		segment.WithAllocateTimeout(cfg.IDGen.AllocateTimeout),
		segment.WithNextWaitTimeout(cfg.IDGen.NextWaitTimeout),
		segment.WithMaxRetries(cfg.IDGen.CASMaxRetries),
	)
}

// newBreakers 从配置构建熔断器集合。
func newBreakers(cfg *config.Config) *resilience.Breakers {
	configs := make([]resilience.BreakerConfig, 0, len(cfg.Resilience.Breakers))
	for _, b := range cfg.Resilience.Breakers {
		configs = append(configs, resilience.BreakerConfig{
			Name:         b.Name,
			Interval:     b.Interval,
			MinRequests:  b.MinRequests,
			FailureRatio: b.FailureRatio,
			OpenTimeout:  b.OpenTimeout,
			HalfOpenMax:  b.HalfOpenMax,
		})
	}
	return resilience.NewBreakers(configs)
}
