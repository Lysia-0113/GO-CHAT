// Package bootstrap 提供配置加载与应用装配（GOCHAT_API.md §11.3 构造顺序）：
//
//	Config → DB / Redis / Kafka / IDGenerator → Repository → Service → Handler / Worker
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
	"github.com/Lysia-0113/GO-CHAT/internal/connection"
	"github.com/Lysia-0113/GO-CHAT/internal/conversation"
	"github.com/Lysia-0113/GO-CHAT/internal/errs"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/idgen"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/idgen/segment"
	kafkainfra "github.com/Lysia-0113/GO-CHAT/internal/infrastructure/kafka"
	mysqlrepo "github.com/Lysia-0113/GO-CHAT/internal/infrastructure/mysql/repository"
	redisinfra "github.com/Lysia-0113/GO-CHAT/internal/infrastructure/redis"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
	"github.com/Lysia-0113/GO-CHAT/internal/resilience"
	"github.com/Lysia-0113/GO-CHAT/internal/user"
)

// ServiceContext 是应用装配容器（GOCHAT_API.md §11.3）。
//
// ServiceContext 在启动阶段初始化一次，是组合根不是全局变量：
//   - 生命周期：进程启动到优雅退出；
//   - 职责：配置、连接池、Kafka、号段器、已构造的领域服务；
//   - 禁止：承载用户/会话/消息等请求级状态，禁止被业务代码作为万能入口使用。
type ServiceContext struct {
	Config Config

	DB         *gorm.DB
	Redis      goredis.UniversalClient
	Kafka      *kafkainfra.Producer
	MessageIDs idgen.IDGenerator
	UserIDs    idgen.IDGenerator
	ConvIDs    idgen.IDGenerator

	UserService         *user.Service
	ConversationService *conversation.Service
	MessageService      *message.Service
}

// NewLogger 创建结构化日志器。
func NewLogger(cfg LogConfig) *slog.Logger {
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
func Build(ctx context.Context, cfg *Config, log *slog.Logger) (*App, error) {
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
	}, newBreakers(cfg, reg), reg, "gateway")
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
	recentCache := redisinfra.NewRecentMessageCache(rdb, scripts, cfg.Presence.RecentCacheTTL, cfg.Presence.RecentCacheMax, redisOpts)
	presence := redisinfra.NewPresenceRegistry(rdb, cfg.Presence.ConnHashTTL, cfg.Presence.UserSetTTL, redisOpts)
	rateLimiter := redisinfra.NewRateLimiter(rdb, scripts, redisOpts)
	rateLimiter.SetObservers(
		func(key string) {
			reg.Counter(metrics.NameRateLimitRejected, "限流拒绝数", map[string]string{"key_type": "send"}).Inc()
		},
		func(key string) { reg.Counter("rate_limit_l1_fallback_total", "本地限流降级次数", nil).Inc() },
	)
	idemStore := redisinfra.NewIdempotencyStore(rdb, scripts, cfg.Presence.IdemProcessingTTL, cfg.Presence.IdemAcceptedTTL, redisOpts)
	pubsub := redisinfra.NewPubsubGateway(rdb, cfg.Server.NodeID, redisOpts)

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

	breakers := newBreakers(cfg, reg)

	messages := message.NewService(message.Dependencies{
		Messages:      msgRepo,
		Members:       convos,
		Conversations: convos,
		RecentCache:   recentCache,
		Publisher:     kafkainfra.NewPublisher(producer, "ws-gateway"),
		RateLimiter:   &rateLimiterAdapter{r: rateLimiter, cfg: cfg.Resilience},
		FastIdem:      idemStore,
		IdemFallback:  true, // Redis 故障时跳过快速幂等，MySQL 唯一索引兜底
	})

	svcCtx := &ServiceContext{
		Config:              *cfg,
		DB:                  db,
		Redis:               rdb,
		Kafka:               producer,
		MessageIDs:          msgIDs,
		UserIDs:             userIDs,
		ConvIDs:             convIDs,
		UserService:         users,
		ConversationService: convos,
		MessageService:      messages,
	}

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
		Brokers:          cfg.Kafka.Brokers,
		Topic:            topics.Persisted(),
		Group:            cfg.Kafka.DeliveryGroup,
		StartOffset:      cfg.Kafka.AutoOffsetReset,
		MaxBytes:         8 * 1024 * 1024,
		SessionTimeout:   10 * time.Second,
		RebalanceTimeout: 10 * time.Second,
		Logger:           kafkainfra.SlogLogger(log),
	})
	if err != nil {
		return nil, fmt.Errorf("kafka deliver consumer: %w", err)
	}

	return &App{
		cfg:    cfg,
		log:    log,
		reg:    reg,
		db:     db,
		redis:  rdb,
		kafka:  producer,
		svcCtx: svcCtx,

		presence: presence,
		pubsub:   pubsub,

		persistConsumer: persistConsumer,
		deliverConsumer: deliverConsumer,
		outboxRepo:      outboxRepo,
		topics:          topics,

		userRepo:        userRepo,
		convRepo:        convRepo,
		msgRepo:         msgRepo,
		msgIDs:          msgIDs,
		userIDs:         userIDs,
		convIDs:         convIDs,
		users:           users,
		convos:          convos,
		messages:        messages,
		connManager:     connManager,
		wsTickets:       wsTickets,
		recentCache:     recentCache,
		rateLimiter:     rateLimiter,
		idemStore:       idemStore,
		breakers:        breakers,
		tokens:          tokens,
		historyBulkhead: resilience.NewBulkhead("history_query", cfg.Resilience.HistoryQueryConcurrency),
		ingressBulkhead: resilience.NewBulkhead("ws_ingress", cfg.Resilience.WSIngressConcurrency),
	}, nil
}

// rateLimiterAdapter 适配 message.RateLimiter 接口。
type rateLimiterAdapter struct {
	r   *redisinfra.RateLimiter
	cfg ResilienceConfig
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
func openMySQL(cfg MySQLConfig, log *slog.Logger) (*gorm.DB, error) {
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
func openRedis(cfg RedisConfig) (*goredis.Client, *redisinfra.Scripts, error) {
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
func newSegmentGenerator(ctx context.Context, bizTag string, repo *mysqlrepo.SegmentRepository, cfg *Config) (*segment.Generator, error) {
	return segment.NewGenerator(ctx, bizTag, repo,
		segment.WithPrefetchRatio(cfg.IDGen.PrefetchRatio),
		segment.WithAllocateTimeout(cfg.IDGen.AllocateTimeout),
		segment.WithNextWaitTimeout(cfg.IDGen.NextWaitTimeout),
		segment.WithMaxRetries(cfg.IDGen.CASMaxRetries),
	)
}

// newBreakers 从配置构建熔断器集合。
func newBreakers(cfg *Config, reg *metrics.Registry) *resilience.Breakers {
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
	return resilience.NewBreakers(configs, reg)
}
