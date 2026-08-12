// Package config 提供全部运行时配置的加载（YAML + 环境变量覆盖）。
//
// 独立成包的原因（GOCHAT_API.md §11.3）：svc.ServiceContext 需要持有 Config，
// 而 bootstrap 要构造 svcCtx；Config 若留在 bootstrap 会造成 bootstrap↔svc 循环导入。
// 与 go-zero 的 internal/config + internal/svc 布局一致。
package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 是全部运行时配置。字段语义对应 5 份设计文档。
type Config struct {
	Server     ServerConfig     `yaml:"server"`
	MySQL      MySQLConfig      `yaml:"mysql"`
	Redis      RedisConfig      `yaml:"redis"`
	Kafka      KafkaConfig      `yaml:"kafka"`
	Auth       AuthConfig       `yaml:"auth"`
	IDGen      IDGenConfig      `yaml:"idgen"`
	Presence   PresenceConfig   `yaml:"presence"`
	Resilience ResilienceConfig `yaml:"resilience"`
	Metrics    MetricsConfig    `yaml:"metrics"`
	Log        LogConfig        `yaml:"log"`
}

type ServerConfig struct {
	// HTTP 监听地址，如 ":8080"
	Addr string `yaml:"addr"`
	// NodeID 多网关节点唯一标识；单节点部署可留空，默认 "node-1"
	NodeID string `yaml:"node_id"`
	// ShutdownTimeout 优雅退出等待时间
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
	// WSReadLimit 单个 WebSocket Frame 最大字节数
	WSReadLimit int64 `yaml:"ws_read_limit"`
	// WSHeartbeatInterval 服务端心跳间隔
	WSHeartbeatInterval time.Duration `yaml:"ws_heartbeat_interval"`
	// WSMissedHeartbeat 连续未响应心跳次数达到该值则断开
	WSMissedHeartbeat int `yaml:"ws_missed_heartbeat"`
	// WSWriteQueueSize 单连接出站队列容量（有界队列，背压保护）
	WSWriteQueueSize int `yaml:"ws_write_queue_size"`
	// WSWriteQueueTimeout 队列持续满的时间阈值，超过则断开慢连接
	WSWriteQueueTimeout time.Duration `yaml:"ws_write_queue_timeout"`
}

type MySQLConfig struct {
	DSN string `yaml:"dsn"`
	// MaxOpenConns / MaxIdleConns / ConnMaxLifetime 主连接池
	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
	// 读写逻辑分离的连接池（GOCHAT_RESILIENCE.md §6.2）
	ReadMaxOpenConns  int `yaml:"read_max_open_conns"`
	WriteMaxOpenConns int `yaml:"write_max_open_conns"`
	// SlowThreshold GORM 慢 SQL 阈值
	SlowThreshold time.Duration `yaml:"slow_threshold"`
}

type RedisConfig struct {
	Addr         string        `yaml:"addr"`
	Password     string        `yaml:"password"`
	DB           int           `yaml:"db"`
	PoolSize     int           `yaml:"pool_size"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	// TopicPrefix 环境后缀之前缀，如 "im.message.ingress.dev"
	TopicPrefix string `yaml:"topic_prefix"`
	// Producer 配置
	ProducerTimeout time.Duration `yaml:"producer_timeout"`
	ProducerAcksAll bool          `yaml:"producer_acks_all"`
	// Consumer Group 名
	PersistGroup  string `yaml:"persist_group"`
	DeliveryGroup string `yaml:"delivery_group"`
	// Consumer 配置
	AutoOffsetReset string `yaml:"auto_offset_reset"`
	MaxPollRecords  int    `yaml:"max_poll_records"`
	// 持久化失败重试：最大次数 + 退避序列
	PersistMaxRetries int           `yaml:"persist_max_retries"`
	PersistBackoff    time.Duration `yaml:"persist_backoff"`
	// Outbox Publisher 配置
	OutboxMaxRetries   int           `yaml:"outbox_max_retries"`
	OutboxBackoff      time.Duration `yaml:"outbox_backoff"`
	OutboxPollInterval time.Duration `yaml:"outbox_poll_interval"`
	OutboxBatchSize    int           `yaml:"outbox_batch_size"`
}

type AuthConfig struct {
	// JWTSecret 生产环境必须通过环境变量 GOChat_AUTH_JWT_SECRET 注入
	JWTSecret      string        `yaml:"jwt_secret"`
	AccessTokenTTL time.Duration `yaml:"access_token_ttl"`
	// TicketTTL WebSocket 一次性票据有效期
	TicketTTL time.Duration `yaml:"ticket_ttl"`
	// Argon2id 参数
	Argon2Time    uint32 `yaml:"argon2_time"`
	Argon2Memory  uint32 `yaml:"argon2_memory"`
	Argon2Threads uint8  `yaml:"argon2_threads"`
	Argon2KeyLen  uint32 `yaml:"argon2_key_len"`
}

type IDGenConfig struct {
	// Step 每次向 MySQL 申请的号段长度
	Step int64 `yaml:"step"`
	// PrefetchRatio 当前号段使用达到该比例时后台预加载 next
	PrefetchRatio float64 `yaml:"prefetch_ratio"`
	// AllocateTimeout 申请新号段超时
	AllocateTimeout time.Duration `yaml:"allocate_timeout"`
	// NextWaitTimeout current 用尽、next 未就绪时的等待上限
	NextWaitTimeout time.Duration `yaml:"next_wait_timeout"`
	// CASMaxRetries 分布式申请冲突重试次数
	CASMaxRetries int `yaml:"cas_max_retries"`
}

type PresenceConfig struct {
	// ConnHashTTL 连接路由 HASH 的 TTL（心跳周期 ~30s，建议 90s）
	ConnHashTTL time.Duration `yaml:"conn_hash_ttl"`
	// UserSetTTL 用户在线 ZSET 的 TTL
	UserSetTTL time.Duration `yaml:"user_set_ttl"`
	// RecentCacheTTL 最近消息缓存 TTL
	RecentCacheTTL time.Duration `yaml:"recent_cache_ttl"`
	// RecentCacheMax 每会话缓存的最大消息数
	RecentCacheMax int `yaml:"recent_cache_max"`
	// IdemProcessingTTL 消息入口快速幂等"处理中" TTL
	IdemProcessingTTL time.Duration `yaml:"idem_processing_ttl"`
	// IdemAcceptedTTL 幂等"已接受" TTL
	IdemAcceptedTTL time.Duration `yaml:"idem_accepted_ttl"`
}

type ResilienceConfig struct {
	// 超时（GOCHAT_RESILIENCE.md §4.2）
	RedisReadTimeout      time.Duration `yaml:"redis_read_timeout"`
	RedisWriteTimeout     time.Duration `yaml:"redis_write_timeout"`
	MySQLQueryTimeout     time.Duration `yaml:"mysql_query_timeout"`
	PersistTxTimeout      time.Duration `yaml:"persist_tx_timeout"`
	IngressPublishTimeout time.Duration `yaml:"ingress_publish_timeout"`
	SegmentTimeout        time.Duration `yaml:"segment_timeout"`
	PubsubTimeout         time.Duration `yaml:"pubsub_timeout"`

	// 限流（GOCHAT_RESILIENCE.md §5.2）
	SendPerSecond             float64 `yaml:"send_per_second"`
	SendBurst                 int     `yaml:"send_burst"`
	ConversationPerSecond     float64 `yaml:"conversation_per_second"`
	ConversationBurst         int     `yaml:"conversation_burst"`
	HistoryPerSecond          float64 `yaml:"history_per_second"`
	HistoryBurst              int     `yaml:"history_burst"`
	LoginPerMinute            int     `yaml:"login_per_minute"`
	WSConnectPerMinute        int     `yaml:"ws_connect_per_minute"`
	WSConnectPerUserPerMinute int     `yaml:"ws_connect_per_user_per_minute"`
	ConnInboundPerSecond      float64 `yaml:"conn_inbound_per_second"`
	ConnInboundBurst          int     `yaml:"conn_inbound_burst"`

	// 隔离并发（GOCHAT_RESILIENCE.md §6.1）
	HistoryQueryConcurrency int `yaml:"history_query_concurrency"`
	WSIngressConcurrency    int `yaml:"ws_ingress_concurrency"`
	PersistWorkers          int `yaml:"persist_workers"`
	OutboxWorkers           int `yaml:"outbox_workers"`
	DeliveryWorkers         int `yaml:"delivery_workers"`
	GroupFanoutConcurrency  int `yaml:"group_fanout_concurrency"`

	// 熔断（GOCHAT_RESILIENCE.md §7.3），按 依赖:操作 配置
	Breakers []BreakerConfig `yaml:"breakers"`
}

type BreakerConfig struct {
	// Name 形如 "redis:recent_get"、"kafka:ingress_publish"、"mysql:history_query"
	Name         string        `yaml:"name"`
	Interval     time.Duration `yaml:"interval"`
	MinRequests  uint32        `yaml:"min_requests"`
	FailureRatio float64       `yaml:"failure_ratio"`
	OpenTimeout  time.Duration `yaml:"open_timeout"`
	HalfOpenMax  uint32        `yaml:"half_open_max"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`
}

type LogConfig struct {
	// Level debug / info / warn / error
	Level string `yaml:"level"`
	// Pretty 开发环境开启彩色日志
	Pretty bool `yaml:"pretty"`
}

// LoadConfig 从 path 读取 YAML 配置，随后用 GOChat_ 前缀环境变量覆盖。
// 覆盖规则：GOChat_MYSQL_DSN、GOChat_AUTH_JWT_SECRET 等；数组类暂不支持 env 覆盖。
// 文件不存在时回退为默认配置 + 环境变量覆盖，便于 Docker 等纯环境变量部署。
func LoadConfig(path string) (*Config, error) {
	cfg := defaultConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		applyEnvOverrides(cfg)
		return cfg, nil
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	applyEnvOverrides(cfg)
	return cfg, nil
}

func defaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Addr:                ":8080",
			NodeID:              "node-1",
			ShutdownTimeout:     10 * time.Second,
			WSReadLimit:         64 * 1024,
			WSHeartbeatInterval: 30 * time.Second,
			WSMissedHeartbeat:   2,
			WSWriteQueueSize:    256,
			WSWriteQueueTimeout: 30 * time.Second,
		},
		MySQL: MySQLConfig{
			DSN:               "gochat:gochat@tcp(127.0.0.1:3306)/gochat?charset=utf8mb4&parseTime=True&loc=UTC",
			MaxOpenConns:      40,
			MaxIdleConns:      10,
			ConnMaxLifetime:   30 * time.Minute,
			ReadMaxOpenConns:  24,
			WriteMaxOpenConns: 16,
			SlowThreshold:     200 * time.Millisecond,
		},
		Redis: RedisConfig{
			Addr:         "127.0.0.1:6379",
			DB:           0,
			PoolSize:     40,
			DialTimeout:  1 * time.Second,
			ReadTimeout:  500 * time.Millisecond,
			WriteTimeout: 500 * time.Millisecond,
		},
		Kafka: KafkaConfig{
			Brokers:            []string{"127.0.0.1:9092"},
			ProducerTimeout:    3 * time.Second,
			ProducerAcksAll:    true,
			PersistGroup:       "gochat-message-persist-v1",
			DeliveryGroup:      "gochat-message-delivery-v1",
			AutoOffsetReset:    "earliest",
			MaxPollRecords:     100,
			PersistMaxRetries:  5,
			PersistBackoff:     200 * time.Millisecond,
			OutboxMaxRetries:   10,
			OutboxBackoff:      2 * time.Second,
			OutboxPollInterval: 500 * time.Millisecond,
			OutboxBatchSize:    100,
		},
		Auth: AuthConfig{
			JWTSecret:      "change-me-in-production",
			AccessTokenTTL: 2 * time.Hour,
			TicketTTL:      30 * time.Second,
			Argon2Time:     1,
			Argon2Memory:   64 * 1024,
			Argon2Threads:  4,
			Argon2KeyLen:   32,
		},
		IDGen: IDGenConfig{
			Step:            2000,
			PrefetchRatio:   0.70,
			AllocateTimeout: 100 * time.Millisecond,
			NextWaitTimeout: 50 * time.Millisecond,
			CASMaxRetries:   10,
		},
		Presence: PresenceConfig{
			ConnHashTTL:       90 * time.Second,
			UserSetTTL:        3 * time.Minute,
			RecentCacheTTL:    24 * time.Hour,
			RecentCacheMax:    200,
			IdemProcessingTTL: 5 * time.Second,
			IdemAcceptedTTL:   10 * time.Minute,
		},
		Resilience: ResilienceConfig{
			RedisReadTimeout:      50 * time.Millisecond,
			RedisWriteTimeout:     80 * time.Millisecond,
			MySQLQueryTimeout:     200 * time.Millisecond,
			PersistTxTimeout:      500 * time.Millisecond,
			IngressPublishTimeout: 300 * time.Millisecond,
			SegmentTimeout:        100 * time.Millisecond,
			PubsubTimeout:         100 * time.Millisecond,

			SendPerSecond:             20,
			SendBurst:                 40,
			ConversationPerSecond:     100,
			ConversationBurst:         200,
			HistoryPerSecond:          10,
			HistoryBurst:              20,
			LoginPerMinute:            5,
			WSConnectPerMinute:        60,
			WSConnectPerUserPerMinute: 5,
			ConnInboundPerSecond:      100,
			ConnInboundBurst:          150,

			HistoryQueryConcurrency: 32,
			WSIngressConcurrency:    32,
			PersistWorkers:          8,
			OutboxWorkers:           4,
			DeliveryWorkers:         8,
			GroupFanoutConcurrency:  4,

			Breakers: []BreakerConfig{
				{Name: "redis:recent_get", Interval: 10 * time.Second, MinRequests: 20, FailureRatio: 0.5, OpenTimeout: 5 * time.Second, HalfOpenMax: 3},
				{Name: "redis:gateway_publish", Interval: 10 * time.Second, MinRequests: 20, FailureRatio: 0.5, OpenTimeout: 5 * time.Second, HalfOpenMax: 3},
				{Name: "kafka:ingress_publish", Interval: 10 * time.Second, MinRequests: 10, FailureRatio: 0.3, OpenTimeout: 3 * time.Second, HalfOpenMax: 3},
				{Name: "kafka:persisted_publish", Interval: 10 * time.Second, MinRequests: 10, FailureRatio: 0.3, OpenTimeout: 5 * time.Second, HalfOpenMax: 3},
				{Name: "mysql:history_query", Interval: 10 * time.Second, MinRequests: 20, FailureRatio: 0.5, OpenTimeout: 3 * time.Second, HalfOpenMax: 3},
				{Name: "mysql:id_segment", Interval: 30 * time.Second, MinRequests: 5, FailureRatio: 0.6, OpenTimeout: 10 * time.Second, HalfOpenMax: 1},
			},
		},
		Metrics: MetricsConfig{Enabled: true, Path: "/metrics"},
		Log:     LogConfig{Level: "info", Pretty: false},
	}
}

// applyEnvOverrides 用 GOChat_ 前缀的环境变量覆盖标量配置。
// 约定：GOChat_<SECTION>_<FIELD>，如 GOChat_MYSQL_DSN、GOChat_AUTH_JWT_SECRET。
func applyEnvOverrides(cfg *Config) {
	setStr("GOChat_SERVER_ADDR", &cfg.Server.Addr)
	setStr("GOChat_SERVER_NODE_ID", &cfg.Server.NodeID)
	setStr("GOChat_MYSQL_DSN", &cfg.MySQL.DSN)
	setStr("GOChat_REDIS_ADDR", &cfg.Redis.Addr)
	setStr("GOChat_REDIS_PASSWORD", &cfg.Redis.Password)
	setStr("GOChat_AUTH_JWT_SECRET", &cfg.Auth.JWTSecret)
	setInt("GOChat_REDIS_DB", &cfg.Redis.DB)
	setInt("GOChat_SERVER_WS_WRITE_QUEUE_SIZE", &cfg.Server.WSWriteQueueSize)
	setStr("GOChat_KAFKA_BROKERS", &kafkaBrokersEnv) // 逗号分隔，最后统一处理
}

var kafkaBrokersEnv string

// setStr 读取环境变量并赋值。
func setStr(key string, dst *string) {
	if v, ok := os.LookupEnv(key); ok {
		*dst = v
	}
}

func setInt(key string, dst *int) {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			*dst = n
		}
	}
}

// Normalize 在装配前做派生字段处理（Kafka Broker 列表等）。
func (c *Config) Normalize() {
	if kafkaBrokersEnv != "" {
		parts := strings.Split(kafkaBrokersEnv, ",")
		brokers := make([]string, 0, len(parts))
		for _, p := range parts {
			if p = strings.TrimSpace(p); p != "" {
				brokers = append(brokers, p)
			}
		}
		if len(brokers) > 0 {
			c.Kafka.Brokers = brokers
		}
	}
	if c.Kafka.TopicPrefix != "" {
		c.Kafka.TopicPrefix = strings.TrimSuffix(c.Kafka.TopicPrefix, ".")
	}
}
