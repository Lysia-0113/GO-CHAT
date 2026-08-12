// Package metrics 定义全部可观测指标，基于官方 prometheus/client_golang。
//
// 设计约定：
//   - 所有指标在启动前声明为全局变量（编译期固定，运行时不新建）；
//   - 无标签指标用 NewCounter/NewGauge，直接 Inc()/Set()；
//   - 带标签指标用 New*Vec，标签键在定义时声明，调用只给值
//     （WithLabelValues），拼错在编译期暴露；
//   - 名字合法性、同名冲突由注册时校验（MustRegister 失败直接 panic）。
//
// 指标清单对应 GOCHAT_RESILIENCE.md §11.1 / GOCHAT_KAFKA.md §11.2。
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ---- 计数器（只增不减，名字以 _total 结尾是 Prometheus 约定）----

// RateLimitRejected 限流拒绝数（按 key_type 区分维度）。
var RateLimitRejected = prometheus.NewCounterVec(
	prometheus.CounterOpts{Name: "rate_limit_rejected_total", Help: "限流拒绝数"},
	[]string{"key_type"},
)

// RateLimitL1Fallback 本地限流降级次数。
var RateLimitL1Fallback = prometheus.NewCounter(
	prometheus.CounterOpts{Name: "rate_limit_l1_fallback_total", Help: "本地限流降级次数"},
)

// WSTicketCreated 票据创建数。
var WSTicketCreated = prometheus.NewCounter(
	prometheus.CounterOpts{Name: "ws_ticket_created_total", Help: "票据创建数"},
)

// WSTicketReplayRejected 票据重放拒绝数。
var WSTicketReplayRejected = prometheus.NewCounter(
	prometheus.CounterOpts{Name: "ws_ticket_replay_rejected_total", Help: "票据重放拒绝数"},
)

// CloseReason 连接关闭原因分布。
var CloseReason = prometheus.NewCounterVec(
	prometheus.CounterOpts{Name: "close_reason_total", Help: "连接关闭原因"},
	[]string{"reason"},
)

// WSIngressReceived 收到的 message.send 总数。
var WSIngressReceived = prometheus.NewCounter(
	prometheus.CounterOpts{Name: "ws_ingress_received_total", Help: "收到的 message.send 总数"},
)

// PublishFailed Kafka 发布失败数。
var PublishFailed = prometheus.NewCounter(
	prometheus.CounterOpts{Name: "publish_failed_total", Help: "Kafka 发布失败数"},
)

// PushDropped 回执推送丢弃数。
var PushDropped = prometheus.NewCounter(
	prometheus.CounterOpts{Name: "push_dropped_total", Help: "回执推送丢弃数"},
)

// SlowConnectionClosed 慢连接断开数。
var SlowConnectionClosed = prometheus.NewCounter(
	prometheus.CounterOpts{Name: "websocket_slow_connection_closed_total", Help: "慢连接断开数"},
)

// KafkaProducerError 生产者失败数（按 topic 区分维度）。
var KafkaProducerError = prometheus.NewCounterVec(
	prometheus.CounterOpts{Name: "kafka_producer_error_total", Help: "生产者失败数"},
	[]string{"topic"},
)

// KafkaProducerSend 生产者发送总数。
var KafkaProducerSend = prometheus.NewCounterVec(
	prometheus.CounterOpts{Name: "kafka_producer_send_total", Help: "生产者发送总数"},
	[]string{"topic"},
)

// PersistSuccess 持久化成功数。
var PersistSuccess = prometheus.NewCounter(
	prometheus.CounterOpts{Name: "persist_success_total", Help: "持久化成功数"},
)

// PersistRetry 持久化重试次数。
var PersistRetry = prometheus.NewCounter(
	prometheus.CounterOpts{Name: "persist_retry_total", Help: "持久化重试次数"},
)

// PersistIdempotent 重复消费命中。
var PersistIdempotent = prometheus.NewCounter(
	prometheus.CounterOpts{Name: "persist_idempotent_total", Help: "重复消费命中"},
)

// KafkaDLQ 进入死信的事件数。
var KafkaDLQ = prometheus.NewCounterVec(
	prometheus.CounterOpts{Name: "kafka_dlq_total", Help: "进入死信的事件数"},
	[]string{"topic"},
)

// RecentCacheFallback 缓存回源次数。
var RecentCacheFallback = prometheus.NewCounter(
	prometheus.CounterOpts{Name: "recent_cache_fallback_total", Help: "缓存回源次数"},
)

// OnlineQueryFailed 在线状态查询失败数。
var OnlineQueryFailed = prometheus.NewCounter(
	prometheus.CounterOpts{Name: "online_query_failed_total", Help: "在线状态查询失败数"},
)

// PushToConnFailed 投递推送失败数。
var PushToConnFailed = prometheus.NewCounter(
	prometheus.CounterOpts{Name: "push_to_connection_failed_total", Help: "投递推送失败数"},
)

// OutboxPublishError Outbox 发布失败数。
var OutboxPublishError = prometheus.NewCounter(
	prometheus.CounterOpts{Name: "outbox_publish_error_total", Help: "Outbox 发布失败数"},
)

// ---- 仪表盘（可上可下，当前值）----

// WSConnectionActive 在线连接数。
var WSConnectionActive = prometheus.NewGauge(
	prometheus.GaugeOpts{Name: "websocket_connection_active", Help: "在线连接数"},
)

// IDSegmentRemaining 号段剩余库存（按 biz_tag 与节点区分维度）。
var IDSegmentRemaining = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{Name: "id_segment_remaining", Help: "号段剩余库存"},
	[]string{"biz_tag", "node"},
)

// BreakerState 熔断器状态 0=closed 1=open 2=half-open。
var BreakerState = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{Name: "breaker_state", Help: "熔断器状态 0=closed 1=open 2=half-open"},
	[]string{"dependency"},
)

// BulkheadQueueLength 隔离舱占用（按 worker 区分维度）。
var BulkheadQueueLength = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{Name: "bulkhead_queue_length", Help: "隔离舱占用"},
	[]string{"worker"},
)

// OutboxPending 待投递 Outbox 数量。
var OutboxPending = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{Name: "outbox_pending_count", Help: "待投递 Outbox 数量"},
	[]string{"event_type"},
)

// OutboxOldestAge 最老待投递记录年龄（秒）。
var OutboxOldestAge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{Name: "outbox_oldest_age_seconds", Help: "最老待投递记录年龄"},
	[]string{"event_type"},
)

// ---- 直方图（延迟分布，用于校准超时）----

// DependencyDuration 依赖调用耗时（GOCHAT_RESILIENCE.md §4.2 超时校准）。
// operation 取值：redis_recent_read / mysql_history_query / kafka:ingress_publish
// / kafka:persisted_publish / kafka:dlq_publish。
// 桶位覆盖超时表：Redis 50ms、MySQL 200ms、Kafka 300ms 均在桶内。
var DependencyDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "dependency_duration_seconds",
		Help:    "依赖调用耗时",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	},
	[]string{"operation"},
)

// New 创建注册表并注册全部指标（启动时调用一次）。
func New() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		RateLimitRejected, RateLimitL1Fallback,
		WSTicketCreated, WSTicketReplayRejected, CloseReason,
		WSIngressReceived, PublishFailed, PushDropped, SlowConnectionClosed,
		KafkaProducerError, KafkaProducerSend,
		PersistSuccess, PersistRetry, PersistIdempotent, KafkaDLQ,
		RecentCacheFallback, OnlineQueryFailed, PushToConnFailed, OutboxPublishError,
		WSConnectionActive, IDSegmentRemaining, BreakerState, BulkheadQueueLength,
		OutboxPending, OutboxOldestAge, DependencyDuration,
	)
	return reg
}

// Handler 返回标准 /metrics HTTP 处理器（promhttp 自带 Content-Type 与编码）。
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
