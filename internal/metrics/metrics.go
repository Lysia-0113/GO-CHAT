// Package metrics 提供轻量级进程内指标注册表，以 Prometheus 文本格式暴露。
//
// P0 只需要计数器和仪表盘两类指标；标签维度来自配置的固定键集合，
// 避免生产环境标签爆炸。实现线程安全，不依赖外部服务。
package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Metric 是带标签的指标值。
type Metric struct {
	Name   string
	Help   string
	Labels map[string]string
}

func (m Metric) key() string {
	return m.Name + "{" + labelKey(m.Labels) + "}"
}

func labelKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		if b.Len() > 0 {
			b.WriteString(",")
		}
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(labels[k])
	}
	return b.String()
}

func labelString(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("{")
	for i, k := range keys {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "%s=%q", k, labels[k])
	}
	b.WriteString("}")
	return b.String()
}

// Registry 保存全部指标。
type Registry struct {
	mu       sync.RWMutex
	counters map[string]*counter
	gauges   map[string]*gauge
}

type counter struct {
	metric Metric
	value  atomic.Int64
}

type gauge struct {
	metric Metric
	value  atomic.Int64
}

// New 创建空指标注册表。
func New() *Registry {
	return &Registry{
		counters: make(map[string]*counter),
		gauges:   make(map[string]*gauge),
	}
}

// Counter 获取（或创建）一个计数器。
func (r *Registry) Counter(name, help string, labels map[string]string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := Metric{Name: name, Help: help, Labels: labels}
	k := m.key()
	c, ok := r.counters[k]
	if !ok {
		c = &counter{metric: m}
		r.counters[k] = c
	}
	return &Counter{c}
}

// Gauge 获取（或创建）一个仪表盘。
func (r *Registry) Gauge(name, help string, labels map[string]string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := Metric{Name: name, Help: help, Labels: labels}
	k := m.key()
	g, ok := r.gauges[k]
	if !ok {
		g = &gauge{metric: m}
		r.gauges[k] = g
	}
	return &Gauge{g}
}

// Render 输出 Prometheus 文本格式。
func (r *Registry) Render() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var b strings.Builder
	counters := make([]*counter, 0, len(r.counters))
	for _, c := range r.counters {
		counters = append(counters, c)
	}
	sort.Slice(counters, func(i, j int) bool { return counters[i].metric.key() < counters[j].metric.key() })
	for _, c := range counters {
		writeMetric(&b, c.metric, "counter", float64(c.value.Load()))
	}
	gauges := make([]*gauge, 0, len(r.gauges))
	for _, g := range r.gauges {
		gauges = append(gauges, g)
	}
	sort.Slice(gauges, func(i, j int) bool { return gauges[i].metric.key() < gauges[j].metric.key() })
	for _, g := range gauges {
		writeMetric(&b, g.metric, "gauge", float64(g.value.Load()))
	}
	return b.String()
}

func writeMetric(b *strings.Builder, m Metric, typ string, value float64) {
	fmt.Fprintf(b, "# HELP %s %s\n", m.Name, m.Help)
	fmt.Fprintf(b, "# TYPE %s %s\n", m.Name, typ)
	fmt.Fprintf(b, "%s%s %v\n", m.Name, labelString(m.Labels), value)
}

// Counter 的递增接口。
type Counter struct{ c *counter }

func (c *Counter) Inc()         { c.c.value.Add(1) }
func (c *Counter) Add(n int64)  { c.c.value.Add(n) }
func (c *Counter) Value() int64 { return c.c.value.Load() }

// Gauge 的设置接口。
type Gauge struct{ g *gauge }

func (g *Gauge) Set(v int64)  { g.g.value.Store(v) }
func (g *Gauge) Add(v int64)  { g.g.value.Add(v) }
func (g *Gauge) Value() int64 { return g.g.value.Load() }

// 常用辅助指标命名（与 GOCHAT_RESILIENCE.md §11.1 / GOCHAT_KAFKA.md §11.2 对齐）。
const (
	NameRateLimitRejected      = "rate_limit_rejected_total"
	NameBreakerState           = "breaker_state"
	NameDependencyTimeout      = "dependency_timeout_total"
	NameBulkheadQueueLength    = "bulkhead_queue_length"
	NameBulkheadRejected       = "bulkhead_rejected_total"
	NameWSWriteQueueLength     = "websocket_write_queue_length"
	NameWSWriteQueueFull       = "websocket_write_queue_full_total"
	NameOutboxPending          = "outbox_pending_count"
	NameOutboxOldestAge        = "outbox_oldest_age_seconds"
	NameOutboxPublishError     = "outbox_publish_error_total"
	NameIDSegmentRemaining     = "id_segment_remaining"
	NamePersistSuccess         = "persist_success_total"
	NamePersistRetry           = "persist_retry_total"
	NamePersistIdempotent      = "persist_idempotent_total"
	NameRecentCacheHit         = "recent_cache_hit_total"
	NameRecentCacheFallback    = "recent_cache_fallback_total"
	NameWSConnectionActive     = "websocket_connection_active"
	NamePresenceStaleCleanup   = "presence_stale_cleanup_total"
	NameWSTicketCreated        = "ws_ticket_created_total"
	NameWSTicketConsumed       = "ws_ticket_consumed_total"
	NameWSTicketReplayRejected = "ws_ticket_replay_rejected_total"
	NameIdemFastHit            = "idem_fast_hit_total"
	NameKafkaDLQ               = "kafka_dlq_total"

	// 可观测性补充（2026-08 压测埋点）：消息管线每站一个计数器
	NameWSIngressReceived = "ws_ingress_received_total"
	NamePublishFailed     = "publish_failed_total"
	NamePushDropped       = "push_dropped_total"
	NameCloseReason       = "close_reason_total"
	NameOnlineQueryFailed = "online_query_failed_total"
	NamePushToConnFailed  = "push_to_connection_failed_total"
)

// NowUnix 供外部记录时间戳。
func NowUnix() float64 { return float64(time.Now().UnixNano()) / 1e9 }
