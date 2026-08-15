// Package dlq 是死信队列消费者：消费 im.message.dlq，
// 每条记录 Prometheus 指标后提交 offset（最小版：不重放、不落表、不告警）。
//
// 目的：让 DLQ 的构成可观测（数量 + 错误类型分布），
// 通过 /metrics 的 kafka_dlq_total{topic, code} 查询；
// 后续是否自动重放由数据决定。
package dlq

import (
	"context"
	"encoding/json"
	"time"

	kafkainfra "github.com/Lysia-0113/GO-CHAT/internal/infrastructure/kafka"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
	"github.com/Lysia-0113/GO-CHAT/internal/svc"
)

// Worker 死信队列消费者。
type Worker struct {
	svcCtx *svc.ServiceContext
}

func New(svcCtx *svc.ServiceContext) *Worker {
	return &Worker{svcCtx: svcCtx}
}

// Run 消费 im.message.dlq 直至 ctx 取消；返回 nil 表示优雅退出。
func (w *Worker) Run(appCtx context.Context) error {
	for {
		msg, err := w.svcCtx.DLQConsumer.FetchMessage(appCtx)
		if err != nil {
			if appCtx.Err() != nil {
				return nil // 优雅退出
			}
			w.svcCtx.Log.Error("dlq fetch failed", "error", err.Error())
			select {
			case <-appCtx.Done():
				return nil
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		// 提取原始 topic 与错误码（与写入侧 kafka_dlq_total 对齐）
		topic, code := parseDLQ(msg.Value)
		metrics.KafkaDLQ.WithLabelValues(topic, code).Inc()

		// 最小版：不重放、不落表；提交失败退避重试（避免重投后重复计数）
		for {
			err := w.svcCtx.DLQConsumer.CommitMessages(appCtx, msg)
			if err == nil {
				break
			}
			w.svcCtx.Log.Error("dlq commit failed", "offset", msg.Offset, "error", err.Error())
			select {
			case <-appCtx.Done():
				return nil
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
}

// parseDLQ 解析 DLQ 载荷，返回 (FailedTopic, ErrorCode)；解析失败时返回兜底值。
func parseDLQ(raw []byte) (string, string) {
	var env kafkainfra.Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return "unknown", "PARSE_ERROR"
	}
	var payload kafkainfra.DLQPayload
	if err := json.Unmarshal(env.Data, &payload); err != nil {
		return "unknown", "PARSE_ERROR"
	}
	topic := payload.FailedTopic
	if topic == "" {
		topic = "unknown"
	}
	return topic, payload.ErrorCode
}
