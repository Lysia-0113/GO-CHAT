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
	"github.com/prometheus/client_golang/prometheus"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/idgen/segment"
	"github.com/Lysia-0113/GO-CHAT/internal/metrics"
	"github.com/Lysia-0113/GO-CHAT/internal/svc"
	httptransport "github.com/Lysia-0113/GO-CHAT/internal/transport/http"
	"github.com/Lysia-0113/GO-CHAT/internal/transport/websocket"
	"github.com/Lysia-0113/GO-CHAT/internal/worker/deliver"
	"github.com/Lysia-0113/GO-CHAT/internal/worker/dlq"
	"github.com/Lysia-0113/GO-CHAT/internal/worker/outbox"
	"github.com/Lysia-0113/GO-CHAT/internal/worker/persist"
)

// App 是装配完成的应用：svcCtx 持有全部依赖，reg 仅供 /metrics 输出
// （GOCHAT_API.md §11.3：ServiceContext 即服务定位器）。
type App struct {
	svcCtx *svc.ServiceContext
	reg    *prometheus.Registry
}

// Run 启动 HTTP 服务与全部 Worker，阻塞直到 ctx 取消后优雅退出。
func (a *App) Run(appCtx context.Context) error {
	// ---- Worker 启动 ----
	persistWorker := persist.New(a.svcCtx, persist.Config{
		MaxRetries:    a.svcCtx.Config.Kafka.PersistMaxRetries,
		Backoff:       a.svcCtx.Config.Kafka.PersistBackoff,
		TxTimeout:     a.svcCtx.Config.Resilience.PersistTxTimeout,
		NumPartitions: a.svcCtx.Config.Kafka.NumPartitions,
	})
	outboxPublisher := outbox.New(a.svcCtx, outbox.Config{
		MaxRetries:   a.svcCtx.Config.Kafka.OutboxMaxRetries,
		Backoff:      a.svcCtx.Config.Kafka.OutboxBackoff,
		PollInterval: a.svcCtx.Config.Kafka.OutboxPollInterval,
		BatchSize:    a.svcCtx.Config.Kafka.OutboxBatchSize,
		InstanceID:   a.svcCtx.Config.Server.NodeID,
	})
	deliverWorker := deliver.New(a.svcCtx, deliver.Config{
		NumPartitions: a.svcCtx.Config.Kafka.NumPartitions,
	})
	dlqWorker := dlq.New(a.svcCtx)

	// persist：1 个分发 goroutine + 每分区 1 个处理 goroutine（按分区串行，保证会话顺序）
	go func() {
		if err := persistWorker.Run(appCtx); err != nil {
			a.svcCtx.Log.Error("persist worker exited", "error", err.Error())
		}
	}()
	// dlq：最小版死信消费者（计数+提交，无重放/落表）
	go func() {
		if err := dlqWorker.Run(appCtx); err != nil {
			a.svcCtx.Log.Error("dlq worker exited", "error", err.Error())
		}
	}()
	// deliver：与 persist 相同的分区串行模型（1 分发 + 每分区 1 处理）
	go func() {
		if err := deliverWorker.Run(appCtx); err != nil {
			a.svcCtx.Log.Error("deliver worker exited", "error", err.Error())
		}
	}()
	for i := 0; i < a.svcCtx.Config.Resilience.OutboxWorkers; i++ {
		go func() {
			if err := outboxPublisher.Run(appCtx); err != nil {
				a.svcCtx.Log.Error("outbox publisher exited", "error", err.Error())
			}
		}()
	}

	// ---- 指标循环（号段库存、熔断状态、Outbox 积压） ----
	go a.metricsLoop(appCtx)

	// ---- HTTP / WebSocket ----
	wsHandler := websocket.NewHandler(appCtx, a.svcCtx, websocket.HandlerConfig{
		ReadLimit:              a.svcCtx.Config.Server.WSReadLimit,
		HeartbeatInterval:      a.svcCtx.Config.Server.WSHeartbeatInterval,
		MissedHeartbeat:        a.svcCtx.Config.Server.WSMissedHeartbeat,
		WriteQueueSize:         a.svcCtx.Config.Server.WSWriteQueueSize,
		WriteQueueTimeout:      a.svcCtx.Config.Server.WSWriteQueueTimeout,
		InboundRatePerSec:      a.svcCtx.Config.Resilience.ConnInboundPerSecond,
		InboundRateBurst:       a.svcCtx.Config.Resilience.ConnInboundBurst,
		WSConnectPerMinute:     a.svcCtx.Config.Resilience.WSConnectPerMinute,
		WSConnectPerUserMinute: a.svcCtx.Config.Resilience.WSConnectPerUserPerMinute,
	}, a.svcCtx.Log)

	router := httptransport.New(a.svcCtx, wsHandler, a.metricsHandler(), a.kafkaReady)

	gin.SetMode(gin.ReleaseMode)
	srv := &http.Server{
		Addr:              a.svcCtx.Config.Server.Addr,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		a.svcCtx.Log.Info("server started", "addr", a.svcCtx.Config.Server.Addr, "node_id", a.svcCtx.Config.Server.NodeID)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-appCtx.Done():
		a.svcCtx.Log.Info("shutting down")
	case err := <-errCh:
		return err
	}

	// ---- 优雅退出 ----
	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.svcCtx.Config.Server.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		a.svcCtx.Log.Warn("http shutdown", "error", err.Error())
	}
	a.svcCtx.PersistConsumer.Close()
	a.svcCtx.DeliverConsumer.Close()
	a.svcCtx.DLQConsumer.Close()
	a.svcCtx.Kafka.Close()
	if sqlDB, err := a.svcCtx.DB.DB(); err == nil {
		_ = sqlDB.Close()
	}
	_ = a.svcCtx.Redis.Close()
	a.svcCtx.Log.Info("server stopped")
	return nil
}

// kafkaReady 就绪探针：能读取 Topic 元数据即视为可用。
func (a *App) kafkaReady(ctx context.Context) error {
	client := &kafkago.Client{
		Addr:    kafkago.TCP(a.svcCtx.Config.Kafka.Brokers...),
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
				"im_user": a.svcCtx.UserIDs, "im_conversation": a.svcCtx.ConvIDs, "im_message": a.svcCtx.MessageIDs,
			} {
				st := gen.State()
				metrics.IDSegmentRemaining.WithLabelValues(name, a.svcCtx.Config.Server.NodeID).Set(float64(st.Remaining))
			}
			// 熔断器状态
			for name, st := range a.svcCtx.Breakers.States() {
				metrics.BreakerState.WithLabelValues(name).Set(float64(st))
			}
			// 连接数
			metrics.WSConnectionActive.Set(float64(a.svcCtx.ConnManager.Count()))
			// 隔离舱占用
			metrics.BulkheadQueueLength.WithLabelValues("history_query").Set(float64(a.svcCtx.HistoryBulkhead.Active()))
			metrics.BulkheadQueueLength.WithLabelValues("ws_ingress").Set(float64(a.svcCtx.IngressBulkhead.Active()))
		}
	}
}

// metricsHandler 输出 Prometheus 文本格式指标（promhttp 标准处理器）。
func (a *App) metricsHandler() gin.HandlerFunc {
	if !a.svcCtx.Config.Metrics.Enabled {
		return nil
	}
	return gin.WrapH(metrics.Handler(a.reg))
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
