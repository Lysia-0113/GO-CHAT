package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// HealthHandler 处理 /health/live 与 /health/ready（GOCHAT_API.md §2.3）。
type HealthHandler struct {
	db      *gorm.DB
	redis   *goredis.Client
	readyFn func(ctx context.Context) error // Kafka 等额外就绪检查
}

func NewHealthHandler(db *gorm.DB, redis *goredis.Client, readyFn func(ctx context.Context) error) *HealthHandler {
	return &HealthHandler{db: db, redis: redis, readyFn: readyFn}
}

// Live 进程存活探针：进程存在即 200。
func (h *HealthHandler) Live(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "alive"})
}

// Ready 依赖就绪检查：MySQL、Redis、Kafka 任一不可用返回 503。
func (h *HealthHandler) Ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	checks := make(map[string]string)
	ok := true

	if h.db != nil {
		sqlDB, err := h.db.DB()
		if err == nil {
			err = sqlDB.PingContext(ctx)
		}
		checks["mysql"] = statusOf(err)
		ok = ok && err == nil
	}
	if h.redis != nil {
		err := h.redis.Ping(ctx).Err()
		checks["redis"] = statusOf(err)
		ok = ok && err == nil
	}
	if h.readyFn != nil {
		err := h.readyFn(ctx)
		checks["kafka"] = statusOf(err)
		ok = ok && err == nil
	}

	code := http.StatusOK
	if !ok {
		code = http.StatusServiceUnavailable
	}
	c.JSON(code, gin.H{"status": map[bool]string{true: "ready", false: "not_ready"}[ok], "checks": checks})
}

func statusOf(err error) string {
	if err == nil {
		return "ok"
	}
	return "error: " + err.Error()
}
