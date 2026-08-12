// Command server 启动 GO-CHAT 服务：HTTP + WebSocket + Kafka Workers。
//
// 用法：
//
//	GOChat_CONFIG=./config/config.yaml go run ./cmd/server
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Lysia-0113/GO-CHAT/internal/bootstrap"
	"github.com/Lysia-0113/GO-CHAT/internal/config"
)

func main() {
	configPath := flag.String("config", envOr("GOChat_CONFIG", "./config/config.yaml"), "配置文件路径")
	flag.Parse()

	log := slog.Default()
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		slog.Error("加载配置失败", "error", err.Error(), "path", *configPath)
		os.Exit(1)
	}
	log = bootstrap.NewLogger(cfg.Log)

	// 应用根 ctx：Kafka Consumer、Outbox、号段预加载等后台任务从这里派生
	// （GOCHAT_API.md §11.3.3：不复用任何 HTTP/WebSocket 请求 ctx）
	appCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := bootstrap.Build(appCtx, cfg, log)
	if err != nil {
		log.Error("应用装配失败", "error", err.Error())
		os.Exit(1)
	}

	if err := app.Run(appCtx); err != nil {
		log.Error("服务异常退出", "error", err.Error())
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
