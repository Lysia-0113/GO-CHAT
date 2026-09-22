GO ?= go
CONFIG ?= ./config/config.yaml

.PHONY: build run migrate test test-race vet lint tidy fmt kafka-init docker-build docker-up docker-migrate docker-logs

build:
	$(GO) build -o bin/gochat ./cmd/server

run:
	$(GO) run ./cmd/server -config $(CONFIG)

migrate:
	$(GO) run ./cmd/migrate -config $(CONFIG)

test:
	$(GO) test ./... -count=1

test-race:
	$(GO) test ./... -race -count=1

vet:
	$(GO) vet ./...

lint:
	$(GO) vet ./... && $(GO) build ./...

tidy:
	$(GO) mod tidy

kafka-init:
	./scripts/init-kafka.sh

fmt:
	gofmt -l -w .

# ---- Docker 部署（云服务器上用这套） ----

# 构建 app 镜像（Dockerfile + 源码）
docker-build:
	docker compose build

# 启动依赖、初始化 Kafka Topic，再启动 app（首次会自动构建镜像）
docker-up:
	docker compose up -d mysql redis kafka
	KAFKA_USE_DOCKER=1 ./scripts/init-kafka.sh
	docker compose up -d app

# 初始化数据库表（只跑一次；之后再跑也不会重复建表）
docker-migrate:
	docker compose --profile tools run --rm migrate

# 实时看 app 日志
docker-logs:
	docker compose logs -f app
