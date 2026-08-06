# GO-CHAT 应用镜像
#
# 分两个阶段：
#   1. builder：用完整 Go 环境编译出两个静态二进制
#   2. runtime：丢弃编译器，只把二进制放进极简 alpine 系统
#
# 用法（一般由 docker-compose 调用，无需手动执行）：
#   docker build -t gochat .

# ---------- 阶段 1：编译 ----------
FROM golang:1.26 AS builder

WORKDIR /src

# 先只拷贝依赖清单并下载，利用 Docker 层缓存：
# 只要 go.mod/go.sum 没变，这层就复用，不用每次重新下载依赖
COPY go.mod go.sum ./
RUN go mod download

# 再拷贝全部源码（migrations 已 embed 进二进制，不需要单独拷 SQL）
COPY . .

# CGO_ENABLED=0：纯静态编译，产物不依赖系统动态库，任何 Linux 都能跑
# -trimpath：产物里不带本机绝对路径，避免泄漏目录结构
# -ldflags="-s -w"：去掉调试符号，减小镜像体积
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/gochat ./cmd/server \
    && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/gochat-migrate ./cmd/migrate

# ---------- 阶段 2：运行 ----------
FROM alpine:3.21

# ca-certificates：Go 程序访问 HTTPS（如后续对接外部服务）需要根证书
# tzdata：容器内默认 UTC，装时区数据供配置/日志使用
# adduser：创建非 root 用户，容器内以低权限运行，降低被攻破后的影响面
RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -u 10001 appuser

WORKDIR /app

# 只拷贝编译产物，编译器和源码都不进最终镜像
COPY --from=builder /out/gochat /out/gochat-migrate ./

USER appuser

# 告知 Docker 该容器监听 8080（仅文档作用，不真正开放端口）
EXPOSE 8080

# 容器启动时执行的命令；docker-compose 里可被 command 覆盖（migrate 服务就是这么做）
ENTRYPOINT ["/app/gochat"]
