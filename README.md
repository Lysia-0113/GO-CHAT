# GO-CHAT

基于 Go + Gin 的 IM 后端（P0），支撑单聊、群聊、历史消息查询、离线补偿、消息幂等、同会话有序处理和多节点连接路由。

设计文档（本项目开发依据）：

| 文档 | 内容 |
| --- | --- |
| [GOCHAT_API.md](../GOCHAT_API.md) | HTTP/WebSocket 接口、消息状态语义、幂等、号段模式 |
| [GOCHAT_DATABASE.md](../GOCHAT_DATABASE.md) | MySQL 表结构、持久化事务、Outbox、游标设计 |
| [GOCHAT_KAFKA.md](../GOCHAT_KAFKA.md) | 消息链路、Topic、Envelope、Worker、DLQ |
| [GOCHAT_REDIS.md](../GOCHAT_REDIS.md) | 最近消息缓存、在线路由、票据、幂等、限流 |
| [GOCHAT_RESILIENCE.md](../GOCHAT_RESILIENCE.md) | 超时、限流、熔断、隔离、降级、重试 |

## 架构总览

```text
客户端
  ↓ WebSocket message.send
Gin / WebSocket Gateway（鉴权、成员校验、限流、快速幂等）
  ↓ Kafka im.message.ingress（Key = conversation_id，acks=all）
Persist Worker（MySQL 事务：messages + conversations.last_seq + message_outbox）
  ↓ Outbox Publisher
Kafka im.message.persisted
  ↓ Delivery Worker
Redis 最近消息缓存 + 在线路由 → 本机 ConnectionManager / 跨节点 Pub/Sub
  ↓
接收方 WebSocket（message.new / message.persisted）
```

**数据真相来源**：MySQL 是最终真相；Kafka 是可靠异步通道（At Least Once + 唯一索引幂等）；Redis 只做缓存、在线路由、短期状态；离线用户通过 `after_seq` 从 MySQL 补偿。

## 快速开始

### 1. 启动依赖（MySQL / Redis / Kafka）

```bash
docker compose up -d
```

### 2. 配置

```bash
cp config/config.yaml.example config/config.yaml
# 按实际环境修改；敏感项建议用环境变量注入：
#   GOChat_AUTH_JWT_SECRET / GOChat_MYSQL_DSN / GOChat_KAFKA_BROKERS
```

### 3. 执行迁移

```bash
make migrate          # 等价于 GOChat_CONFIG=./config/config.yaml go run ./cmd/migrate
```

### 4. 启动服务

```bash
make run
```

健康检查：`GET /health/live`（存活）、`GET /health/ready`（MySQL/Redis/Kafka 就绪）、`GET /metrics`（Prometheus 文本格式）。

## 核心链路复现（P0 验收场景）

```bash
# 1. 注册两个用户
curl -s -X POST localhost:8080/api/v1/auth/register -d '{"username":"alice","password":"change-me","nickname":"Alice"}'
curl -s -X POST localhost:8080/api/v1/auth/register -d '{"username":"bob","password":"change-me","nickname":"Bob"}'

# 2. 登录获取 Token
TOKEN_A=$(curl -s -X POST localhost:8080/api/v1/auth/login -d '{"username":"alice","password":"change-me","device_id":"web-1"}' | jq -r .data.access_token)
TOKEN_B=$(curl -s -X POST localhost:8080/api/v1/auth/login -d '{"username":"bob","password":"change-me","device_id":"web-2"}' | jq -r .data.access_token)

# 3. 创建单聊（bob 的 user_id 从注册响应获取）
curl -s -X POST localhost:8080/api/v1/conversations \
  -H "Authorization: Bearer $TOKEN_A" \
  -d '{"type":"single","member_ids":["<bob_user_id>"]}'

# 4. 获取 WebSocket 一次性票据并建立连接
TICKET=$(curl -s -X POST localhost:8080/api/v1/ws-tickets -H "Authorization: Bearer $TOKEN_A" -d '{"device_id":"web-1"}' | jq -r .data.ticket)
websocat "ws://localhost:8080/ws?ticket=$TICKET"

# 5. 发送消息（连接建立后）
#   {"event":"message.send","request_id":"r1","data":{"client_msg_id":"<uuidv7>","conversation_id":"<conv_id>","content_type":"text","content":{"text":"你好"}}}
#   预期收到 message.accepted →（持久化后）message.persisted；接收方收到 message.new

# 6. 离线补偿：断开后发送新消息，重连后
curl -s "localhost:8080/api/v1/conversations/<conv_id>/messages?after_seq=<本地seq>&limit=100" \
  -H "Authorization: Bearer $TOKEN_B"

# 7. 幂等验证：相同 client_msg_id 重发 10 次，MySQL 只产生一条消息
# 8. 已读上报：PUT /api/v1/conversations/<conv_id>/read-cursor {"read_seq":<seq>}
```

## 测试

```bash
make test        # 全部单元测试
make test-race   # race 检测
```

测试不依赖外部基础设施：

- 号段生成器：单实例高并发无重复、多实例号段不重叠、CAS 冲突重试、切换边界、库存耗尽快速失败、重启空洞不复用（`internal/infrastructure/idgen/segment`）
- 消息 Service：发送校验、限流、幂等复用、Kafka 失败释放、历史可见性、已读规则（fake 依赖，不依赖完整 ServiceContext）
- Redis 组件：miniredis 验证 Lua 脚本（票据一次性、缓存裁剪、幂等条件更新、令牌桶、Presence 清理）
- 会话 Service：单聊复用、成员去重、权限校验
- 韧性：熔断只统计技术失败、Half-Open 恢复、隔离舱满快速失败

## 目录结构

```text
cmd/server/            # 服务入口
cmd/migrate/           # 版本化 SQL 迁移
internal/
├── bootstrap/         # 配置加载、ServiceContext 装配、App 运行
├── auth/              # JWT + Argon2id
├── user/              # 用户领域（注册/登录/搜索）
├── conversation/      # 会话领域（单聊/群聊/成员/游标）
├── message/           # 消息领域（发送/历史/已读/送达/事件）
├── connection/        # ConnectionManager / Presence 接口
├── errs/              # 统一错误码
├── resilience/        # 熔断（gobreaker）/隔离舱/超时
├── metrics/           # Prometheus 文本格式指标
├── infrastructure/
│   ├── mysql/         # GORM models + Repository（持久化事务/Outbox/号段 CAS）
│   ├── redis/         # 缓存/Presence/票据/幂等/限流/PubSub（Lua 脚本）
│   ├── kafka/         # Envelope/Producer/Consumer
│   └── idgen/segment/ # 号段生成器（CAS + 双 Buffer）
├── transport/
│   ├── http/          # Gin 路由/中间件/handler
│   └── websocket/     # Ticket 升级/读写循环/心跳/慢连接治理
└── worker/
    ├── persist/       # Kafka → MySQL 持久化
    ├── outbox/        # Outbox → persisted Topic
    └── deliver/       # persisted → 缓存 + 在线投递
migrations/            # 6 张表的版本化 SQL
config/                # 配置示例
```

## 关键机制

- **消息状态**：`sending → accepted → persisted → delivered → read`（Kafka 异步持久化，accepted 不代表落库）
- **幂等**：Redis SET NX 快速拦截（nonce 条件更新）+ `uk_messages_sender_client (sender_id, client_msg_id)` 最终兜底
- **同会话有序**：Kafka Key = conversation_id；持久化事务 `SELECT last_seq FOR UPDATE` 串行分配 seq
- **可靠事件**：messages 与 message_outbox 同事务提交；Outbox Publisher 用 `FOR UPDATE SKIP LOCKED` 多实例领取，失败退避重试，超限进死信
- **ID 生成**：MySQL 号段 + version CAS + 双 Buffer 预加载；message_id 在持久化消费者内分配
- **降级矩阵**：Redis 缓存失败回源 MySQL；Kafka ingress 不可用快速失败（不假成功）；在线投递失败由 after_seq 补偿

## 运维

- 指标：`rate_limit_rejected_total`、`breaker_state`、`outbox_pending_count`、`id_segment_remaining`、`kafka_consumer_lag` 等见各设计文档 §监控
- 日志：请求链路记录 `request_id / user_id / conversation_id / client_msg_id / message_id`，不记录密码、Token、完整消息内容
- 多节点：`server.node_id` 区分网关；Presence 存 Redis，投递按 node_id Pub/Sub；Kafka Consumer Group 每节点各一份

## License

MIT
