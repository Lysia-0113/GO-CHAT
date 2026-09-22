#!/usr/bin/env bash

# 幂等初始化 GO-CHAT Kafka Topic。
#
# 运行方式：
#   ./scripts/init-kafka.sh
#
# Docker Compose：
#   KAFKA_USE_DOCKER=1 ./scripts/init-kafka.sh
#
# 该脚本只负责 Topic/partition 初始化与校验；Consumer Group 的 offset
# 由应用运行时创建和提交，不在这里预创建。

set -euo pipefail

# Docker Compose 会自动读取 .env，但脚本本身不会继承 Compose 的插值结果。
# 这里仅解析简单的 KEY=VALUE 行，不执行 .env 内容，保证 make docker-up
# 使用的 topic 前缀与分区配置也会用于初始化脚本。
load_dotenv() {
  local dotenv_path="${DOTENV_FILE:-.env}"
  [[ -f "$dotenv_path" ]] || return 0

  local line key value
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "$line" || "$line" == \#* ]] && continue
    [[ "$line" == export\ * ]] && line="${line#export }"
    [[ "$line" == *=* ]] || continue

    key="${line%%=*}"
    value="${line#*=}"
    key="${key#"${key%%[![:space:]]*}"}"
    key="${key%"${key##*[![:space:]]}"}"
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    [[ -v "$key" ]] && continue
    if [[ "$value" == \"*\" && "$value" == *\" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
      value="${value:1:${#value}-2}"
    fi
    printf -v "$key" '%s' "$value"
  done < "$dotenv_path"
}

load_dotenv

BROKER="${KAFKA_BROKER:-localhost:9092}"
PREFIX="${KAFKA_TOPIC_PREFIX:-${GOCHAT_KAFKA_TOPIC_PREFIX:-${GOChat_KAFKA_TOPIC_PREFIX:-}}}"
INBOX_PARTITIONS="${KAFKA_INBOX_PARTITIONS:-${GOCHAT_INBOX_PARTITIONS:-${GOCHAT_KAFKA_INBOX_PARTITIONS:-${GOChat_KAFKA_INBOX_PARTITIONS:-3}}}}"
PUSH_PARTITIONS="${KAFKA_PUSH_PARTITIONS:-${GOCHAT_PUSH_PARTITIONS:-${GOCHAT_KAFKA_PUSH_PARTITIONS:-${GOChat_KAFKA_PUSH_PARTITIONS:-3}}}}"
DLQ_PARTITIONS="${KAFKA_DLQ_PARTITIONS:-1}"
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"
RETRIES="${KAFKA_INIT_RETRIES:-60}"
RETRY_INTERVAL="${KAFKA_INIT_RETRY_INTERVAL:-2}"

die() {
  echo "[init-kafka] ERROR: $*" >&2
  exit 1
}

[[ "$INBOX_PARTITIONS" =~ ^[1-9][0-9]*$ ]] || die "inbox partitions must be a positive integer"
[[ "$PUSH_PARTITIONS" =~ ^[1-9][0-9]*$ ]] || die "push partitions must be a positive integer"
[[ "$DLQ_PARTITIONS" =~ ^[1-9][0-9]*$ ]] || die "dlq partitions must be a positive integer"
[[ "$REPLICATION_FACTOR" =~ ^[1-9][0-9]*$ ]] || die "replication factor must be a positive integer"

if [[ "${KAFKA_USE_DOCKER:-0}" == "1" ]]; then
  TOPICS_CMD=(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh)
  BROKER="localhost:9092"
elif command -v kafka-topics.sh >/dev/null 2>&1; then
  TOPICS_CMD=(kafka-topics.sh)
elif [[ -x /opt/kafka/bin/kafka-topics.sh ]]; then
  TOPICS_CMD=(/opt/kafka/bin/kafka-topics.sh)
else
  die "kafka-topics.sh not found; run inside the Kafka image or set KAFKA_USE_DOCKER=1"
fi

topic_name() {
  local name="$1"
  if [[ -n "$PREFIX" ]]; then
    printf '%s.%s\n' "$name" "${PREFIX#.}"
  else
    printf '%s\n' "$name"
  fi
}

run_topics() {
  "${TOPICS_CMD[@]}" --bootstrap-server "$BROKER" "$@"
}

echo "[init-kafka] waiting for broker ${BROKER}"
for ((attempt = 1; attempt <= RETRIES; attempt++)); do
  if run_topics --list >/dev/null 2>&1; then
    break
  fi
  if (( attempt == RETRIES )); then
    die "Kafka broker did not become ready"
  fi
  sleep "$RETRY_INTERVAL"
done

ensure_topic() {
  local topic="$1"
  local expected_partitions="$2"

  if ! run_topics --list | grep -Fxq "$topic"; then
    echo "[init-kafka] creating ${topic} partitions=${expected_partitions} replication_factor=${REPLICATION_FACTOR}"
    run_topics --create \
      --if-not-exists \
      --topic "$topic" \
      --partitions "$expected_partitions" \
      --replication-factor "$REPLICATION_FACTOR" >/dev/null
  fi

  local description
  description="$(run_topics --describe --topic "$topic")"
  local actual_partitions
  actual_partitions="$(awk -F'PartitionCount: ' 'NF > 1 {split($2, a, " "); print a[1]; exit}' <<<"$description")"
  local actual_replication
  actual_replication="$(awk -F'ReplicationFactor: ' 'NF > 1 {split($2, a, " "); print a[1]; exit}' <<<"$description")"

  [[ "$actual_partitions" =~ ^[0-9]+$ ]] || die "cannot read partition count for ${topic}: ${description}"
  [[ "$actual_replication" =~ ^[0-9]+$ ]] || die "cannot read replication factor for ${topic}: ${description}"

  if (( actual_replication != REPLICATION_FACTOR )); then
    die "${topic} replication factor is ${actual_replication}, expected ${REPLICATION_FACTOR}; alter it manually"
  fi
  if (( actual_partitions < expected_partitions )); then
    echo "[init-kafka] increasing ${topic} partitions ${actual_partitions} -> ${expected_partitions}"
    run_topics --alter --topic "$topic" --partitions "$expected_partitions" >/dev/null
  elif (( actual_partitions > expected_partitions )); then
    echo "[init-kafka] ${topic} already has ${actual_partitions} partitions; keeping it (Kafka cannot shrink partitions)"
  fi
}

ensure_topic "$(topic_name im.message.inbox)" "$INBOX_PARTITIONS"
ensure_topic "$(topic_name im.message.push)" "$PUSH_PARTITIONS"
ensure_topic "$(topic_name im.message.dlq)" "$DLQ_PARTITIONS"

echo "[init-kafka] topics are ready"
