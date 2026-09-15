-- 固定逻辑分片：按 conversation_id 的 CRC32 分配，值需与 message.OutboxShardID 一致。
-- 先回填，再改为 NOT NULL；只有 Outbox 增加 shard_id 字段。
ALTER TABLE message_outbox
    ADD COLUMN shard_id SMALLINT UNSIGNED NULL AFTER event_type;

UPDATE message_outbox
SET shard_id = MOD(
    CRC32(CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, '$.conversation_id')) AS CHAR)),
    1024
);

-- 旧版本把超过重试上限的消息直接标为 dead，没有实际写入 DLQ；迁入待写 DLQ 状态。
UPDATE message_outbox
SET status = 4, next_retry_at = UTC_TIMESTAMP(3)
WHERE status = 3;

ALTER TABLE message_outbox
    MODIFY COLUMN status TINYINT NOT NULL DEFAULT 0 COMMENT '0 待投递，1 已投递，2 重试中，3 DLQ 已写入，4 等待写入 DLQ',
    MODIFY COLUMN shard_id SMALLINT UNSIGNED NOT NULL COMMENT 'conversation_id CRC32 shard [0,1023]',
    ADD KEY idx_outbox_shard_dispatch (
        shard_id,
        status,
        next_retry_at,
        created_at
    );
