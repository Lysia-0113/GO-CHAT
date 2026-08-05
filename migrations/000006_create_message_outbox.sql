-- 000006_create_message_outbox.sql
-- 事务 Outbox：与 messages 落库处于同一事务，保证持久化事件最终可投递
CREATE TABLE IF NOT EXISTS message_outbox (
    message_id       BIGINT       NOT NULL COMMENT '对应 messages.id',
    event_type       TINYINT      NOT NULL COMMENT '1 message_persisted',
    payload          JSON         NOT NULL COMMENT '完整不可变事件载荷',
    status           TINYINT      NOT NULL DEFAULT 0 COMMENT '0 待投递，1 已投递，2 重试中，3 死信',
    retry_count      INT          NOT NULL DEFAULT 0,
    next_retry_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    locked_by        VARCHAR(64)  NULL COMMENT 'Publisher 实例标识',
    locked_at        DATETIME(3)  NULL COMMENT '最近一次领取时间',
    published_at     DATETIME(3)  NULL,
    last_error       VARCHAR(1024) NOT NULL DEFAULT '',
    created_at       DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at       DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                    ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (message_id, event_type),
    KEY idx_outbox_dispatch (
        status,
        next_retry_at,
        created_at
    )
) ENGINE = InnoDB COMMENT = '消息持久化事件 Outbox';
