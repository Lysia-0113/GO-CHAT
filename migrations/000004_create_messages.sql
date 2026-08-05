-- 000004_create_messages.sql
-- 消息表：高频表，主键为 (conversation_id, seq) 以支撑会话内顺序历史扫描
CREATE TABLE IF NOT EXISTS messages (
    id                BIGINT       NOT NULL COMMENT '消息 ID，im_message 号段生成',
    conversation_id   BIGINT       NOT NULL COMMENT '会话 ID',
    seq               BIGINT       NOT NULL COMMENT '会话内递增序号，从 1 开始',
    sender_id         BIGINT       NOT NULL COMMENT '发送者用户 ID；系统消息使用约定系统用户',
    client_msg_id     BINARY(16)   NOT NULL COMMENT '客户端 UUIDv7，二进制存储',
    message_type      TINYINT      NOT NULL COMMENT '1 文本，2 图片，3 文件，99 系统',
    content           JSON         NOT NULL COMMENT '消息体；P0 只允许 text 字段',
    content_preview   VARCHAR(255) NOT NULL DEFAULT '' COMMENT '会话列表展示预览',
    status            TINYINT      NOT NULL DEFAULT 1 COMMENT '1 正常，2 已撤回，3 已删除',
    recalled_by       BIGINT       NULL COMMENT '撤回操作者',
    recalled_at       DATETIME(3)  NULL COMMENT '撤回时间',
    client_sent_at    DATETIME(3)  NULL COMMENT '客户端声明发送时间，仅作展示，不作排序依据',
    created_at        DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                      COMMENT '服务端持久化完成时间',
    updated_at        DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                      ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (conversation_id, seq),
    UNIQUE KEY uk_messages_id (id),
    UNIQUE KEY uk_messages_sender_client (
        sender_id,
        client_msg_id
    ),
    KEY idx_messages_sender_created (
        sender_id,
        created_at,
        id
    )
) ENGINE = InnoDB COMMENT = 'IM 历史消息表';
