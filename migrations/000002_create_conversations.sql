-- 000002_create_conversations.sql
-- 会话表：单聊与群聊共用；单聊通过排序后的用户 ID 保证唯一
CREATE TABLE IF NOT EXISTS conversations (
    id                  BIGINT       NOT NULL COMMENT '会话 ID，im_conversation 号段生成',
    type                TINYINT      NOT NULL COMMENT '1 单聊，2 群聊',
    direct_user_low_id  BIGINT       NULL COMMENT '单聊两人中较小的用户 ID',
    direct_user_high_id BIGINT       NULL COMMENT '单聊两人中较大的用户 ID',
    name                VARCHAR(128) NOT NULL DEFAULT '' COMMENT '群名称；单聊由客户端展示对方昵称',
    avatar_url          VARCHAR(512) NOT NULL DEFAULT '' COMMENT '群头像',
    owner_id            BIGINT       NOT NULL COMMENT '创建者；群聊时为群主',
    status              TINYINT      NOT NULL DEFAULT 1 COMMENT '1 正常，2 已解散',
    last_seq            BIGINT       NOT NULL DEFAULT 0 COMMENT '最新已持久化消息的会话序号',
    last_message_id     BIGINT       NULL COMMENT '最新消息 ID',
    last_message_type   TINYINT      NULL COMMENT '最新消息类型',
    last_message_preview VARCHAR(255) NOT NULL DEFAULT '' COMMENT '会话列表预览，内容截断与脱敏',
    last_message_at     DATETIME(3)  NULL COMMENT '最新消息服务端落库时间',
    created_at          DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at          DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                        ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_direct_conversation (
        type,
        direct_user_low_id,
        direct_user_high_id
    ),
    KEY idx_conversation_status_last_message (
        status,
        last_message_at,
        id
    )
) ENGINE = InnoDB COMMENT = '单聊和群聊会话表';
