-- 000003_create_conversation_members.sql
-- 会话成员表：成员关系、送达游标、已读游标
-- 送达/已读采用"最大连续 seq 游标"，避免消息数 × 成员数的回执表
CREATE TABLE IF NOT EXISTS conversation_members (
    conversation_id    BIGINT      NOT NULL COMMENT '会话 ID',
    user_id            BIGINT      NOT NULL COMMENT '用户 ID',
    role               TINYINT     NOT NULL DEFAULT 1 COMMENT '1 成员，5 管理员，10 群主',
    status             TINYINT     NOT NULL DEFAULT 1 COMMENT '1 正常，2 已退出，3 已移除',
    joined_seq         BIGINT      NOT NULL DEFAULT 0 COMMENT '入群时会话最新 seq，旧消息可见性边界',
    last_received_seq  BIGINT      NOT NULL DEFAULT 0 COMMENT '该用户设备已连续收到的最大 seq',
    last_read_seq      BIGINT      NOT NULL DEFAULT 0 COMMENT '该用户已读的最大 seq',
    clear_before_seq   BIGINT      NOT NULL DEFAULT 0 COMMENT '用户本地清空会话前的最大 seq，仅影响展示',
    mute_until         DATETIME(3) NULL COMMENT '禁言截止时间，NULL 表示未禁言',
    created_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                   ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (conversation_id, user_id),
    KEY idx_member_user_status_conversation (
        user_id,
        status,
        conversation_id
    )
) ENGINE = InnoDB COMMENT = '会话成员、送达和已读游标';
