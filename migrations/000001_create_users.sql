-- 000001_create_users.sql
-- 用户表：账号与展示资料，密码仅保存 Argon2id 哈希
-- 用户 ID 由 im_user 号段生成器分配，不使用 AUTO_INCREMENT
CREATE TABLE IF NOT EXISTS users (
    id              BIGINT       NOT NULL COMMENT '用户 ID，im_user 号段生成',
    username        VARCHAR(64)  NOT NULL COMMENT '登录名，小写字母、数字、下划线',
    password_hash   VARCHAR(255) NOT NULL COMMENT 'Argon2id 哈希',
    nickname        VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '展示昵称',
    avatar_url      VARCHAR(512) NOT NULL DEFAULT '' COMMENT '头像地址',
    status          TINYINT      NOT NULL DEFAULT 1 COMMENT '1 正常，2 禁用，3 已注销',
    created_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                    ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_users_username (username),
    KEY idx_users_status_created (status, created_at)
) ENGINE = InnoDB COMMENT = '用户表';
