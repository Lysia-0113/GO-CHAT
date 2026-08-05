-- 000005_create_id_generator.sql
-- 号段分配表：按 biz_tag 批量分配 ID 区间，version CAS 保证并发安全
CREATE TABLE IF NOT EXISTS id_generator (
    biz_tag     VARCHAR(32) NOT NULL COMMENT '业务标识',
    max_id      BIGINT      NOT NULL DEFAULT 0 COMMENT '已分配的最大 ID',
    step        INT         NOT NULL DEFAULT 1000 COMMENT '单次申请号段长度',
    version     BIGINT      NOT NULL DEFAULT 0 COMMENT 'CAS 版本',
    update_time DATETIME(3) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (biz_tag),
    CONSTRAINT chk_id_generator_step CHECK (step > 0)
) ENGINE = InnoDB COMMENT = 'MySQL 号段生成器';

-- 初始化号段；max_id=0 时第一次申请 step=1000 得到 [1, 1000]
INSERT INTO id_generator (biz_tag, max_id, step)
VALUES
    ('im_user', 0, 1000),
    ('im_conversation', 0, 1000),
    ('im_message', 0, 2000)
ON DUPLICATE KEY UPDATE biz_tag = biz_tag;
