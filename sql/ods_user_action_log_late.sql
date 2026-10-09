-- =============================================================================
-- ODS 层迟到数据表 (V2.0 新增)
-- 来源: FlinkOdsJobV2 DedupKeyedProcessFn 的 LATE_DATA_TAG 侧输出流
-- 定义: 事件时间 < Flink Watermark (超出乱序容忍窗口) 的数据
-- 存储引擎: InnoDB (迟到数据量通常小, 按 event_time 天分区便于清理)
-- 用途:
--   1) 事后回补: BI 看板可查询迟到数据并补算
--   2) 乱序诊断: 统计迟到率, 评估 Watermark out_of_orderness 是否合理
--   3) 脏数据溯源: 迟到+脏数据的交叉分析
-- =============================================================================

USE dw_ods;

DROP TABLE IF EXISTS ods_user_action_log_late;

CREATE TABLE ods_user_action_log_late (
    -- ========== 主键 + 业务字段 (与 ods_user_action_log 结构完全一致) ==========
    user_id         VARCHAR(64)  NOT NULL,
    event_time      DATETIME(3)  NOT NULL COMMENT '事件时间 (迟到事件的原始 event_time)',
    event_type      VARCHAR(32)  NOT NULL,
    app_id          VARCHAR(32)  NOT NULL,

    device_id       VARCHAR(128) DEFAULT NULL,
    page            VARCHAR(512) DEFAULT NULL,
    referrer        VARCHAR(512) DEFAULT NULL,
    product_id      VARCHAR(64)  DEFAULT NULL,
    duration_ms     BIGINT       DEFAULT NULL,
    ip              VARCHAR(64)  DEFAULT NULL,
    os_type         VARCHAR(32)  DEFAULT NULL,
    os_version      VARCHAR(32)  DEFAULT NULL,
    net_type        VARCHAR(16)  DEFAULT NULL,
    ext_json        JSON         DEFAULT NULL,

    -- ========== V2.0 特有字段 (允许 NULL: Flink JdbcSink 共用 15 列 INSERT, 这两列留后续补算) ==========
    late_ms          BIGINT      DEFAULT NULL COMMENT '迟到毫秒数 = watermark - event_time (可后置补算)',
    source_watermark DATETIME(3) DEFAULT NULL COMMENT '该条数据被判定迟到时的 watermark (可后置补算)',
    etl_time         DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'ETL 入库时间',

    -- ========== 索引 ==========
    PRIMARY KEY (event_time, user_id, device_id),
    INDEX idx_user_id  (user_id),
    INDEX idx_event_type (event_type),
    INDEX idx_late_ms (late_ms)        -- 快速查询迟到最严重的数据
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COMMENT='ODS层-迟到数据表 (Flink V2.0 Watermark 乱序容忍窗口溢出)'
;

-- =============================================================================
-- V2.0 乱序诊断查询示例
-- =============================================================================

-- 1) 迟到率 = late_table 条数 / main_table 条数
SELECT
  (SELECT COUNT(*) FROM ods_user_action_log)      AS main_cnt,
  (SELECT COUNT(*) FROM ods_user_action_log_late) AS late_cnt,
  ROUND(
    (SELECT COUNT(*) FROM ods_user_action_log_late) * 100.0 /
    NULLIF((SELECT COUNT(*) FROM ods_user_action_log), 0),
    2
  ) AS late_pct
;

-- 2) 迟到分布 (迟到毫秒数直方图)
SELECT
  CASE
    WHEN late_ms < 10_000   THEN '< 10s'
    WHEN late_ms < 30_000   THEN '10s ~ 30s'
    WHEN late_ms < 60_000   THEN '30s ~ 1min'
    WHEN late_ms < 300_000  THEN '1min ~ 5min'
    WHEN late_ms < 1_800_000 THEN '5min ~ 30min'
    ELSE '> 30min'
  END AS late_bucket,
  COUNT(*) AS cnt,
  ROUND(AVG(late_ms) / 1000, 2) AS avg_late_s
FROM ods_user_action_log_late
WHERE etl_time >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
GROUP BY late_bucket
ORDER BY MIN(late_ms);

-- 3) 建议: 若迟到率 > 2%, 考虑调大 Watermark forBoundedOutOfOrderness
--    若迟到率长期 > 5%, 需排查 APP 端时间同步 / 传输链路延迟
