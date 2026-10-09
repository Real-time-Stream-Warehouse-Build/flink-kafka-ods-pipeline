-- =============================================================================
-- ODS 层: 用户行为明细表
-- 对应 Java 类: com.ods.OdsUserActionLog
-- 对应 Flink Job: com.ods.FlinkOdsJob
-- 数据来源: Kafka topic_log (APP 埋点原始日志 JSON)
-- 清洗策略: Flink DirtyDataFilterProcessFn
--
-- 引擎: InnoDB (支持事务 + 行锁, 适合 ODS 层按天分区的 upsert 场景)
-- 分区: 按 event_time 天分区 (yyyy-MM-dd)
-- 主键: (event_time, user_id, device_id) 近似唯一, 用于去重
-- =============================================================================

CREATE DATABASE IF NOT EXISTS dw_ods
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_general_ci;

USE dw_ods;

DROP TABLE IF EXISTS ods_user_action_log;

CREATE TABLE ods_user_action_log (
    -- ========== 主键 + 业务字段 ==========
    user_id         VARCHAR(64)  NOT NULL COMMENT '用户 ID (埋点必填)',
    event_time      DATETIME(3)  NOT NULL COMMENT '事件时间 (事件时间 Event Time, 毫秒精度)',
    event_type      VARCHAR(32)  NOT NULL COMMENT '事件类型: view/click/cart/order/pay/login/logout',
    app_id          VARCHAR(32)  NOT NULL COMMENT '应用来源: mall_app/h5/mini_program',

    -- ========== 扩展字段 ==========
    device_id       VARCHAR(128) DEFAULT NULL COMMENT '设备唯一标识 (IMEI/Android ID/IDFA)',
    page            VARCHAR(512) DEFAULT NULL COMMENT '当前页面路径',
    referrer        VARCHAR(512) DEFAULT NULL COMMENT '来源页面',
    product_id      VARCHAR(64)  DEFAULT NULL COMMENT '商品 ID',
    duration_ms     BIGINT       DEFAULT NULL COMMENT '停留/操作时长 (ms)',
    ip              VARCHAR(64)  DEFAULT NULL COMMENT '用户 IP',
    os_type         VARCHAR(32)  DEFAULT NULL COMMENT '操作系统类型: Android/iOS/Web',
    os_version      VARCHAR(32)  DEFAULT NULL COMMENT '操作系统版本号',
    net_type        VARCHAR(16)  DEFAULT NULL COMMENT '网络类型: WIFI/4G/5G/3G',

    -- ========== 扩展 JSON + ETL 字段 ==========
    ext_json        JSON         DEFAULT NULL COMMENT '原始 JSON 中未知字段快照',
    etl_time        DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'ETL 入库时间 (处理时间 Processing Time)',

    -- ========== 索引 ==========
    PRIMARY KEY (event_time, user_id, device_id),          -- 近似唯一约束 (ODS 层幂等写入)
    INDEX idx_user_id  (user_id),
    INDEX idx_event_type (event_type),
    INDEX idx_etl_time (etl_time)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COMMENT='ODS层-用户行为明细表 (Flink V1.0 清洗入库)'
;

-- =============================================================================
-- 双 11 流量治理场景验证: 查询示例
-- =============================================================================

-- 1. 统计每小时事件量 (验证清洗后数据准确性)
SELECT DATE_FORMAT(event_time, '%Y-%m-%d %H') AS hour_slot,
       event_type,
       COUNT(*)                                AS event_cnt
FROM ods_user_action_log
WHERE event_time >= '2025-11-10 00:00:00'
  AND event_time <  '2025-11-12 00:00:00'
GROUP BY DATE_FORMAT(event_time, '%Y-%m-%d %H'), event_type
ORDER BY hour_slot, event_type;

-- 2. 查看某个用户的完整行为轨迹
SELECT event_time, event_type, page, product_id, duration_ms
FROM ods_user_action_log
WHERE user_id = 'U100001'
ORDER BY event_time ASC;

-- 3. 脏数据率估算 (对比原始 Kafka topic 消息数 vs ODS 入库数)
--    Kafka 原始消息计数可通过 kafka-run-class.sh kafka.tools.GetOffsetShell 获取
SELECT COUNT(*) AS ods_cleaned_cnt FROM ods_user_action_log;
