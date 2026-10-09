-- ========================================================================
-- V3.0 Iceberg ODS 表 DDL
-- ========================================================================
--
-- 设计要点:
--   1) 主键 (PRIMARY KEY) 定义 — Iceberg v3 MERGE/UPSERT 必须
--      CDC 事件按 (event_time, user_id, device_id, event_type) 唯一,
--      主键和 FlinkSink.equalityFieldColumns 完全一致
--   2) 分区策略: DATE(event_time) 日分区
--      CDC 事件时间驱动, 每个分区只含当天数据, 下游 Spark/Flink/Doris
--      读取时分区裁剪快
--   3) 小文件治理 (NameNode 压力) — 四管齐下:
--      a) write.distribution-mode = hash + write.distribution-columns = user_id
--         写入时自动 hash 到 bucket, 避免热点, 分区内均衡分布
--      b) write.target-file-size-bytes = 536870912 (512MB)
--         Flink Iceberg Sink 滚动文件目标大小, 不写小文件
--      c) write.format.default = parquet + compression = zstd
--         压缩率 ≈ 3~5x, 同等数据量 HDFS block 数少
--      d) rewrite.enabled / rewrite.min-input-files / rewrite.target-file-size-bytes
--         作业内/周期性 RewriteFiles Action (见 IcebergCompactionJob.java)
--   4) snapshot retention: 7d, 自动清理旧 manifest + data files
--
-- ========================================================================

-- ----- Iceberg Catalog (HadoopCatalog 本地开发, 生产换 Hive/Nessie) -----
CREATE CATALOG ods_catalog WITH (
    'type'            = 'iceberg',
    'catalog.type'    = 'hadoop',
    'warehouse'       = 'file:///tmp/iceberg/warehouse',
    'property-version'= '1'
);

CREATE DATABASE IF NOT EXISTS ods_catalog.ods COMMENT 'V3.0 ODS 湖';

-- ----- ODS 主表 -----
CREATE TABLE IF NOT EXISTS ods_catalog.ods.user_action_log (
    user_id        STRING       COMMENT '用户ID - 主键',
    event_type     STRING       COMMENT '事件类型 - 主键',
    event_time     TIMESTAMP    COMMENT '事件发生时间 - 主键',
    app_id         STRING       COMMENT '来源应用: mall_app/h5/mini_program',
    device_id      STRING       COMMENT '设备ID - 主键',
    page           STRING       COMMENT '当前页面',
    referrer       STRING       COMMENT '来源页面',
    product_id     STRING       COMMENT '商品ID',
    duration_ms    BIGINT       COMMENT '停留时长(毫秒)',
    ip             STRING       COMMENT '客户端IP',
    os_type        STRING       COMMENT '操作系统类型',
    os_version     STRING       COMMENT '操作系统版本',
    net_type       STRING       COMMENT '网络类型 wifi/4g/5g',
    ext_json       STRING       COMMENT '扩展字段JSON',
    op_type        STRING       COMMENT 'CDC 操作类型 I/U/D',
    op_ts          TIMESTAMP    COMMENT 'CDC binlog 操作时间',
    etl_time       TIMESTAMP    COMMENT 'ETL 入库时间',
    PRIMARY KEY (user_id, event_type, event_time, device_id) NOT ENFORCED
)
PARTITIONED BY (dt DATE COMMENT '事件日期, 由 event_time 派生')
WITH (
    -- ===== 基础格式 =====
    'format-version'                   = '2',                     -- V3 支持 MERGE INTO
    'write-format'                     = 'parquet',
    'compression'                       = 'zstd',                  -- 高压缩比, 省 HDFS block
    'parquet.compression'               = 'ZSTD',

    -- ===== 写入控制 (小文件核心) =====
    'write.distribution-mode'           = 'hash',                  -- hash 分布避免热点
    'write.distribution-columns'        = 'user_id',
    'write.target-file-size-bytes'      = '536870912',             -- 512MB/文件 (默认 128MB)
    'write-open-files-cost-bytes'       = '5368709120',            -- 5GB open files 预算
    'write-max-file-size-bytes'        = '1073741824',            -- 1GB max

    -- ===== Upsert / 主键写入 =====
    'write-mode'                        = 'upsert',                -- CDC 必须

    -- ===== 合并 / 重写 =====
    'rewrite.min-input-files'           = '5',                     -- 分区内 >=5 文件才触发 rewrite
    'rewrite.target-file-size-bytes'    = '536870912',             -- 合并目标 512MB
    'rewrite.max-file-size-bytes'       = '1073741824',
    'rewrite-all'                       = 'false',

    -- ===== Manifest / Snapshot 过期 =====
    'manifest-min-merge-count'          = '5',
    'snapshot-retention'                = '7d',                    -- 保留 7d 快照, 自动清理

    -- ===== 分区裁剪 / 布隆过滤器 =====
    'write.parquet.bloom-filter-enabled.user_id' = 'true',
    'write.parquet.bloom-filter-enabled.event_type' = 'true'
)
COMMENT 'V3.0 ODS 用户行为事件湖表 - Flink CDC + Iceberg'
;


-- ========================================================================
-- Snapshot / Manifest 过期 (清理旧快照, 释放 HDFS 空间)
-- 在 Flink SQL Client 中执行:
-- ========================================================================

-- 查看当前 snapshot
SELECT snapshot_id, committed_at, operation, summary
FROM ods_catalog.ods.user_action_log.snapshots
ORDER BY committed_at DESC
LIMIT 20;

-- 过期快照 (保留 7d 内)
CALL ods_catalog.system.expire_snapshots(
    table => 'ods.user_action_log',
    older_than => TIMESTAMP '2024-01-01 00:00:00',
    retain_last => 5
);

-- 过期 manifest
CALL ods_catalog.system.remove_orphan_files(
    table => 'ods.user_action_log'
);


-- ========================================================================
-- 手动触发小文件合并 (运维 API)
-- ========================================================================

-- 合并当前分区内小文件 (Iceberg 1.4+ CALL API)
CALL ods_catalog.system.rewrite_data_files(
    table => 'ods.user_action_log',
    options => MAP(
        'target-file-size-bytes', '536870912',
        'min-input-files',         '5',
        'max-file-size-bytes',    '1073741824',
        'rewrite-all',             'false',
        'strategy',               'binpack'
    )
);

-- 按指定分区重写
CALL ods_catalog.system.rewrite_data_files(
    table => 'ods.user_action_log',
    predicates => ARRAY['dt = DATE ''2024-06-01'''],
    options => MAP(
        'target-file-size-bytes', '536870912',
        'min-input-files',         '3'
    )
);


-- ========================================================================
-- 数据质量 SQL (运行时诊断)
-- ========================================================================

-- 查看每个分区的小文件数量
SELECT
    dt,
    COUNT(*)          AS total_files,
    SUM(file_size_in_bytes) / 1024 / 1024 / 1024  AS total_gb,
    MIN(file_size_in_bytes) / 1024 / 1024         AS min_mb,
    MAX(file_size_in_bytes) / 1024 / 1024         AS max_mb,
    AVG(file_size_in_bytes) / 1024 / 1024         AS avg_mb
FROM ods_catalog.ods.user_action_log.files
GROUP BY dt
ORDER BY dt DESC
LIMIT 30;

-- 分区行数分布
SELECT dt, COUNT(*) AS rows,
       COUNT(DISTINCT user_id) AS users,
       COUNT(DISTINCT device_id) AS devices
FROM ods_catalog.ods.user_action_log
GROUP BY dt
ORDER BY dt DESC
LIMIT 30;
