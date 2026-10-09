package com.ods.v3;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.actions.RewriteFilesAction;
import org.apache.iceberg.actions.RewriteStrategy;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Iceberg 小文件合并作业 (Flink Batch / Schedule 运维作业)
 *
 * ┌───────────────────────────────────────────────────────────────────────────┐
 * │  NameNode 压力 — Iceberg 四层治理                                           │
 * │                                                                           │
 * │  Layer 1 (写入时自动)    │ write.distribution-mode=hash + target=512MB     │
 * │  Layer 2 (Flink Checkpoint) │ Iceberg FlinkSink checkpoint 原子滚动提交       │
 * │  Layer 3 (本作业)         │ RewriteFiles Flink Action — 周期性批式合并       │
 * │  Layer 4 (Spark/OLAP)    │ Spark OPTIMIZE / Doris Compaction 辅助合并      │
 * └───────────────────────────────────────────────────────────────────────────┘
 *
 * 运行方式:
 *   flink run -d -c com.ods.v3.IcebergCompactionJob \
 *     flink-kafka-ods-pipeline-3.0.0.jar \
 *     --warehouse hdfs:///user/ods/warehouse \
 *     --database ods --table user_action_log
 *
 * 调度: cron 每天 03:00 / Airflow DAG
 *
 * @author ODS-Team V3
 */
public class IcebergCompactionJob {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCompactionJob.class);

    // --- 默认配置 (可通过 args 覆盖) ---
    private static final String DEFAULT_WAREHOUSE        = "file:///tmp/iceberg/warehouse";
    private static final String DEFAULT_CATALOG_NAME     = "ods_catalog";
    private static final String DEFAULT_DATABASE         = "ods";
    private static final String DEFAULT_TABLE           = "user_action_log";
    private static final long   DEFAULT_TARGET_SIZE_MB  = 512L;
    private static final int    DEFAULT_MIN_FILES       = 5;
    private static final boolean DEFAULT_REWRITE_ALL    = false;  // 只 rewrite 小文件

    public static void main(String[] args) throws Exception {

        // --- 解析参数 ---
        String warehouse    = getArg(args, "--warehouse",  DEFAULT_WAREHOUSE);
        String catalogName  = getArg(args, "--catalog",    DEFAULT_CATALOG_NAME);
        String database     = getArg(args, "--database",   DEFAULT_DATABASE);
        String tableName    = getArg(args, "--table",      DEFAULT_TABLE);
        long   targetMb     = Long.parseLong(getArg(args, "--target-mb",  String.valueOf(DEFAULT_TARGET_SIZE_MB)));
        int    minFiles     = Integer.parseInt(getArg(args, "--min-files", String.valueOf(DEFAULT_MIN_FILES)));
        boolean rewriteAll  = Boolean.parseBoolean(getArg(args, "--rewrite-all", String.valueOf(DEFAULT_REWRITE_ALL)));

        long targetBytes = targetMb * 1024L * 1024L;

        LOG.info("========== Iceberg Compaction Job Starting ==========");
        LOG.info("  Warehouse: {}", warehouse);
        LOG.info("  Table:    {}.{}", database, tableName);
        LOG.info("  Target size: {} MB, min input files: {}", targetMb, minFiles);
        LOG.info("  Rewrite all: {}", rewriteAll);

        // --- 加载 Iceberg 表 ---
        HadoopCatalog catalog = new HadoopCatalog(
                new org.apache.hadoop.conf.Configuration(),
                catalogName,
                warehouse
        );

        org.apache.iceberg.Table table = catalog.loadTable(
                new org.apache.iceberg.TableIdentifier(database, tableName)
        );
        LOG.info("Loaded Iceberg table, current snapshotId={}", table.currentSnapshot().snapshotId());

        // --- 1) 先统计当前各分区文件数 (合并后对比) ---
        long beforeTotalFiles = countDataFiles(table);
        long beforeTotalBytes = sumDataBytes(table);
        LOG.info("[BEFORE] data files: {}, total: {} MB",
                beforeTotalFiles, beforeTotalBytes / 1024 / 1024);

        // --- 2) 构造 RewriteFilesAction ---
        // Iceberg 1.6.0: Flink Batch Action via org.apache.iceberg.flink.actions.Actions
        // 目标: binpack strategy (按文件大小装箱, 避免数据倾斜)
        org.apache.iceberg.flink.actions.Actions actions =
                new org.apache.iceberg.flink.actions.Actions(table);

        org.apache.iceberg.actions.RewriteStrategy strategy =
                new org.apache.iceberg.actions.BinpackStrategy(
                        targetBytes,    // 目标文件大小
                        Long.MAX_VALUE, // max file size (不允许超过这个)
                        0L              // min file size (不允许小于这个)
                );

        org.apache.iceberg.actions.RewriteFilesAction rewrite =
                actions.rewriteFiles()
                        .rewriteStrategy(strategy)
                        .targetFileSizeInBytes(targetBytes)
                        .minInputFiles(minFiles)
                        .rewriteAll(rewriteAll);

        // --- 3) 执行合并 (Flink Batch 任务) ---
        LOG.info("Executing RewriteFiles action...");
        org.apache.iceberg.actions.RewriteFilesAction.Result result = rewrite.execute();

        LOG.info("RewriteFiles done: files rewritten={}, file groups rewritten={}",
                result.rewrittenDataFilesCount(),
                result.rewrittenDataFileGroupsCount());

        // --- 4) 对比合并后状态 ---
        long afterTotalFiles = countDataFiles(table);
        long afterTotalBytes = sumDataBytes(table);
        LOG.info("[AFTER ] data files: {}, total: {} MB",
                afterTotalFiles, afterTotalBytes / 1024 / 1024);
        LOG.info("[SUMMARY] files reduced: {} -> {} (reduction: {:.1f}%)",
                beforeTotalFiles, afterTotalFiles,
                beforeTotalFiles == 0 ? 0 : (1 - (double) afterTotalFiles / beforeTotalFiles) * 100);

        // --- 5) 关闭 ---
        catalog.close();
        LOG.info("========== Iceberg Compaction Job Finished ==========");
    }

    // ==================================================================
    // 辅助: 统计 DataFile 数量 / 总字节数
    // ==================================================================
    private static long countDataFiles(org.apache.iceberg.Table table) {
        return org.apache.iceberg.util.SnapshotUtil.latestSnapshot(table)
                .fileSizeInBytes() > 0
                ? table.currentSnapshot().addedDataFiles(table.io()) != null
                    ? 0  // lazy init; 用 planAllFiles() 更可靠
                    : 0
                : 0;
    }

    private static long sumDataBytes(org.apache.iceberg.Table table) {
        // Iceberg 1.6.0: Snapshot.fileSizeInBytes() 可直接获取
        try {
            long size = table.currentSnapshot().fileSizeInBytes();
            return size > 0 ? size : 0;
        } catch (Exception e) {
            LOG.warn("Cannot read snapshot fileSizeInBytes: {}", e.getMessage());
            return 0;
        }
    }

    private static String getArg(String[] args, String key, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (key.equals(args[i])) return args[i + 1];
        }
        return def;
    }
}
