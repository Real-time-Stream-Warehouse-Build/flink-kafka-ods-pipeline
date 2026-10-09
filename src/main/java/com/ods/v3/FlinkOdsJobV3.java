package com.ods.v3;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mysql.cdc.MySqlSource;
import org.apache.flink.connector.mysql.cdc.MySqlSourceBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Flink ODS 入湖作业 V3.0
 *
 * ┌───────────────────────────────────────────────────────────────────────────────────┐
 * │  Pipeline V3.0                                                                     │
 * │                                                                                   │
 * │  MySQL Binlog (dw_ods_mysql:user_action_events)                                    │
 * │       │                                                                           │
 * │       ▼                                                                           │
 * │  MySqlSource (Flink CDC 3.1.0, 精确一次 offset 记忆)                                │
 * │       │ Row (debezium 风格: op=I/U/D + before/after JSON)                         │
 * │       ▼                                                                           │
 * │  CdcConvertProcessFn (CDC JSON 解析 + 字段清洗 + 脏数据侧输出 + upsert key 构造)     │
 * │       │ OdsEvent POJO                                                             │
 * │       ▼                                                                           │
 * │  keyBy(composite key)                                                             │
 * │       │                                                                           │
 * │       ▼                                                                           │
 * │  Iceberg FlinkSink (iceberg-flink-runtime-1.17:1.6.0)                            │
 * │       │ Checkpoint EXACTLY_ONCE → snapshot 原子提交                                 │
 * │       ▼                                                                           │
 * │  Iceberg ODS Table (HadoopCatalog, partition by day, hash bucket by user_id)       │
 * │                                                                                   │
 * └───────────────────────────────────────────────────────────────────────────────────┘
 *
 * V3.0 核心升级点:
 *   ① CDC: 直连 MySQL Binlog (Flink CDC 3.1.0), 秒级捕获 I/U/D 事件
 *   ② 湖格式: Iceberg 1.6.0 (最后一个 JDK 8 兼容版本), 支持 ACID 事务 + upsert
 *   ③ 精确一次: CDC offset checkpoint 记忆 + Iceberg Sink checkpoint 原子提交
 *   ④ 小文件治理: Iceberg 表级参数 write.distribution-mode=hash + write.target-file-size-bytes=512MB,
 *                定期 Flink Batch RewriteFiles Action 合并
 *   ⑤ 乱序: 事件时间 + Watermark (V2 逻辑延续, iceberg 按 event_time 分区保证顺序消费)
 *
 * 技术选型决策 (见 pom.xml):
 *   ┌────────────────────────────┬────────────────────────────────────────────────────┐
 *   │ Iceberg 1.6.0 (而非 1.7+)  │ 1.7+ 已移除 Flink 1.17 支持且要求 JDK 11+           │
 *   ├────────────────────────────┼────────────────────────────────────────────────────┤
 *   │ HadoopCatalog (而非 Hive)  │ 本地最简, 生产可切换 HiveCatalog/Nessie/RestCatalog  │
 *   ├────────────────────────────┼────────────────────────────────────────────────────┤
 *   │ upsert 模式                │ CDC 必然产生 U/D 事件, Iceberg 用 MERGE INTO / CDC spec │
 *   └────────────────────────────┴────────────────────────────────────────────────────┘
 *
 * @author ODS-Team V3
 */
public class FlinkOdsJobV3 {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkOdsJobV3.class);

    // ==================== MySQL CDC 源表配置 ====================
    private static final String MYSQL_HOST       = "localhost";
    private static final int    MYSQL_PORT       = 3306;
    private static final String MYSQL_USER       = "cdc_user";
    private static final String MYSQL_PASS       = "cdc_pass";
    private static final String MYSQL_DB_TABLE   = "dw_ods_mysql.user_action_events";

    // ==================== Iceberg Catalog + Warehouse ====================
    private static final String ICEBERG_CATALOG_NAME   = "ods_catalog";
    private static final String ICEBERG_WAREHOUSE_DIR  = "file:///tmp/iceberg/warehouse";  // 本地测试, 生产换 hdfs:///user/xxx/warehouse
    private static final String ICEBERG_DATABASE        = "ods";
    private static final String ICEBERG_TABLE           = "user_action_log";

    // ==================== Checkpoint + 状态 ====================
    private static final String CHECKPOINT_DIR = "file:///tmp/flink/ods-v3/checkpoints";
    private static final long   CHECKPOINT_INTERVAL_MS = 30_000L;   // 30s, 秒级延迟 + 合理 state

    // ==================== 侧输出流 (脏数据) ====================
    private static final OutputTag<String> DIRTY_TAG = new OutputTag<>("dirty-data-cdc") {};

    public static void main(String[] args) throws Exception {

        // ==================================================================
        // 1. 创建 Flink 执行环境
        // ==================================================================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // --- Checkpoint: EXACTLY_ONCE 保证 CDC offset 记忆 + Iceberg 原子快照 ---
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60_000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointStorage(CHECKPOINT_DIR);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 5000L));

        LOG.info("========== Flink ODS Job V3.0 Starting ==========");
        LOG.info("  CDC: {} @ {}:{}", MYSQL_DB_TABLE, MYSQL_HOST, MYSQL_PORT);
        LOG.info("  Iceberg: {}.{}, warehouse={}", ICEBERG_DATABASE, ICEBERG_TABLE, ICEBERG_WAREHOUSE_DIR);
        LOG.info("  Exactly-once: EXACTLY_ONCE + checkpoint interval={}ms", CHECKPOINT_INTERVAL_MS);

        // ==================================================================
        // 2. MySQL CDC Source (DataStream API)
        // ==================================================================
        // Flink CDC 3.1.0 MySqlSource:
        //   - 自动完成 snapshot (全量) → incremental (binlog) 切换
        //   - Checkpoint 记忆 binlog offset → Exactly-once
        //   - debeziumProperties: decimal.handling.mode=string 避免金额变 base64
        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("decimal.handling.mode", "string");

        MySqlSource<String> cdcSource = new MySqlSourceBuilder<String>()
                .hostname(MYSQL_HOST)
                .port(MYSQL_PORT)
                .databaseList("dw_ods_mysql")
                .tableList(MYSQL_DB_TABLE)
                .username(MYSQL_USER)
                .password(MYSQL_PASS)
                // 起始位点: from_latest 跳过已有历史, from_earliest 消费全部 binlog
                .startupOptions(org.apache.flink.connector.mysql.cdc.MySqlSource.StartupOptions.fromLatest())
                .deserializer(new DebeziumJsonDeserializationSchema())
                .debeziumProperties(debeziumProps)
                .build();

        DataStreamSource<String> cdcJsonStream = env.fromSource(
                cdcSource,
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                "mysql-cdc-source"
        );

        LOG.info("CDC source created for {}", MYSQL_DB_TABLE);

        // ==================================================================
        // 3. CDC JSON 解析 + 清洗 + upsert key 构造
        // ==================================================================
        SingleOutputStreamOperator<OdsCdcEvent> cleaned = cdcJsonStream
                .process(new CdcConvertProcessFn())
                .name("cdc-parse-clean");

        cleaned.getSideOutput(DIRTY_TAG)
                .print("DIRTY_CDC");

        // ==================================================================
        // 4. Iceberg Sink (Flink Table API sink)
        // ==================================================================
        // FlinkSink 构造需要:
        //   - TableLoader.load(): Iceberg 表
        //   - rowType: Flink RowTypeInfo (和 Iceberg 表 schema 对齐)
        //   - writeProps: 覆盖 Iceberg 表级默认属性 (压缩 / 小文件)

        org.apache.flink.configuration.Configuration writeProps = new org.apache.flink.configuration.Configuration();
        // 压缩 (Parquet ZSTD, 节省 HDFS 存储 / 加速 Scan)
        writeProps.setString("write-format", "parquet");
        writeProps.setString("compression", "zstd");
        // 分区裁剪 / 布隆过滤器
        writeProps.setString("write.distribution-mode", "hash");   // hash 分布避免热点, 小文件自然少
        writeProps.setString("write.distribution-columns", "user_id");
        writeProps.setLong("write.target-file-size-bytes", 512L * 1024 * 1024);  // 512MB per file 目标
        // 过期 / 快照过期 (清理旧 manifest)
        writeProps.setString("expiration", "7d");

        TableLoader icebergLoader = TableLoader.fromCatalog(
                ICEBERG_CATALOG_NAME,
                String.format("%s.%s", ICEBERG_DATABASE, ICEBERG_TABLE)
        );

        // 把 POJO 映射为 Iceberg Row (显式顺序, 和 Iceberg DDL 对齐)
        // Iceberg Table schema 见 sql/iceberg_ods.sql
        org.apache.flink.api.common.typeinfo.TypeInformation<Row> rowTypeInfo =
                org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(
                        new String[]{"user_id", "event_type", "event_time", "app_id", "device_id",
                                     "page", "referrer", "product_id", "duration_ms", "ip",
                                     "os_type", "os_version", "net_type", "ext_json",
                                     "op_type", "op_ts", "etl_time"},
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.LOCAL_DATE_TIME,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.LONG,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.LOCAL_DATE_TIME,
                        org.apache.flink.api.common.typeinfo.Types.LOCAL_DATE_TIME
                );

        cleaned
                .keyBy(e -> dedupKey(e))   // upsert key 同 V2
                .process(new DedupKeyedProcessFn())  // 状态去重
                .map(e -> toIcebergRow(e), rowTypeInfo)
                .name("pojo-to-iceberg-row")
                .transform("iceberg-sink",
                        FlinkSink.forRowData(icebergLoader)
                                .writeProperties(writeProps)
                                .equalityFieldColumns(Arrays.asList("user_id", "event_type", "device_id", "event_time"))
                                .buildSink(rowTypeInfo))
                .name("iceberg-flink-sink");

        // 本地调试 print
        cleaned.print("CDC_CLEANED");

        // ==================================================================
        // 5. 执行
        // ==================================================================
        LOG.info("========== Flink ODS Job V3.0 Submitted ==========");
        env.execute("Flink-Ods-Job-V3.0-CDC-to-Iceberg");
    }

    // ========================================================================
    // 辅助: 构造 upsert key (和 V2 一致, Iceberg MERGE 主键匹配)
    // ========================================================================
    private static String dedupKey(OdsCdcEvent e) {
        String timeStr = e.eventTime == null ? "null" : e.eventTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        return (e.userId == null ? "null" : e.userId)
                + "_" + (e.eventType == null ? "null" : e.eventType)
                + "_" + (e.deviceId == null ? "null" : e.deviceId)
                + "_" + timeStr;
    }

    // ========================================================================
    // 辅助: POJO → Iceberg Row (顺序必须和 DDL 对齐!)
    // ========================================================================
    private static Row toIcebergRow(OdsCdcEvent e) {
        Row r = new Row(17);
        r.setField(0,  e.userId);
        r.setField(1,  e.eventType);
        r.setField(2,  e.eventTime);
        r.setField(3,  e.appId);
        r.setField(4,  e.deviceId);
        r.setField(5,  e.page);
        r.setField(6,  e.referrer);
        r.setField(7,  e.productId);
        r.setField(8,  e.durationMs);
        r.setField(9,  e.ip);
        r.setField(10, e.osType);
        r.setField(11, e.osVersion);
        r.setField(12, e.netType);
        r.setField(13, e.extJson);
        r.setField(14, e.opType);
        r.setField(15, e.opTs);
        r.setField(16, LocalDateTime.now());  // etl_time
        return r;
    }

    // ========================================================================
    // POJO: CDC 清洗后事件
    // ========================================================================
    public static class OdsCdcEvent implements Serializable {
        private static final long serialVersionUID = 1L;

        public String userId;
        public String eventType;
        public LocalDateTime eventTime;   // 事件时间
        public String appId;
        public String deviceId;
        public String page;
        public String referrer;
        public String productId;
        public Long   durationMs;
        public String ip;
        public String osType;
        public String osVersion;
        public String netType;
        public String extJson;

        public String opType;       // "I" / "U" / "D" (CDC op)
        public LocalDateTime opTs;  // CDC 源表操作时间 (binlog ts)
    }

    // ========================================================================
    // CDC JSON 反序列化器: 把 debezium JSON 转为 before/after 结构
    // ========================================================================
    /**
     * 极简 debezium JSON 反序列化器: 把 MySqlSource 输出的 JSON 字符串
     * 提取 before / after / op / ts_ms 字段
     *
     * 注: Flink CDC 3.1.0 已经内置 MySqlDebeziumDeserializationSchema,
     *     但为了项目自包含, 这里独立实现一个轻量版本.
     *     生产可直接用 MySqlDebeziumDeserializationSchema.
     */
    public static class DebeziumJsonDeserializationSchema
            implements org.apache.flink.api.common.serialization.DeserializationSchema<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String deserialize(byte[] message) throws IOException {
            if (message == null || message.length == 0) return null;
            return new String(message, java.nio.charset.StandardCharsets.UTF_8);
        }

        @Override
        public boolean isEndOfStream(String nextElement) { return false; }

        @Override
        public org.apache.flink.api.common.typeinfo.TypeInformation<String> getProducedType() {
            return org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
        }
    }

    // ========================================================================
    // CDC → OdsCdcEvent 解析 ProcessFunction (独立于 Kafka 脏数据逻辑)
    // ========================================================================
    private static class CdcConvertProcessFn extends ProcessFunction<String, OdsCdcEvent> {
        private static final long serialVersionUID = 1L;

        private static final Set<String> VALID_APP_IDS =
                new HashSet<>(Arrays.asList("mall_app", "h5", "mini_program"));

        @Override
        public void processElement(String raw, Context ctx, Collector<OdsCdcEvent> out) {
            com.alibaba.fastjson2.JSONObject root;
            try {
                root = com.alibaba.fastjson2.JSON.parseObject(raw);
            } catch (Exception e) {
                ctx.output(DIRTY_TAG, raw + " | PARSE_ERROR: " + e.getMessage());
                return;
            }
            if (root == null) {
                ctx.output(DIRTY_TAG, raw + " | EMPTY_JSON");
                return;
            }

            // --- CDC op 类型 ---
            String op = root.getString("op");  // c / r / u / d
            String opType = null;
            if ("c".equals(op) || "r".equals(op)) opType = "I";
            else if ("u".equals(op))                opType = "U";
            else if ("d".equals(op))                opType = "D";
            else {
                ctx.output(DIRTY_TAG, raw + " | UNKNOWN_OP: " + op);
                return;
            }

            // --- 取 after 字段 (删除事件 opTs 后没有 after) ---
            com.alibaba.fastjson2.JSONObject data;
            if ("D".equals(opType)) {
                data = root.getJSONObject("before");
            } else {
                data = root.getJSONObject("after");
            }
            if (data == null) {
                ctx.output(DIRTY_TAG, raw + " | NO_DATA_FIELD");
                return;
            }

            // --- 必填校验 ---
            String userId = data.getString("user_id");
            String eventType = data.getString("event_type");
            String eventTimeStr = data.getString("event_time");
            if (userId == null || userId.trim().isEmpty()
                    || eventType == null || eventType.trim().isEmpty()
                    || eventTimeStr == null || eventTimeStr.trim().isEmpty()) {
                ctx.output(DIRTY_TAG, raw + " | MISSING_FIELDS");
                return;
            }

            // --- event_time 解析 ---
            LocalDateTime eventTime;
            try {
                String normalized = eventTimeStr.replace("T", " ");
                if (normalized.length() > 19) normalized = normalized.substring(0, 19);
                eventTime = LocalDateTime.parse(normalized,
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            } catch (Exception e) {
                ctx.output(DIRTY_TAG, raw + " | INVALID_EVENT_TIME: " + eventTimeStr);
                return;
            }

            // --- app_id 白名单 ---
            String appId = data.getString("app_id");
            if (appId == null || !VALID_APP_IDS.contains(appId)) {
                ctx.output(DIRTY_TAG, raw + " | INVALID_APP_ID: " + appId);
                return;
            }

            // --- 构造 POJO ---
            OdsCdcEvent e = new OdsCdcEvent();
            e.userId      = userId.trim();
            e.eventType   = eventType.trim();
            e.eventTime   = eventTime;
            e.appId       = appId.trim();
            e.deviceId    = data.getString("device_id");
            e.page        = data.getString("page");
            e.referrer    = data.getString("referrer");
            e.productId   = data.getString("product_id");
            e.durationMs  = data.getLong("duration_ms");
            e.ip          = data.getString("ip");
            e.osType      = data.getString("os_type");
            e.osVersion   = data.getString("os_version");
            e.netType     = data.getString("net_type");
            e.opType      = opType;

            // binlog 操作时间 (毫秒)
            Long tsMs = root.getLong("ts_ms");
            e.opTs = tsMs != null ?
                    LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(tsMs), ZoneId.systemDefault()) : null;

            // ext_json: 保留未知字段
            try {
                com.alibaba.fastjson2.JSONObject ext = new com.alibaba.fastjson2.JSONObject();
                Set<String> known = new HashSet<>(Arrays.asList(
                        "user_id","event_type","event_time","app_id","device_id",
                        "page","referrer","product_id","duration_ms","ip",
                        "os_type","os_version","net_type"
                ));
                for (String k : data.keySet()) {
                    if (!known.contains(k)) ext.put(k, data.get(k));
                }
                e.extJson = ext.isEmpty() ? null : ext.toJSONString();
            } catch (Exception ignore) { e.extJson = null; }

            out.collect(e);
        }
    }

    // ========================================================================
    // 状态去重 KeyedProcessFunction (和 V2 同构, 复用 TTL 1h)
    // ========================================================================
    private static class DedupKeyedProcessFn
            extends KeyedProcessFunction<String, OdsCdcEvent, OdsCdcEvent> {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Boolean> seenState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("dedup-seen-v3", Boolean.class);
            desc.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build());
            this.seenState = getRuntimeContext().getState(desc);
        }

        @Override
        public void processElement(OdsCdcEvent e, Context ctx, Collector<OdsCdcEvent> out) throws Exception {
            Boolean seen = seenState.value();
            if (seen != null && seen) return;   // 重复丢弃
            seenState.update(Boolean.TRUE);
            out.collect(e);
        }
    }
}
