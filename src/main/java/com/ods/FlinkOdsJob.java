package com.ods;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;

/**
 * Flink ODS 清洗作业 (V1.0)
 *
 * 业务背景: 双 11 大促期间 APP 埋点日志乱序、脏数据频发，导致 BI 看板数据漂移。
 *          本作业构建实时清洗层，将杂乱无章的用户行为转化为结构化 ODS 层数据。
 *
 * 核心流程: Kafka Source → JSON 解析 → 字段过滤/脏数据清洗 → MySQL ODS Sink
 *
 * 脏数据定义 (V1.0):
 *   1) JSON 解析失败 (格式错误)
 *   2) 必填字段缺失 (user_id, event_type, event_time)
 *   3) event_time 为未来时间 (时钟偏差) 或早于 2020-01-01
 *   4) app_id 不在合法枚举范围 ["mall_app", "h5", "mini_program"]
 *
 * @author ODS-Team
 */
public class FlinkOdsJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkOdsJob.class);

    /** Kafka Source 配置 */
    private static final String KAFKA_BOOTSTRAP = "localhost:9092";
    private static final String KAFKA_TOPIC    = "topic_log";
    private static final String KAFKA_GROUP    = "flink_ods_group_v1";

    /** MySQL ODS Sink 配置 */
    private static final String MYSQL_URL      = "jdbc:mysql://localhost:3306/dw_ods?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai";
    private static final String MYSQL_USER     = "root";
    private static final String MYSQL_PASS     = "root";
    private static final String MYSQL_TABLE    = "ods_user_action_log";
    private static final int    MYSQL_BATCH    = 500;       // JDBC 批量提交条数
    private static final int    MYSQL_INTERVAL  = 3000;      // JDBC 批量提交间隔 ms
    private static final int    MYSQL_MAX_RETRY = 3;        // 失败重试次数

    /** 脏数据侧输出流标签 */
    private static final OutputTag<String> DIRTY_TAG = new OutputTag<>("dirty-data") {};

    public static void main(String[] args) throws Exception {
        // ========== 1. 创建 Flink 执行环境 ==========
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // Checkpoint: 精准一次语义 (V1.0 开启 checkpoint 以保障数据不丢)
        env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60_000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));

        LOG.info("========== Flink ODS Job V1.0 Starting ==========");
        LOG.info("Kafka: bootstrap={}, topic={}, group={}", KAFKA_BOOTSTRAP, KAFKA_TOPIC, KAFKA_GROUP);
        LOG.info("MySQL: url={}, table={}", MYSQL_URL, MYSQL_TABLE);

        // ========== 2. 构建 Kafka Source ==========
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP);
        kafkaProps.setProperty("group.id", KAFKA_GROUP);
        kafkaProps.setProperty("enable.auto.commit", "false");
        kafkaProps.setProperty("auto.offset.reset", "latest");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(KAFKA_TOPIC)
                .setGroupId(KAFKA_GROUP)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setProperties(kafkaProps)
                .build();

        // 纯消费场景, 不依赖事件时间乱序处理 (V1.0 暂不引入 watermark)
        DataStreamSource<String> rawStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
        );

        LOG.info("Kafka source created, topic={}", KAFKA_TOPIC);

        // ========== 3. JSON 解析 + 脏数据过滤 ==========
        // V1.0 使用 ProcessFunction: 合法数据走主输出, 脏数据走侧输出流
        SingleOutputStreamOperator<OdsUserActionLog> cleanedStream = rawStream
                .process(new DirtyDataFilterProcessFn())
                .name("dirty-data-filter");

        // ========== 4. 脏数据侧输出流 (暂不处理, 仅打印) ==========
        cleanedStream.getSideOutput(DIRTY_TAG)
                .print("DIRTY_DATA");
        LOG.info("Dirty data side output stream attached (v1.0: only print)");

        // ========== 5. MySQL ODS Sink ==========
        String insertSql = "INSERT INTO " + MYSQL_TABLE + " " +
                "(user_id, event_type, event_time, app_id, device_id, " +
                "page, referrer, product_id, duration_ms, ip, os_type, os_version, " +
                "net_type, ext_json, etl_time) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        JdbcStatementBuilder<OdsUserActionLog> statementBuilder = new JdbcStatementBuilder<OdsUserActionLog>() {
            @Override
            public void accept(PreparedStatement ps, OdsUserActionLog log) throws SQLException {
                ps.setString(1,  log.userId);
                ps.setString(2,  log.eventType);
                ps.setTimestamp(3, Timestamp.valueOf(log.eventTime));  // MySQL DATETIME
                ps.setString(4,  log.appId);
                ps.setString(5,  log.deviceId);
                ps.setString(6,  log.page);
                ps.setString(7,  log.referrer);
                ps.setString(8,  log.productId);
                ps.setLong(9,    log.durationMs);
                ps.setString(10, log.ip);
                ps.setString(11, log.osType);
                ps.setString(12, log.osVersion);
                ps.setString(13, log.netType);
                ps.setString(14, log.extJson);
                ps.setTimestamp(15, new Timestamp(System.currentTimeMillis()));  // etl_time
            }
        };

        JdbcExecutionOptions execOptions = JdbcExecutionOptions.builder()
                .withBatchSize(MYSQL_BATCH)
                .withBatchIntervalMs(MYSQL_INTERVAL)
                .withMaxRetries(MYSQL_MAX_RETRY)
                .build();

        JdbcConnectionOptions.JdbcConnectionOptionsBuilder connBuilder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
        connBuilder.withUrl(MYSQL_URL)
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername(MYSQL_USER)
                .withPassword(MYSQL_PASS);

        cleanedStream.addSink(
                JdbcSink.sink(insertSql, statementBuilder, execOptions, connBuilder.build())
        ).name("mysql-ods-sink");

        LOG.info("MySQL sink attached, target table={}", MYSQL_TABLE);

        // ========== 6. 本地调试: 同时打印清洗后数据 ==========
        cleanedStream.print("CLEANED");

        // ========== 7. 执行 ==========
        LOG.info("========== Flink ODS Job V1.0 Submitted ==========");
        env.execute("Flink-Ods-Job-V1.0");
    }

    // ========================================================================
    // 内部类
    // ========================================================================

    /**
     * 脏数据过滤 ProcessFunction
     *
     * 主输出: 清洗后的 OdsUserActionLog POJO
     * 侧输出: 脏数据原始 JSON + 失败原因 (String)
     */
    private static class DirtyDataFilterProcessFn
            extends ProcessFunction<String, OdsUserActionLog> {

        private static final long serialVersionUID = 1L;

        // V1.0 合法 app_id 白名单
        private static final java.util.Set<String> VALID_APP_IDS =
                new java.util.HashSet<>(java.util.Arrays.asList("mall_app", "h5", "mini_program"));

        // 时间边界: 2020-01-01 ~ now + 1h (允许 1h 时钟偏差)
        private static final long MIN_EVENT_TIME_MS =
                java.time.LocalDateTime.of(2020, 1, 1, 0, 0)
                        .atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();

        @Override
        public void processElement(String raw, Context ctx, Collector<OdsUserActionLog> out) {
            // ---------- Step 1: JSON 解析 ----------
            JSONObject json;
            try {
                json = JSON.parseObject(raw);
            } catch (Exception e) {
                ctx.output(DIRTY_TAG, raw + " | PARSE_ERROR: " + e.getMessage());
                return;
            }
            if (json == null || json.isEmpty()) {
                ctx.output(DIRTY_TAG, raw + " | PARSE_ERROR: empty json");
                return;
            }

            // ---------- Step 2: 必填字段校验 ----------
            String userId    = json.getString("user_id");
            String eventType = json.getString("event_type");
            String eventTimeStr = json.getString("event_time");

            StringBuilder missing = new StringBuilder();
            if (userId == null || userId.trim().isEmpty())    missing.append("user_id ");
            if (eventType == null || eventType.trim().isEmpty()) missing.append("event_type ");
            if (eventTimeStr == null || eventTimeStr.trim().isEmpty()) missing.append("event_time ");

            if (missing.length() > 0) {
                ctx.output(DIRTY_TAG, raw + " | MISSING_FIELDS: " + missing);
                return;
            }

            // ---------- Step 3: event_time 合法性 ----------
            java.time.LocalDateTime eventTime;
            try {
                // 兼容格式: 2025-11-11 00:10:00 / 2025-11-11T00:10:00
                String normalized = eventTimeStr.replace("T", " ");
                // 截断到秒精度
                if (normalized.length() > 19) {
                    normalized = normalized.substring(0, 19);
                }
                eventTime = java.time.LocalDateTime.parse(
                        normalized,
                        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                );
            } catch (Exception e) {
                ctx.output(DIRTY_TAG, raw + " | INVALID_EVENT_TIME: " + eventTimeStr);
                return;
            }

            long eventTimeMs = eventTime.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
            long nowPlus1h   = System.currentTimeMillis() + 3_600_000L;

            if (eventTimeMs < MIN_EVENT_TIME_MS) {
                ctx.output(DIRTY_TAG, raw + " | EVENT_TIME_TOO_OLD: " + eventTimeStr);
                return;
            }
            if (eventTimeMs > nowPlus1h) {
                ctx.output(DIRTY_TAG, raw + " | EVENT_TIME_FUTURE: " + eventTimeStr);
                return;
            }

            // ---------- Step 4: app_id 白名单 ----------
            String appId = json.getString("app_id");
            if (appId == null || !VALID_APP_IDS.contains(appId)) {
                ctx.output(DIRTY_TAG, raw + " | INVALID_APP_ID: " + appId);
                return;
            }

            // ---------- Step 5: 构造清洗后 POJO ----------
            OdsUserActionLog cleaned = new OdsUserActionLog();
            cleaned.userId      = userId.trim();
            cleaned.eventType   = eventType.trim();
            cleaned.eventTime   = eventTime;    // LocalDateTime (供 POJO 使用, Sink 转 Timestamp)
            cleaned.appId       = appId.trim();
            cleaned.deviceId    = json.getString("device_id");
            cleaned.page        = json.getString("page");
            cleaned.referrer    = json.getString("referrer");
            cleaned.productId   = json.getString("product_id");
            cleaned.durationMs  = json.getLongValue("duration_ms");   // null → 0
            cleaned.ip          = json.getString("ip");
            cleaned.osType      = json.getString("os_type");
            cleaned.osVersion   = json.getString("os_version");
            cleaned.netType     = json.getString("net_type");
            // ext_json: 保留除已知字段外的所有其他扩展字段, 作为 JSON 字符串
            try {
                // 过滤已知字段后, 剩余的放入 ext_json
                JSONObject ext = new JSONObject();
                java.util.Set<String> knownKeys = new java.util.HashSet<>(java.util.Arrays.asList(
                        "user_id","event_type","event_time","app_id","device_id",
                        "page","referrer","product_id","duration_ms","ip",
                        "os_type","os_version","net_type"
                ));
                for (String key : json.keySet()) {
                    if (!knownKeys.contains(key)) {
                        ext.put(key, json.get(key));
                    }
                }
                cleaned.extJson = ext.isEmpty() ? null : ext.toJSONString();
            } catch (Exception ignore) {
                cleaned.extJson = null;
            }

            out.collect(cleaned);
        }
    }

    // ========================================================================
    // 备用: 简单 FlatMapFunction (若不使用侧输出流时可替换)
    // ========================================================================

    /**
     * 简易版本: 丢弃脏数据, 不输出侧流
     */
    @SuppressWarnings("unused")
    private static class SimpleCleanFlatMap implements FlatMapFunction<String, OdsUserActionLog> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String raw, Collector<OdsUserActionLog> out) {
            try {
                JSONObject json = JSON.parseObject(raw);
                if (json == null) return;

                String userId = json.getString("user_id");
                String eventType = json.getString("event_type");
                String eventTimeStr = json.getString("event_time");
                if (userId == null || eventType == null || eventTimeStr == null) return;

                OdsUserActionLog log = new OdsUserActionLog();
                log.userId = userId.trim();
                log.eventType = eventType.trim();
                String normalized = eventTimeStr.replace("T", " ");
                if (normalized.length() > 19) normalized = normalized.substring(0, 19);
                log.eventTime = java.time.LocalDateTime.parse(
                        normalized, java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                log.appId = json.getString("app_id");
                log.deviceId = json.getString("device_id");
                log.page = json.getString("page");
                log.productId = json.getString("product_id");
                log.durationMs = json.getLongValue("duration_ms");
                log.ip = json.getString("ip");
                log.osType = json.getString("os_type");
                log.netType = json.getString("net_type");

                out.collect(log);
            } catch (Exception e) {
                // 忽略脏数据
            }
        }
    }
}
