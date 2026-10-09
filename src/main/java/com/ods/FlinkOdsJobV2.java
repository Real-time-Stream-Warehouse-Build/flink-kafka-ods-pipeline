package com.ods;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Flink ODS 清洗作业 (V2.0)
 *
 * 相对于 V1.0 的核心升级:
 *
 *  ┌─────────────────────────────────────────────────────────────────────────┐
 *  │  Pipeline V1.0  (纯处理时间, 无乱序处理, 无状态去重)                      │
 *  │                                                                         │
 *  │   Kafka ──► DirtyFilter ──► MySQL                                       │
 *  │                              (脏数据仅侧输出 print)                      │
 *  └─────────────────────────────────────────────────────────────────────────┘
 *
 *  ┌─────────────────────────────────────────────────────────────────────────┐
 *  │  Pipeline V2.0  (事件时间 + Watermark + RocksDB 精准去重 + 迟到侧输出)   │
 *  │                                                                         │
 *  │   Kafka ──► DirtyFilter ──► Watermark(乱序 5s) ──► keyBy ──► DedupFn    │
 *  │                                                               │         │
 *  │                                                    ┌─────────┼─────────┐│
 *  │                                                    ▼         ▼         ▼│
 *  │                                                 MySQL    LATE_DATA   DIRTY│
 *  │                                                ods_main    (侧输出)   (侧输出)│
 *  │                                                               │         ││
 *  │                                                               ▼         ▼│
 *  │                                                          MySQL ods_late print│
 *  └─────────────────────────────────────────────────────────────────────────┘
 *
 * 关键配置:
 *   - Watermark: 允许 5s 乱序 (forBoundedOutOfOrderness)
 *   - 去重状态: RocksDB Embedded, incremental checkpoint, StateTtl 1h
 *   - 迟到数据定义: event_time < current_watermark (已超出乱序窗口)
 *   - 迟到数据单独落表: ods_user_action_log_late (不阻塞主链路)
 *
 * @author ODS-Team (V2.0)
 */
public class FlinkOdsJobV2 {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkOdsJobV2.class);

    // ==================== 基础配置 ====================
    private static final String KAFKA_BOOTSTRAP = "localhost:9092";
    private static final String KAFKA_TOPIC    = "topic_log";
    private static final String KAFKA_GROUP    = "flink_ods_group_v2";

    private static final String MYSQL_URL      = "jdbc:mysql://localhost:3306/dw_ods?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai";
    private static final String MYSQL_USER     = "root";
    private static final String MYSQL_PASS     = "root";
    private static final String MYSQL_TABLE_MAIN   = "ods_user_action_log";
    private static final String MYSQL_TABLE_LATE   = "ods_user_action_log_late";

    private static final int    MYSQL_BATCH    = 500;
    private static final int    MYSQL_INTERVAL  = 3000;
    private static final int    MYSQL_MAX_RETRY = 3;

    // ==================== V2.0 新增配置 ====================

    /** Watermark 乱序容忍窗口: 双 11 大促埋点传输延迟经验值 ~ 3-5s */
    private static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);

    /** 状态 TTL: 去重 ValueState 的空闲过期时间. 双 11 高峰 1h 内同一用户重复行为可能需去重 */
    private static final Time DEDUP_STATE_TTL = Time.hours(1);

    /** Checkpoint 存储路径 (RocksDB incremental checkpoint 需要) */
    private static final String CHECKPOINT_DIR = "file:///tmp/flink/ods-v2/checkpoints";

    /** 允许最大并发 checkpoint 数 (RocksDB 内存占用敏感, 建议 1) */
    private static final int    MAX_CONCURRENT_CHECKPOINTS = 1;

    // ==================== 侧输出流标签 ====================

    /** V1.0 已有: 脏数据侧输出 (JSON 非法 / 必填缺 / app_id 非法) */
    private static final OutputTag<String> DIRTY_TAG = new OutputTag<>("dirty-data-v2") {};

    /** V2.0 新增: 迟到数据侧输出 (事件时间 < watermark) */
    private static final OutputTag<OdsUserActionLog> LATE_DATA_TAG = new OutputTag<>("late-data") {};

    // ==================================================================
    // 主入口
    // ==================================================================

    public static void main(String[] args) throws Exception {
        // =============== 1. 创建执行环境 + RocksDB 状态后端 ===============
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // --- Checkpoint: EXACTLY_ONCE + RocksDB 增量 checkpoint ---
        env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60_000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(MAX_CONCURRENT_CHECKPOINTS);
        env.getCheckpointConfig().setCheckpointStorage(CHECKPOINT_DIR);

        // --- RocksDB Embedded State Backend (开启增量 checkpoint) ---
        EmbeddedRocksDBStateBackend rocksBackend = new EmbeddedRocksDBStateBackend(true); // incremental=true
        rocksBackend.setCompressionEnabled(true);     // 压缩 SST 文件, 省磁盘
        // 注: 不调用 setLocalRocksDbDirectory() 时 Flink 自动用临时目录, 兼容 Flink 1.17
        env.setStateBackend(rocksBackend);

        LOG.info("========== Flink ODS Job V2.0 Starting ==========");
        LOG.info("  Watermark: forBoundedOutOfOrderness={}", OUT_OF_ORDERNESS);
        LOG.info("  StateBackend: EmbeddedRocksDB (incremental checkpoint=ENABLED)");
        LOG.info("  Dedup TTL: {}", DEDUP_STATE_TTL);
        LOG.info("  Checkpoint dir: {}", CHECKPOINT_DIR);

        // =============== 2. Kafka Source ===============
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

        DataStreamSource<String> rawStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),   // 先不用 Watermark, 清洗完再赋值
                "kafka-source"
        );

        // =============== 3. 脏数据过滤 (复用 V1.0 逻辑, 但脏数据 tag 名独立) ===============
        SingleOutputStreamOperator<OdsUserActionLog> cleanedStream = rawStream
                .process(new V2DirtyDataFilterProcessFn())
                .name("dirty-data-filter-v2");

        // 脏数据侧输出 (V2 暂不做进一步清洗, 打印 + 后续可落脏表)
        cleanedStream.getSideOutput(DIRTY_TAG).print("DIRTY_DATA_V2");

        // =============== 4. V2.0 新增: Watermark 事件时间分配 ===============
        SingleOutputStreamOperator<OdsUserActionLog> timedStream = cleanedStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OdsUserActionLog>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                                .withTimestampAssigner(new OdsEventTimeAssigner())
                                .withIdleness(Duration.ofSeconds(30))  // 防止某分区空闲导致 watermark 不推进
                )
                .name("assign-watermark");

        // =============== 5. V2.0 新增: keyBy + RocksDB 精准去重 + 迟到侧输出 ===============
        // key: user_id | event_type | device_id | event_time  (复合去重键)
        SingleOutputStreamOperator<OdsUserActionLog> dedupedStream = timedStream
                .keyBy(log -> buildDedupKey(log))
                .process(new DedupKeyedProcessFn())
                .name("rocksdb-dedup");

        // 迟到数据侧输出 → 专用 JdbcSink (17 列, 含 late_ms + source_watermark)
        String lateInsertSql = "INSERT INTO " + MYSQL_TABLE_LATE + " " +
                "(user_id, event_type, event_time, app_id, device_id, " +
                "page, referrer, product_id, duration_ms, ip, os_type, os_version, " +
                "net_type, ext_json, late_ms, source_watermark, etl_time) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        JdbcStatementBuilder<OdsUserActionLog> lateStatementBuilder = new JdbcStatementBuilder<OdsUserActionLog>() {
            @Override
            public void accept(PreparedStatement ps, OdsUserActionLog log) throws SQLException {
                ps.setString(1,  log.userId);
                ps.setString(2,  log.eventType);
                ps.setTimestamp(3, Timestamp.valueOf(log.eventTime));
                ps.setString(4,  log.appId);
                ps.setString(5,  log.deviceId);
                ps.setString(6,  log.page);
                ps.setString(7,  log.referrer);
                ps.setString(8,  log.productId);
                ps.setLong(9,    log.durationMs == null ? 0L : log.durationMs);
                ps.setString(10, log.ip);
                ps.setString(11, log.osType);
                ps.setString(12, log.osVersion);
                ps.setString(13, log.netType);
                ps.setString(14, log.extJson);
                ps.setObject(15, log.lateMs);                          // V2 诊断字段
                if (log.sourceWatermarkMs != null) {
                    ps.setTimestamp(16, new Timestamp(log.sourceWatermarkMs));
                } else {
                    ps.setTimestamp(16, null);
                }
                ps.setTimestamp(17, new Timestamp(System.currentTimeMillis()));
            }
        };

        JdbcExecutionOptions execOpts = JdbcExecutionOptions.builder()
                .withBatchSize(MYSQL_BATCH)
                .withBatchIntervalMs(MYSQL_INTERVAL)
                .withMaxRetries(MYSQL_MAX_RETRY)
                .build();
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder connOpts =
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(MYSQL_URL)
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(MYSQL_USER)
                        .withPassword(MYSQL_PASS);

        dedupedStream.getSideOutput(LATE_DATA_TAG)
                .addSink(JdbcSink.sink(lateInsertSql, lateStatementBuilder, execOpts, connOpts.build()))
                .name("mysql-late-sink");

        // =============== 6. 主链路: 清洗 + 去重后 → MySQL ODS 主表 ===============
        dedupedStream.addSink(buildMysqlJdbcSink(MYSQL_TABLE_MAIN))
                .name("mysql-ods-sink-v2");

        // 本地调试 print
        dedupedStream.print("CLEANED_V2_MAIN");
        dedupedStream.getSideOutput(LATE_DATA_TAG).print("LATE_DATA_V2");

        // =============== 7. 执行 ===============
        LOG.info("========== Flink ODS Job V2.0 Submitted ==========");
        env.execute("Flink-Ods-Job-V2.0");
    }

    // ==================================================================
    // 辅助方法: 构造去重复合键
    // ==================================================================

    /**
     * 构造精准去重键
     * 规则: user_id + event_type + device_id + event_time 格式化字符串
     *
     * 为什么这样选?
     *   - user_id + event_type 本身不能唯一定位 (同一用户同一类型多次操作)
     *   - 加 device_id 覆盖多端登录场景
     *   - 加 event_time 精确到秒, 同一秒同一设备同一事件认为是重复
     *   - 组合键同时作为 keyBy 分区键, 相同键确保状态命中
     */
    private static String buildDedupKey(OdsUserActionLog log) {
        String timeStr = log.eventTime != null
                ? log.eventTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
                : "null";
        return (log.userId == null ? "null" : log.userId)
                + "_" + (log.eventType == null ? "null" : log.eventType)
                + "_" + (log.deviceId == null ? "null" : log.deviceId)
                + "_" + timeStr;
    }

    // ==================================================================
    // 辅助方法: 构造 JDBC Sink (主表 / 迟到表共用同一 POJO, SQL 不同)
    // ==================================================================

    private static org.apache.flink.connector.jdbc.JdbcSink<OdsUserActionLog> buildMysqlJdbcSink(String tableName) {
        String insertSql = "INSERT INTO " + tableName + " " +
                "(user_id, event_type, event_time, app_id, device_id, " +
                "page, referrer, product_id, duration_ms, ip, os_type, os_version, " +
                "net_type, ext_json, etl_time) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        JdbcStatementBuilder<OdsUserActionLog> statementBuilder = new JdbcStatementBuilder<OdsUserActionLog>() {
            @Override
            public void accept(PreparedStatement ps, OdsUserActionLog log) throws SQLException {
                ps.setString(1,  log.userId);
                ps.setString(2,  log.eventType);
                ps.setTimestamp(3, Timestamp.valueOf(log.eventTime));
                ps.setString(4,  log.appId);
                ps.setString(5,  log.deviceId);
                ps.setString(6,  log.page);
                ps.setString(7,  log.referrer);
                ps.setString(8,  log.productId);
                ps.setLong(9,    log.durationMs == null ? 0L : log.durationMs);
                ps.setString(10, log.ip);
                ps.setString(11, log.osType);
                ps.setString(12, log.osVersion);
                ps.setString(13, log.netType);
                ps.setString(14, log.extJson);
                ps.setTimestamp(15, new Timestamp(System.currentTimeMillis()));
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

        return JdbcSink.sink(insertSql, statementBuilder, execOptions, connBuilder.build());
    }

    // ==================================================================
    // 内部类 1: V2 脏数据过滤 (复用 V1 规则, 独立 tag)
    // ==================================================================

    private static class V2DirtyDataFilterProcessFn
            extends ProcessFunction<String, OdsUserActionLog> {

        private static final long serialVersionUID = 1L;

        private static final Set<String> VALID_APP_IDS =
                new HashSet<>(Arrays.asList("mall_app", "h5", "mini_program"));

        private static final long MIN_EVENT_TIME_MS =
                LocalDateTime.of(2020, 1, 1, 0, 0)
                        .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        @Override
        public void processElement(String raw, Context ctx, Collector<OdsUserActionLog> out) {
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

            String userId     = json.getString("user_id");
            String eventType  = json.getString("event_type");
            String eventTimeStr = json.getString("event_time");

            StringBuilder missing = new StringBuilder();
            if (userId == null || userId.trim().isEmpty())         missing.append("user_id ");
            if (eventType == null || eventType.trim().isEmpty())  missing.append("event_type ");
            if (eventTimeStr == null || eventTimeStr.trim().isEmpty()) missing.append("event_time ");
            if (missing.length() > 0) {
                ctx.output(DIRTY_TAG, raw + " | MISSING_FIELDS: " + missing);
                return;
            }

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

            long eventTimeMs = eventTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            long nowPlus1h   = System.currentTimeMillis() + 3_600_000L;
            if (eventTimeMs < MIN_EVENT_TIME_MS) {
                ctx.output(DIRTY_TAG, raw + " | EVENT_TIME_TOO_OLD: " + eventTimeStr);
                return;
            }
            if (eventTimeMs > nowPlus1h) {
                ctx.output(DIRTY_TAG, raw + " | EVENT_TIME_FUTURE: " + eventTimeStr);
                return;
            }

            String appId = json.getString("app_id");
            if (appId == null || !VALID_APP_IDS.contains(appId)) {
                ctx.output(DIRTY_TAG, raw + " | INVALID_APP_ID: " + appId);
                return;
            }

            // 构造清洗后 POJO
            OdsUserActionLog cleaned = new OdsUserActionLog();
            cleaned.userId    = userId.trim();
            cleaned.eventType = eventType.trim();
            cleaned.eventTime = eventTime;
            cleaned.appId     = appId.trim();
            cleaned.deviceId  = json.getString("device_id");
            cleaned.page      = json.getString("page");
            cleaned.referrer  = json.getString("referrer");
            cleaned.productId = json.getString("product_id");
            cleaned.durationMs = json.getLongValue("duration_ms");
            cleaned.ip        = json.getString("ip");
            cleaned.osType    = json.getString("os_type");
            cleaned.osVersion = json.getString("os_version");
            cleaned.netType   = json.getString("net_type");

            try {
                JSONObject ext = new JSONObject();
                Set<String> knownKeys = new HashSet<>(Arrays.asList(
                        "user_id","event_type","event_time","app_id","device_id",
                        "page","referrer","product_id","duration_ms","ip",
                        "os_type","os_version","net_type"
                ));
                for (String key : json.keySet()) {
                    if (!knownKeys.contains(key)) ext.put(key, json.get(key));
                }
                cleaned.extJson = ext.isEmpty() ? null : ext.toJSONString();
            } catch (Exception ignore) {
                cleaned.extJson = null;
            }

            out.collect(cleaned);
        }
    }

    // ==================================================================
    // 内部类 2: Watermark 事件时间提取器
    // ==================================================================

    /**
     * 从 OdsUserActionLog.eventTime (LocalDateTime) 提取事件时间戳 (ms)
     * Flink Watermark 需要: 一条 DataStream 的每个元素 → 一个 long 毫秒时间
     */
    private static class OdsEventTimeAssigner
            implements SerializableTimestampAssigner<OdsUserActionLog> {

        private static final long serialVersionUID = 1L;

        @Override
        public long extractTimestamp(OdsUserActionLog event, long recordTimestamp) {
            if (event.eventTime == null) {
                return recordTimestamp;   // fallback: 用处理时间
            }
            return event.eventTime
                    .atZone(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();
        }
    }

    // ==================================================================
    // 内部类 3: RocksDB 精准去重 + 迟到数据识别
    // ==================================================================

    /**
     * 去重核心: KeyedProcessFunction
     *
     * 每条进入此算子的数据, 其 Key 为 buildDedupKey() 复合键.
     * RocksDB 维护 ValueState<Boolean> seen.
     *   - 若 seen 为 null: 第一次见到该键 → 写入 seen=true → 输出
     *   - 若 seen 为 true: 重复数据 → 丢弃
     *
     * 迟到检测:
     *   WatermarkStrategy.forBoundedOutOfOrderness(5s) 保证 watermark
     *   落后于最新事件时间 5s. 若某条数据的 event_time < watermark,
     *   说明它错过了 5s 的乱序容忍窗口, 属于迟到数据 → 侧输出.
     *
     * 状态清理:
     *   StateTtlConfig 配置 1h 空闲过期. 过期后 ValueState 自动清理,
     *   避免 RocksDB 状态无限膨胀.
     */
    private static class DedupKeyedProcessFn
            extends KeyedProcessFunction<String, OdsUserActionLog, OdsUserActionLog> {

        private static final long serialVersionUID = 1L;

        /** 去重状态: 该 key 是否已见过 */
        private transient ValueState<Boolean> seenState;

        /** 迟到计数器 (监控用) */
        private transient org.apache.flink.api.common.state.ValueState<Long> lateCounter;

        @Override
        public void open(Configuration parameters) throws Exception {
            // --- 去重状态: ValueState + TTL ---
            ValueStateDescriptor<Boolean> dedupDesc =
                    new ValueStateDescriptor<>("dedup-seen", Boolean.class);

            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(DEDUP_STATE_TTL)
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            dedupDesc.enableTimeToLive(ttlConfig);

            this.seenState = getRuntimeContext().getState(dedupDesc);

            // --- 迟到计数器 (带统一 TTL, 避免独立状态膨胀) ---
            ValueStateDescriptor<Long> lateDesc =
                    new ValueStateDescriptor<>("late-counter", Long.class);
            lateDesc.enableTimeToLive(ttlConfig);
            this.lateCounter = getRuntimeContext().getState(lateDesc);

            LOG.info("DedupKeyedProcessFn opened, TTL={}", DEDUP_STATE_TTL);
        }

        @Override
        public void processElement(OdsUserActionLog value, Context ctx, Collector<OdsUserActionLog> out)
                throws Exception {

            // ------ Step 1: 迟到数据检测 ------
            // WatermarkStrategy 保证: watermark = max_event_time - out_of_orderness.
            // event_time < watermark 意味着这条消息在超过乱序容忍窗口后才到达.
            long eventTs = ctx.timestamp();   // Flink 已用 TimestampAssigner 提取
            long wm      = ctx.timerService().currentWatermark();

            if (wm > 0 && eventTs < wm) {
                // --- 迟到! 走侧输出, 并填充诊断字段 ---
                Long cnt = lateCounter.value();
                long newCnt = (cnt == null ? 0L : cnt) + 1L;
                lateCounter.update(newCnt);

                // 诊断字段: 写入 OdsUserActionLog 新字段, 供 late 表 JdbcSink 落库
                value.lateMs          = wm - eventTs;   // 迟到多少 ms
                value.sourceWatermarkMs = wm;           // 判定时刻的 watermark

                LOG.debug("LATE_DATA detected: eventTime={}, watermark={}, lateMs={}, user={}, dedupCount={}",
                        value.eventTime, wm, value.lateMs, value.userId, newCnt);
                ctx.output(LATE_DATA_TAG, value);
                return;   // 迟到数据不进入去重, 直接侧输出
            }

            // ------ Step 2: RocksDB 精准去重 ------
            Boolean seen = seenState.value();
            if (seen != null && seen) {
                // 重复 → 丢弃
                LOG.trace("DEDUP_SKIP: key={}", ctx.getCurrentKey());
                return;
            }

            // 第一次见到 → 标记 + 输出
            seenState.update(Boolean.TRUE);
            out.collect(value);
        }
    }
}
