package com.ods;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * 测试数据生产者: 模拟双 11 大促期间的 APP 埋点日志
 *
 * 用法: 先启 Kafka, 运行 main, 即可向 topic_log 写入一批"纯净 + 脏"混合数据
 *       用于验证 FlinkOdsJob 的清洗能力
 *
 * 脏数据模拟:
 *   ✗ JSON 格式错误 (字符串截断)
 *   ✗ 必填字段缺失 (缺 user_id / event_type / event_time)
 *   ✗ event_time 为未来时间 (时钟偏差, 模拟分布式环境)
 *   ✗ event_time 为 1999 年 (脏数据)
 *   ✗ app_id 非法值 (枚举外)
 *   ✓ 正常数据
 */
public class DirtyLogProducer {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String TOPIC     = "topic_log";

    private static final List<String> EVENT_TYPES = Arrays.asList(
            "view", "click", "cart", "order", "pay", "login", "logout", "search"
    );
    private static final List<String> APP_IDS = Arrays.asList(
            "mall_app", "h5", "mini_program"
    );
    private static final List<String> INVALID_APP_IDS = Arrays.asList(
            "pc_web", "unknown", "", null
    );
    private static final List<String> OS_TYPES = Arrays.asList(
            "Android", "iOS", "Web"
    );
    private static final List<String> NET_TYPES = Arrays.asList(
            "WIFI", "4G", "5G", "3G"
    );
    private static final List<String> PAGES = Arrays.asList(
            "/home", "/product/detail?id=1001", "/cart", "/order/confirm", "/pay", "/search?kw=iphone"
    );

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP);
        props.setProperty("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("acks",             "1");
        props.setProperty("linger.ms",        "5");
        props.setProperty("batch.size",       "16384");

        org.apache.kafka.clients.producer.KafkaProducer<String, String> producer =
                new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        Random rand = new Random();

        // ===== 批次 1: 正常数据 x 10 =====
        System.out.println("===== BATCH 1: 正常数据 (10 条) =====");
        for (int i = 0; i < 10; i++) {
            String json = buildNormalLog(rand);
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "normal-" + i, json));
            System.out.println("  [OK] " + json);
        }

        // ===== 批次 2: 脏数据 =====
        System.out.println("\n===== BATCH 2: 脏数据 (6 条) =====");

        // 2.1 JSON 格式错误 (截断)
        String brokenJson = "{\"user_id\":\"U999\",\"event_type\":\"view\",\"app_id\":\"mall_app\"";
        producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "dirty-parse", brokenJson));
        System.out.println("  [DIRTY-PARSE] " + brokenJson);

        // 2.2 缺 user_id
        String missingUserId = "{\"event_type\":\"click\",\"event_time\":\"2025-11-11 00:00:00\",\"app_id\":\"mall_app\"}";
        producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "dirty-miss", missingUserId));
        System.out.println("  [DIRTY-MISS]  " + missingUserId);

        // 2.3 event_time 未来时间 (+2h)
        String futureTime = JSONObject.of(
                "user_id", "U888",
                "event_type", "pay",
                "event_time", java.time.LocalDateTime.now().plusHours(2).toString().replace("T", " "),
                "app_id", "mall_app"
        ).toJSONString();
        producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "dirty-future", futureTime));
        System.out.println("  [DIRTY-FUTURE]" + futureTime);

        // 2.4 event_time 1999 年
        String tooOld = JSONObject.of(
                "user_id", "U777",
                "event_type", "view",
                "event_time", "1999-01-01 00:00:00",
                "app_id", "h5"
        ).toJSONString();
        producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "dirty-old", tooOld));
        System.out.println("  [DIRTY-OLD]   " + tooOld);

        // 2.5 app_id 非法值
        String invalidApp = JSONObject.of(
                "user_id", "U666",
                "event_type", "cart",
                "event_time", "2025-11-11 00:00:00",
                "app_id", "pc_web"
        ).toJSONString();
        producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "dirty-app", invalidApp));
        System.out.println("  [DIRTY-APP]   " + invalidApp);

        // 2.6 空字符串
        producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "dirty-empty", ""));
        System.out.println("  [DIRTY-EMPTY] (empty string)");

        // ===== 批次 3: 混合持续流 (每秒 2 条, 持续 15s) =====
        System.out.println("\n===== BATCH 3: 持续混合流 (15s) =====");
        for (int i = 0; i < 30; i++) {
            boolean isDirty = rand.nextInt(100) < 15;  // 15% 脏数据率
            String json = isDirty ? buildRandomDirty(rand) : buildNormalLog(rand);
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "mix-" + i, json));
            System.out.println("  [" + (isDirty ? "DIRTY" : "OK") + "] " + json);
            Thread.sleep(500);
        }

        producer.flush();
        producer.close();
        System.out.println("\n===== 发送完成 =====");
    }

    /** 构造一条正常埋点 JSON */
    private static String buildNormalLog(Random rand) {
        JSONObject json = new JSONObject();
        json.put("user_id",    "U" + String.format("%05d", 10000 + rand.nextInt(90000)));
        json.put("event_type", EVENT_TYPES.get(rand.nextInt(EVENT_TYPES.size())));
        json.put("event_time", java.time.LocalDateTime.now().withNano(0).toString().replace("T", " "));
        json.put("app_id",     APP_IDS.get(rand.nextInt(APP_IDS.size())));
        json.put("device_id",  "DEV-" + Long.toHexString(rand.nextLong() & 0xFFFFFFFFL));
        json.put("page",       PAGES.get(rand.nextInt(PAGES.size())));
        json.put("product_id", "P" + (1000 + rand.nextInt(9000)));
        json.put("duration_ms", rand.nextInt(10_000));
        json.put("ip",         "192.168." + rand.nextInt(256) + "." + rand.nextInt(256));
        json.put("os_type",    OS_TYPES.get(rand.nextInt(OS_TYPES.size())));
        json.put("os_version", "1" + rand.nextInt(15) + ".0");
        json.put("net_type",   NET_TYPES.get(rand.nextInt(NET_TYPES.size())));
        // 扩展字段
        json.put("trace_id",   "T" + System.currentTimeMillis() + "-" + rand.nextInt(1000));
        json.put("session_id", "S" + rand.nextInt(100000));
        return json.toJSONString();
    }

    /** 随机构造一条脏数据 */
    private static String buildRandomDirty(Random rand) {
        int type = rand.nextInt(4);
        switch (type) {
            case 0:
                // JSON 截断
                return "{\"user_id\":\"U999\",\"event_type\":\"view\":";
            case 1:
                // 缺字段
                return JSONObject.of(
                        "user_id", "U" + rand.nextInt(9999),
                        "event_time", java.time.LocalDateTime.now().toString().replace("T", " ")
                ).toJSONString();  // 缺 event_type
            case 2:
                // 非法 app_id
                return JSONObject.of(
                        "user_id", "U" + rand.nextInt(9999),
                        "event_type", "click",
                        "event_time", java.time.LocalDateTime.now().toString().replace("T", " "),
                        "app_id", INVALID_APP_IDS.get(rand.nextInt(INVALID_APP_IDS.size()))
                ).toJSONString();
            case 3:
                // 空 JSON
                return "{}";
            default:
                return buildNormalLog(rand);
        }
    }
}
