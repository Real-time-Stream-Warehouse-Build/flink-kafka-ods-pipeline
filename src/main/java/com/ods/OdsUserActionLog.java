package com.ods;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * ODS 层用户行为日志 POJO
 *
 * 对应 MySQL 建表 DDL: sql/ods_user_action_log.sql
 *
 * 字段来源: APP 埋点原始 JSON 清洗后保留的核心字段
 *   - 必填: user_id, event_type, event_time, app_id
 *   - 可选: 其他 12 个字段
 *   - ext_json: 原始 JSON 中未知字段的快照 (用于后续扩展)
 */
public class OdsUserActionLog implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 用户 ID */
    public String userId;

    /** 事件类型: view / click / cart / order / pay / login / logout ... */
    public String eventType;

    /** 事件发生时间 (事件时间 Event Time) */
    public LocalDateTime eventTime;

    /** 应用来源: mall_app / h5 / mini_program */
    public String appId;

    /** 设备唯一标识 (IMEI / Android ID / IDFA) */
    public String deviceId;

    /** 当前页面路径 (e.g. /product/detail?id=123) */
    public String page;

    /** 来源页面 (referrer) */
    public String referrer;

    /** 商品 ID (若为商品相关事件) */
    public String productId;

    /** 停留/操作时长 (ms) */
    public Long durationMs;

    /** 用户 IP */
    public String ip;

    /** 操作系统类型: Android / iOS / Web */
    public String osType;

    /** 操作系统版本号 */
    public String osVersion;

    /** 网络类型: WIFI / 4G / 5G / 3G */
    public String netType;

    /** 扩展字段 JSON (原始 JSON 中未知 key 的快照) */
    public String extJson;

    @Override
    public String toString() {
        return "OdsUserActionLog{" +
                "userId='" + userId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                ", appId='" + appId + '\'' +
                ", deviceId='" + deviceId + '\'' +
                ", page='" + page + '\'' +
                ", referrer='" + referrer + '\'' +
                ", productId='" + productId + '\'' +
                ", durationMs=" + durationMs +
                ", ip='" + ip + '\'' +
                ", osType='" + osType + '\'' +
                ", osVersion='" + osVersion + '\'' +
                ", netType='" + netType + '\'' +
                ", extJson='" + extJson + '\'' +
                '}';
    }
}
