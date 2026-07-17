package com.fraud.detection.util;

import com.alibaba.fastjson.JSONObject;
import com.fraud.detection.model.Alert;

/**
 * 告警JSON序列化工具类
将Alert对象转换为Doris写入用的JSON格式
包含风险等级划分、空值处理等功能
 */
public class AlertJsonUtil {

/**
     * 将告警对象转为Doris写入用的JSON字符串
     * 包含所有字段:告警ID/账户/类型/置信度/来源/路径/时间/可解释性信息/交易上下文
     * @param alert 告警对象
     * @return JSON字符串
     */
    public static String toDorisJson(Alert alert) {
        JSONObject json = new JSONObject(true);
        json.put("alert_id", value(alert.alertId));
        json.put("account_id", value(alert.accountId));
        json.put("fraud_type", value(alert.fraudType));
        json.put("confidence", alert.confidence);
        json.put("source", value(alert.source));
        json.put("behavior_path", value(alert.behaviorPath));
        json.put("alert_time", alert.timeStr != null ? alert.timeStr : formatTimeStr(alert.timestamp));
        json.put("details", value(alert.details));
        json.put("risk_level", riskLevel(alert.confidence));
        json.put("explanation_summary", value(alert.explanationSummary));

        // 交易上下文字段
        json.put("amount", alert.amount);
        json.put("city", value(alert.city));
        json.put("name_orig", value(alert.nameOrig));
        json.put("name_dest", value(alert.nameDest));
        json.put("type", value(alert.type));
        json.put("device_id", value(alert.deviceId));
        json.put("ip_segment", value(alert.ipSegment));
        json.put("transaction_hour", alert.transactionHour);
        json.put("is_abroad", value(alert.isAbroad));
        json.put("device_risk_level", value(alert.deviceRiskLevel));

        if (alert.explanation != null) {
            json.put("top_features", alert.explanation.toJson());
            json.put("graph_path", value(alert.explanation.graphPath));
        } else {
            json.put("top_features", "");
            json.put("graph_path", "");
        }
        json.put("dt", alert.timestamp > 0 ? formatTimeStr(alert.timestamp) : formatTimeStr(System.currentTimeMillis()));
        return json.toJSONString();
    }

/**
     * 时间戳转HH:mm:ss.SSS格式
     */
    private static String formatTimeStr(long ts) {
        if (ts <= 0) return "00:00:00.000";
        java.util.Calendar cal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("Asia/Shanghai"));
        cal.setTimeInMillis(ts);
        int h = cal.get(java.util.Calendar.HOUR_OF_DAY);
        int m = cal.get(java.util.Calendar.MINUTE);
        int s = cal.get(java.util.Calendar.SECOND);
        int ms = cal.get(java.util.Calendar.MILLISECOND);
        return String.format("%02d:%02d:%02d.%03d", h, m, s, ms);
    }

/**
     * 根据置信度划分风险等级
     * >=0.9为HIGH(高危),>=0.75为MEDIUM(中危),其余LOW(低危)
     */
    private static String riskLevel(double confidence) {
        if (confidence >= 0.9) {
            return "HIGH";
        }
        if (confidence >= 0.75) {
            return "MEDIUM";
        }
        return "LOW";
    }

/**
     * 空值处理:null或空字符串返回"UNKNOWN"
     */
    private static String value(String value) {
        return value == null || value.trim().isEmpty() ? "UNKNOWN" : value.trim();
    }
}
