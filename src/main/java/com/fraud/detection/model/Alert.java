package com.fraud.detection.model;

import java.io.Serializable;

/**
 * 告警实体类 - 检测结果的载体
包含欺诈账户、欺诈类型、置信度、告警来源等信息
由CEP/SQL/Graph/ML四层检测产生，经融合去重后写入Doris
 */
public class Alert implements Serializable {

    public String alertId;
    public String accountId;
    public String fraudType;
    public double confidence;
    public String source;
    public String behaviorPath;
    public long timestamp;
    public String timeStr;       // 时分秒毫秒格式: "HH:mm:ss.SSS"
    public String details;
    public AlertExplanation explanation;
    public String explanationSummary;

    // 交易上下文字段(从Transaction继承)
    public double amount;         // 交易金额
    public String city;           // 交易城市
    public String nameOrig;       // 发起方账户
    public String nameDest;       // 接收方账户
    public String type;           // 交易类型
    public String deviceId;       // 设备ID
    public String ipSegment;      // IP段
    public int transactionHour;   // 交易小时数
    public String isAbroad;       // 境内/境外
    public String deviceRiskLevel;// 设备风险等级

/**
     * 默认构造函数
     * 自动生成告警ID: ALERT_时间戳_随机数
     */
    public Alert() {
        this.alertId = "ALERT_" + System.currentTimeMillis() + "_" +
                (int)(Math.random() * 10000);
        this.timestamp = System.currentTimeMillis();
        this.timeStr = formatTimeStr(this.timestamp);
        this.accountId = "UNKNOWN";
        this.fraudType = "UNKNOWN";
        this.source = "UNKNOWN";
        this.behaviorPath = "NONE";
        this.details = "";
        this.explanationSummary = "";
        this.amount = 0.0;
        this.city = "UNKNOWN";
        this.nameOrig = "UNKNOWN";
        this.nameDest = "UNKNOWN";
        this.type = "UNKNOWN";
        this.deviceId = "UNKNOWN";
        this.ipSegment = "UNKNOWN";
        this.transactionHour = 0;
        this.isAbroad = "LOCAL";
        this.deviceRiskLevel = "LOW";
    }

    /**
     * 从Transaction对象复制上下文字段到Alert
     */
    public static Alert fromTransaction(Transaction tx, String fraudType, double confidence, String source) {
        Alert alert = new Alert(tx.nameOrig, fraudType, confidence, source);
        alert.amount = tx.amount;
        alert.city = tx.city;
        alert.nameOrig = tx.nameOrig;
        alert.nameDest = tx.nameDest;
        alert.type = tx.type;
        alert.deviceId = tx.deviceId;
        alert.ipSegment = tx.ipSegment;
        alert.transactionHour = tx.transactionHour;
        alert.isAbroad = tx.isAbroad;
        alert.deviceRiskLevel = tx.deviceRiskLevel;
        alert.timeStr = formatTimeStr(tx.eventTime);
        return alert;
    }

    /**
     * 便捷方法：从Transaction复制上下文（金额、城市、设备等）
     */
    public Alert withTransactionContext(Transaction tx) {
        if (tx != null) {
            this.amount = tx.amount;
            this.city = tx.city != null ? tx.city : "UNKNOWN";
            this.nameOrig = tx.nameOrig;
            this.nameDest = tx.nameDest;
            this.type = tx.type;
            this.deviceId = tx.deviceId != null ? tx.deviceId : "UNKNOWN";
            this.ipSegment = tx.ipSegment != null ? tx.ipSegment : "UNKNOWN";
            this.transactionHour = tx.transactionHour;
            this.isAbroad = tx.isAbroad != null ? tx.isAbroad : "LOCAL";
            this.deviceRiskLevel = tx.deviceRiskLevel != null ? tx.deviceRiskLevel : "LOW";
            if (tx.eventTime > 0) {
                this.timestamp = tx.eventTime;
                this.timeStr = formatTimeStr(tx.eventTime);
            }
        }
        return this;
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

    public Alert(String accountId, String fraudType, double confidence) {
        this();
        this.accountId = normalize(accountId, "UNKNOWN");
        this.fraudType = normalize(fraudType, "UNKNOWN");
        this.confidence = confidence;
        this.source = "UNKNOWN";
    }

/**
     * 构造函数
     * @param accountId 被标记的账户ID
     * @param fraudType 欺诈类型描述
     * @param confidence 置信度(0~1,表示欺诈概率)
     * @param source 告警来源(CEP_RULE/ML/FUSION等)
     */
    public Alert(String accountId, String fraudType, double confidence, String source) {
        this(accountId, fraudType, confidence);
        this.source = normalize(source, "UNKNOWN");
    }

    private static String normalize(String value, String defaultValue) {
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }

    @Override
/**
     * 告警对象的字符串表示
     * 用于日志输出和调试
     */
    public String toString() {
        return String.format(
                "ALERT[%s] Account=%s, Type=%s, Confidence=%.2f, Source=%s, Path=%s, Details=%s",
                alertId, accountId, fraudType, confidence, source, behaviorPath, details
        );
    }
}
