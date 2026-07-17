package com.fraud.detection.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Doris表结构初始化工具类
 自动创建数据库和4张结果表:告警结果/评估指标/评估详情/验证结果
 在CombinedJob启动时调用,确保表结构存在
 */
public class DorisSchemaInitializer {

    private static final String DEFAULT_JDBC_URL = "jdbc:mysql://192.168.10.10:9030/";
    private static final String DEFAULT_DATABASE = "final";
    private static final String DEFAULT_USERNAME = "root";
    private static final String DEFAULT_PASSWORD = "123456";

    /**
     * 入口方法:创建数据库和4张结果表
     * 使用JDBC连接Doris,执行DDL语句
     * @param jdbcUrl Doris JDBC连接地址
     * @param username 用户名
     * @param password 密码
     * @param databaseName 数据库名
     */
    public static void initialize() {
        initialize(DEFAULT_JDBC_URL, DEFAULT_DATABASE, DEFAULT_USERNAME, DEFAULT_PASSWORD);
    }

    /**
     * 入口方法:创建数据库和4张结果表
     * 使用JDBC连接Doris,执行DDL语句
     * @param jdbcUrl Doris JDBC连接地址
     * @param username 用户名
     * @param password 密码
     * @param databaseName 数据库名
     */
    public static void initialize(String jdbcUrl, String database, String username, String password) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                 Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + database);

                statement.executeUpdate(buildAlertResultTableSql(database));
                statement.executeUpdate(buildEvaluationMetricsTableSql(database));
                statement.executeUpdate(buildEvaluationDetailTableSql(database));
                statement.executeUpdate(buildValidationResultTableSql(database));
                statement.executeUpdate(buildNormalTransactionTableSql(database));
                statement.executeUpdate(buildFeedbackConfirmedTableSql(database));
                statement.executeUpdate(buildFeedbackFalsePositiveTableSql(database));
                statement.executeUpdate(buildDriftEventTableSql(database));
                statement.executeUpdate(buildFeedbackStatsTableSql(database));
            }
            System.out.println("Doris 数据库和结果表初始化完成: " + database);
        } catch (Exception e) {
            System.err.println("Doris 数据库和表初始化失败: " + e.getMessage());
        }
    }

    /**
     * 构建告警结果表DDL
     * 24个字段,UNIQUE KEY(alert_id)
     * 存储最终告警、可解释性信息及交易上下文
     * 时间字段统一使用HH:mm:ss.SSS字符串格式
     */
    private static String buildAlertResultTableSql(String database) {
        return "CREATE TABLE IF NOT EXISTS " + database + ".fraud_alert_result ("
                + "alert_id VARCHAR(128),"
                + "account_id VARCHAR(128),"
                + "fraud_type VARCHAR(255),"
                + "confidence DOUBLE,"
                + "source VARCHAR(128),"
                + "behavior_path STRING,"
                + "alert_time VARCHAR(32),"
                + "details STRING,"
                + "risk_level VARCHAR(32),"
                + "explanation_summary STRING,"
                + "top_features STRING,"
                + "graph_path STRING,"
                // 交易上下文字段
                + "amount DOUBLE,"
                + "city VARCHAR(64),"
                + "name_orig VARCHAR(128),"
                + "name_dest VARCHAR(128),"
                + "type VARCHAR(64),"
                + "device_id VARCHAR(128),"
                + "ip_segment VARCHAR(64),"
                + "transaction_hour INT,"
                + "is_abroad VARCHAR(32),"
                + "device_risk_level VARCHAR(32),"
                + "dt VARCHAR(32)"
                + ") UNIQUE KEY(alert_id) "
                + "DISTRIBUTED BY HASH(alert_id) BUCKETS 10 "
                + "PROPERTIES (\"replication_num\" = \"1\")";
    }

    /**
     * 构建聚合评估指标表DDL - 简化版
     * 只保留基础计数字段,P/R/F1直接从fraud_validation_result表计算
     * 时间字段使用HH:mm:ss.SSS格式
     */
    private static String buildEvaluationMetricsTableSql(String database) {
        return "CREATE TABLE IF NOT EXISTS " + database + ".fraud_evaluation_metrics ("
                + "metric_time VARCHAR(32),"
                + "total_transactions BIGINT,"
                + "total_alerts BIGINT,"
                + "cep_alerts BIGINT,"
                + "ml_alerts BIGINT,"
                + "fusion_alerts BIGINT,"
                + "alert_rate DOUBLE"
                + ") UNIQUE KEY(metric_time) "
                + "DISTRIBUTED BY HASH(metric_time) BUCKETS 10 "
                + "PROPERTIES (\"replication_num\" = \"1\")";
    }

    /**
     * 构建按欺诈类型评估详情表DDL - 简化版
     * 时间字段使用HH:mm:ss.SSS格式
     */
    private static String buildEvaluationDetailTableSql(String database) {
        return "CREATE TABLE IF NOT EXISTS " + database + ".fraud_evaluation_detail ("
                + "metric_time VARCHAR(32),"
                + "fraud_type VARCHAR(128),"
                + "alert_count BIGINT,"
                + "real_count BIGINT,"
                + "type_precision DOUBLE,"
                + "type_recall DOUBLE,"
                + "type_f1 DOUBLE,"
                + "avg_confidence DOUBLE"
                + ") UNIQUE KEY(metric_time, fraud_type) "
                + "DISTRIBUTED BY HASH(metric_time) BUCKETS 10 "
                + "PROPERTIES (\"replication_num\" = \"1\")";
    }

    /**
     * 构建ML验证结果表DDL
     * 20个字段,含逐条预测结果和累积指标
     * 时间字段使用HH:mm:ss.SSS格式
     */
    private static String buildValidationResultTableSql(String database) {
        return "CREATE TABLE IF NOT EXISTS " + database + ".fraud_validation_result ("
                + "sample_id VARCHAR(128),"
                + "account_id VARCHAR(128),"
                + "actual_label VARCHAR(32),"
                + "predicted_label VARCHAR(32),"
                + "is_correct BOOLEAN,"
                + "confidence DOUBLE,"
                + "fraud_type VARCHAR(255),"
                + "sequence_length INT,"
                + "total_samples BIGINT,"
                + "fraud_samples BIGINT,"
                + "normal_samples BIGINT,"
                + "true_positives BIGINT,"
                + "false_positives BIGINT,"
                + "true_negatives BIGINT,"
                + "false_negatives BIGINT,"
                + "precision DOUBLE,"
                + "recall DOUBLE,"
                + "f1_score DOUBLE,"
                + "accuracy DOUBLE,"
                + "prediction_threshold DOUBLE,"
                + "dt VARCHAR(32)"
                + ") UNIQUE KEY(sample_id) "
                + "DISTRIBUTED BY HASH(sample_id) BUCKETS 10 "
                + "PROPERTIES (\"replication_num\" = \"1\")";
    }

    /**
     * 构建正常交易流水表DDL
     * 存储所有正常交易记录，用于流量趋势分析和基线对比
     * 时间字段使用HH:mm:ss.SSS格式
     */
    private static String buildNormalTransactionTableSql(String database) {
        return "CREATE TABLE IF NOT EXISTS " + database + ".fraud_normal_transaction ("
                + "tx_id VARCHAR(128),"
                + "account_id VARCHAR(128),"
                + "name_orig VARCHAR(128),"
                + "name_dest VARCHAR(128),"
                + "type VARCHAR(64),"
                + "amount DOUBLE,"
                + "oldbalance_org DOUBLE,"
                + "newbalance_orig DOUBLE,"
                + "oldbalance_dest DOUBLE,"
                + "newbalance_dest DOUBLE,"
                + "city VARCHAR(64),"
                + "device_id VARCHAR(128),"
                + "device_type VARCHAR(32),"
                + "pay_channel VARCHAR(64),"
                + "ip_segment VARCHAR(64),"
                + "transaction_hour INT,"
                + "daily_tx_count INT,"
                + "device_risk_level VARCHAR(32),"
                + "is_abroad VARCHAR(32),"
                + "tx_time VARCHAR(32),"
                + "dt VARCHAR(32)"
                + ") UNIQUE KEY(tx_id) "
                + "DISTRIBUTED BY HASH(tx_id) BUCKETS 10 "
                + "PROPERTIES (\"replication_num\" = \"1\")";
    }

    /**
     * 构建人工确认欺诈表DDL
     * 存储用户确认的欺诈样本，用于模型重训练正样本
     */
    private static String buildFeedbackConfirmedTableSql(String database) {
        return "CREATE TABLE IF NOT EXISTS " + database + ".fraud_feedback_confirmed ("
                + "feedback_id VARCHAR(128),"
                + "alert_id VARCHAR(128),"
                + "account_id VARCHAR(128),"
                + "fraud_type VARCHAR(255),"
                + "confidence DOUBLE,"
                + "source VARCHAR(128),"
                + "amount DOUBLE,"
                + "city VARCHAR(64),"
                + "device_id VARCHAR(128),"
                + "alert_time VARCHAR(32),"
                + "feedback_time VARCHAR(32),"
                + "operator VARCHAR(64),"
                + "comment STRING"
                + ") UNIQUE KEY(feedback_id) "
                + "DISTRIBUTED BY HASH(feedback_id) BUCKETS 10 "
                + "PROPERTIES (\"replication_num\" = \"1\")";
    }

    /**
     * 构建误报标记表DDL
     * 存储用户标记的误报样本，用于模型优化负样本
     */
    private static String buildFeedbackFalsePositiveTableSql(String database) {
        return "CREATE TABLE IF NOT EXISTS " + database + ".fraud_feedback_false_positive ("
                + "feedback_id VARCHAR(128),"
                + "alert_id VARCHAR(128),"
                + "account_id VARCHAR(128),"
                + "fraud_type VARCHAR(255),"
                + "confidence DOUBLE,"
                + "source VARCHAR(128),"
                + "amount DOUBLE,"
                + "city VARCHAR(64),"
                + "device_id VARCHAR(128),"
                + "alert_time VARCHAR(32),"
                + "feedback_time VARCHAR(32),"
                + "operator VARCHAR(64),"
                + "comment STRING"
                + ") UNIQUE KEY(feedback_id) "
                + "DISTRIBUTED BY HASH(feedback_id) BUCKETS 10 "
                + "PROPERTIES (\"replication_num\" = \"1\")";
    }

    /**
     * 构建概念漂移事件表DDL
     * 存储ADWIN+Page-Hinkley+KS+PSI四算法检测到的数据分布变化事件
     */
    private static String buildDriftEventTableSql(String database) {
        return "CREATE TABLE IF NOT EXISTS " + database + ".fraud_drift_event ("
                + "event_id VARCHAR(128),"
                + "severity VARCHAR(32),"
                + "drift_score DOUBLE,"
                + "event_timestamp BIGINT,"
                + "sample_count BIGINT,"
                + "details STRING,"
                + "dt VARCHAR(32)"
                + ") UNIQUE KEY(event_id) "
                + "DISTRIBUTED BY HASH(event_id) BUCKETS 5 "
                + "PROPERTIES (\"replication_num\" = \"1\")";
    }

    /**
     * 构建反馈统计表DDL
     * 存储Human-in-the-Loop反馈统计和重训练信号
     */
    private static String buildFeedbackStatsTableSql(String database) {
        return "CREATE TABLE IF NOT EXISTS " + database + ".fraud_feedback_stats ("
                + "stat_id VARCHAR(128),"
                + "type VARCHAR(32),"
                + "total_feedback INT,"
                + "confirmed_fraud INT,"
                + "false_positive INT,"
                + "incorrect_type INT,"
                + "false_positive_rate DOUBLE,"
                + "report_time BIGINT,"
                + "dt VARCHAR(32)"
                + ") UNIQUE KEY(stat_id) "
                + "DISTRIBUTED BY HASH(stat_id) BUCKETS 5 "
                + "PROPERTIES (\"replication_num\" = \"1\")";
    }

}
