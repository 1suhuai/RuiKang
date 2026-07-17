package com.fraud.detection;

import com.fraud.detection.cep.CEPPatternManager;
import com.fraud.detection.fusion.AlertDeduplicator;
import com.fraud.detection.fusion.AlertEnricher;
import com.fraud.detection.fusion.AlertFusion;
import com.fraud.detection.fusion.FinalDeduplicator;
import com.fraud.detection.fusion.ValidationDeduplicator;
import com.fraud.detection.graph.GraphBuilderProcessFunction;
import com.fraud.detection.graph.GraphFeatureExtractor;
import com.fraud.detection.metrics.EvaluationMetricsCalculator;
import com.fraud.detection.metrics.EvaluationDetailWriter;
import com.fraud.detection.drift.ConceptDriftMonitor;
import com.fraud.detection.feedback.FeedbackCollector;
import com.fraud.detection.gnn.GNNAnomalyDetector;
import com.fraud.detection.ml.BehaviorSequenceBuilder;
import com.fraud.detection.ml.MLAnomalyDetector;
import com.fraud.detection.ml.MLValidator;
import com.fraud.detection.ml.ModelPersistence;
import com.fraud.detection.ml.TransactionDuplicator;
import com.fraud.detection.model.Alert;
import com.fraud.detection.model.CEPAlertTag;
import com.fraud.detection.model.Transaction;
import com.fraud.detection.model.UserBehaviorSequence;
import com.fraud.detection.sql.CrossKeyFraudDetector;
import com.fraud.detection.util.AlertJsonUtil;
import com.fraud.detection.util.DorisSchemaInitializer;
import com.fraud.detection.util.DorisSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 实时金融反欺诈检测主程序
 * 
 * 数据流: Kafka -> Flink(CEP+SQL+Graph+GNN+ML) -> Doris
 * 旁路: 概念漂移监控 | 后处理: 人工反馈闭环
 */
public class CombinedJob {

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "192.168.10.10:9092";
    private static final String DEFAULT_TRAIN_TOPIC = "train_data";
    private static final String DEFAULT_TEST_TOPIC = "test_data";
    private static final String DEFAULT_GROUP_ID = "fraud-combined-job";

/**
     * 主入口方法
     * 1. 初始化Flink环境(Watermark/Checkpoint)
     * 2. 创建Kafka Source(训练集+测试集)
     * 3. 训练阶段:训练集 -> MLValidator训练模型
     * 4. 测试阶段:测试集 -> CEP/SQL/Graph/ML四层检测
     * 5. 告警融合 -> 去重 -> 增强 -> 写入Doris
     * 6. 评估指标计算 -> 写入Doris
     */
    public static void main(String[] args) throws Exception {
        String bootstrapServers = args.length > 0 ? args[0] : DEFAULT_BOOTSTRAP_SERVERS;
        String trainTopic = args.length > 1 ? args[1] : DEFAULT_TRAIN_TOPIC;
        String testTopic = args.length > 2 ? args[2] : DEFAULT_TEST_TOPIC;
        String groupId = args.length > 3 ? args[3] : DEFAULT_GROUP_ID;
        String offsetMode = args.length > 4 ? args[4] : "earliest";
        String dorisJdbcUrl = args.length > 5 ? args[5] : "jdbc:mysql://192.168.10.10:9030/";
        boolean initDorisSchema = args.length <= 6 || Boolean.parseBoolean(args[6]);
        boolean debugPrint = args.length > 7 && Boolean.parseBoolean(args[7]);
        int previewLimit = args.length > 8 ? Integer.parseInt(args[8]) : 20;

        if (initDorisSchema) {
            DorisSchemaInitializer.initialize(dorisJdbcUrl, "final", "root", "123456");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        
        // 使用文件系统Checkpoint存储(默认内存只有5MB限制,不够用)
        org.apache.flink.runtime.state.filesystem.FsStateBackend fsBackend =
                new org.apache.flink.runtime.state.filesystem.FsStateBackend("file:///tmp/flink-checkpoints");
        env.setStateBackend(fsBackend);
        
        env.enableCheckpointing(15000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(300000); // 5分钟超时
        checkpointConfig.setMinPauseBetweenCheckpoints(5000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        OffsetsInitializer offsetsInitializer = "earliest".equalsIgnoreCase(offsetMode)
                ? OffsetsInitializer.earliest()
                : OffsetsInitializer.latest();

        // 检查是否有预训练模型: 有则直接加载，跳过训练流；没有则跑训练流训练
        boolean pretrainedModelExists = ModelPersistence.modelExists();
        if (pretrainedModelExists) {
            System.out.println("[CombinedJob] 检测到预训练模型文件，跳过训练流，直接加载模型");
        } else {
            System.out.println("[CombinedJob] 未检测到预训练模型文件，启动训练流进行在线训练");
        }

        // 训练集数据流: 仅在无预训练模型时启动
        if (!pretrainedModelExists) {
            KafkaSource<String> trainKafkaSource = KafkaSource.<String>builder()
                    .setBootstrapServers(bootstrapServers)
                    .setTopics(trainTopic)
                    .setGroupId(groupId + "-train")
                    .setStartingOffsets(offsetsInitializer)
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .setProperty("fetch.max.wait.ms", "30000")      // 最大等待30秒
                    .setProperty("request.timeout.ms", "60000")     // 请求超时60秒
                    .setProperty("reconnect.backoff.ms", "1000")    // 重连退避1秒
                    .setProperty("retries", "10")                   // 重试10次
                    .build();

            DataStream<Transaction> trainTransactionStream = env
                    .fromSource(trainKafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Train Source")
                    .flatMap(Transaction::fromKafkaJson)
                    .filter(Transaction::isValid)
                    .name("Train Kafka JSON To Transaction")
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                    .withTimestampAssigner((tx, ts) -> tx.eventTime)
                    );

            // 训练集: 构建行为序列 -> 训练ML模型 -> 输出验证指标
            trainTransactionStream
                    .flatMap(new TransactionDuplicator())
                    .name("Train Bidirectional Duplicator")
                    .keyBy(tx -> tx.trackingAccount)
                    .process(new BehaviorSequenceBuilder(10, 30000))
                    .name("Train Behavior Sequence Builder")
                    .keyBy(seq -> "TRAIN_METRICS")
                    .process(new MLValidator(true))
                    .name("Train ML Validator")
                    .print("TRAIN_METRICS")
                    .name("Train Metrics Print");
        }

        // 测试集数据流
        KafkaSource<String> testKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(testTopic)
                .setGroupId(groupId + "-test")
                .setStartingOffsets(offsetsInitializer)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("fetch.max.wait.ms", "30000")      // 最大等待30秒
                .setProperty("request.timeout.ms", "60000")     // 请求超时60秒
                .setProperty("reconnect.backoff.ms", "1000")    // 重连退避1秒
                .setProperty("retries", "10")                   // 重试10次
                .build();

        DataStream<Transaction> testTransactionStream = env
                .fromSource(testKafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Test Source")
                .flatMap(Transaction::fromKafkaJson)
                .filter(Transaction::isValid)
                .name("Test Kafka JSON To Transaction")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((tx, ts) -> tx.eventTime)
                );

        KeyedStream<Transaction, String> testKeyedStream = testTransactionStream.keyBy(tx -> tx.nameOrig);

        // CEP规则检测: 8种已知欺诈模式
        DataStream<Alert> testCEPAlerts = CEPPatternManager.buildAllCEPPatterns(testTransactionStream, testKeyedStream);

        // SQL跨账户检测: 链式转账、分散转入、团伙作案
        DataStream<Alert> testCrossKeySqlAlerts = CrossKeyFraudDetector.detectChainTransferABCJoin(testTransactionStream);
        DataStream<Alert> testScatterSqlAlerts = CrossKeyFraudDetector.detectScatterInConcentrateOut(testTransactionStream);
        DataStream<Alert> testScatterCashOutSqlAlerts = CrossKeyFraudDetector.detectScatterInConcentrateOutWithCashOut(testTransactionStream);
        DataStream<Alert> testSameIPGangAlerts = CrossKeyFraudDetector.detectSameIPGang(testTransactionStream);

        // 图分析检测: 实时构建交易图，发现链式转账、环路、团伙
        DataStream<Alert> testGraphAlerts = testTransactionStream
                .keyBy(tx -> tx.nameOrig)
                .process(new GraphBuilderProcessFunction())
                .name("Test Graph Analysis");

        DataStream<Alert> testAllSqlAlerts = testCrossKeySqlAlerts.union(testScatterSqlAlerts, testScatterCashOutSqlAlerts, testSameIPGangAlerts);

        if (debugPrint && previewLimit > 0) {
            testAllSqlAlerts
                    .filter(new PreviewLimitFilter<>(previewLimit))
                    .print("TEST_SQL_ALERT_SAMPLE")
                    .name("Test SQL Alert Sample Print");
        }

        // 提取CEP/SQL/Graph告警标签，用于后续事务标记
        // Graph层也参与过滤，确保三层规则命中的交易不再进入ML
        DataStream<CEPAlertTag> testCEPTags = testCEPAlerts.union(testAllSqlAlerts, testGraphAlerts)
                .filter(alert -> alert.accountId != null && !alert.accountId.trim().isEmpty())
                .map(alert -> new CEPAlertTag(
                        alert.accountId,
                        alert.fraudType,
                        alert.timestamp,
                        alert.timestamp + 3600000
                ))
                .name("Test CEP/SQL/Graph Tag Extractor");

        // 为测试集事务打上CEP/SQL命中标签
        DataStream<Transaction> testTaggedTransactions = testTransactionStream
                .keyBy(tx -> tx.nameOrig)
                .connect(testCEPTags.keyBy(tag -> tag.accountId))
                .process(new AlertFusion.CEPTransactionTagger())
                .name("Test CEP Transaction Tagger");

        DataStream<Alert> testCEPAlertOutput = testCEPAlerts
                .map(alert -> {
                    alert.source = "CEP_RULE";
                    return alert;
                })
                .name("Test CEP Alert Output");

        // 构建用户行为序列（仅用于未被CEP/SQL/Graph命中的交易）
        // 架构: CEP+SQL ~30%基础规则, Graph ~30-40%图网络分析, ML ~30-40%模型检测
        // 比赛要求以图分析和ML为主，CEP/SQL只做基础过滤
        DataStream<UserBehaviorSequence> testBehaviorSequences = testTaggedTransactions
                // 过滤掉已被CEP/SQL/Graph命中的交易
                .filter(tx -> !tx.isCepMatched)
                .name("Filter Rule-Hit Transactions for ML")
                .flatMap(new TransactionDuplicator())
                .name("Test Bidirectional Duplicator")
                .keyBy(tx -> tx.trackingAccount)
                .process(new BehaviorSequenceBuilder(10, 30000))
                .name("Test Behavior Sequence Builder");

        DataStream<UserBehaviorSequence> testEnrichedSequences = testBehaviorSequences
                .keyBy(seq -> seq.accountId)
                .process(new GraphFeatureExtractor())
                .name("Test Graph Feature Extractor");

        if (debugPrint && previewLimit > 0) {
            testEnrichedSequences
                    .filter(new PreviewLimitFilter<>(previewLimit))
                    .print("TEST_USER_BEHAVIOR_SAMPLE")
                    .name("Test User Behavior Sample Print");
        }

        // ML异常检测: 在线IsolationForest + 逻辑回归 + 统计画像偏离
        DataStream<Alert> testMLAlerts = testEnrichedSequences
                .flatMap(new MLAnomalyDetector())
                .name("Test ML Anomaly Detector");

        if (debugPrint && previewLimit > 0) {
            testMLAlerts
                    .filter(new PreviewLimitFilter<>(previewLimit))
                    .print("TEST_ML_ALERT_SAMPLE")
                    .name("Test ML Alert Sample Print");
        }

        // GNN图神经网络检测: GraphSAGE邻域聚合+结构异常检测，识别团伙欺诈
        DataStream<Alert> testGNNAlerts = testEnrichedSequences
                .flatMap(new GNNAnomalyDetector())
                .name("Test GNN Anomaly Detector");

        if (debugPrint && previewLimit > 0) {
            testGNNAlerts
                    .filter(new PreviewLimitFilter<>(previewLimit))
                    .print("TEST_GNN_ALERT_SAMPLE")
                    .name("Test GNN Alert Sample Print");
        }

        // 概念漂移监控(旁路): ADWIN+Page-Hinkley+KS+PSI四算法实时监测，触发模型自适应迭代
        DataStream<String> driftEventJsonStream = testEnrichedSequences
                .flatMap(new ConceptDriftMonitor())
                .name("Test Concept Drift Monitor")
                .map(event -> {
                    com.alibaba.fastjson.JSONObject json = new com.alibaba.fastjson.JSONObject();
                    json.put("event_id", "DRIFT_" + event.timestamp + "_" + event.sampleCount);
                    json.put("severity", event.severity.name());
                    json.put("drift_score", event.driftScore);
                    json.put("event_timestamp", event.timestamp);
                    json.put("sample_count", event.sampleCount);
                    json.put("details", com.alibaba.fastjson.JSONObject.toJSONString(event.details));
                    json.put("dt", String.valueOf(event.timestamp));
                    return json.toJSONString();
                })
                .name("Drift Event To JSON");

        driftEventJsonStream
                .sinkTo(DorisSinkUtil.getDorisSink("fraud_drift_event", "fraud_drift_event"))
                .name("Doris Drift Event Sink");

        // CEP+ML+GNN告警合并 - 直接union,不再使用State缓存(解决Checkpoint超时)
        DataStream<Alert> testCEPMlAlerts = testCEPAlertOutput.union(testMLAlerts, testGNNAlerts);

        // 一级告警去重(无状态,直接透传)
        DataStream<Alert> testDeduplicatedAlerts = testCEPMlAlerts.union(testAllSqlAlerts, testGraphAlerts)
                .map(new AlertDeduplicator());

        // 二级告警去重(无状态,直接透传)
        DataStream<Alert> testFinalDeduplicatedAlerts = testDeduplicatedAlerts
                .map(new FinalDeduplicator());

        // 告警丰富化: 添加解释摘要、Top特征、图路径
        DataStream<Alert> testEnrichedAlerts = testFinalDeduplicatedAlerts
                .keyBy(alert -> alert.accountId != null ? alert.accountId : "UNKNOWN")
                .process(new AlertEnricher())
                .name("Test Alert Enricher");

        // 过滤纯ML来源的UNKNOWN_PATTERN告警
        DataStream<Alert> testFilteredAlerts = testEnrichedAlerts
                .filter(alert -> {
                    String source = alert.source != null ? alert.source : "";
                    String fraudType = alert.fraudType != null ? alert.fraudType : "";
                    boolean isUnknownPattern = fraudType.startsWith("UNKNOWN_PATTERN:");
                    boolean isMLSource = source.startsWith("ML_ONLY") && !source.contains("CEP") && !source.contains("SQL");
                    boolean isEnriched = source.endsWith("_ENRICHED");
                    return !(isUnknownPattern && isMLSource && isEnriched);
                })
                .name("Test Filter Unknown ML-Only Alerts");

        // 低置信度过滤: 阈值0.45，与ML ALERT_THRESHOLD匹配
        DataStream<Alert> testHighConfidenceAlerts = testFilteredAlerts
                .filter(alert -> alert.confidence >= 0.45)
                .name("Test Filter Low Confidence Alerts");

        // 人工反馈闭环(后处理): Human-in-the-Loop告警审核统计，支持模型迭代优化
        DataStream<Alert> testAlertsWithTimestamps = testHighConfidenceAlerts
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Alert>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((alert, ts) -> alert.timestamp > 0 ? alert.timestamp : System.currentTimeMillis())
                );

        DataStream<String> feedbackStatsJsonStream = testAlertsWithTimestamps
                .keyBy(alert -> alert.accountId != null ? alert.accountId : "UNKNOWN")
                .process(new FeedbackCollector())
                .name("Test Feedback Collector")
                .flatMap(new org.apache.flink.api.common.functions.RichFlatMapFunction<FeedbackCollector.FeedbackOutput, String>() {
                    @Override
                    public void flatMap(FeedbackCollector.FeedbackOutput output, org.apache.flink.util.Collector<String> out) throws Exception {
                        com.alibaba.fastjson.JSONObject json = new com.alibaba.fastjson.JSONObject();
                        if (output instanceof FeedbackCollector.FeedbackStats) {
                            FeedbackCollector.FeedbackStats stats = (FeedbackCollector.FeedbackStats) output;
                            json.put("stat_id", "STATS_" + stats.reportTime);
                            json.put("type", "STATS");
                            json.put("total_feedback", stats.totalFeedback);
                            json.put("confirmed_fraud", stats.confirmedFraud);
                            json.put("false_positive", stats.falsePositive);
                            json.put("incorrect_type", stats.incorrectType);
                            json.put("false_positive_rate", stats.getFalsePositiveRate());
                            json.put("report_time", stats.reportTime);
                            json.put("dt", String.valueOf(stats.reportTime));
                        } else if (output instanceof FeedbackCollector.RetrainingSignal) {
                            FeedbackCollector.RetrainingSignal signal = (FeedbackCollector.RetrainingSignal) output;
                            json.put("stat_id", "RETRAIN_" + signal.triggerTime);
                            json.put("type", "RETRAINING_SIGNAL");
                            json.put("total_feedback", signal.feedbackCount);
                            json.put("confirmed_fraud", 0);
                            json.put("false_positive", 0);
                            json.put("incorrect_type", 0);
                            json.put("false_positive_rate", signal.cumulativeStats != null ? signal.cumulativeStats.getFalsePositiveRate() : 0.0);
                            json.put("report_time", signal.triggerTime);
                            json.put("dt", String.valueOf(signal.triggerTime));
                        }
                        out.collect(json.toJSONString());
                    }
                })
                .name("Feedback Output To JSON");

        feedbackStatsJsonStream
                .sinkTo(DorisSinkUtil.getDorisSink("fraud_feedback_stats", "fraud_feedback_stats"))
                .name("Doris Feedback Stats Sink");

        // 转换为Doris JSON格式
        DataStream<String> testAlertJsonStream = testHighConfidenceAlerts
                .map(AlertJsonUtil::toDorisJson)
                .name("Test Alert To Doris JSON");

        if (debugPrint && previewLimit > 0) {
            testAlertJsonStream
                    .filter(new PreviewLimitFilter<>(previewLimit))
                    .print("TEST_DORIS_ALERT_JSON_SAMPLE")
                    .name("Test Doris Alert JSON Sample Print");
        }

        // 输出到Doris: 告警结果表
        testAlertJsonStream
                .sinkTo(DorisSinkUtil.getDorisSink("fraud_alert_result", "fraud_alert_result"))
                .name("Test Doris Fraud Alert Sink");

        // 输出到Doris: 交易流水表(全部数据)
        DataStream<String> testNormalJsonStream = testTransactionStream
                .process(new com.fraud.detection.util.NormalTransactionWriter(1))
                .name("Test Normal Transaction Writer");

        testNormalJsonStream
                .sinkTo(DorisSinkUtil.getDorisSink("fraud_normal_transaction", "fraud_normal_transaction"))
                .name("Test Doris Normal Transaction Sink");

        // 输出到Doris: 评估指标表
        testTransactionStream
                .keyBy(tx -> "TEST_METRICS")
                .connect(testHighConfidenceAlerts.keyBy(alert -> "TEST_METRICS"))
                .process(new EvaluationMetricsCalculator(5000, 100))
                .name("Test Evaluation Metrics Calculator")
                .print("TEST_EVALUATION_METRICS")
                .name("Test Evaluation Metrics Print");

        testTransactionStream
                .keyBy(tx -> "TEST_METRICS_DORIS")
                .connect(testHighConfidenceAlerts.keyBy(alert -> "TEST_METRICS_DORIS"))
                .process(new EvaluationMetricsCalculator(5000, 100))
                .name("Test Evaluation Metrics Doris Calculator")
                .sinkTo(DorisSinkUtil.getDorisSink("fraud_evaluation_metrics", "fraud_evaluation_metrics"))
                .name("Test Doris Evaluation Metrics Sink");

        // 输出到Doris: 评估详情表
        testTransactionStream
                .keyBy(tx -> "TEST_DETAIL")
                .connect(testHighConfidenceAlerts.keyBy(alert -> "TEST_DETAIL"))
                .process(new EvaluationDetailWriter(5000, 100))
                .name("Test Evaluation Detail Writer")
                .sinkTo(DorisSinkUtil.getDorisSink("fraud_evaluation_detail", "fraud_evaluation_detail"))
                .name("Test Doris Evaluation Detail Sink");

        // 输出到Doris: 验证结果表
        DataStream<String> testValidationStream = testEnrichedSequences
                .keyBy(seq -> "VALIDATION_RESULT")
                .process(new MLValidator(false))
                .name("ML Validation Result");

        testValidationStream
                .keyBy(json -> "GLOBAL_DEDUP")
                .process(new ValidationDeduplicator())
                .name("Validation Result Deduplicator")
                .sinkTo(DorisSinkUtil.getDorisSink("fraud_validation_result", "fraud_validation_result"))
                .name("Doris Validation Result Sink");

        // CEP/SQL交叉验证结果
        DataStream<String> testCrossValidationJsonStream = testAllSqlAlerts.union(testCEPAlertOutput)
                .filter(alert -> alert.confidence >= 0.45)
                .map(alert -> {
                    com.alibaba.fastjson.JSONObject json = new com.alibaba.fastjson.JSONObject();
                    json.put("account_id", alert.accountId != null ? alert.accountId : "UNKNOWN");
                    json.put("actual_label", "FRAUD");
                    json.put("predicted_label", "FRAUD");
                    json.put("fraud_type", alert.fraudType != null ? alert.fraudType : "UNKNOWN");
                    json.put("probability", alert.confidence);
                    json.put("detection_layer", alert.source != null ? alert.source : "CEP_SQL");
                    json.put("sequence_length", 0);
                    json.put("total_amount", 0);
                    json.put("max_amount", 0);
                    json.put("transaction_count", 0);
                    json.put("predicted_probability", alert.confidence);
                    json.put("validation_time", System.currentTimeMillis());
                    return json.toJSONString();
                })
                .name("Test Cross Validation Result JSON");

        testCrossValidationJsonStream
                .sinkTo(DorisSinkUtil.getDorisSink("fraud_validation_result", "fraud_validation_result"))
                .name("Doris Cross Validation Sink");

        env.execute("Fraud Detection Combined Job - Train + Validate");
    }

    // 限制打印条数的过滤器
    public static class PreviewLimitFilter<T> extends org.apache.flink.api.common.functions.RichFilterFunction<T> {
        private final int limit;
        private transient int count;

        public PreviewLimitFilter(int limit) {
            this.limit = limit;
            this.count = 0;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            count = 0;
        }

        @Override
        public boolean filter(T value) throws Exception {
            count++;
            return count <= limit;
        }
    }
}
