package com.fraud.detection.ml;

import com.fraud.detection.model.Transaction;
import com.fraud.detection.model.UserBehaviorSequence;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 预训练入口: 单独运行训练流，训练完成后保存模型到文件
 *
 * 使用方式:
 *   先运行此类消费train_data topic完成预训练 → 生成 models/fraud_model.ser
 *   然后运行CombinedJob消费test_data topic，MLAnomalyDetector会自动加载预训练模型
 *
 * 流程:
 *   1. Kafka train_data → Transaction → BehaviorSequence → MLValidator(trainingMode=true)
 *   2. MLValidator训练GlobalMLModelCache中的模型
 *   3. 训练结束(close)时自动保存模型到 models/fraud_model.ser
 */
public class PreTrainJob {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = args.length > 0 ? args[0] : "192.168.10.10:9092";
        String trainTopic = args.length > 1 ? args[1] : "train_data";
        String groupId = "fraud-pretrain";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> trainKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(trainTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("fetch.max.wait.ms", "30000")
                .setProperty("request.timeout.ms", "60000")
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

        // 构建行为序列
        DataStream<UserBehaviorSequence> trainBehaviorSequences = trainTransactionStream
                .flatMap(new com.fraud.detection.ml.TransactionDuplicator())
                .name("Train Bidirectional Duplicator")
                .keyBy(tx -> tx.trackingAccount)
                .process(new com.fraud.detection.ml.BehaviorSequenceBuilder(10, 30000))
                .name("Train Behavior Sequence Builder");

        // 图特征提取
        DataStream<UserBehaviorSequence> trainEnrichedSequences = trainBehaviorSequences
                .keyBy(seq -> seq.accountId)
                .process(new com.fraud.detection.graph.GraphFeatureExtractor())
                .name("Train Graph Feature Extractor");

        // 训练模型 + 保存
        trainEnrichedSequences
                .keyBy(seq -> "TRAIN_METRICS")
                .process(new MLValidator(true))
                .name("Pre-Train ML Validator")
                .print("PRETRAIN_METRICS")
                .name("Pre-Train Metrics Print");

        env.execute("Fraud Detection - Pre-Train Job");

        // 作业结束后额外保存一次（确保最终模型被保存）
        GlobalMLModelCache cache = GlobalMLModelCache.getInstance();
        if (cache.isModelReady()) {
            ModelPersistence.saveModel(cache);
            System.out.println("[PreTrainJob] 预训练完成！模型已保存到 models/fraud_model.ser");
            System.out.println("[PreTrainJob] " + cache.getTrainingStats());
        } else {
            System.out.println("[PreTrainJob] 预训练未完成，模型未就绪");
        }
    }
}
