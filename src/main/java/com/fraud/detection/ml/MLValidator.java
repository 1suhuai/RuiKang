package com.fraud.detection.ml;

import com.alibaba.fastjson.JSONObject;
import com.fraud.detection.model.UserBehaviorSequence;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ML模型验证器 - 评估模型预测准确性
 * 对比预测结果与真实标签(isFraud)
 * 计算TP/FP/TN/FN,输出逐条验证结果到Doris
 * 支持训练模式和测试模式
 * 训练模式下训练模型并输出验证指标，验证模式下使用全局模型进行预测评估
 */
public class MLValidator extends KeyedProcessFunction<String, UserBehaviorSequence, String> {

    private final boolean trainingMode;
    private transient ModelTrainer.OnlineLogisticRegressionModel classifier;
    private transient ModelTrainer.StatisticsModel statisticsModel;
    private transient ModelTrainer.PathFrequencyModel pathModel;
    private transient ValueState<Long> totalSamples;
    private transient ValueState<Long> truePositives;
    private transient ValueState<Long> falsePositives;
    private transient ValueState<Long> trueNegatives;
    private transient ValueState<Long> falseNegatives;
    private transient ValueState<Long> fraudSamples;
    private transient ValueState<Long> normalSamples;
    private transient ValueState<Long> sampleSequence;
    private static final java.util.Set<String> SHARED_ENTITIES = new java.util.HashSet<>(java.util.Arrays.asList(
            "C8888888", "C9999999", "UNKNOWN", "NONE"
    ));
    private static final double PREDICTION_THRESHOLD = 0.5;
    private transient long localTrainCount;

    public MLValidator() {
        this.trainingMode = true;
    }
    
    public MLValidator(boolean trainingMode) {
        this.trainingMode = trainingMode;
    }

    @Override
    public void open(Configuration parameters) {
        classifier = new ModelTrainer.OnlineLogisticRegressionModel();
        statisticsModel = new ModelTrainer.StatisticsModel();
        pathModel = new ModelTrainer.PathFrequencyModel();

        totalSamples = getRuntimeContext().getState(new ValueStateDescriptor<>("val_total_samples", Long.class));
        truePositives = getRuntimeContext().getState(new ValueStateDescriptor<>("val_tp", Long.class));
        falsePositives = getRuntimeContext().getState(new ValueStateDescriptor<>("val_fp", Long.class));
        trueNegatives = getRuntimeContext().getState(new ValueStateDescriptor<>("val_tn", Long.class));
        falseNegatives = getRuntimeContext().getState(new ValueStateDescriptor<>("val_fn", Long.class));
        fraudSamples = getRuntimeContext().getState(new ValueStateDescriptor<>("val_fraud_samples", Long.class));
        normalSamples = getRuntimeContext().getState(new ValueStateDescriptor<>("val_normal_samples", Long.class));
        sampleSequence = getRuntimeContext().getState(new ValueStateDescriptor<>("val_sample_sequence", Long.class));
        localTrainCount = 0;
    }

    @Override
    public void processElement(UserBehaviorSequence sequence, Context ctx, Collector<String> out) throws Exception {
        if (sequence == null || sequence.features == null || sequence.sequenceLength < 2) {
            return;
        }
        if (SHARED_ENTITIES.contains(sequence.accountId)) {
            return;
        }

        // 训练模式: 训练模型并更新全局缓存
        if (trainingMode) {
            GlobalMLModelCache cache = GlobalMLModelCache.getInstance();
            cache.updateModel(sequence, sequence.isFraud);
            classifier.train(sequence, sequence.isFraud);
            statisticsModel.train(sequence);
            pathModel.train(sequence.behaviorPath);
            localTrainCount++;
            if (localTrainCount % 10000 == 0) {
                System.out.println("[MLValidator] 训练进度: 已训练 " + localTrainCount + " 个样本 | " + cache.getTrainingStats());
            }
            // 模型就绪后每5000条保存一次
            if (cache.isModelReady() && localTrainCount % 5000 == 0) {
                ModelPersistence.saveModel(cache);
            }
        }
        
        // 使用模型预测
        boolean globalReady = GlobalMLModelCache.getInstance().isModelReady();
        ModelTrainer.Prediction prediction;
        if (!trainingMode && globalReady) {
            GlobalMLModelCache.ModelSnapshot model = GlobalMLModelCache.getInstance().getModel();
            prediction = model != null ? model.classifier.predict(sequence) : classifier.predict(sequence);
        } else if (trainingMode) {
            prediction = classifier.predict(sequence);
        } else {
            classifier.train(sequence, sequence.isFraud);
            prediction = classifier.predict(sequence);
        }
        
        double probability = prediction.finalProbability;
        boolean predicted = probability >= PREDICTION_THRESHOLD;
        boolean actual = sequence.isFraud == 1;
        updateStats(actual, predicted, sequence);
        out.collect(buildMetricsJson(sequence, probability, predicted, actual));
    }

    private void updateStats(boolean actual, boolean predicted, UserBehaviorSequence sequence) throws Exception {
        totalSamples.update(getValue(totalSamples) + 1);
        if (actual) {
            fraudSamples.update(getValue(fraudSamples) + 1);
            truePositives.update(predicted ? getValue(truePositives) + 1 : getValue(truePositives));
            falseNegatives.update(predicted ? getValue(falseNegatives) : getValue(falseNegatives) + 1);
        } else {
            normalSamples.update(getValue(normalSamples) + 1);
            falsePositives.update(predicted ? getValue(falsePositives) + 1 : getValue(falsePositives));
            trueNegatives.update(predicted ? getValue(trueNegatives) : getValue(trueNegatives) + 1);
        }
    }

    private String buildMetricsJson(UserBehaviorSequence sequence, double probability, boolean predicted, boolean actual) throws Exception {
        long tp = getValue(truePositives);
        long fp = getValue(falsePositives);
        long tn = getValue(trueNegatives);
        long fn = getValue(falseNegatives);
        long total = getValue(totalSamples);
        long fraud = getValue(fraudSamples);
        long normal = getValue(normalSamples);
        long seq = getValue(sampleSequence) + 1;
        sampleSequence.update(seq);
        
        double precision = (tp + fp) > 0 ? (double) tp / (tp + fp) : 0.0;
        double recall = (tp + fn) > 0 ? (double) tp / (tp + fn) : 0.0;
        double f1 = (precision + recall) > 0 ? 2 * precision * recall / (precision + recall) : 0.0;
        double accuracy = total > 0 ? (double) (tp + tn) / total : 0.0;
        
        JSONObject json = new JSONObject(true);
        json.put("sample_id", "VAL_SAMPLE_" + seq);
        json.put("account_id", sequence.accountId);
        json.put("actual_label", actual ? "FRAUD" : "NORMAL");
        json.put("predicted_label", predicted ? "FRAUD" : "NORMAL");
        json.put("is_correct", predicted == actual);
        json.put("confidence", round(probability));
        json.put("fraud_type", sequence.fraudType);
        json.put("sequence_length", sequence.sequenceLength);
        json.put("total_samples", total);
        json.put("fraud_samples", fraud);
        json.put("normal_samples", normal);
        json.put("true_positives", tp);
        json.put("false_positives", fp);
        json.put("true_negatives", tn);
        json.put("false_negatives", fn);
        json.put("precision", round(precision));
        json.put("recall", round(recall));
        json.put("f1_score", round(f1));
        json.put("accuracy", round(accuracy));
        json.put("prediction_threshold", PREDICTION_THRESHOLD);
        json.put("dt", formatTimeStr(System.currentTimeMillis()));
        return json.toJSONString();
    }

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

    private long getValue(ValueState<Long> state) throws Exception {
        Long value = state.value();
        return value == null ? 0L : value;
    }

    private double round(double value) {
        return Math.round(value * 10000.0) / 10000.0;
    }

    @Override
    public void close() {
        if (trainingMode && localTrainCount > 0) {
            GlobalMLModelCache cache = GlobalMLModelCache.getInstance();
            if (cache.isModelReady()) {
                ModelPersistence.saveModel(cache);
                System.out.println("[MLValidator] 训练结束，最终模型已保存 | 训练样本数: " + localTrainCount);
            }
        }
    }
}
