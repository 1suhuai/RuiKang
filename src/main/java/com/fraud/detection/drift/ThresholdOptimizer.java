package com.fraud.detection.drift;

import com.fraud.detection.ml.ModelTrainer;
import com.fraud.detection.model.UserBehaviorSequence;

import java.io.Serializable;
import java.util.*;

/**
 * 阈值优化器
 * 基于Precision-Recall曲线自动调整检测阈值
 * 平衡误报率(FPR)和召回率(Recall)
 * 每500个样本或60秒自动重新校准
 */
public class ThresholdOptimizer implements Serializable {

    private static final int RECALIBRATE_SAMPLE_INTERVAL = 500;
    private static final long RECALIBRATE_TIME_INTERVAL_MS = 60_000; // 60秒
    private static final double DEFAULT_THRESHOLD = 0.35;
    private static final double MIN_THRESHOLD = 0.15;
    private static final double MAX_THRESHOLD = 0.65;

    // 最优目标: F1最大化，同时控制FPR
    private static final double MAX_ACCEPTABLE_FPR = 0.10; // 最大可接受误报率10%

    private double currentOptimalThreshold = DEFAULT_THRESHOLD;
    private long lastRecalibrateTime = System.currentTimeMillis();
    private int samplesSinceLastRecalibrate = 0;
    private boolean hasEnoughData = false;

    // 预测结果缓冲区，用于阈值优化
    private final List<ScoreLabelPair> scoreBuffer = new ArrayList<>();
    private static final int MAX_BUFFER_SIZE = 5000;

    /**
     * 记录一次预测结果
     */
    public void recordPrediction(double score, int actualLabel) {
        scoreBuffer.add(new ScoreLabelPair(score, actualLabel));
        if (scoreBuffer.size() > MAX_BUFFER_SIZE) {
            scoreBuffer.subList(0, scoreBuffer.size() - MAX_BUFFER_SIZE).clear();
        }
        samplesSinceLastRecalibrate++;
    }

    /**
     * 检查是否需要重新校准阈值
     */
    public boolean shouldRecalibrate() {
        return samplesSinceLastRecalibrate >= RECALIBRATE_SAMPLE_INTERVAL
                || (System.currentTimeMillis() - lastRecalibrateTime) >= RECALIBRATE_TIME_INTERVAL_MS;
    }

    /**
     * 执行阈值重新校准
     * 在缓冲区数据上搜索最优阈值
     * @return 是否发生了阈值变更
     */
    public boolean recalibrate() {
        if (scoreBuffer.size() < 200) {
            // 样本不足，跳过
            return false;
        }

        double newThreshold = findOptimalThreshold(scoreBuffer);
        boolean changed = Math.abs(newThreshold - currentOptimalThreshold) > 0.01;
        currentOptimalThreshold = newThreshold;
        samplesSinceLastRecalibrate = 0;
        lastRecalibrateTime = System.currentTimeMillis();
        hasEnoughData = true;
        return changed;
    }

    /**
     * 搜索最优阈值
     * 使用F1分数 + FPR约束
     */
    private double findOptimalThreshold(List<ScoreLabelPair> pairs) {
        int total = pairs.size();
        if (total < 20) return DEFAULT_THRESHOLD;

        // 统计真实正负样本数
        int totalPositives = 0;
        int totalNegatives = 0;
        for (ScoreLabelPair p : pairs) {
            if (p.actualLabel == 1) totalPositives++;
            else totalNegatives++;
        }

        if (totalPositives == 0 || totalNegatives == 0) {
            // 没有正样本或负样本，返回默认值
            return DEFAULT_THRESHOLD;
        }

        // 遍历候选阈值
        double bestThreshold = DEFAULT_THRESHOLD;
        double bestF1 = 0;
        double step = 0.01;

        for (double t = MIN_THRESHOLD; t <= MAX_THRESHOLD; t += step) {
            int tp = 0, fp = 0, tn = 0, fn = 0;

            for (ScoreLabelPair p : pairs) {
                boolean predicted = p.score >= t;
                boolean actual = p.actualLabel == 1;

                if (predicted && actual) tp++;
                else if (predicted && !actual) fp++;
                else if (!predicted && actual) fn++;
                else tn++;
            }

            double precision = (tp + fp) > 0 ? (double) tp / (tp + fp) : 0;
            double recall = (tp + fn) > 0 ? (double) tp / (tp + fn) : 0;
            double fpr = totalNegatives > 0 ? (double) fp / totalNegatives : 0;

            // F1分数
            double f1 = (precision + recall) > 0
                    ? 2 * precision * recall / (precision + recall)
                    : 0;

            // FPR约束：如果误报率过高，惩罚F1
            double adjustedF1 = fpr > MAX_ACCEPTABLE_FPR
                    ? f1 * Math.max(0, 1 - (fpr - MAX_ACCEPTABLE_FPR) * 5)
                    : f1;

            if (adjustedF1 > bestF1) {
                bestF1 = adjustedF1;
                bestThreshold = t;
            }
        }

        return bestThreshold;
    }

    /**
     * 为给定模型和数据集优化阈值（用于重训后校准）
     */
    public double optimizeThreshold(ModelTrainer.OnlineLogisticRegressionModel model,
                                    List<UserBehaviorSequence> data) {
        if (data == null || data.size() < 50) return DEFAULT_THRESHOLD;

        List<ScoreLabelPair> pairs = new ArrayList<>();
        for (UserBehaviorSequence seq : data) {
            if (seq == null || seq.features == null || seq.features.length == 0) continue;
            ModelTrainer.Prediction pred = model.predict(seq);
            pairs.add(new ScoreLabelPair(pred.finalProbability, seq.isFraud));
        }

        if (pairs.size() < 50) return DEFAULT_THRESHOLD;
        return findOptimalThreshold(pairs);
    }

    /**
     * 保守模式阈值优化（紧急重训时使用）
     * 使用更严格的FPR约束，降低误报
     */
    public double optimizeThresholdConservative(ModelTrainer.OnlineLogisticRegressionModel model,
                                                 List<UserBehaviorSequence> data) {
        if (data == null || data.size() < 50) return DEFAULT_THRESHOLD;

        List<ScoreLabelPair> pairs = new ArrayList<>();
        for (UserBehaviorSequence seq : data) {
            if (seq == null || seq.features == null || seq.features.length == 0) continue;
            ModelTrainer.Prediction pred = model.predict(seq);
            pairs.add(new ScoreLabelPair(pred.finalProbability, seq.isFraud));
        }

        if (pairs.size() < 50) return DEFAULT_THRESHOLD;

        // 使用更严格的FPR上限(5%)
        double strictFPR = 0.05;
        double bestThreshold = DEFAULT_THRESHOLD;
        double bestScore = 0;
        int totalNegatives = 0;
        for (ScoreLabelPair p : pairs) {
            if (p.actualLabel == 0) totalNegatives++;
        }

        for (double t = 0.25; t <= 0.55; t += 0.01) {
            int tp = 0, fp = 0, fn = 0;
            for (ScoreLabelPair p : pairs) {
                if (p.score >= t && p.actualLabel == 1) tp++;
                else if (p.score >= t && p.actualLabel == 0) fp++;
                else if (p.score < t && p.actualLabel == 1) fn++;
            }

            double precision = (tp + fp) > 0 ? (double) tp / (tp + fp) : 0;
            double recall = (tp + fn) > 0 ? (double) tp / (tp + fn) : 0;
            double fpr = totalNegatives > 0 ? (double) fp / totalNegatives : 0;
            double f1 = (precision + recall) > 0 ? 2 * precision * recall / (precision + recall) : 0;

            // 严格约束：FPR > 5% 直接淘汰
            double score = fpr <= strictFPR ? f1 : 0;
            if (score > bestScore) {
                bestScore = score;
                bestThreshold = t;
            }
        }

        return bestThreshold;
    }

    /**
     * 计算当前缓冲区上的性能指标快照
     */
    public PerformanceMetrics getPerformanceSnapshot() {
        if (scoreBuffer.isEmpty()) {
            return new PerformanceMetrics(0, 0, 0, 0, 0, 0, currentOptimalThreshold, 0, 0, 0);
        }

        int tp = 0, fp = 0, tn = 0, fn = 0;
        for (ScoreLabelPair p : scoreBuffer) {
            boolean predicted = p.score >= currentOptimalThreshold;
            boolean actual = p.actualLabel == 1;

            if (predicted && actual) tp++;
            else if (predicted && !actual) fp++;
            else if (!predicted && actual) fn++;
            else tn++;
        }

        double precision = (tp + fp) > 0 ? (double) tp / (tp + fp) : 0;
        double recall = (tp + fn) > 0 ? (double) tp / (tp + fn) : 0;
        double fpr = (fp + tn) > 0 ? (double) fp / (fp + tn) : 0;
        double f1 = (precision + recall) > 0 ? 2 * precision * recall / (precision + recall) : 0;
        double accuracy = (tp + tn + fp + fn) > 0
                ? (double) (tp + tn) / (tp + tn + fp + fn) : 0;

        return new PerformanceMetrics(tp, fp, tn, fn, precision, recall,
                currentOptimalThreshold, fpr, f1, accuracy);
    }

    public double getCurrentOptimalThreshold() { return currentOptimalThreshold; }
    public boolean hasEnoughData() { return hasEnoughData; }

    /**
     * 分数-标签对，用于阈值优化
     */
    private static class ScoreLabelPair implements Serializable {
        final double score;
        final int actualLabel;

        ScoreLabelPair(double score, int actualLabel) {
            this.score = score;
            this.actualLabel = actualLabel;
        }
    }

    /**
     * 性能指标快照
     */
    public static class PerformanceMetrics implements Serializable {
        public final int tp, fp, tn, fn;
        public final double precision, recall, fpr, f1, accuracy;
        public final double threshold;

        public PerformanceMetrics(int tp, int fp, int tn, int fn,
                                  double precision, double recall, double threshold,
                                  double fpr, double f1, double accuracy) {
            this.tp = tp;
            this.fp = fp;
            this.tn = tn;
            this.fn = fn;
            this.precision = precision;
            this.recall = recall;
            this.threshold = threshold;
            this.fpr = fpr;
            this.f1 = f1;
            this.accuracy = accuracy;
        }

        @Override
        public String toString() {
            return String.format("Metrics{threshold=%.3f, P=%.4f, R=%.4f, F1=%.4f, FPR=%.4f, Acc=%.4f, TP=%d, FP=%d, TN=%d, FN=%d}",
                    threshold, precision, recall, f1, fpr, accuracy, tp, fp, tn, fn);
        }
    }
}
