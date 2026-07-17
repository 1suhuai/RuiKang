package com.fraud.detection.drift;

import com.fraud.detection.drift.ConceptDriftMonitor.DriftEvent;
import com.fraud.detection.drift.ConceptDriftMonitor.DriftSeverity;
import com.fraud.detection.ml.ModelTrainer;
import com.fraud.detection.model.UserBehaviorSequence;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

/**
 * 自动修复引擎
 * 响应漂移事件，执行自动化修复动作
 * 
 * 修复策略矩阵：
 * - MINOR:   上调检测阈值5%
 * - MODERATE: 使用近期窗口(1000样本)重训模型
 * - SEVERE:  全量模型重训 + 阈值重新校准
 * - CRITICAL: 紧急回退至纯规则检测(CEP+SQL)，触发全量重训
 * 
 * 维护模型版本历史，在新旧模型间A/B测试后才切换
 */
public class AutoRepairEngine extends RichFlatMapFunction<DriftEvent, AutoRepairEngine.RepairAction> {

    private transient ModelVersionHistory versionHistory;
    private transient ABTestRunner abTest;
    private transient DataWindowManager dataWindowManager;
    private transient ThresholdOptimizer thresholdOptimizer;
    private transient ModelTrainer.OnlineLogisticRegressionModel currentModel;
    private transient double currentThreshold;
    private transient boolean emergencyRuleOnly;  // 紧急规则模式

    // 默认阈值
    private static final double DEFAULT_THRESHOLD = 0.35;

    @Override
    public void open(Configuration parameters) {
        versionHistory = new ModelVersionHistory();
        abTest = new ABTestRunner();
        dataWindowManager = new DataWindowManager();
        thresholdOptimizer = new ThresholdOptimizer();
        currentModel = new ModelTrainer.OnlineLogisticRegressionModel();
        currentThreshold = DEFAULT_THRESHOLD;
        emergencyRuleOnly = false;
    }

    @Override
    public void flatMap(DriftEvent event, Collector<RepairAction> out) {
        if (event == null) return;

        RepairAction action;
        switch (event.severity) {
            case MINOR:
                action = handleMinorDrift(event);
                break;
            case MODERATE:
                action = handleModerateDrift(event);
                break;
            case SEVERE:
                action = handleSevereDrift(event);
                break;
            case CRITICAL:
                action = handleCriticalDrift(event);
                break;
            default:
                return;
        }

        if (action != null) {
            out.collect(action);
        }
    }

    /**
     * MINOR漂移：上调检测阈值5%
     * 轻度分布变化，暂不重训，仅调整阈值提高容错
     */
    private RepairAction handleMinorDrift(DriftEvent event) {
        double oldThreshold = currentThreshold;
        currentThreshold = Math.min(0.70, currentThreshold * 1.05); // 上限0.70

        RepairAction action = new RepairAction(
                RepairType.THRESHOLD_ADJUST,
                DriftSeverity.MINOR,
                event.sampleCount,
                System.currentTimeMillis(),
                String.format("阈值上调5%%: %.4f -> %.4f", oldThreshold, currentThreshold)
        );
        action.details = String.format(
                "oldThreshold=%.4f, newThreshold=%.4f, driftScore=%.4f",
                oldThreshold, currentThreshold, event.driftScore
        );
        return action;
    }

    /**
     * MODERATE漂移：使用近期1000个样本重训模型
     * 中度分布偏移，用最新数据微调模型
     */
    private RepairAction handleModerateDrift(DriftEvent event) {
        List<UserBehaviorSequence> recentData = dataWindowManager.getRecentWindow();

        if (recentData.size() < 100) {
            return new RepairAction(
                    RepairType.SKIP_RETRAIN,
                    DriftSeverity.MODERATE,
                    event.sampleCount,
                    System.currentTimeMillis(),
                    "样本不足100，跳过重训"
            );
        }

        // 保存旧模型版本
        String oldVersionId = versionHistory.saveVersion(currentModel, currentThreshold, "pre-moderate-retrain");

        // 创建新模型并在近期数据上训练
        ModelTrainer.OnlineLogisticRegressionModel newModel = new ModelTrainer.OnlineLogisticRegressionModel();
        int trainCount = 0;
        for (UserBehaviorSequence seq : recentData) {
            if (seq != null && seq.features != null && seq.features.length > 0) {
                // 使用isFraud标签（如有）
                int label = (seq.isFraud == 1) ? 1 : 0;
                newModel.train(seq, label);
                trainCount++;
            }
        }

        // A/B测试：比较新旧模型
        ABTestRunner.ABTestResult abResult = abTest.runABTest(currentModel, newModel, recentData);

        // 如果新模型更好，切换
        if (abResult.newModelWins) {
            versionHistory.markActive(versionHistory.saveVersion(newModel, currentThreshold, "moderate-retrain"));
            currentModel = newModel;

            return new RepairAction(
                    RepairType.MODEL_RETRAIN,
                    DriftSeverity.MODERATE,
                    event.sampleCount,
                    System.currentTimeMillis(),
                    String.format("近期重训完成: 训练%d样本, A/B胜率%.2f%%", trainCount, abResult.winRate * 100)
            );
        } else {
            // 新模型不如旧模型，保留旧版本
            return new RepairAction(
                    RepairType.RETRAIN_REJECTED,
                    DriftSeverity.MODERATE,
                    event.sampleCount,
                    System.currentTimeMillis(),
                    String.format("重训被A/B拒绝: 新模型胜率仅%.2f%%", abResult.winRate * 100)
            );
        }
    }

    /**
     * SEVERE漂移：全量模型重训 + 阈值重新校准
     * 严重分布变化，需要大规模重新训练
     */
    private RepairAction handleSevereDrift(DriftEvent event) {
        // 获取历史窗口数据
        List<UserBehaviorSequence> historicalData = dataWindowManager.getHistoricalWindow();

        if (historicalData.size() < 500) {
            return new RepairAction(
                    RepairType.SKIP_RETRAIN,
                    DriftSeverity.SEVERE,
                    event.sampleCount,
                    System.currentTimeMillis(),
                    "历史样本不足500，降级为近期重训"
            );
        }

        String oldVersionId = versionHistory.saveVersion(currentModel, currentThreshold, "pre-severe-retrain");

        // 全量训练
        ModelTrainer.OnlineLogisticRegressionModel newModel = new ModelTrainer.OnlineLogisticRegressionModel();
        int trainCount = 0;
        int fraudCount = 0;
        for (UserBehaviorSequence seq : historicalData) {
            if (seq != null && seq.features != null && seq.features.length > 0) {
                int label = (seq.isFraud == 1) ? 1 : 0;
                newModel.train(seq, label);
                trainCount++;
                fraudCount += label;
            }
        }

        // 阈值优化
        double newThreshold = thresholdOptimizer.optimizeThreshold(newModel, historicalData);

        // A/B测试
        ABTestRunner.ABTestResult abResult = abTest.runABTest(currentModel, newModel, historicalData);

        if (abResult.newModelWins) {
            versionHistory.markActive(versionHistory.saveVersion(newModel, newThreshold, "severe-retrain"));
            double oldThreshold = currentThreshold;
            currentModel = newModel;
            currentThreshold = newThreshold;

            // 退出紧急模式
            if (emergencyRuleOnly) {
                emergencyRuleOnly = false;
            }

            return new RepairAction(
                    RepairType.FULL_RETRAIN,
                    DriftSeverity.SEVERE,
                    event.sampleCount,
                    System.currentTimeMillis(),
                    String.format("全量重训完成: %d样本(%d欺诈), 阈值%.4f->%.4f, A/B胜率%.2f%%",
                            trainCount, fraudCount, oldThreshold, newThreshold, abResult.winRate * 100)
            );
        } else {
            return new RepairAction(
                    RepairType.RETRAIN_REJECTED,
                    DriftSeverity.SEVERE,
                    event.sampleCount,
                    System.currentTimeMillis(),
                    String.format("全量重训被A/B拒绝: 胜率%.2f%%, 保留旧模型", abResult.winRate * 100)
            );
        }
    }

    /**
     * CRITICAL漂移：紧急回退至规则引擎 + 全量重训
     * 紧急分布崩溃，ML模型不可信，仅依赖CEP+SQL规则
     */
    private RepairAction handleCriticalDrift(DriftEvent event) {
        // 进入紧急模式
        emergencyRuleOnly = true;

        // 保存当前模型为紧急前版本
        String emergencyVersionId = versionHistory.saveVersion(currentModel, currentThreshold, "emergency-fallback");
        versionHistory.markActive(emergencyVersionId);

        // 获取基线窗口数据进行彻底重训
        List<UserBehaviorSequence> baselineData = dataWindowManager.getBaselineWindow();
        List<UserBehaviorSequence> recentData = dataWindowManager.getRecentWindow();

        // 合并近期+基线数据
        List<UserBehaviorSequence> combinedData = new ArrayList<>(baselineData);
        combinedData.addAll(recentData);

        if (combinedData.size() < 200) {
            return new RepairAction(
                    RepairType.EMERGENCY_FALLBACK,
                    DriftSeverity.CRITICAL,
                    event.sampleCount,
                    System.currentTimeMillis(),
                    "紧急回退规则引擎: 数据不足无法立即重训"
            );
        }

        // 彻底重新训练
        ModelTrainer.OnlineLogisticRegressionModel newModel = new ModelTrainer.OnlineLogisticRegressionModel();
        int trainCount = 0;
        int fraudCount = 0;
        for (UserBehaviorSequence seq : combinedData) {
            if (seq != null && seq.features != null && seq.features.length > 0) {
                int label = (seq.isFraud == 1) ? 1 : 0;
                newModel.train(seq, label);
                trainCount++;
                fraudCount += label;
            }
        }

        // 激进阈值校准
        double newThreshold = thresholdOptimizer.optimizeThresholdConservative(newModel, combinedData);

        return new RepairAction(
                RepairType.EMERGENCY_FALLBACK,
                DriftSeverity.CRITICAL,
                event.sampleCount,
                System.currentTimeMillis(),
                String.format("紧急回退+全量重训: %d样本(%d欺诈), 新阈值%.4f, 规则引擎已启用",
                        trainCount, fraudCount, newThreshold)
        );
    }

    // ========== Getter ==========

    public double getCurrentThreshold() { return currentThreshold; }
    public boolean isEmergencyRuleOnly() { return emergencyRuleOnly; }
    public ModelVersionHistory getVersionHistory() { return versionHistory; }

    // ========== 内部类 ==========

    /**
     * 修复动作类型
     */
    public enum RepairType implements Serializable {
        THRESHOLD_ADJUST,    // 阈值调整
        MODEL_RETRAIN,       // 模型重训
        FULL_RETRAIN,        // 全量重训
        EMERGENCY_FALLBACK,  // 紧急回退
        RETRAIN_REJECTED,    // 重训被拒绝
        SKIP_RETRAIN         // 跳过重训
    }

    /**
     * 修复动作输出
     */
    public static class RepairAction implements Serializable {
        public final RepairType type;
        public final DriftSeverity triggeredBy;
        public final long sampleCount;
        public final long timestamp;
        public final String summary;
        public String details;

        public RepairAction(RepairType type, DriftSeverity triggeredBy,
                            long sampleCount, long timestamp, String summary) {
            this.type = type;
            this.triggeredBy = triggeredBy;
            this.sampleCount = sampleCount;
            this.timestamp = timestamp;
            this.summary = summary;
        }

        @Override
        public String toString() {
            return String.format("RepairAction{type=%s, severity=%s, summary='%s'}",
                    type, triggeredBy, summary);
        }
    }

    /**
     * 模型版本历史
     * 记录每次模型变更的元数据
     */
    public static class ModelVersionHistory implements Serializable {
        private final List<ModelVersion> versions = new ArrayList<>();
        private String activeVersionId;

        /**
         * 保存模型版本
         */
        public String saveVersion(ModelTrainer.OnlineLogisticRegressionModel model,
                                  double threshold, String reason) {
            String versionId = "v" + System.currentTimeMillis() + "_" + versions.size();
            ModelVersion version = new ModelVersion(
                    versionId,
                    System.currentTimeMillis(),
                    threshold,
                    reason,
                    false
            );
            versions.add(version);

            // 限制版本数量，保留最近20个
            if (versions.size() > 20) {
                versions.subList(0, versions.size() - 20).clear();
            }

            return versionId;
        }

        /**
         * 标记活跃版本
         */
        public void markActive(String versionId) {
            this.activeVersionId = versionId;
        }

        public String getActiveVersionId() { return activeVersionId; }
        public List<ModelVersion> getVersions() { return Collections.unmodifiableList(versions); }
    }

    /**
     * 模型版本记录
     */
    public static class ModelVersion implements Serializable {
        public final String versionId;
        public final long createdAt;
        public final double threshold;
        public final String reason;
        public final boolean isEmergency;

        public ModelVersion(String versionId, long createdAt, double threshold,
                           String reason, boolean isEmergency) {
            this.versionId = versionId;
            this.createdAt = createdAt;
            this.threshold = threshold;
            this.reason = reason;
            this.isEmergency = isEmergency;
        }

        @Override
        public String toString() {
            return String.format("ModelVersion{id=%s, time=%d, threshold=%.4f, reason='%s'}",
                    versionId, createdAt, threshold, reason);
        }
    }

    /**
     * A/B测试运行器
     * 对比新旧模型性能，决定是否可以切换
     */
    public static class ABTestRunner implements Serializable {
        private static final int MIN_SAMPLE_SIZE = 50;     // 最小测试样本数
        private static final double WIN_THRESHOLD = 0.52;  // 新模型胜率超过52%才切换
        private static final double MIN_IMPROVEMENT = 0.01; // 至少1%的提升

        /**
         * 运行A/B测试
         * 在测试数据上比较新旧模型，计算各自的准确率
         */
        public ABTestResult runABTest(ModelTrainer.OnlineLogisticRegressionModel oldModel,
                                      ModelTrainer.OnlineLogisticRegressionModel newModel,
                                      List<UserBehaviorSequence> testData) {
            if (testData == null || testData.size() < MIN_SAMPLE_SIZE) {
                // 样本不足，默认切换
                return new ABTestResult(true, 0.55, 0, 0, 0, 0);
            }

            int oldCorrect = 0;
            int newCorrect = 0;
            int total = 0;

            for (UserBehaviorSequence seq : testData) {
                if (seq == null || seq.features == null || seq.features.length == 0) continue;

                int actualLabel = (seq.isFraud == 1) ? 1 : 0;
                total++;

                // 旧模型预测
                ModelTrainer.Prediction oldPred = oldModel.predict(seq);
                boolean oldCorrectPred = (oldPred.finalProbability >= 0.35 && actualLabel == 1)
                        || (oldPred.finalProbability < 0.35 && actualLabel == 0);
                if (oldCorrectPred) oldCorrect++;

                // 新模型预测
                ModelTrainer.Prediction newPred = newModel.predict(seq);
                boolean newCorrectPred = (newPred.finalProbability >= 0.35 && actualLabel == 1)
                        || (newPred.finalProbability < 0.35 && actualLabel == 0);
                if (newCorrectPred) newCorrect++;
            }

            double oldAccuracy = total > 0 ? (double) oldCorrect / total : 0;
            double newAccuracy = total > 0 ? (double) newCorrect / total : 0;

            // 判定新模型是否胜出
            boolean newModelWins = (newAccuracy > oldAccuracy + MIN_IMPROVEMENT)
                    && (newAccuracy >= WIN_THRESHOLD);

            return new ABTestResult(
                    newModelWins,
                    newAccuracy,
                    oldAccuracy,
                    newCorrect,
                    oldCorrect,
                    total
            );
        }

        public static class ABTestResult implements Serializable {
            public final boolean newModelWins;
            public final double winRate;
            public final double oldAccuracy;
            public final int newCorrect;
            public final int oldCorrect;
            public final int total;

            public ABTestResult(boolean newModelWins, double winRate, double oldAccuracy,
                               int newCorrect, int oldCorrect, int total) {
                this.newModelWins = newModelWins;
                this.winRate = winRate;
                this.oldAccuracy = oldAccuracy;
                this.newCorrect = newCorrect;
                this.oldCorrect = oldCorrect;
                this.total = total;
            }
        }
    }
}
