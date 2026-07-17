package com.fraud.detection.explain;

import java.io.Serializable;
import java.util.Calendar;

/**
 * 风险等级分类器 — 基于多信号综合判定告警风险等级
 *
 * 综合考量：
 *   1. 置信度 (confidence)
 *   2. 触发规则数量 (triggeredRules)
 *   3. 图风险评分 (graphRiskScore)
 *   4. 交易金额 (transactionAmount)
 *   5. 时间维度 (凌晨交易更高风险)
 *
 * 输出等级：
 *   CRITICAL (≥0.90) — 紧急处置
 *   HIGH     (≥0.75) — 快速介入
 *   MEDIUM   (≥0.50) — 持续监控
 *   LOW      (<0.50)  — 常规记录
 */
public class RiskLevelClassifier implements Serializable {

    /**
     * 风险等级枚举
     */
    public enum RiskLevel {
        CRITICAL("极高风险", "立即冻结账户并上报反洗钱部门", 0.90),
        HIGH("高风险", "快速介入调查并联系持卡人确认", 0.75),
        MEDIUM("中等风险", "持续监控并加强审核", 0.50),
        LOW("低风险", "常规记录备查", 0.0);

        private final String label;
        private final String actionHint;
        private final double threshold;

        RiskLevel(String label, String actionHint, double threshold) {
            this.label = label;
            this.actionHint = actionHint;
            this.threshold = threshold;
        }

        public String getLabel() { return label; }
        public String getActionHint() { return actionHint; }
        public double getThreshold() { return threshold; }
    }

    /**
     * 分类结果
     */
    public static class ClassificationResult implements Serializable {
        public RiskLevel level;
        public double compositeScore;   // 综合评分 0~1
        public String reasoning;        // 分类依据说明

        public ClassificationResult(RiskLevel level, double compositeScore, String reasoning) {
            this.level = level;
            this.compositeScore = compositeScore;
            this.reasoning = reasoning;
        }

        /**
         * 获取中文风险等级描述
         */
        public String getLevelLabel() {
            return level.getLabel();
        }

        /**
         * 获取对应的处置建议
         */
        public String getActionHint() {
            return level.getActionHint();
        }
    }

    /**
     * 默认分类 — 仅基于置信度
     */
    public static ClassificationResult classify(double confidence) {
        return classify(confidence, 0, 0.0, 0.0, 12);
    }

    /**
     * 完整分类 — 综合多信号判定
     *
     * @param confidence        欺诈置信度 (0~1)
     * @param triggeredRuleCount 触发规则数量
     * @param graphRiskScore    图分析风险评分 (0~1)
     * @param transactionAmount 交易金额
     * @param hourOfDay         交易时间 (0~23)
     */
    public static ClassificationResult classify(
            double confidence,
            int triggeredRuleCount,
            double graphRiskScore,
            double transactionAmount,
            int hourOfDay) {

        StringBuilder reasoning = new StringBuilder();

        // —— 1. 置信度权重 (0.40) ——
        double confidenceComponent = clamp(confidence) * 0.40;
        reasoning.append(String.format("置信度=%.2f(权重40%%→%.2f)", confidence, confidenceComponent));

        // —— 2. 规则命中数量权重 (0.20) ——
        double ruleComponent = Math.min(triggeredRuleCount / 5.0, 1.0) * 0.20;
        reasoning.append(String.format(", 规则数=%d(权重20%%→%.2f)", triggeredRuleCount, ruleComponent));

        // —— 3. 图风险评分权重 (0.15) ——
        double graphComponent = clamp(graphRiskScore) * 0.15;
        reasoning.append(String.format(", 图风险=%.2f(权重15%%→%.2f)", graphRiskScore, graphComponent));

        // —— 4. 交易金额权重 (0.15) ——
        // 金额分段: <5000→0.1, <20000→0.4, <100000→0.7, ≥100000→1.0
        double amountScore = amountScore(transactionAmount);
        double amountComponent = amountScore * 0.15;
        reasoning.append(String.format(", 金额=%.0f(权重15%%→%.2f)", transactionAmount, amountComponent));

        // —— 5. 时间维度权重 (0.10) ——
        // 凌晨 0~5 点风险系数最高
        double timeScore = timeRiskScore(hourOfDay);
        double timeComponent = timeScore * 0.10;
        reasoning.append(String.format(", 时段=%d时(权重10%%→%.2f)", hourOfDay, timeComponent));

        // 综合评分
        double compositeScore = confidenceComponent + ruleComponent + graphComponent
                + amountComponent + timeComponent;
        compositeScore = clamp(compositeScore);

        // 判定等级
        RiskLevel level = RiskLevel.LOW;
        if (compositeScore >= 0.90) {
            level = RiskLevel.CRITICAL;
        } else if (compositeScore >= 0.75) {
            level = RiskLevel.HIGH;
        } else if (compositeScore >= 0.50) {
            level = RiskLevel.MEDIUM;
        }

        reasoning.append(String.format(" → 综合评分=%.2f → %s", compositeScore, level.getLabel()));

        return new ClassificationResult(level, compositeScore, reasoning.toString());
    }

    /**
     * 基于当前时间推断小时
     */
    public static ClassificationResult classifyWithNow(
            double confidence,
            int triggeredRuleCount,
            double graphRiskScore,
            double transactionAmount) {
        int hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
        return classify(confidence, triggeredRuleCount, graphRiskScore, transactionAmount, hour);
    }

    /**
     * 获取等级的简要中文描述
     */
    public static String getLevelDescription(double compositeScore) {
        if (compositeScore >= 0.90) {
            return "极高";
        } else if (compositeScore >= 0.75) {
            return "高";
        } else if (compositeScore >= 0.50) {
            return "中等";
        } else {
            return "低";
        }
    }

    // —— 内部工具方法 ——

    private static double clamp(double value) {
        return Math.max(0.0, Math.min(1.0, value));
    }

    /**
     * 金额风险评分
     */
    private static double amountScore(double amount) {
        if (amount < 0) return 0.0;
        if (amount < 5000) return 0.1;
        if (amount < 20000) return 0.4;
        if (amount < 100000) return 0.7;
        return 1.0;
    }

    /**
     * 时段风险评分 (凌晨0-5点最危险)
     */
    private static double timeRiskScore(int hour) {
        if (hour >= 0 && hour <= 3) return 1.0;       // 深夜高危
        if (hour >= 4 && hour <= 5) return 0.8;       // 凌晨较高
        if (hour >= 6 && hour <= 7) return 0.3;       // 清晨
        if (hour >= 22 && hour <= 23) return 0.5;     // 夜间
        return 0.1;                                     // 白天正常时段
    }
}
