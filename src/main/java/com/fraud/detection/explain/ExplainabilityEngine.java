package com.fraud.detection.explain;

import com.fraud.detection.model.Alert;
import com.fraud.detection.model.AlertExplanation;
import com.fraud.detection.model.FeatureContribution;
import com.fraud.detection.model.UserBehaviorSequence;

import java.io.Serializable;
import java.util.*;

/**
 * 可解释性引擎 - 告警解释生成
 * 为每个告警生成:
 * 1. Top特征贡献度列表(影响评分的关键因子)
 * 2. 命中规则列表(CEP/SQL/Graph)
 * 3. 资金流向路径(图分析结果)
 * 4. 一句话自然语言摘要
 * 是“机器照护人”理念的核心实现
 */
public class ExplainabilityEngine implements Serializable {

    // 特征名称映射（与 UserBehaviorSequence.features 数组索引对应）
    private static final String[] FEATURE_NAMES = {
            "平均交易金额",         // 0
            "金额标准差",           // 1
            "最大单笔金额",         // 2
            "总交易金额",           // 3
            "城市变更次数",         // 4
            "设备变更次数",         // 5
            "渠道变更次数",         // 6
            "凌晨交易笔数",         // 7
            "高风险设备次数",       // 8
            "境外交易次数",         // 9
            "不同城市数",           // 10
            "不同设备数",           // 11
            "不同渠道数",           // 12
            "余额变动比率",         // 13
            "转账提现比率",         // 14
            "交易速度分",           // 15
            "交易间隔均值",         // 16
            "交易间隔方差",         // 17
            "金额趋势斜率",         // 18
            "频率加速度",           // 19
            "出度(转出目标数)",     // 20
            "最大单笔转出占比",     // 21
            "不同目标账户数",       // 22
            "图环路数",             // 23
            "图最大链深度",         // 24
            "图团伙大小",           // 25
            "图入金总额",           // 26
            "图出金总额",           // 27
            "图资金流比",           // 28
            "图总度数"              // 29
    };

    private static final String[] FEATURE_KEYS = {
            "avgAmount", "stdAmount", "maxAmount", "totalAmount",
            "cityChanges", "deviceChanges", "channelChanges",
            "nightCount", "highRiskCount", "abroadCount",
            "distinctCityCount", "distinctDeviceCount", "distinctChannelCount",
            "amountBalanceRatio", "cashOutTransferRatio", "velocityScore",
            "intervalMean", "intervalVariance", "amountTrendSlope", "frequencyAcceleration",
            "outDegree", "maxSingleTransferRatio", "distinctDestCount",
            "graphCycleCount", "graphMaxChainDepth", "graphCommunitySize",
            "graphInAmount", "graphOutAmount", "graphFlowBalance", "graphTotalDegree"
    };

    // ML 模型权重（与 ModelTrainer.OnlineLogisticRegressionModel 中的初始权重对应）
    private static final double[] MODEL_WEIGHTS = {
            0.15, 0.10, 0.20, 0.18, 0.14, 0.18, 0.10,
            0.14, 0.22, 0.22, 0.12, 0.12, 0.08,
            0.25, 0.16, 0.13, 0.18, 0.20, 0.23, 0.16,
            0.13, 0.20, 0.10,
            0.18, 0.15, 0.20, 0.12, 0.12, 0.15, 0.10
    };

    /**
     * 为告警生成完整解释
     */
    public AlertExplanation explainAlert(Alert alert, UserBehaviorSequence sequence) {
        AlertExplanation explanation = new AlertExplanation();

        // 1. 特征贡献度分析
        if (sequence != null && sequence.features != null) {
            explanation.topFeatures = analyzeFeatureContributions(sequence);
        }

        // 2. 规则命中解释
        explanation.triggeredRules = identifyTriggeredRules(alert, sequence);

        // 3. 图路径解释
        if (alert.behaviorPath != null && !alert.behaviorPath.isEmpty()) {
            explanation.graphPath = alert.behaviorPath;
        } else if (sequence != null && sequence.behaviorPath != null) {
            explanation.graphPath = sequence.behaviorPath;
        }

        // 4. 生成总结
        explanation.buildSummary();

        return explanation;
    }

    /**
     * 分析特征贡献度 — 基于权重 × 归一化特征值
     */
    public List<FeatureContribution> analyzeFeatureContributions(UserBehaviorSequence sequence) {
        List<FeatureContribution> contributions = new ArrayList<>();

        if (sequence.features == null) return contributions;

        int featureCount = Math.min(sequence.features.length, MODEL_WEIGHTS.length);

        for (int i = 0; i < featureCount; i++) {
            double rawValue = sequence.features[i];
            if (Double.isNaN(rawValue) || Double.isInfinite(rawValue)) rawValue = 0;

            // 归一化（简化版：基于 bootstrap 标准差）
            double std = getBootstrapStd(i);
            double normalized = std > 0 ? Math.min(3.0, Math.max(-3.0, rawValue / std)) / 3.0 : 0;

            double weight = i < MODEL_WEIGHTS.length ? MODEL_WEIGHTS[i] : 0.1;
            double contribution = weight * normalized;

            String name = i < FEATURE_NAMES.length ? FEATURE_NAMES[i] : "feature_" + i;
            String key = i < FEATURE_KEYS.length ? FEATURE_KEYS[i] : "f" + i;

            contributions.add(new FeatureContribution(
                    name, key, rawValue, normalized, weight, contribution));
        }

        // 按贡献度绝对值降序排列
        contributions.sort((a, b) -> Double.compare(Math.abs(b.contribution), Math.abs(a.contribution)));

        return contributions;
    }

    /**
     * 识别触发的规则
     */
    public List<String> identifyTriggeredRules(Alert alert, UserBehaviorSequence sequence) {
        List<String> rules = new ArrayList<>();

        if (alert.source == null) return rules;

        // CEP 规则
        if (alert.source.contains("CEP")) {
            if (alert.fraudType != null) {
                rules.add("CEP:" + alert.fraudType);
            }
        }

        // ML 规则 — 基于特征阈值判断
        if (alert.source.contains("ML") && sequence != null) {
            if (sequence.abroadTxCount > 0 && sequence.maxAmount >= 15000) {
                rules.add("ML:境外大额交易");
            }
            if (sequence.nightTxCount >= 1 && sequence.highRiskDeviceCount >= 1 && sequence.maxAmount >= 8000) {
                rules.add("ML:凌晨高风险交易");
            }
            if (sequence.cityChangeCount >= 1 && sequence.deviceChangeCount >= 1 && sequence.maxAmount >= 20000) {
                rules.add("ML:异地跨设备异常");
            }
            if (sequence.totalAmount >= 40000 && sequence.highRiskDeviceCount >= 1) {
                rules.add("ML:大额高风险累计");
            }
            if (sequence.cashOutTransferRatio >= 0.5 && sequence.maxAmount >= 15000) {
                rules.add("ML:集中提现模式");
            }
            if (sequence.amountTrendSlope > 8000) {
                rules.add("ML:金额递增趋势");
            }
            if (sequence.frequencyAcceleration >= 2.0) {
                rules.add("ML:交易频率加速");
            }
            if (sequence.intervalVariance > 1000 && sequence.cashOutTransferRatio >= 0.4) {
                rules.add("ML:间隔不规律+高提现比");
            }
            if (sequence.outDegree >= 3 && sequence.maxSingleTransferRatio >= 0.4) {
                rules.add("ML:多目标分散转出");
            }
        }

        // 图分析规则
        if (alert.source.contains("GRAPH")) {
            if (alert.fraudType != null) {
                rules.add("GRAPH:" + alert.fraudType);
            }
        }

        // SQL 规则
        if (alert.source.contains("SQL")) {
            rules.add("SQL:" + (alert.fraudType != null ? alert.fraudType : "跨账户检测"));
        }

        return rules;
    }

    /**
     * 生成简短的解释摘要（用于直接存储到 Doris）
     */
    public String buildBriefExplanation(Alert alert, UserBehaviorSequence sequence) {
        StringBuilder sb = new StringBuilder();

        // Top 3 风险因子
        if (sequence != null && sequence.features != null) {
            List<FeatureContribution> contributions = analyzeFeatureContributions(sequence);
            int count = 0;
            for (FeatureContribution fc : contributions) {
                if (count >= 3) break;
                if (fc.contribution > 0) {
                    if (count > 0) sb.append("|");
                    sb.append(fc.featureName).append("=").append(String.format("%.0f", fc.featureValue));
                    count++;
                }
            }
        }

        // 触发的规则
        List<String> rules = identifyTriggeredRules(alert, sequence);
        if (!rules.isEmpty()) {
            if (sb.length() > 0) sb.append(";");
            sb.append("规则:").append(rules.get(0));
            if (rules.size() > 1) sb.append("等").append(rules.size()).append("条");
        }

        return sb.toString();
    }

    private double getBootstrapStd(int index) {
        double[] stds = {
                90000, 50000, 180000, 300000,   // 0-3
                1.2, 1.2, 1.0, 1.0,            // 4-7
                0.8, 0.6, 1.0, 1.0,            // 8-11
                0.8, 0.35, 0.35, 1.5,          // 12-15
                60.0, 800.0, 5000.0, 1.5,      // 16-19
                3.0, 0.3, 3.0,                  // 20-22
                2.0, 2.0, 3.0,                  // 23-25
                50000.0, 50000.0, 1.0, 5.0      // 26-29
        };
        return index < stds.length ? stds[index] : 1.0;
    }
}
