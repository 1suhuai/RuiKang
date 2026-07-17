package com.fraud.detection.model;

import java.io.Serializable;

/**
 * 特征贡献度类 - 记录特征对评分的影响
 * 用于解释ML模型预测结果,说明哪些特征推高/降低了欺诈评分
 * 是告警可解释性的核心数据结构
 */
public class FeatureContribution implements Serializable {

    public String featureName;      // 特征中文名
    public String featureKey;       // 特征英文键
    public double featureValue;     // 原始特征值
    public double normalizedValue;  // 归一化后的值
    public double weight;           // 模型权重
    public double contribution;     // 贡献度 = weight × normalizedValue
    public String direction;        // "推高风险" / "降低风险"

    public FeatureContribution() {}

    public FeatureContribution(String featureName, String featureKey,
                                double featureValue, double normalizedValue,
                                double weight, double contribution) {
        this.featureName = featureName;
        this.featureKey = featureKey;
        this.featureValue = featureValue;
        this.normalizedValue = normalizedValue;
        this.weight = weight;
        this.contribution = contribution;
        this.direction = contribution >= 0 ? "推高风险" : "降低风险";
    }

    @Override
    public String toString() {
        return String.format("%s=%.2f(贡献%.4f,%s)", featureName, featureValue, contribution, direction);
    }

    /**
     * 转为可读的 JSON 字符串片段
     */
    public String toJsonFragment() {
        return String.format("{\"name\":\"%s\",\"value\":%.2f,\"weight\":%.4f,\"contribution\":%.4f,\"direction\":\"%s\"}",
                featureName, featureValue, weight, contribution, direction);
    }
}
