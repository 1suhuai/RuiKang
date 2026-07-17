package com.fraud.detection.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 告警可解释性对象 - 人类可读解释
 * 提供特征贡献度、规则命中、资金路径等信息
 * 体现“机器照护人”理念,让告警结果可理解、可追溯
 * 包含自然语言总结
 */
public class AlertExplanation implements Serializable {

    public List<FeatureContribution> topFeatures;
    public List<String> triggeredRules;
    public String graphPath;
    public String summary;

    public AlertExplanation() {
        this.topFeatures = new ArrayList<>();
        this.triggeredRules = new ArrayList<>();
        this.graphPath = "";
        this.summary = "";
    }

    /**
     * 生成一句话自然语言总结
     */
    public String buildSummary() {
        StringBuilder sb = new StringBuilder();

        if (!triggeredRules.isEmpty()) {
            sb.append("命中规则: ").append(String.join(", ", triggeredRules));
        }

        if (!topFeatures.isEmpty()) {
            if (sb.length() > 0) sb.append("; ");
            sb.append("关键风险因子: ");
            int count = 0;
            for (FeatureContribution fc : topFeatures) {
                if (count >= 3) break;
                if (fc.contribution > 0) {
                    if (count > 0) sb.append(", ");
                    sb.append(fc.featureName).append("=").append(String.format("%.1f", fc.featureValue));
                    count++;
                }
            }
        }

        if (graphPath != null && !graphPath.isEmpty()) {
            if (sb.length() > 0) sb.append("; ");
            sb.append("资金路径: ").append(truncate(graphPath, 100));
        }

        this.summary = sb.toString();
        return this.summary;
    }

    /**
     * 转为 JSON 字符串（用于 Doris 存储）
     */
    public String toJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"topFeatures\":[");
        for (int i = 0; i < topFeatures.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(topFeatures.get(i).toJsonFragment());
        }
        sb.append("],\"triggeredRules\":[");
        for (int i = 0; i < triggeredRules.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(escapeJson(triggeredRules.get(i))).append("\"");
        }
        sb.append("],\"graphPath\":\"").append(escapeJson(graphPath != null ? graphPath : "")).append("\"");
        sb.append("}");
        return sb.toString();
    }

    private String truncate(String value, int maxLen) {
        if (value == null) return "";
        return value.length() <= maxLen ? value : value.substring(0, maxLen) + "...";
    }

    private String escapeJson(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }
}
