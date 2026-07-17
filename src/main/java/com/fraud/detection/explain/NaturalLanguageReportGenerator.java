package com.fraud.detection.explain;

import com.fraud.detection.model.Alert;
import com.fraud.detection.model.AlertExplanation;
import com.fraud.detection.model.FeatureContribution;
import com.fraud.detection.explain.FraudPatternDescriber.PatternInfo;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 自然语言报告生成器 — 为欺诈告警生成专业中文调查报告
 *
 * 支持三种输出格式：
 *   PLAIN_TEXT — 纯文本（控制台/日志/邮件）
 *   HTML       — 结构化HTML（Web面板展示）
 *   JSON       — 结构化JSON（API返回/存储）
 *
 * 报告内容：
 *   1. 基本信息（账户、时间、风险等级）
 *   2. 欺诈类型判定（类型、置信度、检测来源）
 *   3. 关键风险因子分析（Top N 特征贡献）
 *   4. 行为路径还原（交易时间线）
 *   5. 规则命中详情（CEP/ML/Graph/SQL）
 *   6. 处置建议（按风险等级推荐）
 */
public class NaturalLanguageReportGenerator implements Serializable {

    /** 输出格式枚举 */
    public enum OutputFormat {
        PLAIN_TEXT, HTML, JSON
    }

    // —— 分隔线常量 ——
    private static final String BORDER = "═══════════════════════════════════════";
    private static final String BORDER_THIN = "───────────────────────────────────";

    // —— 置信度描述映射 ——
    private static String describeConfidence(double confidence) {
        if (confidence >= 0.95) return "极高";
        if (confidence >= 0.90) return "很高";
        if (confidence >= 0.80) return "高";
        if (confidence >= 0.70) return "较高";
        if (confidence >= 0.60) return "中等";
        if (confidence >= 0.50) return "偏低";
        if (confidence >= 0.30) return "低";
        return "极低";
    }

    // —— 时间格式化 ——
    private static String formatTimestamp(long timestamp) {
        if (timestamp <= 0) return "未知";
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return sdf.format(new Date(timestamp));
        } catch (Exception e) {
            return String.valueOf(timestamp);
        }
    }

    private static String formatTimeOnly(long timestamp) {
        if (timestamp <= 0) return "--:--";
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
            return sdf.format(new Date(timestamp));
        } catch (Exception e) {
            return "--:--";
        }
    }

    // —— 金额格式化 ——
    private static String formatAmount(double amount) {
        if (amount >= 10000) {
            return String.format("%.1f万", amount / 10000.0);
        }
        return String.format("%.0f", amount);
    }

    private static String formatAmountFull(double amount) {
        return String.format("¥%,.2f", amount);
    }

    // —— 来源描述 ——
    private static String describeSource(String source) {
        if (source == null) return "未知";
        StringBuilder sb = new StringBuilder();
        if (source.contains("CEP")) sb.append("CEP规则命中");
        if (source.contains("ML")) {
            if (sb.length() > 0) sb.append(" + ");
            sb.append("ML模型确认");
        }
        if (source.contains("GRAPH")) {
            if (sb.length() > 0) sb.append(" + ");
            sb.append("图分析确认");
        }
        if (source.contains("SQL")) {
            if (sb.length() > 0) sb.append(" + ");
            sb.append("SQL跨账户检测");
        }
        if (source.contains("FUSION")) {
            if (sb.length() > 0) sb.append(" + ");
            sb.append("多源融合");
        }
        return sb.length() > 0 ? sb.toString() : source;
    }

    // ===================== 主入口 =====================

    /**
     * 生成纯文本报告（默认格式）
     *
     * @param alert       告警对象
     * @param explanation 告警解释对象
     * @return 格式化后的纯文本调查报告
     */
    public static String generateReport(Alert alert, AlertExplanation explanation) {
        return generateReport(alert, explanation, OutputFormat.PLAIN_TEXT);
    }

    /**
     * 生成指定格式的报告
     */
    public static String generateReport(Alert alert, AlertExplanation explanation, OutputFormat format) {
        if (alert == null) {
            return handleNullAlert(format);
        }

        // 确保 explanation 不为 null
        if (explanation == null) {
            explanation = new AlertExplanation();
        }

        switch (format) {
            case HTML:
                return generateHTML(alert, explanation);
            case JSON:
                return generateJSON(alert, explanation);
            default:
                return generatePlainText(alert, explanation);
        }
    }

    // ===================== 纯文本报告 =====================

    private static String generatePlainText(Alert alert, AlertExplanation explanation) {
        StringBuilder sb = new StringBuilder();

        sb.append(BORDER).append("\n");
        sb.append("金融反诈智能守护系统 - 欺诈调查报告\n");
        sb.append(BORDER).append("\n\n");

        // 基本信息
        sb.append("【基本信息】\n");
        sb.append(String.format("账户ID: %s | 检测时间: %s | 风险等级: %s\n",
                nvl(alert.accountId),
                formatTimestamp(alert.timestamp),
                computeRiskLabel(alert, explanation)));
        sb.append(String.format("告警ID: %s\n", nvl(alert.alertId)));
        sb.append("\n");

        // 欺诈类型判定
        sb.append("【欺诈类型判定】\n");
        PatternInfo pInfo = FraudPatternDescriber.describe(alert.fraudType);
        sb.append(String.format("判定结果: %s\n", nvl(pInfo.displayName)));
        sb.append(String.format("置信度: %.2f (%s)\n", alert.confidence, describeConfidence(alert.confidence)));
        sb.append(String.format("检测来源: %s\n", describeSource(alert.source)));
        sb.append(String.format("严重程度: %s\n", nvl(pInfo.severity)));
        sb.append("\n");

        // 模式解释
        sb.append("【模式解释】\n");
        sb.append(String.format("%s\n\n", nvl(pInfo.humanExplanation)));

        // 关键风险因子分析
        appendPlainTextRiskFactors(sb, explanation);

        // 行为路径还原
        appendPlainTextBehaviorPath(sb, alert, explanation);

        // 规则命中详情
        appendPlainTextRules(sb, alert, explanation);

        // 图路径分析
        appendPlainTextGraphPath(sb, alert, explanation);

        // 处置建议
        appendPlainTextRecommendations(sb, alert, explanation, pInfo);

        sb.append(BORDER).append("\n");

        return sb.toString();
    }

    private static void appendPlainTextRiskFactors(StringBuilder sb, AlertExplanation explanation) {
        sb.append("【关键风险因子分析】\n");

        List<FeatureContribution> features = explanation.topFeatures;
        if (features == null || features.isEmpty()) {
            sb.append("暂无风险因子数据\n\n");
            return;
        }

        sb.append("Top 风险因子:\n");
        int count = 0;
        for (FeatureContribution fc : features) {
            if (count >= 5) break;
            if (fc == null || fc.featureName == null) continue;

            String direction = (fc.contribution >= 0) ? "推高风险" : "降低风险";
            String arrow = (fc.contribution >= 0) ? "↑" : "↓";
            sb.append(String.format("  %d. %s%s (贡献度: %s%.2f, %s)\n",
                    count + 1,
                    fc.featureName,
                    arrow,
                    (fc.contribution >= 0 ? "+" : ""),
                    fc.contribution,
                    direction));
            count++;
        }
        sb.append("\n");
    }

    private static void appendPlainTextBehaviorPath(StringBuilder sb, Alert alert, AlertExplanation explanation) {
        sb.append("【行为路径还原】\n");

        String behaviorPath = alert.behaviorPath;
        if (behaviorPath == null || behaviorPath.isEmpty() || "NONE".equals(behaviorPath)) {
            if (explanation.graphPath != null && !explanation.graphPath.isEmpty()) {
                behaviorPath = explanation.graphPath;
            }
        }

        if (behaviorPath == null || behaviorPath.isEmpty() || "NONE".equals(behaviorPath)) {
            sb.append("暂无行为路径数据\n\n");
            return;
        }

        // 尝试解析行为路径
        if (behaviorPath.contains("->")) {
            String[] steps = behaviorPath.split("->");
            for (int i = 0; i < steps.length; i++) {
                String step = steps[i].trim();
                String label = decodeBehaviorStep(step, alert);
                String riskTag = isHighRiskStep(step, alert) ? " (高风险)" : "";
                sb.append(String.format("  %s %s%s\n",
                        formatTimeOnly(alert.timestamp + i * 120000L),
                        label,
                        riskTag));
            }
        } else if (behaviorPath.contains(",")) {
            // 逗号分隔的路径
            String[] parts = behaviorPath.split(",");
            for (String part : parts) {
                sb.append(String.format("  → %s\n", part.trim()));
            }
        } else {
            sb.append(String.format("  %s\n", behaviorPath));
        }

        sb.append("\n");
    }

    private static String decodeBehaviorStep(String step, Alert alert) {
        // 尝试提取金额信息
        if (step.contains(":")) {
            return step;
        }
        // 尝试根据欺诈类型推断
        String fraudType = alert.fraudType != null ? alert.fraudType : "";
        if (fraudType.contains("小额试探")) {
            return "试探性小额转账 → " + step;
        }
        if (fraudType.contains("链式")) {
            return "链路中转 → " + step;
        }
        return step;
    }

    private static boolean isHighRiskStep(String step, Alert alert) {
        return step.contains("境外") || step.contains("HIGH")
                || step.contains("C境外") || step.contains("ABROAD");
    }

    private static void appendPlainTextRules(StringBuilder sb, Alert alert, AlertExplanation explanation) {
        sb.append("【规则命中详情】\n");

        List<String> rules = explanation.triggeredRules;
        if (rules == null || rules.isEmpty()) {
            // 从告警来源推断
            String source = alert.source != null ? alert.source : "";
            if (source.contains("CEP")) {
                sb.append(String.format("  CEP规则: %s ✓\n", alert.fraudType != null ? alert.fraudType : "未识别"));
            }
            if (source.contains("ML")) {
                sb.append(String.format("  ML模型: 逻辑回归(%.2f) ✓\n", alert.confidence));
            }
            if (source.contains("GRAPH")) {
                sb.append("  图分析: 检测到异常资金链路 ✓\n");
            }
            if (source.contains("SQL")) {
                sb.append(String.format("  SQL检测: %s ✓\n", alert.fraudType != null ? alert.fraudType : "跨账户检测"));
            }
            sb.append("\n");
            return;
        }

        for (String rule : rules) {
            if (rule == null) continue;
            String tag = formatRuleTag(rule);
            sb.append(String.format("  %s ✓\n", tag));
        }
        sb.append("\n");
    }

    private static String formatRuleTag(String rule) {
        if (rule.startsWith("CEP:")) {
            return "CEP规则: " + rule.substring(4);
        }
        if (rule.startsWith("ML:")) {
            return "ML模型: " + rule.substring(3);
        }
        if (rule.startsWith("GRAPH:")) {
            return "图分析: " + rule.substring(6);
        }
        if (rule.startsWith("SQL:")) {
            return "SQL检测: " + rule.substring(4);
        }
        return rule;
    }

    private static void appendPlainTextGraphPath(StringBuilder sb, Alert alert, AlertExplanation explanation) {
        String graphPath = explanation.graphPath;
        if (graphPath == null || graphPath.isEmpty() || "NONE".equals(graphPath)) {
            return;
        }

        // 如果行为路径已经展示过，这里只展示图分析特有路径
        if (graphPath.equals(alert.behaviorPath)) {
            return;
        }

        sb.append("【图路径分析】\n");
        sb.append(String.format("  %s\n\n", graphPath));
    }

    private static void appendPlainTextRecommendations(StringBuilder sb, Alert alert,
                                                        AlertExplanation explanation, PatternInfo pInfo) {
        sb.append("【处置建议】\n");

        RiskLevelClassifier.ClassificationResult classification =
                RiskLevelClassifier.classify(alert.confidence);

        String accountId = nvl(alert.accountId);

        // 根据风险等级输出建议
        sb.append(String.format("  风险等级: %s (综合评分: %.2f)\n",
                classification.getLevelLabel(), classification.compositeScore));
        sb.append(String.format("  分类依据: %s\n\n", classification.reasoning));

        // 分级处置建议
        sb.append("  建议操作:\n");
        switch (classification.level) {
            case CRITICAL:
                sb.append(String.format("  1. 【紧急】立即冻结账户 %s\n", accountId));
                sb.append("  2. 紧急拦截所有进行中交易\n");
                sb.append("  3. 上报反洗钱部门及公安机关\n");
                sb.append("  4. 追踪资金流向，启动挽损流程\n");
                break;
            case HIGH:
                sb.append(String.format("  1. 立即冻结账户 %s\n", accountId));
                sb.append("  2. 联系持卡人确认交易真实性\n");
                sb.append("  3. 上报反洗钱部门\n");
                break;
            case MEDIUM:
                sb.append(String.format("  1. 限制账户 %s 交易额度\n", accountId));
                sb.append("  2. 触发二次身份验证\n");
                sb.append("  3. 持续监控账户行为\n");
                break;
            default:
                sb.append(String.format("  1. 记录账户 %s 异常行为\n", accountId));
                sb.append("  2. 加入观察名单持续监控\n");
                break;
        }

        // 基于欺诈类型的专项建议
        if (pInfo.recommendedAction != null && !pInfo.recommendedAction.isEmpty()) {
            sb.append("\n  专项建议:\n");
            String[] actions = pInfo.recommendedAction.split("\n");
            for (String action : actions) {
                String trimmed = action.trim();
                if (!trimmed.isEmpty()) {
                    sb.append(String.format("  %s\n", trimmed));
                }
            }
        }

        sb.append("\n");
    }

    // ===================== HTML报告 =====================

    private static String generateHTML(Alert alert, AlertExplanation explanation) {
        StringBuilder sb = new StringBuilder();

        sb.append("<!DOCTYPE html>\n<html lang=\"zh-CN\">\n<head>\n");
        sb.append("<meta charset=\"UTF-8\">\n");
        sb.append("<title>金融反诈智能守护系统 - 欺诈调查报告</title>\n");
        sb.append("<style>\n");
        sb.append("body { font-family: 'Microsoft YaHei', sans-serif; margin: 20px; background: #f5f5f5; }\n");
        sb.append(".report { max-width: 800px; margin: 0 auto; background: #fff; padding: 30px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }\n");
        sb.append("h1 { text-align: center; color: #1a1a2e; font-size: 20px; margin-bottom: 20px; padding-bottom: 10px; border-bottom: 2px solid #e74c3c; }\n");
        sb.append("h2 { color: #2c3e50; font-size: 16px; margin-top: 20px; padding-left: 10px; border-left: 3px solid #e74c3c; }\n");
        sb.append("table { width: 100%; border-collapse: collapse; margin: 10px 0; }\n");
        sb.append("td { padding: 8px 12px; border-bottom: 1px solid #eee; }\n");
        sb.append("td:first-child { color: #666; width: 120px; font-weight: bold; }\n");
        sb.append(".risk-critical { color: #e74c3c; font-weight: bold; }\n");
        sb.append(".risk-high { color: #e67e22; font-weight: bold; }\n");
        sb.append(".risk-medium { color: #f39c12; }\n");
        sb.append(".risk-low { color: #27ae60; }\n");
        sb.append(".factor-up { color: #e74c3c; }\n");
        sb.append(".factor-down { color: #27ae60; }\n");
        sb.append("ul { padding-left: 20px; }\n");
        sb.append("li { margin: 5px 0; }\n");
        sb.append(".footer { margin-top: 30px; text-align: center; color: #999; font-size: 12px; border-top: 1px solid #eee; padding-top: 10px; }\n");
        sb.append("</style>\n</head>\n<body>\n");

        sb.append("<div class=\"report\">\n");
        sb.append("<h1>金融反诈智能守护系统 — 欺诈调查报告</h1>\n");

        // 基本信息
        sb.append("<h2>基本信息</h2>\n<table>\n");
        sb.append(tr("账户ID", nvl(alert.accountId)));
        sb.append(tr("告警ID", nvl(alert.alertId)));
        sb.append(tr("检测时间", formatTimestamp(alert.timestamp)));
        sb.append(tr("风险等级", computeRiskLabel(alert, explanation), getRiskClass(alert)));
        sb.append("</table>\n");

        // 欺诈类型判定
        sb.append("<h2>欺诈类型判定</h2>\n<table>\n");
        PatternInfo pInfo = FraudPatternDescriber.describe(alert.fraudType);
        sb.append(tr("判定结果", nvl(pInfo.displayName)));
        sb.append(tr("置信度", String.format("%.2f (%s)", alert.confidence, describeConfidence(alert.confidence))));
        sb.append(tr("检测来源", describeSource(alert.source)));
        sb.append(tr("严重程度", nvl(pInfo.severity)));
        sb.append("</table>\n");

        // 模式解释
        sb.append("<h2>模式解释</h2>\n<p>").append(escapeHtml(nvl(pInfo.humanExplanation))).append("</p>\n");

        // 风险因子
        sb.append("<h2>关键风险因子分析</h2>\n");
        if (explanation.topFeatures != null && !explanation.topFeatures.isEmpty()) {
            sb.append("<table>\n");
            sb.append("<tr><td>排名</td><td>特征名称</td><td>贡献度</td><td>方向</td></tr>\n");
            int count = 0;
            for (FeatureContribution fc : explanation.topFeatures) {
                if (count >= 5) break;
                if (fc == null || fc.featureName == null) continue;
                String cls = fc.contribution >= 0 ? "factor-up" : "factor-down";
                String arrow = fc.contribution >= 0 ? "↑" : "↓";
                String dir = fc.contribution >= 0 ? "推高风险" : "降低风险";
                sb.append(String.format("<tr><td>%d</td><td>%s</td><td class=\"%s\">%s%.2f</td><td>%s %s</td></tr>\n",
                        count + 1, fc.featureName, cls, arrow, fc.contribution, arrow, dir));
                count++;
            }
            sb.append("</table>\n");
        } else {
            sb.append("<p>暂无风险因子数据</p>\n");
        }

        // 规则命中
        sb.append("<h2>规则命中详情</h2>\n<ul>\n");
        if (explanation.triggeredRules != null && !explanation.triggeredRules.isEmpty()) {
            for (String rule : explanation.triggeredRules) {
                sb.append(String.format("<li>%s ✓</li>\n", escapeHtml(formatRuleTag(rule))));
            }
        } else {
            sb.append(String.format("<li>%s ✓</li>\n", escapeHtml(describeSource(alert.source))));
        }
        sb.append("</ul>\n");

        // 处置建议
        sb.append("<h2>处置建议</h2>\n");
        RiskLevelClassifier.ClassificationResult cr = RiskLevelClassifier.classify(alert.confidence);
        sb.append(String.format("<p>风险等级: <span class=\"%s\">%s</span> (评分: %.2f)</p>\n",
                getRiskClass(alert), cr.getLevelLabel(), cr.compositeScore));
        sb.append("<ul>\n");
        String accountId = nvl(alert.accountId);
        switch (cr.level) {
            case CRITICAL:
                sb.append(String.format("<li>【紧急】立即冻结账户 %s</li>\n", accountId));
                sb.append("<li>紧急拦截所有进行中交易</li>\n");
                sb.append("<li>上报反洗钱部门及公安机关</li>\n");
                sb.append("<li>追踪资金流向，启动挽损流程</li>\n");
                break;
            case HIGH:
                sb.append(String.format("<li>立即冻结账户 %s</li>\n", accountId));
                sb.append("<li>联系持卡人确认交易真实性</li>\n");
                sb.append("<li>上报反洗钱部门</li>\n");
                break;
            case MEDIUM:
                sb.append(String.format("<li>限制账户 %s 交易额度</li>\n", accountId));
                sb.append("<li>触发二次身份验证</li>\n");
                sb.append("<li>持续监控账户行为</li>\n");
                break;
            default:
                sb.append(String.format("<li>记录账户 %s 异常行为</li>\n", accountId));
                sb.append("<li>加入观察名单持续监控</li>\n");
                break;
        }
        sb.append("</ul>\n");

        sb.append("<div class=\"footer\">金融反诈智能守护系统 | 报告生成时间: ").append(formatTimestamp(System.currentTimeMillis())).append("</div>\n");
        sb.append("</div>\n</body>\n</html>");

        return sb.toString();
    }

    private static String tr(String label, String value) {
        return String.format("<tr><td>%s</td><td>%s</td></tr>\n", escapeHtml(label), escapeHtml(value));
    }

    private static String tr(String label, String value, String cls) {
        return String.format("<tr><td>%s</td><td class=\"%s\">%s</td></tr>\n", escapeHtml(label), cls, escapeHtml(value));
    }

    private static String escapeHtml(String value) {
        if (value == null) return "";
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;").replace("'", "&#39;");
    }

    private static String getRiskClass(Alert alert) {
        RiskLevelClassifier.ClassificationResult cr = RiskLevelClassifier.classify(alert.confidence);
        switch (cr.level) {
            case CRITICAL: return "risk-critical";
            case HIGH: return "risk-high";
            case MEDIUM: return "risk-medium";
            default: return "risk-low";
        }
    }

    // ===================== JSON报告 =====================

    private static String generateJSON(Alert alert, AlertExplanation explanation) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");

        sb.append("  \"report\": {\n");
        sb.append("    \"title\": \"金融反诈智能守护系统 - 欺诈调查报告\",\n");
        sb.append(String.format("    \"generatedAt\": \"%s\",\n", formatTimestamp(System.currentTimeMillis())));
        sb.append("  },\n");

        // 基本信息
        sb.append("  \"basicInfo\": {\n");
        sb.append(String.format("    \"accountId\": \"%s\",\n", escapeJson(nvl(alert.accountId))));
        sb.append(String.format("    \"alertId\": \"%s\",\n", escapeJson(nvl(alert.alertId))));
        sb.append(String.format("    \"detectionTime\": \"%s\",\n", formatTimestamp(alert.timestamp)));
        RiskLevelClassifier.ClassificationResult cr = RiskLevelClassifier.classify(alert.confidence);
        sb.append(String.format("    \"riskLevel\": \"%s\",\n", cr.getLevelLabel()));
        sb.append(String.format("    \"compositeScore\": %.2f\n", cr.compositeScore));
        sb.append("  },\n");

        // 欺诈类型判定
        PatternInfo pInfo = FraudPatternDescriber.describe(alert.fraudType);
        sb.append("  \"fraudJudgment\": {\n");
        sb.append(String.format("    \"fraudType\": \"%s\",\n", escapeJson(nvl(pInfo.displayName))));
        sb.append(String.format("    \"confidence\": %.2f,\n", alert.confidence));
        sb.append(String.format("    \"confidenceLevel\": \"%s\",\n", describeConfidence(alert.confidence)));
        sb.append(String.format("    \"source\": \"%s\",\n", escapeJson(describeSource(alert.source))));
        sb.append(String.format("    \"severity\": \"%s\",\n", nvl(pInfo.severity)));
        sb.append(String.format("    \"explanation\": \"%s\"\n", escapeJson(nvl(pInfo.humanExplanation))));
        sb.append("  },\n");

        // 风险因子
        sb.append("  \"riskFactors\": [\n");
        if (explanation.topFeatures != null) {
            int count = 0;
            for (FeatureContribution fc : explanation.topFeatures) {
                if (count >= 5) break;
                if (fc == null || fc.featureName == null) continue;
                if (count > 0) sb.append(",\n");
                sb.append(String.format("    {\"name\": \"%s\", \"contribution\": %.4f, \"direction\": \"%s\"}",
                        escapeJson(fc.featureName), fc.contribution,
                        fc.contribution >= 0 ? "推高风险" : "降低风险"));
                count++;
            }
        }
        sb.append("\n  ],\n");

        // 规则命中
        sb.append("  \"triggeredRules\": [\n");
        if (explanation.triggeredRules != null) {
            for (int i = 0; i < explanation.triggeredRules.size(); i++) {
                if (i > 0) sb.append(",\n");
                sb.append(String.format("    \"%s\"", escapeJson(formatRuleTag(explanation.triggeredRules.get(i)))));
            }
        }
        sb.append("\n  ],\n");

        // 行为路径
        sb.append(String.format("  \"behaviorPath\": \"%s\",\n", escapeJson(nvl(alert.behaviorPath))));

        // 图路径
        sb.append(String.format("  \"graphPath\": \"%s\",\n", escapeJson(nvl(explanation.graphPath))));

        // 处置建议
        sb.append("  \"recommendations\": [\n");
        String accountId = nvl(alert.accountId);
        String[] recs = getRecommendations(cr, accountId, pInfo);
        for (int i = 0; i < recs.length; i++) {
            if (i > 0) sb.append(",\n");
            sb.append(String.format("    \"%s\"", escapeJson(recs[i])));
        }
        sb.append("\n  ]\n");

        sb.append("}");
        return sb.toString();
    }

    private static String[] getRecommendations(RiskLevelClassifier.ClassificationResult cr,
                                                String accountId, PatternInfo pInfo) {
        switch (cr.level) {
            case CRITICAL:
                return new String[]{
                        "【紧急】立即冻结账户 " + accountId,
                        "紧急拦截所有进行中交易",
                        "上报反洗钱部门及公安机关",
                        "追踪资金流向，启动挽损流程"
                };
            case HIGH:
                return new String[]{
                        "立即冻结账户 " + accountId,
                        "联系持卡人确认交易真实性",
                        "上报反洗钱部门"
                };
            case MEDIUM:
                return new String[]{
                        "限制账户 " + accountId + " 交易额度",
                        "触发二次身份验证",
                        "持续监控账户行为"
                };
            default:
                return new String[]{
                        "记录账户 " + accountId + " 异常行为",
                        "加入观察名单持续监控"
                };
        }
    }

    // ===================== 工具方法 =====================

    private static String nvl(String value) {
        return value == null || value.trim().isEmpty() ? "N/A" : value;
    }

    private static String computeRiskLabel(Alert alert, AlertExplanation explanation) {
        int ruleCount = (explanation.triggeredRules != null) ? explanation.triggeredRules.size() : 0;
        double graphScore = alert.confidence * 0.5; // 简化估算
        RiskLevelClassifier.ClassificationResult cr =
                RiskLevelClassifier.classify(alert.confidence, ruleCount, graphScore, 0, 12);
        return cr.getLevelLabel();
    }

    private static String handleNullAlert(OutputFormat format) {
        switch (format) {
            case JSON:
                return "{\"error\": \"告警对象为空，无法生成报告\"}";
            case HTML:
                return "<html><body><h1>错误</h1><p>告警对象为空，无法生成报告</p></body></html>";
            default:
                return "错误：告警对象为空，无法生成报告";
        }
    }

    private static String escapeJson(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }
}
