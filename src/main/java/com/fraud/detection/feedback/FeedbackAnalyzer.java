package com.fraud.detection.feedback;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 反馈分析器
 *
 * 分析收集到的反馈数据，识别误报模式和检测层性能问题。
 *
 * 分析维度:
 * 1. 各欺诈类型的误报率排名
 * 2. 各检测层(CEP/SQL/Graph/ML)的误报贡献
 * 3. 误报的共同特征(金额区间、时间段、设备等)
 * 4. 阈值调整建议
 */
public class FeedbackAnalyzer {

    // 各欺诈类型统计
    private final Map<String, TypeStats> typeStatsMap = new ConcurrentHashMap<>();

    // 各检测层统计
    private final Map<String, LayerStats> layerStatsMap = new ConcurrentHashMap<>();

    // 误报特征分布
    private final List<FeedbackCollector.FeedbackRecord> falsePositiveSamples = Collections.synchronizedList(new ArrayList<>());

    // 总计数
    private final AtomicInteger totalFeedbacks = new AtomicInteger(0);
    private final AtomicInteger totalFalsePositives = new AtomicInteger(0);

    /**
     * 分析单条反馈
     */
    public void analyzeFeedback(FeedbackCollector.FeedbackRecord feedback) {
        totalFeedbacks.incrementAndGet();

        // 按反馈类型统计（使用alertId作为key，因为FeedbackRecord没有fraudType字段）
        TypeStats typeStats = typeStatsMap.computeIfAbsent(feedback.alertId, k -> new TypeStats(k));
        typeStats.totalCount++;
        if (feedback.feedbackType == FeedbackCollector.FeedbackType.FALSE_POSITIVE) {
            typeStats.falsePositiveCount++;
            totalFalsePositives.incrementAndGet();
            falsePositiveSamples.add(feedback);
        } else if (feedback.feedbackType == FeedbackCollector.FeedbackType.CONFIRMED_FRAUD) {
            typeStats.confirmedCount++;
        } else if (feedback.feedbackType == FeedbackCollector.FeedbackType.INCORRECT_TYPE) {
            typeStats.incorrectTypeCount++;
        }

        // 按检测层统计
        String layer = extractDetectionLayer(feedback);
        LayerStats layerStats = layerStatsMap.computeIfAbsent(layer, k -> new LayerStats(k));
        layerStats.totalCount++;
        if (feedback.feedbackType == FeedbackCollector.FeedbackType.FALSE_POSITIVE) {
            layerStats.falsePositiveCount++;
        }
    }

    /**
     * 提取告警来源对应的检测层
     */
    private String extractDetectionLayer(FeedbackCollector.FeedbackRecord feedback) {
        // 从alertId提取检测层信息，或从备注中解析
        String comment = feedback.comment != null ? feedback.comment : "";
        String alertId = feedback.alertId != null ? feedback.alertId : "";
        String source = "";
        if (alertId.contains("CEP")) source = "CEP";
        else if (alertId.contains("SQL")) source = "SQL";
        else if (alertId.contains("Graph") || alertId.contains("GNN")) source = "GRAPH";
        else if (alertId.contains("ML")) source = "ML";
        else if (comment.contains("CEP")) source = "CEP";
        else if (comment.contains("SQL")) source = "SQL";
        else if (comment.contains("Graph") || comment.contains("GNN")) source = "GRAPH";
        else if (comment.contains("ML")) source = "ML";

        return source.isEmpty() ? "UNKNOWN" : source;
    }

    /**
     * 生成分析报告
     */
    public String generateReport() {
        StringBuilder sb = new StringBuilder();
        sb.append("═══════════════════════════════════════\n");
        sb.append("         反馈分析报告\n");
        sb.append("═══════════════════════════════════════\n\n");

        // 总体统计
        sb.append("【总体统计】\n");
        sb.append(String.format("总反馈数: %d\n", totalFeedbacks.get()));
        sb.append(String.format("误报数: %d (误报率: %.1f%%)\n",
                totalFalsePositives.get(),
                (double) totalFalsePositives.get() / Math.max(totalFeedbacks.get(), 1) * 100));
        sb.append("\n");

        // 各欺诈类型误报率排名
        sb.append("【各欺诈类型误报率排名】\n");
        List<TypeStats> sortedByFPR = new ArrayList<>(typeStatsMap.values());
        sortedByFPR.sort((a, b) -> Double.compare(b.getFalsePositiveRate(), a.getFalsePositiveRate()));
        for (TypeStats ts : sortedByFPR) {
            if (ts.totalCount >= 5) {
                sb.append(String.format("  %-20s 总数:%3d 误报:%3d 误报率:%.1f%%\n",
                        ts.fraudType, ts.totalCount, ts.falsePositiveCount, ts.getFalsePositiveRate() * 100));
            }
        }
        sb.append("\n");

        // 各检测层误报贡献
        sb.append("【各检测层误报贡献】\n");
        List<LayerStats> sortedLayers = new ArrayList<>(layerStatsMap.values());
        sortedLayers.sort((a, b) -> Integer.compare(b.falsePositiveCount, a.falsePositiveCount));
        for (LayerStats ls : sortedLayers) {
            if (ls.totalCount > 0) {
                sb.append(String.format("  %-10s 总数:%3d 误报:%3d 误报率:%.1f%%\n",
                        ls.layer, ls.totalCount, ls.falsePositiveCount, ls.getFalsePositiveRate() * 100));
            }
        }
        sb.append("\n");

        // 误报特征分析
        if (!falsePositiveSamples.isEmpty()) {
            sb.append("【误报特征分析】\n");
            analyzeFalsePositiveFeatures(sb);
            sb.append("\n");
        }

        // 阈值调整建议
        sb.append("【阈值调整建议】\n");
        generateThresholdSuggestions(sb);

        sb.append("═══════════════════════════════════════\n");
        return sb.toString();
    }

    /**
     * 分析误报特征
     */
    private void analyzeFalsePositiveFeatures(StringBuilder sb) {
        // 时间段分布
        int nightCount = 0;
        for (FeedbackCollector.FeedbackRecord fp : falsePositiveSamples) {
            int hour = (int) ((fp.timestamp % 86400000) / 3600000);
            if (hour >= 0 && hour < 6) nightCount++;
        }
        sb.append(String.format("  凌晨(0-6点)误报占比: %.1f%%\n",
                (double) nightCount / falsePositiveSamples.size() * 100));

        // 审核员分布
        long uniqueReviewers = falsePositiveSamples.stream()
                .map(f -> f.reviewerId)
                .filter(Objects::nonNull)
                .distinct()
                .count();
        sb.append(String.format("  涉及审核员数: %d\n", uniqueReviewers));

        // 备注关键词
        Map<String, Integer> keywordCounts = new HashMap<>();
        for (FeedbackCollector.FeedbackRecord fp : falsePositiveSamples) {
            if (fp.comment != null && !fp.comment.isEmpty()) {
                String[] words = fp.comment.split("[\\s,;，；]+");
                for (String word : words) {
                    if (word.length() > 1) {
                        keywordCounts.merge(word, 1, Integer::sum);
                    }
                }
            }
        }
        if (!keywordCounts.isEmpty()) {
            sb.append("  误报备注高频词:\n");
            keywordCounts.entrySet().stream()
                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                    .limit(5)
                    .forEach(e -> sb.append(String.format("    %s: %d次\n", e.getKey(), e.getValue())));
        }
    }

    /**
     * 生成阈值调整建议
     */
    private void generateThresholdSuggestions(StringBuilder sb) {
        for (TypeStats ts : typeStatsMap.values()) {
            if (ts.totalCount >= 10 && ts.getFalsePositiveRate() > 0.20) {
                double currentThreshold = ts.getDefaultThreshold();
                double suggestedThreshold = Math.min(currentThreshold + 0.05, 0.95);
                sb.append(String.format("  ⚠ 告警'%s'误报率%.1f%%过高，建议将阈值从%.2f调高至%.2f\n",
                        ts.fraudType, ts.getFalsePositiveRate() * 100, currentThreshold, suggestedThreshold));
            }
        }

        for (LayerStats ls : layerStatsMap.values()) {
            if (ls.totalCount >= 20 && ls.getFalsePositiveRate() > 0.15) {
                sb.append(String.format("  ⚠ 检测层'%s'误报率%.1f%%，建议优化该层检测逻辑\n",
                        ls.layer, ls.getFalsePositiveRate() * 100));
            }
        }

        if (totalFalsePositives.get() == 0 && totalFeedbacks.get() > 20) {
            sb.append("  ✓ 当前无误报，可考虑适当降低阈值以捕获更多欺诈\n");
        }
    }

    /**
     * 输出JSON格式的分析结果
     */
    public String toJson() {
        JSONObject json = new JSONObject();
        json.put("total_feedbacks", totalFeedbacks.get());
        json.put("total_false_positives", totalFalsePositives.get());
        json.put("overall_fp_rate",
                (double) totalFalsePositives.get() / Math.max(totalFeedbacks.get(), 1));

        JSONArray typeArray = new JSONArray();
        for (TypeStats ts : typeStatsMap.values()) {
            if (ts.totalCount >= 5) {
                JSONObject obj = new JSONObject();
                obj.put("fraud_type", ts.fraudType);
                obj.put("total_count", ts.totalCount);
                obj.put("false_positive_count", ts.falsePositiveCount);
                obj.put("confirmed_count", ts.confirmedCount);
                obj.put("fp_rate", ts.getFalsePositiveRate());
                typeArray.add(obj);
            }
        }
        json.put("type_stats", typeArray);

        JSONArray layerArray = new JSONArray();
        for (LayerStats ls : layerStatsMap.values()) {
            JSONObject obj = new JSONObject();
            obj.put("layer", ls.layer);
            obj.put("total_count", ls.totalCount);
            obj.put("false_positive_count", ls.falsePositiveCount);
            obj.put("fp_rate", ls.getFalsePositiveRate());
            layerArray.add(obj);
        }
        json.put("layer_stats", layerArray);

        return json.toJSONString();
    }

    /**
     * 欺诈类型统计
     */
    static class TypeStats {
        final String fraudType;
        int totalCount = 0;
        int falsePositiveCount = 0;
        int confirmedCount = 0;
        int incorrectTypeCount = 0;

        TypeStats(String fraudType) {
            this.fraudType = fraudType;
        }

        double getFalsePositiveRate() {
            return (double) falsePositiveCount / Math.max(totalCount, 1);
        }

        double getDefaultThreshold() {
            // 根据欺诈类型返回默认阈值
            switch (fraudType) {
                case "小额试探大额转出": return 0.85;
                case "多层链式洗钱": return 0.90;
                case "异地跨设备突发大额": return 0.85;
                case "分散转入集中提现": return 0.80;
                case "多渠道轮番转账": return 0.80;
                case "凌晨分批掏空": return 0.80;
                case "小额掩护大额跑路": return 0.85;
                case "团伙同IP批量作案": return 0.85;
                default: return 0.50;
            }
        }
    }

    /**
     * 检测层统计
     */
    static class LayerStats {
        final String layer;
        int totalCount = 0;
        int falsePositiveCount = 0;

        LayerStats(String layer) {
            this.layer = layer;
        }

        double getFalsePositiveRate() {
            return (double) falsePositiveCount / Math.max(totalCount, 1);
        }
    }
}
