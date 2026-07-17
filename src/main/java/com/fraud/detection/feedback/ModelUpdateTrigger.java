package com.fraud.detection.feedback;

import com.fraud.detection.model.Alert;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 模型更新触发器
 *
 * 监控反馈统计并决定何时触发模型重训练。
 *
 * 触发条件(任一满足即触发):
 * 1. 累计 ≥50 条新反馈
 * 2. 近期误报率 >10%
 * 3. 检测到反馈模式漂移(某种欺诈类型误报突增)
 */
public class ModelUpdateTrigger {

    // 触发阈值
    private static final int MIN_FEEDBACK_COUNT = 50;
    private static final double MAX_FALSE_POSITIVE_RATE = 0.10;
    private static final int RECENT_WINDOW_SIZE = 100; // 近期反馈窗口

    // 反馈计数
    private final AtomicLong totalFeedbackCount = new AtomicLong(0);
    private final AtomicLong feedbackSinceLastRetrain = new AtomicLong(0);
    private final AtomicLong falsePositiveCount = new AtomicLong(0);

    // 近期反馈缓冲区
    private final Queue<FeedbackCollector.FeedbackRecord> recentFeedbacks = new LinkedList<>();

    // 各欺诈类型误报统计
    private final Map<String, Integer> fraudTypeFalsePositiveCounts = new ConcurrentHashMap<>();
    private final Map<String, Integer> fraudTypeTotalFeedbackCounts = new ConcurrentHashMap<>();

    // 上次重训时间戳
    private volatile long lastRetrainTimestamp = System.currentTimeMillis();

    // 最小重训间隔(5分钟)
    private static final long MIN_RETRAIN_INTERVAL_MS = 5 * 60 * 1000;

    /**
     * 处理新的反馈，检查是否需要触发重训
     * @return RetrainDecision: 是否需要重训及原因
     */
    public RetrainDecision onNewFeedback(FeedbackCollector.FeedbackRecord feedback) {
        totalFeedbackCount.incrementAndGet();
        long newCount = feedbackSinceLastRetrain.incrementAndGet();

        // 更新近期反馈缓冲区
        recentFeedbacks.offer(feedback);
        while (recentFeedbacks.size() > RECENT_WINDOW_SIZE) {
            recentFeedbacks.poll();
        }

        // 统计误报
        if (feedback.feedbackType == FeedbackCollector.FeedbackType.FALSE_POSITIVE) {
            falsePositiveCount.incrementAndGet();
            fraudTypeFalsePositiveCounts.merge(feedback.alertId, 1, Integer::sum);
        }
        fraudTypeTotalFeedbackCounts.merge(feedback.alertId, 1, Integer::sum);

        // 检查触发条件
        // 条件1: 反馈数量达到阈值
        if (newCount >= MIN_FEEDBACK_COUNT) {
            return new RetrainDecision(
                    true,
                    "FEEDBACK_COUNT",
                    String.format("累计%d条新反馈(阈值%d)", newCount, MIN_FEEDBACK_COUNT),
                    calculateCurrentStats()
            );
        }

        // 条件2: 误报率超过阈值
        double currentFPR = (double) falsePositiveCount.get() / Math.max(totalFeedbackCount.get(), 1);
        if (currentFPR > MAX_FALSE_POSITIVE_RATE) {
            return new RetrainDecision(
                    true,
                    "HIGH_FALSE_POSITIVE_RATE",
                    String.format("误报率%.1f%%超过阈值%.1f%%", currentFPR * 100, MAX_FALSE_POSITIVE_RATE * 100),
                    calculateCurrentStats()
            );
        }

        // 条件3: 某欺诈类型误报突增
        String spikeType = detectFalsePositiveSpike();
        if (spikeType != null) {
            return new RetrainDecision(
                    true,
                    "FRAUD_TYPE_SPIKE",
                    String.format("欺诈类型'%s'误报突增", spikeType),
                    calculateCurrentStats()
            );
        }

        return new RetrainDecision(false, "NONE", "", calculateCurrentStats());
    }

    /**
     * 检测某欺诈类型的误报是否突增
     */
    private String detectFalsePositiveSpike() {
        for (Map.Entry<String, Integer> entry : fraudTypeTotalFeedbackCounts.entrySet()) {
            String type = entry.getKey();
            int total = entry.getValue();
            int fp = fraudTypeFalsePositiveCounts.getOrDefault(type, 0);

            if (total >= 10) { // 至少10条反馈才判断
                double typeFPR = (double) fp / total;
                if (typeFPR > 0.30) { // 该类型误报率>30%视为突增
                    return type;
                }
            }
        }
        return null;
    }

    /**
     * 检查是否应该重训(考虑时间间隔)
     */
    public RetrainDecision shouldRetrain() {
        long now = System.currentTimeMillis();
        long timeSinceLastRetrain = now - lastRetrainTimestamp;

        if (timeSinceLastRetrain < MIN_RETRAIN_INTERVAL_MS) {
            return new RetrainDecision(
                    false,
                    "TOO_SOON",
                    String.format("距离上次重训仅%d秒(最小间隔%d秒)",
                            timeSinceLastRetrain / 1000, MIN_RETRAIN_INTERVAL_MS / 1000),
                    calculateCurrentStats()
            );
        }

        return onNewFeedback(null);
    }

    /**
     * 标记重训已完成，重置计数器
     */
    public void markRetrainComplete() {
        feedbackSinceLastRetrain.set(0);
        falsePositiveCount.set(0);
        lastRetrainTimestamp = System.currentTimeMillis();
        recentFeedbacks.clear();
    }

    private Map<String, Object> calculateCurrentStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("total_feedbacks", totalFeedbackCount.get());
        stats.put("feedbacks_since_retrain", feedbackSinceLastRetrain.get());
        stats.put("false_positive_count", falsePositiveCount.get());
        stats.put("false_positive_rate",
                (double) falsePositiveCount.get() / Math.max(totalFeedbackCount.get(), 1));
        stats.put("recent_window_size", recentFeedbacks.size());
        stats.put("fraud_type_fp_rates", calculateTypeFPRates());
        return stats;
    }

    private Map<String, Double> calculateTypeFPRates() {
        Map<String, Double> rates = new HashMap<>();
        for (String type : fraudTypeTotalFeedbackCounts.keySet()) {
            int total = fraudTypeTotalFeedbackCounts.get(type);
            int fp = fraudTypeFalsePositiveCounts.getOrDefault(type, 0);
            rates.put(type, (double) fp / Math.max(total, 1));
        }
        return rates;
    }

    /**
     * 重训决策结果
     */
    public static class RetrainDecision {
        public final boolean shouldRetrain;
        public final String reason;
        public final String description;
        public final Map<String, Object> stats;

        public RetrainDecision(boolean shouldRetrain, String reason, String description, Map<String, Object> stats) {
            this.shouldRetrain = shouldRetrain;
            this.reason = reason;
            this.description = description;
            this.stats = stats;
        }

        @Override
        public String toString() {
            if (shouldRetrain) {
                return String.format("[需要重训] 原因: %s - %s", reason, description);
            }
            return String.format("[暂不重训] %s", description);
        }
    }
}
