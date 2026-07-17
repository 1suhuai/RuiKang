package com.fraud.detection.feedback;

import com.fraud.detection.model.Alert;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 告警审核队列
 *
 * 模拟人工分析师的告警审核队列，支持:
 * - 按优先级排序(风险等级 + 置信度)
 * - 审核状态跟踪: PENDING → REVIEWED → CONFIRMED / REJECTED
 * - 审核工作量统计
 * - 审核时效监控
 */
public class AlertReviewQueue {

    /**
     * 审核状态枚举
     */
    public enum ReviewStatus {
        PENDING,      // 待审核
        REVIEWED,     // 已审核
        CONFIRMED,    // 确认欺诈
        REJECTED      // 拒绝(误报)
    }

    /**
     * 队列项
     */
    public static class ReviewItem implements Comparable<ReviewItem> {
        public final String alertId;
        public final String accountId;
        public final String fraudType;
        public final double confidence;
        public final double amount;
        public final long timestamp;
        public final String riskLevel;
        public volatile ReviewStatus status = ReviewStatus.PENDING;
        public volatile String reviewerId;
        public volatile long reviewTimestamp;
        public volatile String reviewNotes;

        // 优先级分数: 风险等级权重 * 置信度
        private double priorityScore;

        public ReviewItem(Alert alert) {
            this.alertId = alert.alertId;
            this.accountId = alert.accountId;
            this.fraudType = alert.fraudType;
            this.confidence = alert.confidence;
            this.amount = parseAmount(alert);
            this.timestamp = alert.timestamp;
            this.riskLevel = classifyRiskLevel(alert.confidence);
            this.priorityScore = calculatePriorityScore();
        }

        private double calculatePriorityScore() {
            double riskWeight;
            switch (riskLevel) {
                case "CRITICAL": riskWeight = 4.0; break;
                case "HIGH":     riskWeight = 3.0; break;
                case "MEDIUM":   riskWeight = 2.0; break;
                default:         riskWeight = 1.0; break;
            }
            return riskWeight * confidence;
        }

        private String classifyRiskLevel(double conf) {
            if (conf >= 0.90) return "CRITICAL";
            if (conf >= 0.75) return "HIGH";
            if (conf >= 0.50) return "MEDIUM";
            return "LOW";
        }

        private static double parseAmount(Alert alert) {
            if (alert.details == null || alert.details.isEmpty()) return 0;
            try {
                com.alibaba.fastjson.JSONObject json = com.alibaba.fastjson.JSON.parseObject(alert.details);
                return json.getDoubleValue("amount");
            } catch (Exception e) {
                return 0;
            }
        }

        @Override
        public int compareTo(ReviewItem other) {
            return Double.compare(other.priorityScore, this.priorityScore);
        }
    }

    // 审核队列(优先级队列)
    private final PriorityBlockingQueue<ReviewItem> queue = new PriorityBlockingQueue<>();

    // 已审核项
    private final Map<String, ReviewItem> reviewedItems = new ConcurrentHashMap<>();

    // 统计计数器
    private final AtomicInteger pendingCount = new AtomicInteger(0);
    private final AtomicInteger reviewedCount = new AtomicInteger(0);
    private final AtomicInteger confirmedCount = new AtomicInteger(0);
    private final AtomicInteger rejectedCount = new AtomicInteger(0);

    // 审核时效统计
    private final List<Long> reviewLatencies = Collections.synchronizedList(new ArrayList<>());

    /**
     * 将告警加入审核队列
     */
    public void enqueue(Alert alert) {
        ReviewItem item = new ReviewItem(alert);
        queue.offer(item);
        pendingCount.incrementAndGet();
    }

    /**
     * 取出下一个待审核项(按优先级)
     */
    public ReviewItem dequeue() {
        ReviewItem item = queue.poll();
        if (item != null) {
            pendingCount.decrementAndGet();
        }
        return item;
    }

    /**
     * 审核一条告警
     * @param alertId 告警ID
     * @param reviewerId 审核员ID
     * @param confirmed 是否确认欺诈
     * @param notes 审核备注
     */
    public void review(String alertId, String reviewerId, boolean confirmed, String notes) {
        // 先从队列中移除(如果还在)
        ReviewItem item = null;
        Iterator<ReviewItem> it = queue.iterator();
        while (it.hasNext()) {
            ReviewItem next = it.next();
            if (next.alertId.equals(alertId)) {
                it.remove();
                item = next;
                pendingCount.decrementAndGet();
                break;
            }
        }

        if (item == null) {
            // 可能已经被审核过了
            item = reviewedItems.get(alertId);
            if (item != null) return;
        }

        // 标记审核结果
        item.status = confirmed ? ReviewStatus.CONFIRMED : ReviewStatus.REJECTED;
        item.reviewerId = reviewerId;
        item.reviewTimestamp = System.currentTimeMillis();
        item.reviewNotes = notes;

        long latency = item.reviewTimestamp - item.timestamp;
        reviewLatencies.add(latency);

        reviewedItems.put(alertId, item);
        reviewedCount.incrementAndGet();
        if (confirmed) {
            confirmedCount.incrementAndGet();
        } else {
            rejectedCount.incrementAndGet();
        }
    }

    /**
     * 获取队列统计
     */
    public QueueStats getStats() {
        return new QueueStats(
                pendingCount.get(),
                reviewedCount.get(),
                confirmedCount.get(),
                rejectedCount.get(),
                getAvgReviewLatency(),
                getTotalAmount()
        );
    }

    private long getAvgReviewLatency() {
        if (reviewLatencies.isEmpty()) return 0;
        return (long) reviewLatencies.stream().mapToLong(Long::longValue).average().orElse(0);
    }

    private double getTotalAmount() {
        return reviewedItems.values().stream()
                .mapToDouble(item -> item.amount)
                .sum();
    }

    /**
     * 获取待审核数量
     */
    public int getPendingCount() {
        return pendingCount.get();
    }

    /**
     * 生成审核工作量报告
     */
    public String generateWorkloadReport() {
        StringBuilder sb = new StringBuilder();
        sb.append("═══════════════════════════════════════\n");
        sb.append("         审核工作量报告\n");
        sb.append("═══════════════════════════════════════\n\n");

        QueueStats stats = getStats();
        sb.append(String.format("待审核: %d 条\n", stats.pendingCount));
        sb.append(String.format("已审核: %d 条\n", stats.reviewedCount));
        sb.append(String.format("  确认欺诈: %d 条\n", stats.confirmedCount));
        sb.append(String.format("  判定误报: %d 条\n", stats.rejectedCount));
        sb.append(String.format("平均审核延迟: %d ms\n", stats.avgLatencyMs));
        sb.append(String.format("审核确认率: %.1f%%\n",
                stats.reviewedCount > 0 ? (double) stats.confirmedCount / stats.reviewedCount * 100 : 0));
        sb.append(String.format("涉及金额: ¥%.0f\n", stats.totalAmount));

        sb.append("\n═══════════════════════════════════════\n");
        return sb.toString();
    }

    /**
     * 队列统计
     */
    public static class QueueStats {
        public final int pendingCount;
        public final int reviewedCount;
        public final int confirmedCount;
        public final int rejectedCount;
        public final long avgLatencyMs;
        public final double totalAmount;

        public QueueStats(int pendingCount, int reviewedCount, int confirmedCount,
                         int rejectedCount, long avgLatencyMs, double totalAmount) {
            this.pendingCount = pendingCount;
            this.reviewedCount = reviewedCount;
            this.confirmedCount = confirmedCount;
            this.rejectedCount = rejectedCount;
            this.avgLatencyMs = avgLatencyMs;
            this.totalAmount = totalAmount;
        }
    }
}
