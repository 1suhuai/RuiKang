package com.fraud.detection.feedback;

import com.fraud.detection.model.Alert;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 人工反馈收集器 - Human-in-the-Loop 核心组件
 *
 * 功能:
 * 1. 接收人工审核反馈（CONFIRMED_FRAUD / FALSE_POSITIVE / INCORRECT_TYPE）
 * 2. 将反馈关联到对应告警，存入本地缓存
 * 3. 当反馈数量达到阈值（默认50条）时触发模型更新信号
 * 4. 定期输出反馈统计信息
 *
 * 使用方式:
 * - 上游输入: Alert (待审核的告警)
 * - 通过 processFeedback() 方法注入人工反馈
 * - 下游输出: FeedbackStats (反馈统计) / RetrainingSignal (重训练信号)
 */
public class FeedbackCollector extends KeyedProcessFunction<String, Alert, FeedbackCollector.FeedbackOutput> {

    /** 触发模型更新的反馈数量阈值 */
    private static final int RETRAIN_THRESHOLD = 50;
    /** 反馈统计输出间隔(ms) */
    private static final long STATS_INTERVAL_MS = 60000;

    /** 本地反馈缓存 - 模拟反馈队列 */
    private transient ListState<FeedbackRecord> feedbackBuffer;
    /** 已收集反馈计数 */
    private transient ValueState<Integer> feedbackCount;
    /** 各类反馈计数 */
    private transient MapState<FeedbackType, Integer> typeCounters;
    /** 上次统计输出时间 */
    private transient ValueState<Long> lastStatsTime;
    /** 累计统计（不随阈值重置） */
    private transient ValueState<FeedbackCollector.CumulativeStats> cumulativeStats;

    @Override
    public void open(Configuration parameters) throws Exception {
        feedbackBuffer = getRuntimeContext().getListState(
                new ListStateDescriptor<>("feedback-buffer", FeedbackRecord.class));
        feedbackCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("feedback-count", Integer.class, 0));
        typeCounters = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("type-counters", FeedbackType.class, Integer.class));
        lastStatsTime = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-stats-time", Long.class, 0L));
        cumulativeStats = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cumulative-stats", FeedbackCollector.CumulativeStats.class));

        // 定时器将在第一次processElement时注册（见checkAndRegisterStatsTimer）
    }

    /**
     * 处理输入的告警
     * 将告警加入待审核队列，并检查是否需要输出反馈统计
     */
    @Override
    public void processElement(Alert alert, Context ctx, Collector<FeedbackOutput> out) throws Exception {
        // 注册定时器用于定期统计输出
        Long lastTime = lastStatsTime.value();
        long now = ctx.timestamp();
        if (lastTime == null || lastTime == 0) {
            // 首次触发，注册第一个定时器
            long firstFire = now + STATS_INTERVAL_MS;
            ctx.timerService().registerEventTimeTimer(firstFire);
            lastStatsTime.update(now);
        }

        // 告警进入待审核状态（由AlertReviewQueue管理）
        // 此处仅记录告警到达
        String alertId = alert.alertId;
        if (alertId != null) {
            // 可在此处关联后续反馈
        }
    }

    /**
     * 接收人工反馈 - 核心入口
     *
     * @param alertId     关联的告警ID
     * @param feedbackType 反馈类型
     * @param reviewerId   审核人ID
     * @param comment      审核备注
     */
    public void processFeedback(String alertId, FeedbackType feedbackType,
                                String reviewerId, String comment) throws Exception {
        FeedbackRecord record = new FeedbackRecord(
                alertId, feedbackType, reviewerId, comment, System.currentTimeMillis());

        feedbackBuffer.add(record);

        // 更新类型计数器
        Integer count = typeCounters.get(feedbackType);
        typeCounters.put(feedbackType, count == null ? 1 : count + 1);

        // 更新总计数
        int total = feedbackCount.value() == null ? 0 : feedbackCount.value();
        total++;
        feedbackCount.update(total);

        // 更新累计统计
        FeedbackCollector.CumulativeStats stats = cumulativeStats.value();
        if (stats == null) {
            stats = new FeedbackCollector.CumulativeStats();
        }
        stats.totalFeedback++;
        switch (feedbackType) {
            case CONFIRMED_FRAUD:
                stats.confirmedFraud++;
                break;
            case FALSE_POSITIVE:
                stats.falsePositive++;
                break;
            case INCORRECT_TYPE:
                stats.incorrectType++;
                break;
        }
        cumulativeStats.update(stats);

        System.out.println("[FeedbackCollector] 收到反馈: alertId=" + alertId
                + ", type=" + feedbackType.getDescription()
                + ", 累计=" + total);

        // 检查是否达到重训练阈值
        // 注意：processFeedback是工具方法，无Collector上下文，重训练信号由onTimer定期输出
        if (total >= RETRAIN_THRESHOLD) {
            // 标记需要重训练，实际信号在定时器中输出
            System.out.println("[FeedbackCollector] 达到重训练阈值: 累计" + total + "条反馈");
            // 重置计数器
            feedbackCount.update(0);
            feedbackBuffer.clear();
            for (FeedbackType ft : FeedbackType.values()) {
                typeCounters.put(ft, 0);
            }
        }
    }

    /**
     * 定时器回调 - 定期输出反馈统计
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<FeedbackOutput> out) throws Exception {
        FeedbackCollector.CumulativeStats stats = cumulativeStats.value();
        if (stats != null && stats.totalFeedback > 0) {
            FeedbackStats feedbackStats = new FeedbackStats(
                    stats.totalFeedback,
                    stats.confirmedFraud,
                    stats.falsePositive,
                    stats.incorrectType,
                    System.currentTimeMillis());
            out.collect(feedbackStats);
        }

        // 注册下一次定时器
        long nextFire = timestamp + STATS_INTERVAL_MS;
        ctx.timerService().registerEventTimeTimer(nextFire);
    }

    /**
     * 获取当前反馈缓存快照
     */
    public List<FeedbackRecord> getFeedbackSnapshot() throws Exception {
        List<FeedbackRecord> snapshot = new ArrayList<>();
        for (FeedbackRecord record : feedbackBuffer.get()) {
            snapshot.add(record);
        }
        return snapshot;
    }

    /**
     * 获取当前反馈统计
     */
    public FeedbackCollector.CumulativeStats getCumulativeStats() throws Exception {
        FeedbackCollector.CumulativeStats stats = cumulativeStats.value();
        return stats != null ? stats.clone() : new FeedbackCollector.CumulativeStats();
    }

    // ==================== 内部数据类 ====================

    /**
     * 反馈类型枚举
     */
    public enum FeedbackType implements Serializable {
        /** 确认欺诈 - 审核人确认告警正确 */
        CONFIRMED_FRAUD("确认欺诈"),
        /** 误报 - 正常行为被错误标记为欺诈 */
        FALSE_POSITIVE("误报"),
        /** 类型错误 - 确实存在异常但分类不正确 */
        INCORRECT_TYPE("类型错误");

        private final String description;

        FeedbackType(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    /**
     * 反馈记录 - 存储单次人工审核结果
     */
    public static class FeedbackRecord implements Serializable {
        public String alertId;
        public FeedbackType feedbackType;
        public String reviewerId;
        public String comment;
        public long timestamp;

        public FeedbackRecord() {}

        public FeedbackRecord(String alertId, FeedbackType feedbackType,
                              String reviewerId, String comment, long timestamp) {
            this.alertId = alertId;
            this.feedbackType = feedbackType;
            this.reviewerId = reviewerId;
            this.comment = comment;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("Feedback[alert=%s, type=%s, reviewer=%s, time=%d]",
                    alertId, feedbackType, reviewerId, timestamp);
        }
    }

    /**
     * 累计统计信息
     */
    public static class CumulativeStats implements Serializable {
        public int totalFeedback = 0;
        public int confirmedFraud = 0;
        public int falsePositive = 0;
        public int incorrectType = 0;

        public double getFalsePositiveRate() {
            return totalFeedback > 0 ? (double) falsePositive / totalFeedback : 0.0;
        }

        public double getConfirmRate() {
            return totalFeedback > 0 ? (double) confirmedFraud / totalFeedback : 0.0;
        }

        public CumulativeStats clone() {
            CumulativeStats copy = new CumulativeStats();
            copy.totalFeedback = this.totalFeedback;
            copy.confirmedFraud = this.confirmedFraud;
            copy.falsePositive = this.falsePositive;
            copy.incorrectType = this.incorrectType;
            return copy;
        }

        @Override
        public String toString() {
            return String.format(
                    "统计[总数=%d, 确认=%d, 误报=%d, 类型错误=%d, 误报率=%.2f%%]",
                    totalFeedback, confirmedFraud, falsePositive, incorrectType,
                    getFalsePositiveRate() * 100);
        }
    }

    /**
     * 反馈输出基类
     */
    public static class FeedbackOutput implements Serializable {
        public OutputType type;

        public FeedbackOutput(OutputType type) {
            this.type = type;
        }

        public enum OutputType {
            STATS, RETRAINING_SIGNAL
        }
    }

    /**
     * 反馈统计输出
     */
    public static class FeedbackStats extends FeedbackOutput {
        public int totalFeedback;
        public int confirmedFraud;
        public int falsePositive;
        public int incorrectType;
        public long reportTime;

        public FeedbackStats(int total, int confirmed, int fp, int incorrect, long time) {
            super(OutputType.STATS);
            this.totalFeedback = total;
            this.confirmedFraud = confirmed;
            this.falsePositive = fp;
            this.incorrectType = incorrect;
            this.reportTime = time;
        }

        public double getFalsePositiveRate() {
            return totalFeedback > 0 ? (double) falsePositive / totalFeedback : 0.0;
        }

        @Override
        public String toString() {
            return String.format(
                    "[反馈统计] 总数=%d, 确认欺诈=%d, 误报=%d, 类型错误=%d, 误报率=%.2f%%",
                    totalFeedback, confirmedFraud, falsePositive, incorrectType,
                    getFalsePositiveRate() * 100);
        }
    }

    /**
     * 重训练信号 - 触发ModelTrainer重新训练
     */
    public static class RetrainingSignal extends FeedbackOutput {
        public int feedbackCount;
        public Map<FeedbackType, Integer> typeDistribution;
        public FeedbackCollector.CumulativeStats cumulativeStats;
        public long triggerTime;

        public RetrainingSignal(int count, Map<FeedbackType, Integer> distribution,
                                FeedbackCollector.CumulativeStats stats) {
            super(OutputType.RETRAINING_SIGNAL);
            this.feedbackCount = count;
            this.typeDistribution = distribution;
            this.cumulativeStats = stats;
            this.triggerTime = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return String.format(
                    "[重训练信号] 触发反馈数=%d, 分布=%s, 累计误报率=%.2f%%",
                    feedbackCount, typeDistribution,
                    cumulativeStats.getFalsePositiveRate() * 100);
        }
    }
}
