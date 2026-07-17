package com.fraud.detection.metrics;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fraud.detection.model.Alert;
import com.fraud.detection.model.Transaction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 吞吐量监控器 - 追踪系统各层处理速率
 * 
 * 监控指标:
 * - TPS: 每秒处理交易数 (Transactions Per Second)
 * - APS: 每秒生成告警数 (Alerts Per Second)
 * - 各检测层吞吐量: CEP/SQL/Graph/ML
 * - 1分钟/5分钟/15分钟滚动平均
 * - 系统容量利用率
 * 
 * 用法: 在数据流中插入 .process(new ThroughputMonitor(layerName))
 *       例如: stream.process(new ThroughputMonitor("cep_layer"))
 */
public class ThroughputMonitor<T> extends ProcessFunction<T, T> {

    /** 滑动窗口大小(秒), 用于计算瞬时TPS */
    private static final int WINDOW_SIZE_SECONDS = 60;
    /** 输出间隔(秒) */
    private static final int EMIT_INTERVAL_SECONDS = 10;

    /** 层名称标识 */
    private final String layerName;
    /** 系统目标TPS (竞赛指标: 10000+) */
    private final double targetTps;

    /** 处理事件计数 */
    private transient AtomicLong totalCount;
    /** 告警计数 (仅Alert类型有效) */
    private transient AtomicLong alertCount;
    /** 上次输出时间 */
    private transient AtomicLong lastEmitTime;
    /** 时间窗口事件戳队列 */
    private transient ConcurrentLinkedQueue<Long> timestampWindow;
    /** 每分钟事件计数 (用于滚动平均) */
    private transient List<Long> minuteBuckets;
    /** 当前分钟桶索引 */
    private transient AtomicLong currentMinuteIndex;
    /** 启动时间 */
    private transient long startTime;

    /**
     * @param layerName 层名称, 如 "kafka_ingest", "cep_layer", "sql_layer", "graph_layer", "ml_layer", "alert_output"
     */
    public ThroughputMonitor(String layerName) {
        this(layerName, 10000.0);
    }

    /**
     * @param layerName 层名称
     * @param targetTps 目标TPS
     */
    public ThroughputMonitor(String layerName, double targetTps) {
        this.layerName = layerName;
        this.targetTps = targetTps;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        totalCount = new AtomicLong(0);
        alertCount = new AtomicLong(0);
        lastEmitTime = new AtomicLong(0);
        timestampWindow = new ConcurrentLinkedQueue<>();
        minuteBuckets = new ArrayList<>(Collections.nCopies(15, 0L)); // 15分钟桶
        currentMinuteIndex = new AtomicLong(0);
        startTime = System.currentTimeMillis();
    }

    @Override
    public void processElement(T element, Context ctx, Collector<T> out) throws Exception {
        long now = System.currentTimeMillis();
        totalCount.incrementAndGet();

        // 记录时间戳到滑动窗口
        timestampWindow.offer(now);

        // 清理超过窗口的旧时间戳
        long cutoff = now - WINDOW_SIZE_SECONDS * 1000;
        while (!timestampWindow.isEmpty() && timestampWindow.peek() < cutoff) {
            timestampWindow.poll();
        }

        // 更新分钟桶
        long minuteIndex = (now - startTime) / 60000;
        ensureMinuteBuckets(minuteIndex);
        synchronized (minuteBuckets) {
            minuteBuckets.set((int) (minuteIndex % 15), minuteBuckets.get((int) (minuteIndex % 15)) + 1);
        }

        // 如果是Alert类型, 增加告警计数
        if (element instanceof Alert) {
            alertCount.incrementAndGet();
        }

        // 检查是否需要输出
        long lastEmit = lastEmitTime.get();
        if (lastEmit == 0 || now - lastEmit >= EMIT_INTERVAL_SECONDS * 1000) {
            if (lastEmitTime.compareAndSet(lastEmit, now)) {
                String report = buildThroughputReport(now);
                System.out.println("[吞吐量指标] " + report);
            }
        }

        out.collect(element);
    }

    /**
     * 确保分钟桶数组足够大
     */
    private void ensureMinuteBuckets(long minuteIndex) {
        while (minuteBuckets.size() <= minuteIndex) {
            minuteBuckets.add(0L);
        }
    }

    /**
     * 构建吞吐量报告
     */
    private String buildThroughputReport(long now) {
        JSONObject report = new JSONObject(true);
        report.put("report_time", now);
        report.put("layer", layerName);
        report.put("uptime_seconds", (now - startTime) / 1000);

        // 瞬时TPS (最近60秒)
        double instantTps = computeInstantTps(now);
        report.put("instant_tps", round(instantTps));

        // 瞬时APS
        double instantAps = computeInstantAps(now);
        report.put("instant_aps", round(instantAps));

        // 累计计数
        report.put("total_processed", totalCount.get());
        report.put("total_alerts", alertCount.get());

        // 1分钟/5分钟/15分钟滚动平均
        report.put("rolling_average_tps", computeRollingAverages(now));

        // 容量利用率
        double utilization = targetTps > 0 ? (instantTps / targetTps * 100.0) : 0;
        report.put("capacity_utilization_percent", round(Math.min(utilization, 100.0)));
        report.put("target_tps", targetTps);
        report.put("meets_target", instantTps >= targetTps);

        return report.toJSONString();
    }

    /**
     * 计算瞬时TPS (基于滑动窗口)
     */
    private double computeInstantTps(long now) {
        long cutoff = now - WINDOW_SIZE_SECONDS * 1000;
        long countInWindow = 0;
        for (Long ts : timestampWindow) {
            if (ts >= cutoff) countInWindow++;
        }
        return countInWindow / (double) WINDOW_SIZE_SECONDS;
    }

    /**
     * 计算瞬时APS (告警/秒)
     * 注意: APS基于Alert流, 如果当前层不是Alert流则返回0
     */
    private double computeInstantAps(long now) {
        // 简化计算: 使用累计告警数/运行时间
        long uptimeSeconds = Math.max(1, (now - startTime) / 1000);
        return alertCount.get() / (double) uptimeSeconds;
    }

    /**
     * 计算1/5/15分钟滚动平均TPS
     */
    private JSONObject computeRollingAverages(long now) {
        JSONObject rolling = new JSONObject(true);

        // 1分钟平均: 最近1个桶
        rolling.put("1min", round(computeAverageForMinutes(1)));

        // 5分钟平均: 最近5个桶
        rolling.put("5min", round(computeAverageForMinutes(5)));

        // 15分钟平均: 最近15个桶
        rolling.put("15min", round(computeAverageForMinutes(15)));

        return rolling;
    }

    /**
     * 计算最近N分钟的平均TPS
     */
    private double computeAverageForMinutes(int minutes) {
        long currentMinute = (System.currentTimeMillis() - startTime) / 60000;
        int size = minuteBuckets.size();
        if (size == 0) return 0;

        long total = 0;
        int count = 0;
        for (int i = 0; i < minutes; i++) {
            int idx = (int) ((currentMinute - i) % size);
            if (idx >= 0 && idx < size) {
                total += minuteBuckets.get(idx);
                count++;
            }
        }

        return count > 0 ? total / (double) (count * 60) : 0;
    }

    private double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    // ========== 便捷工厂方法 ==========

    /**
     * 创建Kafka接入层吞吐量监控器
     */
    public static <T> ThroughputMonitor<T> kafkaIngest() {
        return new ThroughputMonitor<>("kafka_ingest", 10000.0);
    }

    /**
     * 创建CEP层吞吐量监控器
     */
    public static <T> ThroughputMonitor<T> cepLayer() {
        return new ThroughputMonitor<>("cep_layer", 8000.0);
    }

    /**
     * 创建SQL层吞吐量监控器
     */
    public static <T> ThroughputMonitor<T> sqlLayer() {
        return new ThroughputMonitor<>("sql_layer", 7000.0);
    }

    /**
     * 创建Graph层吞吐量监控器
     */
    public static <T> ThroughputMonitor<T> graphLayer() {
        return new ThroughputMonitor<>("graph_layer", 5000.0);
    }

    /**
     * 创建ML层吞吐量监控器
     */
    public static <T> ThroughputMonitor<T> mlLayer() {
        return new ThroughputMonitor<>("ml_layer", 3000.0);
    }

    /**
     * 创建Alert输出层吞吐量监控器
     */
    public static ThroughputMonitor<Alert> alertOutput() {
        return new ThroughputMonitor<>("alert_output", 1000.0);
    }
}
