package com.fraud.detection.metrics;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fraud.detection.model.Transaction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

/**
 * 端到端延迟追踪器 - RichFlatMapFunction 替代实现
 * 
 * 在Flink流式管道中追踪每笔交易在各处理阶段的延迟:
 * T0: Kafka接入时间 (Transaction.fromJsonObject时记录)
 * T1: CEP检测完成
 * T2: SQL检测完成
 * T3: 图分析完成
 * T4: ML检测完成
 * T5: 告警生成并输出
 * 
 * 维护滚动窗口统计: 平均值、P50、P95、P99、最大值
 * 每1000笔交易或每30秒输出一次延迟指标
 * 
 * 用法: 在管道各阶段之后调用 .process(new LatencyTracker(stageName))
 *       或在最终告警流使用 .process(new LatencyTracker("t5_final"))
 */
public class LatencyTracker extends ProcessFunction<Transaction, Transaction> {

    /** 输出间隔: 每N笔交易触发一次 */
    private static final int EMIT_EVERY_EVENTS = 1000;
    /** 输出间隔: 每N秒触发一次 */
    private static final long EMIT_EVERY_SECONDS = 30;

    /** 阶段名称: t1/t2/t3/t4/t5 */
    private final String stageName;

    /** 已处理交易计数 */
    private transient AtomicLong eventCount;
    /** 上次输出时间 */
    private transient AtomicLong lastEmitTime;
    /** 延迟样本集合(滚动窗口, 最大保留10000条) */
    private transient ConcurrentLinkedQueue<LatencySample> latencyWindow;
    /** 按阶段统计的延迟累加器 */
    private transient Map<String, DoubleAdder> stageLatencySum;
    /** 按阶段统计的样本计数 */
    private transient Map<String, LongAdder> stageLatencyCount;

    /**
     * @param stageName 阶段名称: "t1_cep", "t2_sql", "t3_graph", "t4_ml", "t5_alert"
     */
    public LatencyTracker(String stageName) {
        this.stageName = stageName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        eventCount = new AtomicLong(0);
        lastEmitTime = new AtomicLong(System.currentTimeMillis());
        latencyWindow = new ConcurrentLinkedQueue<>();
        stageLatencySum = new HashMap<>();
        stageLatencyCount = new HashMap<>();

        // 初始化所有阶段的累加器
        for (String stage : new String[]{"t1", "t2", "t3", "t4", "t5"}) {
            stageLatencySum.put(stage, new DoubleAdder());
            stageLatencyCount.put(stage, new LongAdder());
        }
    }

    @Override
    public void processElement(Transaction tx, Context ctx, Collector<Transaction> out) throws Exception {
        long now = System.currentTimeMillis();

        // 记录当前阶段时间戳
        tx.recordLatency(stageName, now);

        // 计算本阶段延迟 (从T0到当前阶段)
        long stageLatency = tx.getLatencyTimestamp(stageName) - tx.getLatencyTimestamp("t0");
        if (stageLatency >= 0) {
            stageLatencySum.get(stageName).add(stageLatency);
            stageLatencyCount.get(stageName).increment();
        }

        // 添加到延迟窗口
        LatencySample sample = new LatencySample(tx, now, stageLatency);
        latencyWindow.offer(sample);

        // 维护窗口大小上限, 防止内存溢出
        while (latencyWindow.size() > 10000) {
            latencyWindow.poll();
        }

        long count = eventCount.incrementAndGet();

        // 检查是否需要输出
        boolean shouldEmit = (count % EMIT_EVERY_EVENTS == 0) ||
                (now - lastEmitTime.get() >= EMIT_EVERY_SECONDS * 1000);

        if (shouldEmit) {
            String report = buildLatencyReport(count, now);
            System.out.println("[延迟指标] " + report);
            lastEmitTime.set(now);
        }

        out.collect(tx);
    }

    /**
     * 构建延迟指标报告 (JSON格式)
     */
    private String buildLatencyReport(long totalCount, long now) {
        JSONObject report = new JSONObject(true);
        report.put("report_time", now);
        report.put("total_transactions_tracked", totalCount);

        // 总体端到端延迟统计
        JSONObject overallLatency = computeOverallPercentiles();
        report.put("overall_end_to_end_latency_ms", overallLatency);

        // 各阶段延迟统计
        JSONObject stageBreakdown = new JSONObject(true);
        for (String stage : new String[]{"t1", "t2", "t3", "t4", "t5"}) {
            stageBreakdown.put(stage, computeStagePercentiles(stage));
        }
        report.put("stage_latency_breakdown_ms", stageBreakdown);

        // 竞赛目标对比
        report.put("competition_targets", buildCompetitionTargets(overallLatency));

        // 最近1000条样本的详细分布
        report.put("recent_sample_count", Math.min(latencyWindow.size(), 1000));

        return report.toJSONString();
    }

    /**
     * 计算总体端到端延迟的百分位数
     */
    private JSONObject computeOverallPercentiles() {
        List<Long> latencies = new ArrayList<>();
        for (LatencySample sample : latencyWindow) {
            long e2e = sample.transaction.getTotalLatency();
            if (e2e >= 0) {
                latencies.add(e2e);
            }
        }
        if (latencies.isEmpty()) {
            return emptyPercentileJson();
        }
        Collections.sort(latencies);
        return percentileJson(latencies);
    }

    /**
     * 计算单个阶段的延迟百分位数
     */
    private JSONObject computeStagePercentiles(String stage) {
        List<Long> latencies = new ArrayList<>();
        for (LatencySample sample : latencyWindow) {
            long stageTs = sample.transaction.getLatencyTimestamp(stage);
            long t0 = sample.transaction.getLatencyTimestamp("t0");
            if (stageTs > 0 && t0 > 0) {
                latencies.add(stageTs - t0);
            }
        }
        if (latencies.isEmpty()) {
            return emptyPercentileJson();
        }
        Collections.sort(latencies);
        return percentileJson(latencies);
    }

    /**
     * 根据排序后的延迟列表计算百分位数
     */
    private JSONObject percentileJson(List<Long> sortedLatencies) {
        int n = sortedLatencies.size();
        if (n == 0) return emptyPercentileJson();

        JSONObject json = new JSONObject(true);
        json.put("avg", round(sortedLatencies.stream().mapToLong(Long::longValue).average().orElse(0)));
        json.put("p50", sortedLatencies.get((int) (n * 0.50)));
        json.put("p90", sortedLatencies.get(Math.min((int) (n * 0.90), n - 1)));
        json.put("p95", sortedLatencies.get(Math.min((int) (n * 0.95), n - 1)));
        json.put("p99", sortedLatencies.get(Math.min((int) (n * 0.99), n - 1)));
        json.put("max", sortedLatencies.get(n - 1));
        json.put("min", sortedLatencies.get(0));
        json.put("sample_count", n);
        return json;
    }

    /**
     * 空百分位数JSON (无数据时)
     */
    private JSONObject emptyPercentileJson() {
        JSONObject json = new JSONObject(true);
        json.put("avg", 0);
        json.put("p50", 0);
        json.put("p90", 0);
        json.put("p95", 0);
        json.put("p99", 0);
        json.put("max", 0);
        json.put("min", 0);
        json.put("sample_count", 0);
        return json;
    }

    /**
     * 构建竞赛目标达成情况
     */
    private JSONObject buildCompetitionTargets(JSONObject overall) {
        JSONObject targets = new JSONObject(true);
        targets.put("target_e2e_p95_ms", 50);
        targets.put("target_e2e_p99_ms", 100);
        targets.put("target_throughput_tps", 10000);
        targets.put("target_memory_gb", 2);

        double p95 = overall.getDoubleValue("p95");
        double p99 = overall.getDoubleValue("p99");
        targets.put("actual_p95_ms", p95);
        targets.put("actual_p99_ms", p99);
        targets.put("p95_meets_target", p95 > 0 && p95 < 50);
        targets.put("p99_meets_target", p99 > 0 && p99 < 100);

        return targets;
    }

    private double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    /**
     * 延迟样本 - 单次交易在各阶段的延迟数据
     */
    static class LatencySample {
        final Transaction transaction;
        final long captureTime;
        final long currentStageLatency;

        LatencySample(Transaction tx, long captureTime, long stageLatency) {
            this.transaction = tx;
            this.captureTime = captureTime;
            this.currentStageLatency = stageLatency;
        }
    }
}
