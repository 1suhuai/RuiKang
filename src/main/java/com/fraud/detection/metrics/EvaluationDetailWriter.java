package com.fraud.detection.metrics;

import com.alibaba.fastjson.JSONObject;
import com.fraud.detection.model.Alert;
import com.fraud.detection.model.Transaction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 按类型评估详情写入器
 * 分别计算每种欺诈类型的P/R/F1/平均置信度
 * 输出11种欺诈类型的细分评估结果
 * 结果写入Doris fraud_evaluation_detail表
 */
public class EvaluationDetailWriter extends CoProcessFunction<Transaction, Alert, String> {

    private transient ValueState<Long> totalTransactions;
    private transient ValueState<Long> lastEmitTime;
    private transient ValueState<Long> eventsSinceLastEmit;
    private transient ValueState<Boolean> timerRegistered;

    private transient MapState<String, Long> alertsByFraudType;
    private transient MapState<String, Long> realFraudByType;
    private transient MapState<String, Double> confidenceSumByType;
    private transient MapState<String, Long> confidenceCountByType;

    private final long emitIntervalMs;
    private final long emitEveryEvents;

    // 降低输出频率: 每5000条事件或每60秒输出一次汇总，避免数据膨胀
    public EvaluationDetailWriter() {
        this(60000, 5000);
    }

    public EvaluationDetailWriter(long emitIntervalMs, long emitEveryEvents) {
        this.emitIntervalMs = emitIntervalMs;
        this.emitEveryEvents = emitEveryEvents;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        totalTransactions = getRuntimeContext().getState(new ValueStateDescriptor<>("detail_total_transactions", Long.class));
        lastEmitTime = getRuntimeContext().getState(new ValueStateDescriptor<>("detail_last_emit_time", Long.class));
        eventsSinceLastEmit = getRuntimeContext().getState(new ValueStateDescriptor<>("detail_events_since_last_emit", Long.class));
        timerRegistered = getRuntimeContext().getState(new ValueStateDescriptor<>("detail_timer_registered", Boolean.class));

        alertsByFraudType = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("detail_alerts_by_type", String.class, Long.class));
        realFraudByType = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("detail_real_fraud_by_type", String.class, Long.class));
        confidenceSumByType = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("detail_confidence_sum_by_type", String.class, Double.class));
        confidenceCountByType = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("detail_confidence_count_by_type", String.class, Long.class));
    }

    @Override
    public void processElement1(Transaction tx, Context ctx, Collector<String> out) throws Exception {
        totalTransactions.update(value(totalTransactions) + 1);

        if (tx.isFraud == 1 || tx.isFlaggedFraud == 1) {
            String fraudType = tx.fraudType != null && !tx.fraudType.isEmpty() ? tx.fraudType : "UNKNOWN";
            Long current = realFraudByType.get(fraudType);
            realFraudByType.put(fraudType, (current != null ? current : 0L) + 1);
        }
        emitIfNeeded(ctx, out);
    }

    @Override
    public void processElement2(Alert alert, Context ctx, Collector<String> out) throws Exception {
        String fraudType = alert.fraudType != null && !alert.fraudType.isEmpty() ? alert.fraudType : "UNKNOWN";

        Long currentAlertCount = alertsByFraudType.get(fraudType);
        alertsByFraudType.put(fraudType, (currentAlertCount != null ? currentAlertCount : 0L) + 1);

        Double currentConfidenceSum = confidenceSumByType.get(fraudType);
        confidenceSumByType.put(fraudType, (currentConfidenceSum != null ? currentConfidenceSum : 0.0) + alert.confidence);

        Long currentConfidenceCount = confidenceCountByType.get(fraudType);
        confidenceCountByType.put(fraudType, (currentConfidenceCount != null ? currentConfidenceCount : 0L) + 1);

        emitIfNeeded(ctx, out);
    }

    private void emitIfNeeded(Context ctx, Collector<String> out) throws Exception {
        long now = ctx.timestamp();
        long events = value(eventsSinceLastEmit) + 1;
        eventsSinceLastEmit.update(events);

        Boolean registered = timerRegistered.value();
        if (registered == null || !registered) {
            ctx.timerService().registerEventTimeTimer(now + emitIntervalMs);
            timerRegistered.update(true);
        }

        Long last = lastEmitTime.value();
        if (last == null || events >= emitEveryEvents || now - last >= emitIntervalMs) {
            emit(now, out);
        }
    }

    private void emit(long now, Collector<String> out) throws Exception {
        lastEmitTime.update(now);
        eventsSinceLastEmit.update(0L);

        List<String> jsons = buildDetailJsons(now);
        for (String json : jsons) {
            out.collect(json);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        timerRegistered.update(false);
        long currentTransactions = value(totalTransactions);
        if (currentTransactions > 0) {
            emit(timestamp, out);
            ctx.timerService().registerEventTimeTimer(timestamp + emitIntervalMs);
            timerRegistered.update(true);
        }
    }

    private List<String> buildDetailJsons(long now) throws Exception {
        List<String> results = new ArrayList<>();

        for (Map.Entry<String, Long> entry : alertsByFraudType.entries()) {
            String fraudType = entry.getKey();
            long alertCount = entry.getValue();

            long realCount = 0;
            Long realValue = realFraudByType.get(fraudType);
            if (realValue != null) {
                realCount = realValue;
            }

            double typePrecision = alertCount == 0 ? 0.0 : Math.min(1.0, realCount * 1.0 / alertCount);
            double typeRecall = realCount == 0 ? 0.0 : Math.min(1.0, alertCount * 1.0 / realCount);
            double typeF1 = (typePrecision + typeRecall) == 0 ? 0.0
                    : 2.0 * typePrecision * typeRecall / (typePrecision + typeRecall);

            double avgConfidence = 0.0;
            Long confidenceCount = confidenceCountByType.get(fraudType);
            Double confidenceSum = confidenceSumByType.get(fraudType);
            if (confidenceCount != null && confidenceCount > 0 && confidenceSum != null) {
                avgConfidence = confidenceSum / confidenceCount;
            }

            JSONObject json = new JSONObject(true);
            json.put("metric_time", formatTimeStr(now));
            json.put("fraud_type", fraudType);
            json.put("alert_count", alertCount);
            json.put("real_count", realCount);
            json.put("type_precision", String.format("%.4f", typePrecision));
            json.put("type_recall", String.format("%.4f", typeRecall));
            json.put("type_f1", String.format("%.4f", typeF1));
            json.put("avg_confidence", String.format("%.4f", avgConfidence));

            results.add(json.toJSONString());
        }

        return results;
    }

    private static String formatTimeStr(long ts) {
        if (ts <= 0) return "00:00:00.000";
        java.util.Calendar cal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("Asia/Shanghai"));
        cal.setTimeInMillis(ts);
        int h = cal.get(java.util.Calendar.HOUR_OF_DAY);
        int m = cal.get(java.util.Calendar.MINUTE);
        int s = cal.get(java.util.Calendar.SECOND);
        int ms = cal.get(java.util.Calendar.MILLISECOND);
        return String.format("%02d:%02d:%02d.%03d", h, m, s, ms);
    }

    private long value(ValueState<Long> state) throws Exception {
        Long value = state.value();
        return value == null ? 0L : value;
    }
}
