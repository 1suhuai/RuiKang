package com.fraud.detection.metrics;

import com.alibaba.fastjson.JSONObject;
import com.fraud.detection.model.Alert;
import com.fraud.detection.model.Transaction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;

/**
 * 基础指标计算器 - 只输出计数
 * Precision/Recall/F1 等直接读 fraud_validation_result 表用 Doris SQL 计算
 */
public class EvaluationMetricsCalculator extends CoProcessFunction<Transaction, Alert, String> {

    private transient ValueState<Long> totalTransactions;
    private transient ValueState<Long> totalAlerts;
    private transient ValueState<Long> cepAlerts;
    private transient ValueState<Long> mlAlerts;
    private transient ValueState<Long> fusionAlerts;
    private transient ValueState<Long> lastEmitTime;
    private transient ValueState<Long> eventsSinceLastEmit;
    private transient ValueState<Boolean> timerRegistered;

    private final long emitIntervalMs;
    private final long emitEveryEvents;

    // 降低输出频率: 每5000条事件或每60秒输出一次汇总，避免数据膨胀
    public EvaluationMetricsCalculator() {
        this(60000, 5000);
    }

    public EvaluationMetricsCalculator(long emitIntervalMs) {
        this(emitIntervalMs, 100);
    }

    public EvaluationMetricsCalculator(long emitIntervalMs, long emitEveryEvents) {
        this.emitIntervalMs = emitIntervalMs;
        this.emitEveryEvents = emitEveryEvents;
    }

    @Override
    public void open(Configuration parameters) {
        totalTransactions = getRuntimeContext().getState(new ValueStateDescriptor<>("metrics_total_tx", Long.class));
        totalAlerts = getRuntimeContext().getState(new ValueStateDescriptor<>("metrics_total_alerts", Long.class));
        cepAlerts = getRuntimeContext().getState(new ValueStateDescriptor<>("metrics_cep_alerts", Long.class));
        mlAlerts = getRuntimeContext().getState(new ValueStateDescriptor<>("metrics_ml_alerts", Long.class));
        fusionAlerts = getRuntimeContext().getState(new ValueStateDescriptor<>("metrics_fusion_alerts", Long.class));
        lastEmitTime = getRuntimeContext().getState(new ValueStateDescriptor<>("metrics_last_emit_time", Long.class));
        eventsSinceLastEmit = getRuntimeContext().getState(new ValueStateDescriptor<>("metrics_events", Long.class));
        timerRegistered = getRuntimeContext().getState(new ValueStateDescriptor<>("metrics_timer", Boolean.class));
    }

    @Override
    public void processElement1(Transaction tx, Context ctx, Collector<String> out) throws Exception {
        totalTransactions.update(value(totalTransactions) + 1);
        emitIfNeeded(ctx, out);
    }

    @Override
    public void processElement2(Alert alert, Context ctx, Collector<String> out) throws Exception {
        totalAlerts.update(value(totalAlerts) + 1);
        String source = alert.source == null ? "UNKNOWN" : alert.source;
        if (source.contains("CEP")) {
            cepAlerts.update(value(cepAlerts) + 1);
        }
        if (source.contains("ML")) {
            mlAlerts.update(value(mlAlerts) + 1);
        }
        if (source.contains("FUSION") || source.contains("MERGED")) {
            fusionAlerts.update(value(fusionAlerts) + 1);
        }
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
        out.collect(buildMetricsJson(now));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        timerRegistered.update(false);
        long currentTransactions = value(totalTransactions);
        long currentAlerts = value(totalAlerts);
        if (currentTransactions > 0 || currentAlerts > 0) {
            emit(timestamp, out);
            ctx.timerService().registerEventTimeTimer(timestamp + emitIntervalMs);
            timerRegistered.update(true);
        }
    }

    private String buildMetricsJson(long now) throws Exception {
        long totalTx = value(totalTransactions);
        long alerts = value(totalAlerts);
        double alertRate = totalTx > 0 ? (double) alerts / totalTx : 0.0;

        JSONObject json = new JSONObject(true);
        json.put("metric_time", formatTimeStr(now));
        json.put("total_transactions", totalTx);
        json.put("total_alerts", alerts);
        json.put("cep_alerts", value(cepAlerts));
        json.put("ml_alerts", value(mlAlerts));
        json.put("fusion_alerts", value(fusionAlerts));
        json.put("alert_rate", format(alertRate));
        return json.toJSONString();
    }

    private long value(ValueState<Long> state) throws Exception {
        Long value = state.value();
        return value == null ? 0L : value;
    }

    private double format(double value) {
        DecimalFormat decimalFormat = new DecimalFormat("0.0000");
        return Double.parseDouble(decimalFormat.format(value));
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
}
