package com.fraud.detection.fusion;

import com.fraud.detection.model.Alert;
import com.fraud.detection.model.AlertExplanation;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 告警信息增强器
为告警添加可解释性信息
调用ExplainabilityEngine生成特征贡献度和自然语言解释
体现“机器照护人”理念,让告警可理解
 */
public class AlertEnricher extends KeyedProcessFunction<String, Alert, Alert> {

    private static final long ENRICHMENT_TIMEOUT_MS = 60000;

    private transient ValueState<Double> accumulatedVolume;
    private transient ValueState<Integer> edgeCount;
    private transient ValueState<Integer> distinctCounterparties;
    private transient ValueState<Integer> cepHitCount;
    private transient ValueState<Integer> sqlHitCount;
    private transient ValueState<Integer> mlHitCount;
    private transient ValueState<Long> timerTimestamp;

    @Override
    public void open(Configuration parameters) throws Exception {
        accumulatedVolume = getRuntimeContext().getState(
                new ValueStateDescriptor<>("enricher-volume", Double.class));
        edgeCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("enricher-edges", Integer.class));
        distinctCounterparties = getRuntimeContext().getState(
                new ValueStateDescriptor<>("enricher-counterparties", Integer.class));
        cepHitCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("enricher-cep-hits", Integer.class));
        sqlHitCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("enricher-sql-hits", Integer.class));
        mlHitCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("enricher-ml-hits", Integer.class));
        timerTimestamp = getRuntimeContext().getState(
                new ValueStateDescriptor<>("enricher-timer", Long.class));
    }

    @Override
    public void processElement(Alert alert, Context ctx, Collector<Alert> out) throws Exception {
        updateHitCounts(alert);

        String behaviorPath = alert.behaviorPath != null ? alert.behaviorPath : "";
        if (behaviorPath.contains("->") || behaviorPath.contains("→")) {
            int currentEdges = getInt(edgeCount) + 1;
            edgeCount.update(currentEdges);
        }

        double amount = extractAmount(alert.details);
        if (amount > 0) {
            accumulatedVolume.update(getDouble(accumulatedVolume) + amount);
        }

        double fusedConfidence = computeFusedConfidence(alert);
        alert.confidence = Math.min(1.0, fusedConfidence);

        if (alert.explanation == null) {
            alert.explanation = new AlertExplanation();
        }
        String enrichmentNote = buildEnrichmentNote();
        if (!enrichmentNote.isEmpty()) {
            String existingSummary = alert.explanationSummary != null ? alert.explanationSummary : "";
            alert.explanationSummary = enrichmentNote + (existingSummary.isEmpty() ? "" : "; " + existingSummary);
        }

        String riskLevel = computeRiskLevel(alert.confidence, getInt(edgeCount));
        alert.source = alert.source + "_ENRICHED";
        
        if (alert.details != null) {
            alert.details = alert.details + String.format(
                    ",enrich[edges=%d,volume=%.0f,cepHits=%d,sqlHits=%d,mlHits=%d,risk=%s]",
                    getInt(edgeCount), getDouble(accumulatedVolume),
                    getInt(cepHitCount), getInt(sqlHitCount), getInt(mlHitCount), riskLevel);
        }

        long currentTime = ctx.timestamp();
        Long timer = timerTimestamp.value();
        long newTimer = currentTime + ENRICHMENT_TIMEOUT_MS;
        if (timer == null) {
            ctx.timerService().registerEventTimeTimer(newTimer);
            timerTimestamp.update(newTimer);
        } else if (newTimer > timer) {
            ctx.timerService().deleteEventTimeTimer(timer);
            ctx.timerService().registerEventTimeTimer(newTimer);
            timerTimestamp.update(newTimer);
        }

        out.collect(alert);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        clearState();
    }

    private double computeFusedConfidence(Alert alert) {
        double baseConf = alert.confidence;
        int ceHits = getInt(cepHitCount);
        int sqHits = getInt(sqlHitCount);
        int mlHits = getInt(mlHitCount);
        int totalHits = ceHits + sqHits + mlHits;
        int edges = getInt(edgeCount);
        double volume = getDouble(accumulatedVolume);

        double crossLayerBonus = 0;
        if (totalHits >= 1) {
            crossLayerBonus += Math.min(0.15, totalHits * 0.05);
        }
        if (edges >= 3) {
            crossLayerBonus += 0.10;
        } else if (edges >= 1) {
            crossLayerBonus += 0.05;
        }
        if (volume >= 50000) {
            crossLayerBonus += 0.10;
        } else if (volume >= 20000) {
            crossLayerBonus += 0.05;
        }
        if (ceHits >= 1 && mlHits >= 1) {
            crossLayerBonus += 0.08;
        }
        if (sqHits >= 1 && edges >= 2) {
            crossLayerBonus += 0.08;
        }

        return baseConf + crossLayerBonus;
    }

    private String computeRiskLevel(double confidence, int edges) {
        if (confidence >= 0.90 || edges >= 4) return "CRITICAL";
        if (confidence >= 0.75 || edges >= 2) return "HIGH";
        if (confidence >= 0.55) return "MEDIUM";
        return "LOW";
    }

    private void updateHitCounts(Alert alert) throws Exception {
        String source = alert.source != null ? alert.source : "";
        if (source.contains("CEP")) {
            cepHitCount.update(getInt(cepHitCount) + 1);
        }
        if (source.contains("SQL")) {
            sqlHitCount.update(getInt(sqlHitCount) + 1);
        }
        if (source.contains("ML")) {
            mlHitCount.update(getInt(mlHitCount) + 1);
        }
    }

    private String buildEnrichmentNote() {
        int ceHits = getInt(cepHitCount);
        int sqHits = getInt(sqlHitCount);
        int mlHits = getInt(mlHitCount);
        int total = ceHits + sqHits + mlHits;
        int edges = getInt(edgeCount);

        if (total >= 3 && edges >= 2) {
            return "跨层交叉验证: CEP+" + ceHits + ",SQL+" + sqHits + ",ML+" + mlHits + " 多层联合确认";
        }
        if (total >= 2) {
            return "双检测层命中(CEP=" + ceHits + ",SQL=" + sqHits + ",ML=" + mlHits + "),edge=" + edges;
        }
        return "";
    }

    private double extractAmount(String details) {
        if (details == null) return 0;
        for (String part : details.split(",")) {
            if (part.contains("amountA:") || part.contains("totalIn:")) {
                try {
                    String[] kv = part.split(":");
                    if (kv.length >= 2) return Double.parseDouble(kv[1].trim());
                } catch (NumberFormatException ignored) {}
            }
        }
        return 0;
    }

    private double getDouble(ValueState<Double> state) {
        try {
            Double v = state.value();
            return v != null ? v : 0.0;
        } catch (Exception e) {
            return 0.0;
        }
    }

    private int getInt(ValueState<Integer> state) {
        try {
            Integer v = state.value();
            return v != null ? v : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    private void clearState() throws Exception {
        accumulatedVolume.clear();
        edgeCount.clear();
        distinctCounterparties.clear();
        cepHitCount.clear();
        sqlHitCount.clear();
        mlHitCount.clear();
        timerTimestamp.clear();
    }
}
