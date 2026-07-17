package com.fraud.detection.graph;

import com.fraud.detection.model.Alert;
import com.fraud.detection.model.Transaction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Flink 算子：构建实时交易图并进行图分析
 * 按 nameOrig keyBy，维护每个账户的局部交易图
 * 每隔 N 笔交易或定时触发分析，输出图分析告警
 * 使用KeyedProcessFunction维护图结构
 * 每来一笔交易,添加一条有向边(转出方->转入方)
 * 定时输出更新后的图结构
 */
public class GraphBuilderProcessFunction
        extends KeyedProcessFunction<String, Transaction, Alert> {

    private static final int ANALYZE_EVERY_N_TX = 3;
    private static final long TIMER_INTERVAL_MS = 60000;
    private static final int MAX_EDGES_PER_ACCOUNT = 50;

    private static final StateTtlConfig GRAPH_TTL = StateTtlConfig
            .newBuilder(Time.minutes(2))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanupInRocksdbCompactFilter(60000L)
            .build();

    private transient GraphAnalyzer graphAnalyzer;

    // 每个账户的局部交易图（用序列化的边列表存储）
    private transient MapState<String, List<TransactionGraph.Edge>> outEdgeState;
    private transient MapState<String, List<TransactionGraph.Edge>> inEdgeState;
    private transient ValueState<Integer> txCountState;
    private transient ValueState<Integer> suspiciousTxCountState;
    private transient ValueState<Long> lastAnalyzeTime;
    private transient ValueState<Boolean> timerRegistered;
    private transient ValueState<Transaction> lastTxState;

    // IP/设备关联索引
    private transient MapState<String, List<String>> ipAccountState;
    private transient MapState<String, List<String>> deviceAccountState;

    @Override
    public void open(Configuration parameters) {
        graphAnalyzer = new GraphAnalyzer();

        MapStateDescriptor<String, List<TransactionGraph.Edge>> outDesc =
                new MapStateDescriptor<>("graph-out-edges", String.class, (Class<List<TransactionGraph.Edge>>) (Class<?>) List.class);
        outDesc.enableTimeToLive(GRAPH_TTL);
        outEdgeState = getRuntimeContext().getMapState(outDesc);

        MapStateDescriptor<String, List<TransactionGraph.Edge>> inDesc =
                new MapStateDescriptor<>("graph-in-edges", String.class, (Class<List<TransactionGraph.Edge>>) (Class<?>) List.class);
        inDesc.enableTimeToLive(GRAPH_TTL);
        inEdgeState = getRuntimeContext().getMapState(inDesc);

        ValueStateDescriptor<Integer> txCountDesc =
                new ValueStateDescriptor<>("graph-tx-count", Integer.class);
        txCountDesc.enableTimeToLive(GRAPH_TTL);
        txCountState = getRuntimeContext().getState(txCountDesc);

        ValueStateDescriptor<Integer> suspiciousCountDesc =
                new ValueStateDescriptor<>("graph-suspicious-tx-count", Integer.class);
        suspiciousCountDesc.enableTimeToLive(GRAPH_TTL);
        suspiciousTxCountState = getRuntimeContext().getState(suspiciousCountDesc);

        ValueStateDescriptor<Long> analyzeTimeDesc =
                new ValueStateDescriptor<>("graph-last-analyze", Long.class);
        analyzeTimeDesc.enableTimeToLive(GRAPH_TTL);
        lastAnalyzeTime = getRuntimeContext().getState(analyzeTimeDesc);

        ValueStateDescriptor<Boolean> timerDesc =
                new ValueStateDescriptor<>("graph-timer-registered", Boolean.class);
        timerDesc.enableTimeToLive(GRAPH_TTL);
        timerRegistered = getRuntimeContext().getState(timerDesc);

        MapStateDescriptor<String, List<String>> ipDesc =
                new MapStateDescriptor<>("graph-ip-accounts", String.class, (Class<List<String>>) (Class<?>) List.class);
        ipDesc.enableTimeToLive(GRAPH_TTL);
        ipAccountState = getRuntimeContext().getMapState(ipDesc);

        MapStateDescriptor<String, List<String>> deviceDesc =
                new MapStateDescriptor<>("graph-device-accounts", String.class, (Class<List<String>>) (Class<?>) List.class);
        deviceDesc.enableTimeToLive(GRAPH_TTL);
        deviceAccountState = getRuntimeContext().getMapState(deviceDesc);

        ValueStateDescriptor<Transaction> lastTxDesc =
                new ValueStateDescriptor<>("graph-last-tx", Transaction.class);
        lastTxDesc.enableTimeToLive(GRAPH_TTL);
        lastTxState = getRuntimeContext().getState(lastTxDesc);
    }

    @Override
    public void processElement(Transaction tx, Context ctx, Collector<Alert> out) throws Exception {
        if (!tx.isValid()) return;

        lastTxState.update(tx);

        String accountId = tx.nameOrig;

        // 更新出边
        TransactionGraph.Edge edge = new TransactionGraph.Edge(
                tx.nameOrig, tx.nameDest, tx.amount, tx.eventTime, tx.type);

        List<TransactionGraph.Edge> outEdges = getEdgeList(outEdgeState, accountId);
        if (outEdges.size() >= MAX_EDGES_PER_ACCOUNT) {
            outEdges.remove(0);
        }
        outEdges.add(edge);
        outEdgeState.put(accountId, outEdges);

        // 更新目标账户的入边
        List<TransactionGraph.Edge> destInEdges = getEdgeList(inEdgeState, tx.nameDest);
        if (destInEdges.size() >= MAX_EDGES_PER_ACCOUNT) {
            destInEdges.remove(0);
        }
        destInEdges.add(edge);
        inEdgeState.put(tx.nameDest, destInEdges);

        // 更新 IP/设备关联
        if (tx.ipSegment != null && !tx.ipSegment.isEmpty()) {
            List<String> ipAccounts = getAccountList(ipAccountState, tx.ipSegment);
            if (!ipAccounts.contains(accountId)) {
                if (ipAccounts.size() >= 20) {
                    ipAccounts.remove(0);
                }
                ipAccounts.add(accountId);
                ipAccountState.put(tx.ipSegment, ipAccounts);
            }
        }
        if (tx.deviceId != null && !tx.deviceId.isEmpty()) {
            List<String> deviceAccounts = getAccountList(deviceAccountState, tx.deviceId);
            if (!deviceAccounts.contains(accountId)) {
                if (deviceAccounts.size() >= 20) {
                    deviceAccounts.remove(0);
                }
                deviceAccounts.add(accountId);
                deviceAccountState.put(tx.deviceId, deviceAccounts);
            }
        }

        // 更新交易计数
        Integer count = txCountState.value();
        count = (count == null) ? 1 : count + 1;
        txCountState.update(count);

        if (isSuspicious(tx)) {
            Integer suspiciousCount = suspiciousTxCountState.value();
            suspiciousCount = (suspiciousCount == null) ? 1 : suspiciousCount + 1;
            suspiciousTxCountState.update(suspiciousCount);
        }

        if (count % ANALYZE_EVERY_N_TX == 0) {
            analyzeAndEmit(accountId, ctx, out);
        }

        // 注册定时器
        Boolean registered = timerRegistered.value();
        if (registered == null || !registered) {
            long timerTime = tx.eventTime + TIMER_INTERVAL_MS;
            ctx.timerService().registerEventTimeTimer(timerTime);
            timerRegistered.update(true);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        timerRegistered.update(false);

        String accountId = ctx.getCurrentKey();
        Long lastTime = lastAnalyzeTime.value();
        long now = System.currentTimeMillis();

        // 定时分析，避免过于频繁
        if (lastTime == null || now - lastTime >= TIMER_INTERVAL_MS) {
            analyzeAndEmit(accountId, ctx, out);
        }

        // 重新注册定时器
        ctx.timerService().registerEventTimeTimer(timestamp + TIMER_INTERVAL_MS);
        timerRegistered.update(true);
    }

    private void analyzeAndEmit(String accountId, Context ctx, Collector<Alert> out) throws Exception {
        Integer suspiciousCount = suspiciousTxCountState.value();
        if (suspiciousCount == null || suspiciousCount < 2) {
            return;
        }
        TransactionGraph graph = buildLocalGraph();
        List<Alert> alerts = graphAnalyzer.analyzeAccount(graph, accountId, System.currentTimeMillis());
        Transaction lastTx = lastTxState.value();
        for (Alert alert : alerts) {
            if (lastTx != null) {
                alert.withTransactionContext(lastTx);
            }
            out.collect(alert);
        }
        lastAnalyzeTime.update(System.currentTimeMillis());
    }

    private boolean isSuspicious(Transaction tx) {
        if ("HIGH".equals(tx.deviceRiskLevel)) return true;
        if ("ABROAD".equals(tx.isAbroad)) return true;
        if (tx.transactionHour <= 5 || tx.transactionHour >= 22) return true;
        if (tx.amount > 20000) return true;
        return false;
    }

    /**
     * 从 Flink 状态重建局部交易图
     * 从出边推导入边：遍历所有出边，对每条 A→B 的边，同时为 B 添加入边
     * 这样即使 keyBy(nameOrig)，也能看到当前账户的入边
     */
    private TransactionGraph buildLocalGraph() throws Exception {
        TransactionGraph graph = new TransactionGraph();
        String currentAccount = getCurrentKey();

        // 加载所有出边，同时构建入边索引
        for (Map.Entry<String, List<TransactionGraph.Edge>> entry : outEdgeState.entries()) {
            String srcAccount = entry.getKey();
            for (TransactionGraph.Edge edge : entry.getValue()) {
                graph.addEdge(edge.fromAccount, edge.toAccount, edge.amount, edge.timestamp, edge.txType);
            }
        }

        // 加载 IP 关联
        for (Map.Entry<String, List<String>> entry : ipAccountState.entries()) {
            String ip = entry.getKey();
            for (String account : entry.getValue()) {
                graph.addAccountAttributes(account, ip, null);
            }
        }

        // 加载设备关联
        for (Map.Entry<String, List<String>> entry : deviceAccountState.entries()) {
            String device = entry.getKey();
            for (String account : entry.getValue()) {
                graph.addAccountAttributes(account, null, device);
            }
        }

        return graph;
    }

    private String getCurrentKey() throws Exception {
        // 从outEdgeState的第一个key推断当前账户
        for (Map.Entry<String, List<TransactionGraph.Edge>> entry : outEdgeState.entries()) {
            return entry.getKey();
        }
        return null;
    }

    private List<TransactionGraph.Edge> getEdgeList(
            MapState<String, List<TransactionGraph.Edge>> state, String key) throws Exception {
        List<TransactionGraph.Edge> list = state.get(key);
        return list != null ? list : new ArrayList<>();
    }

    private List<String> getAccountList(
            MapState<String, List<String>> state, String key) throws Exception {
        List<String> list = state.get(key);
        return list != null ? list : new ArrayList<>();
    }
}
