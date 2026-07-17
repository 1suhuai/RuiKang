package com.fraud.detection.graph;

import com.fraud.detection.model.Transaction;
import com.fraud.detection.model.UserBehaviorSequence;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 图特征注入器
 * 将GraphAnalyzer的分析结果注入到UserBehaviorSequence
 * 扩展特征维度:23维基础特征 -> 30维完整特征
 * 是图分析与ML检测的桥梁
 * 在 BehaviorSequenceBuilder 之后运行，为序列补充图维度的特征
 */
public class GraphFeatureExtractor
        extends KeyedProcessFunction<String, UserBehaviorSequence, UserBehaviorSequence> {

    private transient GraphAnalyzer graphAnalyzer;

    // 维护每个账户的交易边（轻量级，只保留最近 N 条）
    private transient MapState<String, List<TransactionGraph.Edge>> recentOutEdges;
    private transient MapState<String, List<TransactionGraph.Edge>> recentInEdges;
    private static final int MAX_EDGES_PER_ACCOUNT = 50;

    @Override
    public void open(Configuration parameters) {
        graphAnalyzer = new GraphAnalyzer();

        recentOutEdges = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("gfe-out-edges", String.class, (Class<List<TransactionGraph.Edge>>) (Class<?>) List.class));
        recentInEdges = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("gfe-in-edges", String.class, (Class<List<TransactionGraph.Edge>>) (Class<?>) List.class));
    }

    @Override
    public void processElement(UserBehaviorSequence sequence, Context ctx,
                                Collector<UserBehaviorSequence> out) throws Exception {
        if (sequence == null || sequence.accountId == null) {
            out.collect(sequence);
            return;
        }

        // 从序列的交易中更新边信息
        if (sequence.transactions != null) {
            for (Transaction tx : sequence.transactions) {
                if (tx.nameOrig != null && tx.nameDest != null) {
                    TransactionGraph.Edge edge = new TransactionGraph.Edge(
                            tx.nameOrig, tx.nameDest, tx.amount, tx.eventTime, tx.type);

                    addEdgeWithLimit(recentOutEdges, tx.nameOrig, edge);

                    // 同时更新目标账户的入边
                    addEdgeWithLimit(recentInEdges, tx.nameDest, edge);
                }
            }
        }

        // 构建局部图并提取特征
        TransactionGraph localGraph = buildLocalGraph(sequence.accountId);
        double[] graphFeatures = graphAnalyzer.extractGraphFeatures(localGraph, sequence.accountId);
        double[] directionFeatures = graphAnalyzer.extractDirectionFeatures(localGraph, sequence.accountId);

        sequence.graphCycleCount = (int) graphFeatures[3];
        sequence.graphMaxChainDepth = (int) graphFeatures[4];
        sequence.graphCommunitySize = (int) graphFeatures[2];
        sequence.graphInAmount = graphFeatures[6];
        sequence.graphOutAmount = graphFeatures[7];
        sequence.graphFlowBalance = graphFeatures[6] > 0 ? graphFeatures[7] / graphFeatures[6] : 0;
        sequence.graphTotalDegree = (int) graphFeatures[5];

        sequence.directionAsymmetry = directionFeatures[0];
        sequence.inOutFreqRatio = directionFeatures[1];
        sequence.netFlowAsymmetry = directionFeatures[2];
        sequence.timeConcentration = directionFeatures[3];

        sequence.timeDensityRatio = calcTimeDensity(sequence);
        sequence.hourRiskScore = calcHourRisk(sequence);

        // 重新计算特征数组（包含新增的图特征）
        sequence.computeFeaturesWithGraph();

        out.collect(sequence);
    }

    private double calcTimeDensity(UserBehaviorSequence sequence) {
        if (sequence.transactions == null || sequence.transactions.size() < 2) return 0.0;

        long maxEventTime = Long.MIN_VALUE;
        for (Transaction tx : sequence.transactions) {
            if (tx.eventTime > maxEventTime) maxEventTime = tx.eventTime;
        }

        long fiveMinAgo = maxEventTime - 5 * 60 * 1000;
        long thirtyMinAgo = maxEventTime - 30 * 60 * 1000;

        int recent5Min = 0;
        int recent30Min = 0;
        for (Transaction tx : sequence.transactions) {
            if (tx.eventTime >= fiveMinAgo) recent5Min++;
            if (tx.eventTime >= thirtyMinAgo) recent30Min++;
        }

        if (recent30Min == 0) return 0.0;
        return (double) recent5Min / recent30Min;
    }

    private double calcHourRisk(UserBehaviorSequence sequence) {
        if (sequence.transactions == null || sequence.transactions.isEmpty()) return 0.0;

        double totalRisk = 0;
        for (Transaction tx : sequence.transactions) {
            int hour = tx.transactionHour;
            if (hour >= 0 && hour < 5) {
                totalRisk += 1.0;
            } else if ((hour >= 22 && hour <= 23) || (hour >= 5 && hour < 7)) {
                totalRisk += 0.6;
            } else if ((hour >= 20 && hour <= 21) || (hour >= 7 && hour < 9)) {
                totalRisk += 0.3;
            }
        }
        return Math.min(totalRisk / sequence.transactions.size(), 1.0);
    }

    private void addEdgeWithLimit(MapState<String, List<TransactionGraph.Edge>> state,
                                   String key, TransactionGraph.Edge edge) throws Exception {
        List<TransactionGraph.Edge> edges = state.get(key);
        if (edges == null) {
            edges = new ArrayList<>();
        }
        if (edges.size() >= MAX_EDGES_PER_ACCOUNT) {
            edges.remove(0); // 移除最老的边
        }
        edges.add(edge);
        state.put(key, edges);
    }

    private TransactionGraph buildLocalGraph(String accountId) throws Exception {
        TransactionGraph graph = new TransactionGraph();

        // 加载该账户的出边
        List<TransactionGraph.Edge> outEdgeList = recentOutEdges.get(accountId);
        if (outEdgeList != null) {
            for (TransactionGraph.Edge e : outEdgeList) {
                graph.addEdge(e.fromAccount, e.toAccount, e.amount, e.timestamp, e.txType);
            }
            // 加载目标账户的出边（二度邻居）
            for (TransactionGraph.Edge e : outEdgeList) {
                List<TransactionGraph.Edge> nextOut = recentOutEdges.get(e.toAccount);
                if (nextOut != null) {
                    for (TransactionGraph.Edge ne : nextOut) {
                        graph.addEdge(ne.fromAccount, ne.toAccount, ne.amount, ne.timestamp, ne.txType);
                    }
                }
            }
        }

        // 加载该账户的入边
        List<TransactionGraph.Edge> inEdgeList = recentInEdges.get(accountId);
        if (inEdgeList != null) {
            for (TransactionGraph.Edge e : inEdgeList) {
                graph.addEdge(e.fromAccount, e.toAccount, e.amount, e.timestamp, e.txType);
            }
            // 加载来源账户的入边（二度邻居）
            for (TransactionGraph.Edge e : inEdgeList) {
                List<TransactionGraph.Edge> nextIn = recentInEdges.get(e.fromAccount);
                if (nextIn != null) {
                    for (TransactionGraph.Edge ne : nextIn) {
                        graph.addEdge(ne.fromAccount, ne.toAccount, ne.amount, ne.timestamp, ne.txType);
                    }
                }
            }
        }

        return graph;
    }
}
