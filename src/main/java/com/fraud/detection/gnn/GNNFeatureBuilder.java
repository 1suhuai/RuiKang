package com.fraud.detection.gnn;

import com.fraud.detection.model.Transaction;
import com.fraud.detection.model.UserBehaviorSequence;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * GNN 特征构建器
 * 
 * 功能:
 * 1. 从交易流中构建 GNN 输入特征
 * 2. 维护每个账户的滑动交易窗口
 * 3. 提取8维GNN专属特征，将特征从36维扩展到44维
 * 
 * GNN 特征维度说明 (8维):
 *   [0] nodeDegree: 节点度（出度+入度归一化）
 *   [1] edgeWeightMean: 邻居边权重均值（平均交易金额）
 *   [2] edgeWeightStd: 邻居边权重标准差
 *   [3] neighborRiskScore: 邻居平均风险分（基于历史交易模式）
 *   [4] communityMembership: 社区成员数（同IP/设备关联账户数）
 *   [5] clusteringCoefficient: 聚类系数（邻居间连接密度）
 *   [6] betweennessCentrality: 介数中心性（资金中转枢纽程度）
 *   [7] graphSAGEEmbeddingNorm: GraphSAGE 嵌入范数（邻居信息聚合强度）
 * 
 * 使用方式: 在 GraphFeatureExtractor 之后接入，输出扩展后的 UserBehaviorSequence
 */
public class GNNFeatureBuilder
        extends KeyedProcessFunction<String, UserBehaviorSequence, UserBehaviorSequence> {

    /** GNN 特征维度数 */
    private static final int GNN_FEATURE_DIM = 8;

    /** 最大滑动窗口交易数 */
    private static final int MAX_WINDOW_SIZE = 20;

    /** GraphSAGE 嵌入计算器 */
    private transient GraphSAGEEmbedding embeddingEngine;

    /** 滑动窗口：当前账户的交易记录 */
    private transient ListState<Transaction> windowTransactions;

    /** 账户的全局交易计数 */
    private transient ValueState<Integer> totalTxCount;

    /** 邻居账户映射：accountId → (neighborId → 累计交易金额) */
    private transient Map<String, Map<String, Double>> localNeighborAmounts;

    /** 社区关联：账户 → 关联IP/设备集合 */
    private transient Map<String, Set<String>> localCommunityAttributes;

    /** 全局邻居图（跨账户），用于社区和聚类计算 */
    private transient Map<String, Map<String, Set<String>>> localGlobalNeighborGraph;

    public GNNFeatureBuilder() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        embeddingEngine = new GraphSAGEEmbedding();
        localNeighborAmounts = new HashMap<>();
        localCommunityAttributes = new HashMap<>();
        localGlobalNeighborGraph = new HashMap<>();
    }

    @Override
    public void processElement(UserBehaviorSequence sequence, Context ctx,
                                Collector<UserBehaviorSequence> out) throws Exception {
        if (sequence == null || sequence.accountId == null) {
            out.collect(sequence);
            return;
        }

        // 1. 更新滑动窗口
        updateWindow(sequence);

        // 2. 构建邻居信息和社区属性
        Map<String, Double> neighbors = buildNeighborMap(sequence);
        Set<String> communityAttrs = buildCommunityAttributes(sequence);

        // 3. 更新全局邻居图
        updateGlobalNeighborGraph(neighbors);

        // 4. 计算 GNN 特征（8维）
        double[] gnnFeatures = computeGNNFeatures(
                sequence, neighbors, communityAttrs, sequence.transactions);

        // 5. 计算 GraphSAGE 嵌入并更新缓存
        updateGNNEmbedding(sequence.accountId, sequence.features, neighbors);

        // 6. 扩展特征向量：36维 → 44维
        extendFeatureVector(sequence, gnnFeatures);

        out.collect(sequence);
    }

    /**
     * 更新滑动窗口，保持最近 MAX_WINDOW_SIZE 笔交易
     */
    private void updateWindow(UserBehaviorSequence sequence) throws Exception {
        List<Transaction> txList = new ArrayList<>();
        for (Transaction tx : windowTransactions.get()) {
            txList.add(tx);
        }

        // 添加新交易
        if (sequence.transactions != null) {
            for (Transaction tx : sequence.transactions) {
                txList.add(tx);
            }
        }

        // 按时间排序，保留最新 MAX_WINDOW_SIZE 笔
        txList.sort(Comparator.comparingLong(tx -> tx.eventTime));
        if (txList.size() > MAX_WINDOW_SIZE) {
            txList = txList.subList(txList.size() - MAX_WINDOW_SIZE, txList.size());
        }

        // 更新状态
        windowTransactions.clear();
        for (Transaction tx : txList) {
            windowTransactions.add(tx);
        }
    }

    /**
     * 构建邻居映射：neighborId → 累计交易金额
     */
    private Map<String, Double> buildNeighborMap(UserBehaviorSequence sequence) throws Exception {
        String key = getRuntimeContext().getIndexOfThisSubtask() + "_" + sequence.accountId;
        Map<String, Double> neighbors = localNeighborAmounts.computeIfAbsent(key, k -> new HashMap<>());

        if (sequence.transactions != null) {
            for (Transaction tx : sequence.transactions) {
                // 转出目标
                if (tx.nameDest != null && !tx.nameDest.equals("UNKNOWN")) {
                    neighbors.merge(tx.nameDest, tx.amount, Double::sum);
                }
            }
        }

        localNeighborAmounts.put(key, neighbors);
        return neighbors;
    }

    /**
     * 构建社区属性集合（IP段 + 设备ID）
     */
    private Set<String> buildCommunityAttributes(UserBehaviorSequence sequence) throws Exception {
        String key = getRuntimeContext().getIndexOfThisSubtask() + "_" + sequence.accountId;
        Set<String> attrs = localCommunityAttributes.computeIfAbsent(key, k -> new HashSet<>());

        if (sequence.transactions != null) {
            for (Transaction tx : sequence.transactions) {
                if (tx.ipSegment != null && !tx.ipSegment.equals("UNKNOWN")) {
                    attrs.add("IP:" + tx.ipSegment);
                }
                if (tx.deviceId != null && !tx.deviceId.equals("UNKNOWN")) {
                    attrs.add("DEV:" + tx.deviceId);
                }
            }
        }

        localCommunityAttributes.put(key, attrs);
        return attrs;
    }

    /**
     * 更新全局邻居图（用于聚类和介数计算）
     */
    private void updateGlobalNeighborGraph(Map<String, Double> neighbors) throws Exception {
        // 这里简化处理：只维护当前KeyedStream内的邻居关系
        // 实际生产中应使用侧输出流或广播状态共享全图信息
    }

    /**
     * 计算8维 GNN 特征
     */
    private double[] computeGNNFeatures(UserBehaviorSequence sequence,
                                         Map<String, Double> neighbors,
                                         Set<String> communityAttrs,
                                         List<Transaction> transactions) throws Exception {
        double[] gnnFeatures = new double[GNN_FEATURE_DIM];

        // [0] 节点度归一化：(outDegree + inDegree) / MAX_WINDOW_SIZE
        int nodeDegree = (neighbors != null) ? neighbors.size() : 0;
        gnnFeatures[0] = Math.min(1.0, (double) nodeDegree / MAX_WINDOW_SIZE);

        // [1][2] 邻居边权重统计（均值和标准差）
        if (neighbors != null && !neighbors.isEmpty()) {
            double sum = 0.0;
            double sumSq = 0.0;
            int count = neighbors.size();
            for (double amount : neighbors.values()) {
                sum += amount;
                sumSq += amount * amount;
            }
            double mean = sum / count;
            double std = Math.sqrt(Math.max(sumSq / count - mean * mean, 0));
            gnnFeatures[1] = mean / 10000.0;  // 归一化到合理范围
            gnnFeatures[2] = std / 10000.0;
        }

        // [3] 邻居风险评分
        gnnFeatures[3] = computeNeighborRiskScore(neighbors, transactions);

        // [4] 社区成员数
        gnnFeatures[4] = (communityAttrs != null)
                ? Math.min(1.0, communityAttrs.size() / 10.0)
                : 0.0;

        // [5] 聚类系数：邻居间的连接密度
        gnnFeatures[5] = computeClusteringCoefficient(sequence, neighbors);

        // [6] 介数中心性：作为资金中转枢纽的程度
        gnnFeatures[6] = computeBetweennessCentrality(transactions);

        // [7] GraphSAGE 嵌入范数
        double[] cachedEmbedding = embeddingEngine.getCachedEmbedding(sequence.accountId);
        if (cachedEmbedding != null) {
            gnnFeatures[7] = vectorNorm(cachedEmbedding);
        } else {
            // 首次计算：基于当前特征和邻居生成嵌入
            List<GraphSAGEEmbedding.NeighborInfo> neighborInfos = buildNeighborInfoList(neighbors);
            double[] embedding = embeddingEngine.computeEmbedding(
                    sequence.accountId, sequence.features, neighborInfos, 2);
            gnnFeatures[7] = vectorNorm(embedding);
        }

        return gnnFeatures;
    }

    /**
     * 计算邻居风险评分
     * 
     * 基于历史交易模式评估邻居的风险：
     * - 大额交易邻居风险更高
     * - 高频交易邻居风险更高
     * - 高风险设备/境外交易邻居风险更高
     */
    private double computeNeighborRiskScore(Map<String, Double> neighbors,
                                             List<Transaction> transactions) {
        if (neighbors == null || neighbors.isEmpty()) {
            return 0.0;
        }

        double totalRisk = 0.0;
        int totalNeighbors = 0;

        // 基于交易特征计算每个邻居的风险分
        for (Map.Entry<String, Double> entry : neighbors.entrySet()) {
            double risk = 0.0;
            double amount = entry.getValue();

            // 大额交易风险
            if (amount > 50000) risk += 0.3;
            else if (amount > 20000) risk += 0.2;
            else if (amount > 10000) risk += 0.1;

            // 高频交互风险
            int interactionCount = countInteractionsWithNeighbor(entry.getKey(), transactions);
            if (interactionCount >= 5) risk += 0.2;
            else if (interactionCount >= 3) risk += 0.1;

            totalRisk += Math.min(1.0, risk);
            totalNeighbors++;
        }

        return totalNeighbors > 0 ? totalRisk / totalNeighbors : 0.0;
    }

    /**
     * 统计与特定邻居的交互次数
     */
    private int countInteractionsWithNeighbor(String neighborId, List<Transaction> transactions) {
        if (transactions == null) return 0;
        int count = 0;
        for (Transaction tx : transactions) {
            if (neighborId.equals(tx.nameDest) || neighborId.equals(tx.nameOrig)) {
                count++;
            }
        }
        return count;
    }

    /**
     * 计算聚类系数
     * 
     * 聚类系数 = 邻居间实际连接数 / 邻居间最大可能连接数
     * 高聚类系数意味着该账户的邻居也互相交易（可能是团伙）
     */
    private double computeClusteringCoefficient(UserBehaviorSequence sequence,
                                                  Map<String, Double> neighbors) {
        if (neighbors == null || neighbors.size() < 2) {
            return 0.0;
        }

        int neighborCount = neighbors.size();
        int possibleConnections = neighborCount * (neighborCount - 1) / 2;
        if (possibleConnections == 0) return 0.0;

        // 统计邻居间的实际连接数
        int actualConnections = 0;
        Set<String> neighborIds = new HashSet<>(neighbors.keySet());

        if (sequence.transactions != null) {
            Set<String> observedPairs = new HashSet<>();
            for (Transaction tx : sequence.transactions) {
                if (neighborIds.contains(tx.nameDest) && !tx.nameDest.equals(sequence.accountId)) {
                    // 这里简化处理：观察到的交易目标如果是邻居，则认为有连接
                    // 实际应该检查邻居之间是否有直接交易
                }
            }
            // 简化实现：基于交易模式推断
            actualConnections = estimateNeighborConnections(neighbors, sequence.transactions);
        }

        return Math.min(1.0, (double) actualConnections / possibleConnections);
    }

    /**
     * 估算邻居间连接数（简化版本）
     */
    private int estimateNeighborConnections(Map<String, Double> neighbors,
                                             List<Transaction> transactions) {
        if (transactions == null) return 0;

        Set<String> neighborIds = new HashSet<>(neighbors.keySet());
        int connections = 0;

        // 检查交易中的来源和目标是否都是邻居
        for (Transaction tx : transactions) {
            if (neighborIds.contains(tx.nameOrig) && neighborIds.contains(tx.nameDest)) {
                connections++;
            }
        }

        return Math.min(connections, neighbors.size() * (neighbors.size() - 1) / 2);
    }

    /**
     * 计算介数中心性
     * 
     * 衡量账户作为资金中转枢纽的程度：
     * 介数 = 流入转出比接近1.0 且 交易对手数量多
     */
    private double computeBetweennessCentrality(List<Transaction> transactions) {
        if (transactions == null || transactions.isEmpty()) {
            return 0.0;
        }

        Set<String> origins = new HashSet<>();
        Set<String> dests = new HashSet<>();
        double totalIn = 0.0;
        double totalOut = 0.0;

        for (Transaction tx : transactions) {
            origins.add(tx.nameOrig);
            dests.add(tx.nameDest);
            if ("TRANSFER".equals(tx.type)) {
                totalOut += tx.amount;
            } else {
                totalIn += tx.amount;
            }
        }

        // 中转枢纽特征：既有大量转入又有大量转出
        double balanceRatio = (totalIn + totalOut) > 0
                ? 2.0 * Math.min(totalIn, totalOut) / (totalIn + totalOut)
                : 0.0;

        int uniqueCounterparties = origins.size() + dests.size();
        double degreeFactor = Math.min(1.0, uniqueCounterparties / 10.0);

        return balanceRatio * degreeFactor;
    }

    /**
     * 构建 NeighborInfo 列表用于 GraphSAGE 嵌入计算
     */
    private List<GraphSAGEEmbedding.NeighborInfo> buildNeighborInfoList(Map<String, Double> neighbors) {
        List<GraphSAGEEmbedding.NeighborInfo> infos = new ArrayList<>();
        if (neighbors != null) {
            for (Map.Entry<String, Double> entry : neighbors.entrySet()) {
                infos.add(new GraphSAGEEmbedding.NeighborInfo(
                        entry.getKey(), entry.getValue(), System.currentTimeMillis()));
            }
        }
        return infos;
    }

    /**
     * 更新 GraphSAGE 嵌入缓存
     */
    private void updateGNNEmbedding(String accountId, double[] selfFeatures,
                                     Map<String, Double> neighbors) {
        List<GraphSAGEEmbedding.NeighborInfo> neighborInfos = buildNeighborInfoList(neighbors);
        embeddingEngine.updateEmbedding(accountId, selfFeatures, neighborInfos, 2);
    }

    /**
     * 扩展特征向量：36维 → 44维
     * 
     * 将GNN特征追加到原始特征向量末尾
     */
    private void extendFeatureVector(UserBehaviorSequence sequence, double[] gnnFeatures) {
        if (sequence.features == null) {
            sequence.features = new double[36];
        }

        int originalDim = sequence.features.length;
        int newDim = originalDim + GNN_FEATURE_DIM;
        double[] extendedFeatures = new double[newDim];

        // 复制原始特征
        System.arraycopy(sequence.features, 0, extendedFeatures, 0, originalDim);
        // 追加GNN特征
        System.arraycopy(gnnFeatures, 0, extendedFeatures, originalDim, GNN_FEATURE_DIM);

        sequence.features = extendedFeatures;
    }

    /**
     * 计算向量 L2 范数
     */
    private double vectorNorm(double[] vector) {
        double sum = 0.0;
        for (double v : vector) {
            sum += v * v;
        }
        return Math.sqrt(sum);
    }

    /**
     * 获取嵌入引擎实例（用于调试）
     */
    public GraphSAGEEmbedding getEmbeddingEngine() {
        return embeddingEngine;
    }
}
