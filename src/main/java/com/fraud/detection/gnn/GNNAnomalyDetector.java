package com.fraud.detection.gnn;

import com.fraud.detection.model.Alert;
import com.fraud.detection.model.Transaction;
import com.fraud.detection.model.UserBehaviorSequence;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * GNN 异常检测器 — 基于图神经网络的欺诈检测层
 * 
 * 本组件实现了一个简化但真实的 Graph Neural Network (GNN) 评分机制，
 * 与现有的 ML Anomaly Detector（逻辑回归/GBDT/孤立森林）形成互补。
 * 
 * 核心算法:
 * 1. GraphSAGE 风格邻域采样：按交易金额采样 Top-K 邻居
 * 2. 2-hop 消息传递：聚合直接邻居 + 间接邻居的信息
 * 3. 节点特征融合：自身特征 + 邻居聚合特征 → 风险评分
 * 4. 结构异常检测：识别图拓扑模式异常（环路、星型分散、链式转账）
 * 5. 阈值告警生成：threshold = 0.4
 * 
 * 特征维度:
 * - 输入: 36维原始特征 或 44维扩展特征（含GNN特征）
 * - 邻居特征: 36维
 * - 聚合后: 融合向量 → 欺诈概率
 * 
 * 集成方式:
 *   DataStream<Alert> gnnAlerts = testEnrichedSequences
 *       .flatMap(new GNNAnomalyDetector())
 *       .name("Test GNN Anomaly Detector");
 */
public class GNNAnomalyDetector extends RichFlatMapFunction<UserBehaviorSequence, Alert> {

    // ======================== 超参数 ========================

    /** 告警阈值 */
    private static final double ALERT_THRESHOLD = 0.40;

    /** 邻域采样：每个节点最多采样 Top-K 邻居 */
    private static final int MAX_NEIGHBORS = 8;

    /** 消息传递跳数 */
    private static final int NUM_HOPS = 2;

    /** GraphSAGE 嵌入引擎 */
    private transient GraphSAGEEmbedding embeddingEngine;

    /** 全局交易图缓存：accountId → 邻居边列表 */
    private transient Map<String, List<LocalGraphEdge>> graphContext;

    /** 统计计数器 */
    private transient int processedCount;
    private transient int alertCount;

    /**
     * 轻量级交易图节点（用于局部邻域构建）
     */
    private static class LocalGraphNode implements java.io.Serializable {
        public String accountId;
        public double[] features;
        public List<LocalGraphEdge> outEdges = new ArrayList<>();
        public List<LocalGraphEdge> inEdges = new ArrayList<>();
        public double riskScore;          // 节点风险评分
        public double embeddingNorm;      // 嵌入范数
        public boolean isCached;          // 是否已有缓存嵌入

        public LocalGraphNode(String accountId) {
            this.accountId = accountId;
        }
    }

    /**
     * 轻量级边
     */
    private static class LocalGraphEdge implements java.io.Serializable {
        public String fromId;
        public String toId;
        public double amount;
        public long timestamp;
        public String txType;
        public double weight;  // 归一化权重

        public LocalGraphEdge(String from, String to, double amount, long timestamp, String txType) {
            this.fromId = from;
            this.toId = to;
            this.amount = amount;
            this.timestamp = timestamp;
            this.txType = txType;
        }
    }

    public GNNAnomalyDetector() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        embeddingEngine = new GraphSAGEEmbedding();
        graphContext = new HashMap<>();
        processedCount = 0;
        alertCount = 0;
    }

    @Override
    public void flatMap(UserBehaviorSequence sequence, Collector<Alert> out) throws Exception {
        if (sequence == null || sequence.features == null || sequence.sequenceLength < 2) {
            return;
        }

        processedCount++;

        // ==================== 第一步: 构建局部图 ====================
        LocalGraph targetNode = buildLocalGraph(sequence);

        // ==================== 第二步: GraphSAGE 邻域采样 ====================
        List<LocalGraphEdge> sampledNeighbors = sampleTopKNeighbors(targetNode, MAX_NEIGHBORS);

        // ==================== 第三步: 2-hop 消息传递 ====================
        double[] aggregatedFeatures = messagePassing(targetNode, sampledNeighbors, NUM_HOPS);

        // ==================== 第四步: 融合特征计算风险评分 ====================
        double gnnRiskScore = computeGNNScore(targetNode, aggregatedFeatures, sequence);

        // ==================== 第五步: 图结构异常检测 ====================
        double structuralScore = detectStructuralAnomalies(targetNode, sequence);

        // ==================== 第六步: 集成评分 ====================
        double finalScore = integrateScores(gnnRiskScore, structuralScore, sequence);

        // ==================== 第七步: 阈值告警 ====================
        if (finalScore >= ALERT_THRESHOLD) {
            Alert alert = createGNNAlert(sequence, finalScore, gnnRiskScore, structuralScore);
            alertCount++;
            out.collect(alert);
        }

        // 定期输出统计
        if (processedCount % 500 == 0) {
            System.out.println(String.format(
                    "[GNNAnomalyDetector] processed=%d, alerts=%d, alertRate=%.2f%%, cache=%s",
                    processedCount, alertCount,
                    100.0 * alertCount / processedCount,
                    embeddingEngine.getCacheStats()));
        }
    }

    // ==================== 局部图构建 ====================

    /**
     * 从 UserBehaviorSequence 构建局部交易图
     * 
     * 提取目标节点及其直接邻居，构建1-hop子图
     * 节点特征从序列的 features 数组获取
     */
    private LocalGraph buildLocalGraph(UserBehaviorSequence sequence) {
        LocalGraph targetNode = new LocalGraph(sequence.accountId);
        targetNode.features = sequence.features;
        targetNode.riskScore = computeInitialNodeRisk(sequence);

        // 从交易记录中提取边信息
        Set<String> neighborSet = new HashSet<>();
        Map<String, Double> neighborTotalAmount = new HashMap<>();

        if (sequence.transactions != null) {
            for (Transaction tx : sequence.transactions) {
                String dest = tx.nameDest;
                if (dest != null && !dest.equals("UNKNOWN") && !dest.equals(sequence.accountId)) {
                    LocalGraphEdge edge = new LocalGraphEdge(
                            sequence.accountId, dest, tx.amount, tx.eventTime, tx.type);
                    edge.weight = normalizeEdgeWeight(tx.amount);
                    targetNode.outEdges.add(edge);
                    neighborSet.add(dest);
                    neighborTotalAmount.merge(dest, tx.amount, Double::sum);
                }
            }
        }

        // 存储邻居交易总量，用于后续采样
        targetNode.neighborAmounts = neighborTotalAmount;
        targetNode.neighborSet = neighborSet;

        // 检查是否有缓存嵌入
        double[] cachedEmbedding = embeddingEngine.getCachedEmbedding(sequence.accountId);
        targetNode.isCached = cachedEmbedding != null;
        targetNode.embeddingNorm = cachedEmbedding != null ? vectorNorm(cachedEmbedding) : 0.0;

        return targetNode;
    }

    /**
     * 计算节点的初始风险评分
     * 
     * 基于序列中可观察的特征进行快速风险评估
     */
    private double computeInitialNodeRisk(UserBehaviorSequence sequence) {
        double risk = 0.0;
        double[] f = sequence.features;

        // 大额交易风险
        if (f.length > 2 && f[2] > 25000) risk += 0.15; // maxAmount
        if (f.length > 3 && f[3] > 80000) risk += 0.10; // totalAmount

        // 高频交易风险
        if (f.length > 15 && f[15] > 3.0) risk += 0.10; // velocityScore

        // 夜间交易风险
        if (f.length > 7 && f[7] > 0) risk += 0.10; // nightTxCount

        // 高风险设备
        if (f.length > 8 && f[8] > 0) risk += 0.12; // highRiskDeviceCount

        // 境外交易
        if (f.length > 9 && f[9] > 0) risk += 0.15; // abroadTxCount

        // 余额掏空风险
        if (f.length > 13 && f[13] > 0.5) risk += 0.12; // amountBalanceRatio

        // 提现/转账比例
        if (f.length > 14 && f[14] > 0.6) risk += 0.08; // cashOutTransferRatio

        // 如果特征向量已扩展到44维，使用GNN特征
        if (f.length >= 44) {
            risk += f[39] * 0.15; // neighborRiskScore (index 36+3)
            risk += f[41] * 0.10; // clusteringCoefficient (index 36+5)
            risk += f[42] * 0.10; // betweennessCentrality (index 36+6)
        }

        return Math.min(1.0, risk);
    }

    /**
     * 边权重归一化
     * 
     * 使用对数变换压缩大额交易的权重差异
     */
    private double normalizeEdgeWeight(double amount) {
        return Math.log1p(amount) / Math.log1p(100000.0); // log(1+amount)/log(1+100000)
    }

    // ==================== GraphSAGE 邻域采样 ====================

    /**
     * 采样 Top-K 邻居（按交易金额排序）
     * 
     * GraphSAGE 的核心思想：不是聚合所有邻居，而是采样固定数量的邻居
     * 这里使用确定性采样：选择交易金额最大的 Top-K 邻居
     * 
     * @param targetNode 目标节点
     * @param k 采样数量
     * @return 采样后的邻居边列表
     */
    private List<LocalGraphEdge> sampleTopKNeighbors(LocalGraph targetNode, int k) {
        if (targetNode.outEdges.isEmpty()) {
            return Collections.emptyList();
        }

        // 按金额降序排序
        List<LocalGraphEdge> sorted = new ArrayList<>(targetNode.outEdges);
        sorted.sort(Comparator.comparingDouble((LocalGraphEdge e) -> e.amount).reversed());

        // 取 Top-K
        int sampleSize = Math.min(k, sorted.size());
        List<LocalGraphEdge> sampled = new ArrayList<>(sampleSize);
        for (int i = 0; i < sampleSize; i++) {
            sampled.add(sorted.get(i));
        }

        return sampled;
    }

    // ==================== 消息传递 ====================

    /**
     * 多跳消息传递（Message Passing）
     * 
     * 模拟 GraphSAGE 的 2-hop 聚合过程：
     * Hop 1: 聚合直接邻居的特征 → 生成一阶聚合向量
     * Hop 2: 聚合邻居的邻居（间接邻居）的特征 → 生成二阶聚合向量
     * 
     * 最终输出 = f(自身特征, 一阶聚合, 二阶聚合)
     */
    private double[] messagePassing(LocalGraph targetNode,
                                     List<LocalGraphEdge> sampledNeighbors,
                                     int numHops) {
        if (sampledNeighbors.isEmpty()) {
            // 无邻居时返回自身特征
            return targetNode.features != null
                    ? Arrays.copyOf(targetNode.features, 36)
                    : new double[36];
        }

        int featureDim = Math.min(36, targetNode.features != null ? targetNode.features.length : 36);

        // ---------- Hop 1: 聚合直接邻居 ----------
        double[] hop1Agg = new double[featureDim];
        double totalWeight1 = 0.0;

        for (LocalGraphEdge edge : sampledNeighbors) {
            double[] neighborFeatures = getNeighborFeatures(edge, targetNode);
            double w = edge.weight > 0 ? edge.weight : 0.5;
            totalWeight1 += w;

            for (int i = 0; i < featureDim; i++) {
                hop1Agg[i] += w * neighborFeatures[i];
            }
        }

        if (totalWeight1 > 0) {
            for (int i = 0; i < featureDim; i++) {
                hop1Agg[i] /= totalWeight1;
            }
        }

        // ---------- Hop 2: 聚合间接邻居（如果跳数>=2） ----------
        double[] hop2Agg = new double[featureDim];
        if (numHops >= 2) {
            double totalWeight2 = 0.0;
            Set<String> visitedNeighbors = new HashSet<>();

            for (LocalGraphEdge edge : sampledNeighbors) {
                visitedNeighbors.add(edge.toId);

                // 获取间接邻居的特征（通过目标节点的序列推断）
                List<LocalGraphEdge> indirectEdges = getIndirectNeighbors(edge, targetNode);
                for (LocalGraphEdge indirectEdge : indirectEdges) {
                    if (visitedNeighbors.contains(indirectEdge.toId)) continue;

                    double[] indirectFeatures = getIndirectNeighborFeatures(indirectEdge);
                    double w = indirectEdge.weight * 0.5; // 二阶权重衰减
                    totalWeight2 += w;

                    for (int i = 0; i < featureDim; i++) {
                        hop2Agg[i] += w * indirectFeatures[i];
                    }
                }
            }

            if (totalWeight2 > 0) {
                for (int i = 0; i < featureDim; i++) {
                    hop2Agg[i] /= totalWeight2;
                }
            }
        }

        // ---------- 融合多跳聚合结果 ----------
        // 融合策略: 自身特征(40%) + 一阶聚合(35%) + 二阶聚合(25%)
        double[] fused = new double[featureDim];
        double alpha = 0.40; // 自身权重
        double beta = 0.35;  // 一阶权重
        double gamma = 0.25; // 二阶权重

        double[] selfFeatures = targetNode.features != null
                ? targetNode.features : new double[featureDim];

        for (int i = 0; i < featureDim; i++) {
            fused[i] = alpha * selfFeatures[i]
                    + beta * hop1Agg[i]
                    + gamma * hop2Agg[i];
        }

        return fused;
    }

    /**
     * 获取邻居节点的特征向量
     * 
     * 优先使用缓存的 GraphSAGE 嵌入
     * 无缓存时使用启发式伪特征
     */
    private double[] getNeighborFeatures(LocalGraphEdge edge, LocalGraph targetNode) {
        double[] cached = embeddingEngine.getCachedEmbedding(edge.toId);
        if (cached != null && cached.length >= 36) {
            return Arrays.copyOf(cached, 36);
        }

        // 启发式伪特征：基于交易金额和类型生成
        double[] pseudo = new double[36];
        double normAmount = Math.log1p(edge.amount) / 15.0;

        // 金额相关特征
        pseudo[0] = normAmount;                    // avgAmount 位置
        pseudo[1] = normAmount * 0.3;             // stdAmount
        pseudo[2] = normAmount * 1.5;             // maxAmount
        pseudo[3] = normAmount * 5.0;             // totalAmount

        // 风险相关特征（基于交易类型推断）
        if ("TRANSFER".equals(edge.txType)) {
            pseudo[14] = 0.8; // cashOutTransferRatio 高
        } else if ("CASH_OUT".equals(edge.txType)) {
            pseudo[14] = 1.0;
        }

        // 时间特征推断
        if (edge.amount > 20000) {
            pseudo[7] = 1.0;  // 夜间交易风险
        }

        return pseudo;
    }

    /**
     * 获取间接邻居（2-hop 邻居）
     * 
     * 简化实现：基于交易序列中的共同目标账户推断
     */
    private List<LocalGraphEdge> getIndirectNeighbors(LocalGraphEdge edge, LocalGraph targetNode) {
        List<LocalGraphEdge> indirect = new ArrayList<>();

        // 检查目标账户的交易序列，寻找共同交易目标
        if (targetNode.neighborAmounts != null) {
            // 简化：使用当前节点的其他邻居作为间接邻居的近似
            for (Map.Entry<String, Double> entry : targetNode.neighborAmounts.entrySet()) {
                if (!entry.getKey().equals(edge.toId)) {
                    LocalGraphEdge ie = new LocalGraphEdge(
                            edge.toId, entry.getKey(), entry.getValue(),
                            System.currentTimeMillis(), "INDIRECT");
                    ie.weight = normalizeEdgeWeight(entry.getValue()) * 0.5; // 二阶衰减
                    indirect.add(ie);
                }
            }
        }

        return indirect;
    }

    /**
     * 获取间接邻居的特征
     */
    private double[] getIndirectNeighborFeatures(LocalGraphEdge edge) {
        double[] features = new double[36];
        double normAmount = Math.log1p(edge.amount) / 15.0;

        features[0] = normAmount * 0.5;
        features[3] = normAmount * 2.5;
        features[14] = 0.5;

        return features;
    }

    // ==================== 风险评分计算 ====================

    /**
     * 计算 GNN 风险评分
     * 
     * 综合以下因素:
     * 1. 融合特征与正常模式的偏离度
     * 2. 邻居风险传播效应
     * 3. 嵌入异常度
     */
    private double computeGNNScore(LocalGraph targetNode, double[] aggregatedFeatures,
                                    UserBehaviorSequence sequence) {
        double score = 0.0;

        // (1) 自身特征风险（来自初始节点风险计算）
        double selfRisk = targetNode.riskScore;
        score += selfRisk * 0.30;

        // (2) 邻居风险传播
        double neighborRisk = computeNeighborPropagationRisk(targetNode);
        score += neighborRisk * 0.25;

        // (3) 特征偏离度：融合特征与"正常模式"的距离
        double deviationScore = computeFeatureDeviation(aggregatedFeatures, sequence);
        score += deviationScore * 0.25;

        // (4) 嵌入异常度：嵌入范数偏离正常范围
        double embeddingScore = computeEmbeddingAnomaly(targetNode);
        score += embeddingScore * 0.20;

        return Math.min(1.0, score);
    }

    /**
     * 计算邻居风险传播分
     * 
     * 如果邻居中有高风险账户，风险会传播到当前账户
     * 使用 PageRank 式的迭代传播：risk(v) = α * self_risk(v) + (1-α) * avg_risk(N(v))
     */
    private double computeNeighborPropagationRisk(LocalGraph targetNode) {
        if (targetNode.outEdges.isEmpty()) {
            return 0.0;
        }

        double totalNeighborRisk = 0.0;
        double totalWeight = 0.0;

        for (LocalGraphEdge edge : targetNode.outEdges) {
            // 估算邻居风险
            double neighborRisk = estimateNeighborRisk(edge);
            double w = edge.weight;
            totalNeighborRisk += w * neighborRisk;
            totalWeight += w;
        }

        return totalWeight > 0 ? totalNeighborRisk / totalWeight : 0.0;
    }

    /**
     * 估算单个邻居的风险分
     */
    private double estimateNeighborRisk(LocalGraphEdge edge) {
        double risk = 0.0;

        // 大额转账风险
        if (edge.amount > 50000) risk += 0.3;
        else if (edge.amount > 20000) risk += 0.2;
        else if (edge.amount > 10000) risk += 0.1;

        // 交易类型风险
        if ("CASH_OUT".equals(edge.txType)) risk += 0.15;
        if ("TRANSFER".equals(edge.txType)) risk += 0.05;

        return Math.min(1.0, risk);
    }

    /**
     * 计算特征偏离度
     * 
     * 将聚合后的特征与"正常模式基准"进行比较
     * 正常模式基准：基于交易统计的期望特征向量
     */
    private double computeFeatureDeviation(double[] aggregatedFeatures, UserBehaviorSequence sequence) {
        double deviation = 0.0;
        int dim = Math.min(36, aggregatedFeatures.length);

        // 正常模式基准（基于行业经验设定）
        // 关键维度的"正常"范围
        double[] normalBenchmarks = new double[dim];
        double[] normalStd = new double[dim];

        // 金额相关：正常交易的金额模式
        normalBenchmarks[0] = 5000.0;  // avgAmount 基准
        normalStd[0] = 3000.0;
        normalBenchmarks[2] = 10000.0; // maxAmount 基准
        normalStd[2] = 5000.0;
        normalBenchmarks[3] = 20000.0; // totalAmount 基准
        normalStd[3] = 15000.0;

        // 行为相关
        normalBenchmarks[4] = 0.0;    // cityChanges 基准（0=正常）
        normalStd[4] = 1.0;
        normalBenchmarks[5] = 0.0;    // deviceChanges
        normalStd[5] = 0.5;
        normalBenchmarks[7] = 0.0;    // nightTxCount
        normalStd[7] = 0.5;
        normalBenchmarks[8] = 0.0;    // highRiskDeviceCount
        normalStd[8] = 0.3;
        normalBenchmarks[9] = 0.0;    // abroadTxCount
        normalStd[9] = 0.2;

        // 计算马氏距离近似（简化版，假设特征独立）
        for (int i = 0; i < dim; i++) {
            if (normalStd[i] > 0) {
                double z = Math.abs(aggregatedFeatures[i] - normalBenchmarks[i]) / normalStd[i];
                deviation += Math.min(1.0, z * 0.1); // z-score 缩放
            }
        }

        return Math.min(1.0, deviation / dim);
    }

    /**
     * 计算嵌入异常度
     * 
     * 嵌入范数异常可能意味着：
     * - 节点连接模式异常（异常多的邻居或异常少的邻居）
     * - 特征聚合后偏离正常嵌入空间
     */
    private double computeEmbeddingAnomaly(LocalGraph targetNode) {
        double norm = targetNode.embeddingNorm;

        // 正常嵌入范数范围：约在 [0.3, 1.5] 之间
        // 超出此范围视为异常
        if (norm < 0.1) return 0.5;   // 几乎无邻居信息 → 可疑
        if (norm > 2.0) return 0.6;   // 嵌入范数过大 → 大量异常邻居
        if (norm < 0.3) return 0.3;   // 嵌入范数偏小 → 信息贫乏

        // 检查嵌入是否已被缓存
        if (!targetNode.isCached) {
            return 0.2; // 首次出现，无缓存 → 轻微可疑
        }

        return 0.0;
    }

    // ==================== 图结构异常检测 ====================

    /**
     * 检测图拓扑结构异常
     * 
     * 识别以下模式:
     * 1. 星型分散转出：一个账户向多个不同目标分散转账
     * 2. 链式转账特征：资金经过多个中间账户流转
     * 3. 集中提现：多个小额转入后大额转出
     * 4. 高频交互：与同一对手频繁交易
     */
    private double detectStructuralAnomalies(LocalGraph targetNode, UserBehaviorSequence sequence) {
        double score = 0.0;

        // (1) 星型分散转出检测
        int outDegree = targetNode.outEdges.size();
        if (outDegree >= 5) score += 0.25;
        else if (outDegree >= 3) score += 0.15;
        else if (outDegree >= 2) score += 0.05;

        // (2) 分散度：不同目标账户的金额分布
        if (targetNode.neighborAmounts != null && !targetNode.neighborAmounts.isEmpty()) {
            double amountVariance = computeAmountVariance(targetNode.neighborAmounts);
            // 低方差 = 均匀分散转账 → 可疑
            double maxAmount = 0;
            for (double amt : targetNode.neighborAmounts.values()) {
                maxAmount = Math.max(maxAmount, amt);
            }
            if (maxAmount > 0 && amountVariance / maxAmount < 0.1 && outDegree >= 3) {
                score += 0.15; // 均匀分散
            }
        }

        // (3) 链式转账特征：检查序列中的资金流向模式
        score += detectChainTransferPattern(sequence) * 0.20;

        // (4) 集中提现检测
        score += detectConcentratedCashOut(sequence) * 0.15;

        // (5) 高频交互检测
        score += detectHighFrequencyInteraction(targetNode) * 0.15;

        return Math.min(1.0, score);
    }

    /**
     * 检测金额分布方差
     */
    private double computeAmountVariance(Map<String, Double> amounts) {
        if (amounts.isEmpty()) return 0.0;

        double sum = 0;
        double sumSq = 0;
        int n = amounts.size();

        for (double v : amounts.values()) {
            sum += v;
            sumSq += v * v;
        }

        return Math.max(sumSq / n - (sum / n) * (sum / n), 0);
    }

    /**
     * 检测链式转账模式
     * 
     * 链式转账特征：交易金额呈现递增/递减趋势，且交易间隔规律
     */
    private double detectChainTransferPattern(UserBehaviorSequence sequence) {
        if (sequence.transactions == null || sequence.transactions.size() < 3) {
            return 0.0;
        }

        List<Transaction> txs = sequence.transactions;
        int chainCount = 0;

        // 检测连续转账模式
        for (int i = 1; i < txs.size() - 1; i++) {
            boolean isChain = "TRANSFER".equals(txs.get(i - 1).type)
                    && "TRANSFER".equals(txs.get(i).type)
                    && "TRANSFER".equals(txs.get(i + 1).type);

            if (isChain) chainCount++;
        }

        double chainRatio = (double) chainCount / Math.max(txs.size() - 2, 1);

        // 检测金额趋势一致性
        double trendConsistency = computeTrendConsistency(txs);

        return Math.min(1.0, chainRatio * 0.5 + trendConsistency * 0.5);
    }

    /**
     * 计算金额趋势一致性
     */
    private double computeTrendConsistency(List<Transaction> txs) {
        if (txs.size() < 3) return 0.0;

        int increasing = 0;
        int decreasing = 0;

        for (int i = 1; i < txs.size(); i++) {
            if (txs.get(i).amount > txs.get(i - 1).amount) {
                increasing++;
            } else if (txs.get(i).amount < txs.get(i - 1).amount) {
                decreasing++;
            }
        }

        // 趋势一致性高 → 可疑
        int total = increasing + decreasing;
        if (total == 0) return 0.0;

        return Math.max((double) increasing / total, (double) decreasing / total);
    }

    /**
     * 检测集中提现模式
     */
    private double detectConcentratedCashOut(UserBehaviorSequence sequence) {
        if (sequence.transactions == null) return 0.0;

        double totalIn = 0;
        double totalOut = 0;
        int cashOutCount = 0;

        for (Transaction tx : sequence.transactions) {
            if ("CASH_OUT".equals(tx.type)) {
                totalOut += tx.amount;
                cashOutCount++;
            } else if ("DEBIT".equals(tx.type)) {
                totalIn += tx.amount;
            }
        }

        // 大量入金后集中提现
        if (totalIn > 0 && totalOut > totalIn * 0.7 && cashOutCount >= 2) {
            return 0.8;
        }

        return 0.0;
    }

    /**
     * 检测高频交互
     */
    private double detectHighFrequencyInteraction(LocalGraph targetNode) {
        if (targetNode.outEdges.isEmpty()) return 0.0;

        // 统计与同一对手的交互次数
        Map<String, Integer> interactionCounts = new HashMap<>();
        for (LocalGraphEdge edge : targetNode.outEdges) {
            interactionCounts.merge(edge.toId, 1, Integer::sum);
        }

        int maxInteractions = 0;
        for (int count : interactionCounts.values()) {
            maxInteractions = Math.max(maxInteractions, count);
        }

        if (maxInteractions >= 5) return 0.6;
        if (maxInteractions >= 3) return 0.3;
        return 0.0;
    }

    // ==================== 集成评分 ====================

    /**
     * 集成 GNN 风险评分和结构异常评分
     * 
     * 使用动态权重：
     * - 如果邻居丰富 → 更依赖 GNN 风险评分
     * - 如果邻居稀少 → 更依赖结构异常评分
     */
    private double integrateScores(double gnnRiskScore, double structuralScore,
                                    UserBehaviorSequence sequence) {
        // 邻居丰富度决定权重分配
        int neighborCount = sequence.outDegree > 0 ? sequence.outDegree : 1;
        double neighborFactor = Math.min(1.0, (double) neighborCount / 5.0);

        // 动态权重
        double wGNN = 0.5 + 0.3 * neighborFactor;    // [0.5, 0.8]
        double wStructural = 0.5 - 0.3 * neighborFactor; // [0.2, 0.5]

        double combinedScore = wGNN * gnnRiskScore + wStructural * structuralScore;

        // 强化极端情况
        if (gnnRiskScore > 0.7 && structuralScore > 0.5) {
            combinedScore = Math.min(1.0, combinedScore * 1.15); // 双重确认加成
        }

        // 大额交易加成
        if (sequence.maxAmount > 50000) {
            combinedScore = Math.min(1.0, combinedScore + 0.05);
        }

        return Math.min(1.0, combinedScore);
    }

    // ==================== 告警生成 ====================

    /**
     * 创建 GNN 告警
     * 
     * 根据评分和特征模式确定欺诈类型
     */
    private Alert createGNNAlert(UserBehaviorSequence sequence, double finalScore,
                                  double gnnRiskScore, double structuralScore) {
        String fraudType = resolveGNNFraudType(sequence, gnnRiskScore, structuralScore);

        Alert alert = new Alert(
                sequence.accountId,
                fraudType,
                finalScore,
                "GNN_GRAPH"
        );
        alert.amount = sequence.totalAmount;
        alert.behaviorPath = sequence.behaviorPath;
        alert.timestamp = sequence.sequenceEndTime > 0
                ? sequence.sequenceEndTime
                : System.currentTimeMillis();

        // 从序列中获取最后一笔交易的上下文信息
        if (!sequence.transactions.isEmpty()) {
            alert.withTransactionContext(sequence.transactions.get(sequence.transactions.size() - 1));
        }

        // 构建详细说明
        alert.details = String.format(
                "gnnScore=%.4f,structuralScore=%.4f,finalScore=%.4f," +
                        "outDegree=%d,neighborCount=%d,maxAmt=%.2f,totalAmt=%.2f," +
                        "velocity=%.2f,nightTx=%d,highRiskDev=%d,abroadTx=%d," +
                        "cashOutRatio=%.2f,balanceRatio=%.2f," +
                        "embedNorm=%.4f,embedCached=%b,path=%s",
                gnnRiskScore, structuralScore, finalScore,
                sequence.outDegree, sequence.distinctDestCount,
                sequence.maxAmount, sequence.totalAmount,
                sequence.velocityScore, sequence.nightTxCount,
                sequence.highRiskDeviceCount, sequence.abroadTxCount,
                sequence.cashOutTransferRatio, sequence.amountBalanceRatio,
                embeddingEngine.getCachedEmbedding(sequence.accountId) != null
                        ? vectorNorm(embeddingEngine.getCachedEmbedding(sequence.accountId))
                        : 0.0,
                embeddingEngine.hasEmbedding(sequence.accountId),
                sequence.behaviorPath
        );

        return alert;
    }

    /**
     * 判定 GNN 检测到的欺诈类型
     */
    private String resolveGNNFraudType(UserBehaviorSequence sequence,
                                        double gnnRiskScore, double structuralScore) {
        // 星型分散转出（洗钱典型模式）
        if (sequence.outDegree >= 4 && structuralScore > 0.5) {
            return "GNN识别星型分散转出洗钱";
        }

        // 链式转账
        if (sequence.outDegree >= 2 && sequence.cashOutTransferRatio > 0.7) {
            return "GNN识别链式转账洗钱";
        }

        // 高风险邻居传播
        if (gnnRiskScore > 0.6 && sequence.outDegree >= 3) {
            return "GNN识别高危邻域关联欺诈";
        }

        // 集中提现
        if (sequence.cashOutTransferRatio > 0.8 && sequence.totalAmount > 30000) {
            return "GNN识别集中提现异常";
        }

        // 图结构异常
        if (structuralScore > 0.6) {
            return "GNN识别图拓扑结构异常";
        }

        // 嵌入异常
        if (gnnRiskScore > 0.5) {
            return "GNN识别嵌入空间异常模式";
        }

        return "GNN综合识别图神经网络异常";
    }

    // ==================== 工具函数 ====================

    /**
     * 计算向量 L2 范数
     */
    private double vectorNorm(double[] vector) {
        if (vector == null) return 0.0;
        double sum = 0.0;
        for (double v : vector) {
            sum += v * v;
        }
        return Math.sqrt(sum);
    }

    /**
     * 扩展的局部图节点（包含邻居映射等额外信息）
     */
    private static class LocalGraph extends LocalGraphNode implements java.io.Serializable {
        public Map<String, Double> neighborAmounts;
        public Set<String> neighborSet;

        public LocalGraph(String accountId) {
            super(accountId);
        }
    }
}
