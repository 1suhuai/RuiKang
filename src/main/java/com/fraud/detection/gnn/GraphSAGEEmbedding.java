package com.fraud.detection.gnn;

import java.io.Serializable;
import java.util.*;

/**
 * GraphSAGE 嵌入计算模块
 * 
 * 功能:
 * 1. Mean aggregator 邻居特征聚合
 * 2. Concat + 线性投影完成节点更新
 * 3. L2 归一化输出嵌入向量
 * 4. 按账户缓存嵌入，支持增量更新
 * 
 * 算法描述 (GraphSAGE-style):
 *   h_v^(k) = sigma( W^(k) * CONCAT( h_v^(k-1), MeanAgg({h_u^(k-1), u ∈ N(v)}) ) )
 * 
 * 其中:
 *   - h_v^(k) 表示节点 v 在第 k 层的嵌入
 *   - MeanAgg 对邻居嵌入求平均
 *   - CONCAT 拼接自身和邻居聚合结果
 *   - W^(k) 是可学习的权重矩阵（此处使用启发式权重，不依赖外部训练框架）
 *   - sigma 为 LeakyReLU 激活函数
 * 
 * 嵌入维度: 36 维（与原始特征维度一致，便于后续融合）
 */
public class GraphSAGEEmbedding implements Serializable {

    /** 嵌入向量维度（与原始特征维度一致） */
    private static final int EMBED_DIM = 36;

    /** 最大缓存账户数，防止内存溢出 */
    private static final int MAX_CACHE_SIZE = 10000;

    /** 缓存: 账户ID → 嵌入向量 */
    private final Map<String, double[]> embeddingCache;

    /** 缓存: 账户ID → 邻居列表（用于增量更新） */
    private final Map<String, List<NeighborInfo>> neighborCache;

    /** 计数器，用于控制缓存淘汰 */
    private int updateCounter;

    /**
     * 邻居信息：记录相邻账户及其边权重（交易金额）
     */
    public static class NeighborInfo implements Serializable {
        public String neighborId;
        public double weight;       // 边权重（交易金额）
        public long lastUpdateTs;   // 最后更新时间

        public NeighborInfo(String neighborId, double weight, long lastUpdateTs) {
            this.neighborId = neighborId;
            this.weight = weight;
            this.lastUpdateTs = lastUpdateTs;
        }
    }

    public GraphSAGEEmbedding() {
        this.embeddingCache = new HashMap<>();
        this.neighborCache = new HashMap<>();
        this.updateCounter = 0;
    }

    // ======================== 核心计算 ========================

    /**
     * 计算指定账户的 GraphSAGE 嵌入
     * 
     * @param accountId  账户ID
     * @param selfFeatures 节点自身的原始特征向量（36维）
     * @param neighbors  邻居信息列表
     * @param numHops    消息传递跳数（1 或 2）
     * @return 归一化后的嵌入向量（36维）
     */
    public double[] computeEmbedding(String accountId, double[] selfFeatures,
                                      List<NeighborInfo> neighbors, int numHops) {
        if (selfFeatures == null || selfFeatures.length != EMBED_DIM) {
            return new double[EMBED_DIM]; // 返回零向量
        }

        // 第0层: 自身特征作为初始嵌入
        double[] currentEmbedding = Arrays.copyOf(selfFeatures, EMBED_DIM);

        // 消息传递：逐层聚合邻居信息
        for (int hop = 0; hop < numHops; hop++) {
            currentEmbedding = messagePassingStep(currentEmbedding, neighbors, hop);
        }

        // L2 归一化
        return l2Normalize(currentEmbedding);
    }

    /**
     * 单步消息传递 (Message Passing Step)
     * 
     * 实现 GraphSAGE 的 Mean Aggregator:
     *   1. 聚合邻居嵌入（加权平均）
     *   2. 拼接 [自身嵌入, 邻居聚合]
     *   3. 线性投影到 EMBED_DIM 维
     *   4. LeakyReLU 激活
     */
    private double[] messagePassingStep(double[] selfEmbedding, List<NeighborInfo> neighbors, int layer) {
        double[] neighborAgg = aggregateNeighbors(neighbors, layer);

        // Concat + Linear Projection
        // CONCAT 维度 = EMBED_DIM + EMBED_DIM = 72
        // 投影矩阵 W ∈ R^(EMBED_DIM × 72)
        double[] projected = new double[EMBED_DIM];
        double[] concat = new double[EMBED_DIM * 2];
        System.arraycopy(selfEmbedding, 0, concat, 0, EMBED_DIM);
        System.arraycopy(neighborAgg, 0, concat, EMBED_DIM, EMBED_DIM);

        // 启发式线性投影（模拟训练后的权重矩阵）
        // 使用分层权重：自身特征权重 > 邻居聚合权重
        // 权重设计考虑欺诈检测场景的特殊性
        double selfWeight = 0.6;
        double neighborWeight = 0.4;

        for (int i = 0; i < EMBED_DIM; i++) {
            // 自投影（对角线权重）+ 跨维度交互（邻近维度加权）
            double selfContribution = concat[i] * selfWeight;
            double crossContribution = concat[i] * 0.1; // 跨维度微弱交互

            // 邻居部分：相邻维度加权
            double neighborContribution = concat[EMBED_DIM + i] * neighborWeight;
            double neighborCross = 0.0;
            // 加入少量交叉维度信息（模拟密集连接）
            for (int j = Math.max(0, i - 2); j <= Math.min(EMBED_DIM - 1, i + 2); j++) {
                if (j != i) {
                    neighborCross += concat[EMBED_DIM + j] * 0.02;
                }
            }

            projected[i] = selfContribution + crossContribution + neighborContribution + neighborCross;
        }

        // LeakyReLU 激活 (negative_slope = 0.2)
        return leakyReLU(projected, 0.2);
    }

    /**
     * Mean Aggregator: 聚合邻居嵌入
     * 
     * 对每个邻居，使用其缓存的嵌入（若存在）或基于交易金额生成的伪嵌入
     * 加权平均（权重 = 边权重 / 总权重）
     */
    private double[] aggregateNeighbors(List<NeighborInfo> neighbors, int layer) {
        double[] agg = new double[EMBED_DIM];

        if (neighbors == null || neighbors.isEmpty()) {
            return agg; // 无邻居时返回零向量
        }

        double totalWeight = 0.0;
        for (NeighborInfo neighbor : neighbors) {
            double[] neighborEmbedding = getNeighborEmbedding(neighbor, layer);
            double w = Math.log1p(neighbor.weight); // 对数缩放，避免大额交易主导
            totalWeight += w;

            for (int i = 0; i < EMBED_DIM; i++) {
                agg[i] += w * neighborEmbedding[i];
            }
        }

        if (totalWeight > 0) {
            for (int i = 0; i < EMBED_DIM; i++) {
                agg[i] /= totalWeight;
            }
        }

        return agg;
    }

    /**
     * 获取邻居的嵌入向量
     * 
     * 优先使用缓存中的嵌入，若无缓存则基于交易金额和特征生成伪嵌入
     */
    private double[] getNeighborEmbedding(NeighborInfo neighbor, int layer) {
        // 尝试从缓存获取
        double[] cached = embeddingCache.get(neighbor.neighborId);
        if (cached != null) {
            // 添加时间衰减：较旧的嵌入降低权重
            double decay = computeTimeDecay(neighbor.lastUpdateTs);
            double[] decayed = new double[EMBED_DIM];
            for (int i = 0; i < EMBED_DIM; i++) {
                decayed[i] = cached[i] * decay;
            }
            return decayed;
        }

        // 无缓存时，基于交易金额生成伪嵌入
        // 伪嵌入使用金额特征在特征空间中定位
        return generatePseudoEmbedding(neighbor.weight, layer);
    }

    /**
     * 基于交易金额生成伪嵌入
     * 
     * 使用确定性哈希函数将金额映射到特征空间
     * 确保相同金额产生相同嵌入（可复现性）
     */
    private double[] generatePseudoEmbedding(double amount, int layer) {
        double[] pseudo = new double[EMBED_DIM];

        // 金额归一化
        double normAmount = Math.log1p(Math.abs(amount)) / 15.0; // log(1+amount)/15 → [0, 1]
        normAmount = Math.min(1.0, normAmount);

        // 使用金额作为种子生成确定性嵌入
        long seed = Double.doubleToLongBits(amount) ^ (layer * 31L);
        Random rng = new Random(seed);

        // 基础嵌入：金额相关的模式
        pseudo[0] = normAmount;                    // 金额比例
        pseudo[1] = 1.0 - normAmount;             // 金额补码
        pseudo[2] = normAmount * (1.0 - normAmount); // 金额分布峰值
        pseudo[3] = Math.sin(normAmount * Math.PI);  // 正弦变换
        pseudo[4] = Math.cos(normAmount * Math.PI);  // 余弦变换

        // 剩余维度使用确定性随机值（带金额偏置）
        for (int i = 5; i < EMBED_DIM; i++) {
            pseudo[i] = rng.nextGaussian() * 0.1 + normAmount * 0.3;
        }

        return pseudo;
    }

    // ======================== 缓存管理 ========================

    /**
     * 更新账户嵌入（增量更新）
     * 
     * 当新交易到达时，更新该账户及其邻居的嵌入
     * 使用延迟计算策略：只更新当前账户，邻居按需更新
     */
    public void updateEmbedding(String accountId, double[] selfFeatures,
                                 List<NeighborInfo> neighbors, int numHops) {
        updateCounter++;

        // 计算新嵌入
        double[] newEmbedding = computeEmbedding(accountId, selfFeatures, neighbors, numHops);
        embeddingCache.put(accountId, newEmbedding);

        // 更新邻居缓存
        neighborCache.put(accountId, new ArrayList<>(neighbors));

        // 缓存管理：定期淘汰旧条目
        if (updateCounter % 500 == 0 && embeddingCache.size() > MAX_CACHE_SIZE) {
            evictOldEntries();
        }
    }

    /**
     * 获取缓存的嵌入
     */
    public double[] getCachedEmbedding(String accountId) {
        return embeddingCache.get(accountId);
    }

    /**
     * 获取缓存的邻居列表
     */
    public List<NeighborInfo> getCachedNeighbors(String accountId) {
        return neighborCache.get(accountId);
    }

    /**
     * 检查嵌入是否已缓存
     */
    public boolean hasEmbedding(String accountId) {
        return embeddingCache.containsKey(accountId);
    }

    /**
     * 缓存统计信息
     */
    public String getCacheStats() {
        return String.format("EmbeddingCache: %d embeddings, %d neighbor lists, updates=%d",
                embeddingCache.size(), neighborCache.size(), updateCounter);
    }

    /**
     * 淘汰策略：移除最早插入的条目（FIFO）
     * 保留 70% 的缓存
     */
    private void evictOldEntries() {
        int targetSize = (int) (MAX_CACHE_SIZE * 0.7);
        List<String> keys = new ArrayList<>(embeddingCache.keySet());

        // 简单 FIFO：移除前 (size - targetSize) 个
        int toRemove = Math.max(0, embeddingCache.size() - targetSize);
        for (int i = 0; i < toRemove && i < keys.size(); i++) {
            String key = keys.get(i);
            embeddingCache.remove(key);
            neighborCache.remove(key);
        }
    }

    // ======================== 工具函数 ========================

    /**
     * L2 归一化
     * 
     * ||x||_2 = sqrt(sum(x_i^2))
     * normalized_x = x / ||x||_2
     */
    public static double[] l2Normalize(double[] vector) {
        double norm = 0.0;
        for (double v : vector) {
            norm += v * v;
        }
        norm = Math.sqrt(norm);

        if (norm < 1e-10) {
            return vector; // 零向量不需要归一化
        }

        double[] result = new double[vector.length];
        for (int i = 0; i < vector.length; i++) {
            result[i] = vector[i] / norm;
        }
        return result;
    }

    /**
     * LeakyReLU 激活函数
     * 
     * f(x) = x           if x > 0
     *      = alpha * x   if x <= 0
     */
    private static double[] leakyReLU(double[] input, double alpha) {
        double[] output = new double[input.length];
        for (int i = 0; i < input.length; i++) {
            output[i] = input[i] > 0 ? input[i] : alpha * input[i];
        }
        return output;
    }

    /**
     * 计算时间衰减因子
     * 
     * 嵌入的时效性：越旧的嵌入权重越低
     * 衰减函数: exp(-Δt / HALF_LIFE)
     */
    private double computeTimeDecay(long lastUpdateTs) {
        long elapsed = System.currentTimeMillis() - lastUpdateTs;
        double halfLife = 3600000.0; // 1小时半衰期
        return Math.exp(-elapsed / halfLife);
    }

    /**
     * 重置所有缓存（用于调试或重启）
     */
    public void reset() {
        embeddingCache.clear();
        neighborCache.clear();
        updateCounter = 0;
    }
}
