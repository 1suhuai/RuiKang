package com.fraud.detection.drift;

import com.fraud.detection.model.UserBehaviorSequence;

import java.io.Serializable;
import java.util.*;

/**
 * 数据窗口管理器
 * 管理多层滑动窗口，为模型重训提供数据切片
 * 
 * 三层窗口设计：
 * - 近期窗口 (Recent Window):  最近1000个样本，用于快速微调
 * - 历史窗口 (Historical Window): 最近10000个样本，用于常规重训
 * - 基线窗口 (Baseline Window):  最近50000个样本，用于紧急全量重训
 * 
 * 使用蓄水池采样(Reservoir Sampling)保证内存效率，
 * 同时维护欺诈样本过采样队列，解决类别不平衡问题
 */
public class DataWindowManager implements Serializable {

    // 窗口大小配置
    private static final int RECENT_WINDOW_SIZE = 1000;
    private static final int HISTORICAL_WINDOW_SIZE = 10000;
    private static final int BASELINE_WINDOW_SIZE = 50000;

    // 蓄水池采样缓冲区
    private final ReservoirSampler recentReservoir;
    private final ReservoirSampler historicalReservoir;
    private final ReservoirSampler baselineReservoir;

    // 欺诈样本专用队列（过采样用）
    private final List<UserBehaviorSequence> fraudSampleBuffer;
    private static final int MAX_FRAUD_BUFFER = 2000;

    // 统计信息
    private long totalSamplesProcessed = 0;
    private long totalFraudSamples = 0;

    public DataWindowManager() {
        recentReservoir = new ReservoirSampler(RECENT_WINDOW_SIZE);
        historicalReservoir = new ReservoirSampler(HISTORICAL_WINDOW_SIZE);
        baselineReservoir = new ReservoirSampler(BASELINE_WINDOW_SIZE);
        fraudSampleBuffer = new ArrayList<>();
    }

    /**
     * 添加新样本到所有窗口
     */
    public void addSample(UserBehaviorSequence seq) {
        if (seq == null || seq.features == null) return;

        totalSamplesProcessed++;

        // 添加到各层蓄水池
        recentReservoir.add(seq);
        historicalReservoir.add(seq);
        baselineReservoir.add(seq);

        // 欺诈样本额外存储
        if (seq.isFraud == 1) {
            totalFraudSamples++;
            if (fraudSampleBuffer.size() < MAX_FRAUD_BUFFER) {
                fraudSampleBuffer.add(deepCopy(seq));
            } else {
                // 环形替换
                fraudSampleBuffer.set((int) (totalFraudSamples % MAX_FRAUD_BUFFER), deepCopy(seq));
            }
        }
    }

    /**
     * 获取近期窗口数据（1000样本）
     * 用于MODERATE漂移的微调训练
     */
    public List<UserBehaviorSequence> getRecentWindow() {
        return recentReservoir.getAll();
    }

    /**
     * 获取历史窗口数据（10000样本）
     * 用于SEVERE漂移的全量重训
     */
    public List<UserBehaviorSequence> getHistoricalWindow() {
        return historicalReservoir.getAll();
    }

    /**
     * 获取基线窗口数据（50000样本）
     * 用于CRITICAL漂移的紧急全量重训
     */
    public List<UserBehaviorSequence> getBaselineWindow() {
        return baselineReservoir.getAll();
    }

    /**
     * 获取近期+欺诈过采样数据
     * 返回近期窗口全部样本 + 额外注入的欺诈样本
     * 用于改善少数类（欺诈）的学习效果
     */
    public List<UserBehaviorSequence> getRecentWithOversampling() {
        List<UserBehaviorSequence> result = new ArrayList<>(recentReservoir.getAll());
        
        // 注入欺诈样本使正负比例达到约1:5
        int fraudNeeded = Math.min(result.size() / 5, fraudSampleBuffer.size());
        if (fraudNeeded > 0) {
            Collections.shuffle(fraudSampleBuffer, new Random(42));
            for (int i = 0; i < fraudNeeded && i < fraudSampleBuffer.size(); i++) {
                result.add(fraudSampleBuffer.get(i));
            }
        }
        
        Collections.shuffle(result, new Random(42));
        return result;
    }

    /**
     * 获取时间切片数据
     * 返回最近N个样本（从所有窗口中取最新的）
     */
    public List<UserBehaviorSequence> getTimeSlice(int n) {
        List<UserBehaviorSequence> all = recentReservoir.getAll();
        if (all.size() <= n) return all;
        
        // 取最后n个
        return all.subList(Math.max(0, all.size() - n), all.size());
    }

    /**
     * 按欺诈类型筛选数据
     * 用于针对性重训特定欺诈模式
     */
    public List<UserBehaviorSequence> getSamplesByFraudType(String fraudType) {
        List<UserBehaviorSequence> result = new ArrayList<>();
        for (UserBehaviorSequence seq : historicalReservoir.getAll()) {
            if (seq.fraudType != null && seq.fraudType.equals(fraudType)) {
                result.add(seq);
            }
        }
        return result;
    }

    /**
     * 获取窗口统计信息
     */
    public WindowStats getStats() {
        return new WindowStats(
                totalSamplesProcessed,
                totalFraudSamples,
                recentReservoir.size(),
                historicalReservoir.size(),
                baselineReservoir.size(),
                fraudSampleBuffer.size()
        );
    }

    /**
     * 深拷贝UserBehaviorSequence
     * 用于欺诈样本缓冲区的独立存储
     */
    private UserBehaviorSequence deepCopy(UserBehaviorSequence src) {
        UserBehaviorSequence copy = new UserBehaviorSequence();
        copy.accountId = src.accountId;
        copy.behaviorPath = src.behaviorPath;
        copy.sequenceLength = src.sequenceLength;
        copy.sequenceStartTime = src.sequenceStartTime;
        copy.sequenceEndTime = src.sequenceEndTime;
        copy.isFraud = src.isFraud;
        copy.fraudType = src.fraudType;
        
        // 复制数值特征
        copy.avgAmount = src.avgAmount;
        copy.stdAmount = src.stdAmount;
        copy.maxAmount = src.maxAmount;
        copy.totalAmount = src.totalAmount;
        copy.cityChangeCount = src.cityChangeCount;
        copy.deviceChangeCount = src.deviceChangeCount;
        copy.channelChangeCount = src.channelChangeCount;
        copy.nightTxCount = src.nightTxCount;
        copy.highRiskDeviceCount = src.highRiskDeviceCount;
        copy.abroadTxCount = src.abroadTxCount;
        copy.distinctCityCount = src.distinctCityCount;
        copy.distinctDeviceCount = src.distinctDeviceCount;
        copy.distinctChannelCount = src.distinctChannelCount;
        copy.amountBalanceRatio = src.amountBalanceRatio;
        copy.cashOutTransferRatio = src.cashOutTransferRatio;
        copy.velocityScore = src.velocityScore;
        copy.intervalMean = src.intervalMean;
        copy.intervalVariance = src.intervalVariance;
        copy.amountTrendSlope = src.amountTrendSlope;
        copy.frequencyAcceleration = src.frequencyAcceleration;
        copy.outDegree = src.outDegree;
        copy.inDegree = src.inDegree;
        copy.maxSingleTransferRatio = src.maxSingleTransferRatio;
        copy.distinctDestCount = src.distinctDestCount;
        copy.graphCycleCount = src.graphCycleCount;
        copy.graphMaxChainDepth = src.graphMaxChainDepth;
        copy.graphCommunitySize = src.graphCommunitySize;
        copy.graphInAmount = src.graphInAmount;
        copy.graphOutAmount = src.graphOutAmount;
        copy.graphFlowBalance = src.graphFlowBalance;
        copy.graphTotalDegree = src.graphTotalDegree;
        copy.directionAsymmetry = src.directionAsymmetry;
        copy.inOutFreqRatio = src.inOutFreqRatio;
        copy.netFlowAsymmetry = src.netFlowAsymmetry;
        copy.timeConcentration = src.timeConcentration;
        copy.timeDensityRatio = src.timeDensityRatio;
        copy.hourRiskScore = src.hourRiskScore;
        
        // 复制特征向量
        if (src.features != null) {
            copy.features = src.features.clone();
        }
        
        return copy;
    }

    /**
     * 蓄水池采样实现
     * 在未知总量的数据流中，等概率保留n个样本
     * 算法：第i个样本被选入的概率 = n/i
     * 空间复杂度: O(n)，与数据流总量无关
     */
    public static class ReservoirSampler implements Serializable {
        private final int capacity;
        private final Random random;
        private final List<UserBehaviorSequence> reservoir;
        private long count;

        public ReservoirSampler(int capacity) {
            this.capacity = capacity;
            this.random = new Random(42);
            this.reservoir = new ArrayList<>(capacity);
            this.count = 0;
        }

        /**
         * 添加样本到蓄水池
         * 前capacity个样本直接放入
         * 后续样本以 capacity/count 的概率替换已有样本
         */
        public void add(UserBehaviorSequence item) {
            if (item == null) return;
            count++;

            if (reservoir.size() < capacity) {
                // 未满，直接加入
                reservoir.add(item);
            } else {
                // 已满，蓄水池采样
                int j = random.nextInt((int) Math.min(count, Integer.MAX_VALUE));
                if (j < capacity) {
                    reservoir.set(j, item);
                }
            }
        }

        public List<UserBehaviorSequence> getAll() {
            return new ArrayList<>(reservoir);
        }

        public int size() {
            return reservoir.size();
        }

        public long getCount() {
            return count;
        }
    }

    /**
     * 窗口统计信息
     */
    public static class WindowStats implements Serializable {
        public final long totalProcessed;
        public final long totalFraud;
        public final int recentSize;
        public final int historicalSize;
        public final int baselineSize;
        public final int fraudBufferSize;

        public WindowStats(long totalProcessed, long totalFraud,
                          int recentSize, int historicalSize,
                          int baselineSize, int fraudBufferSize) {
            this.totalProcessed = totalProcessed;
            this.totalFraud = totalFraud;
            this.recentSize = recentSize;
            this.historicalSize = historicalSize;
            this.baselineSize = baselineSize;
            this.fraudBufferSize = fraudBufferSize;
        }

        public double getFraudRate() {
            return totalProcessed > 0 ? (double) totalFraud / totalProcessed : 0;
        }

        @Override
        public String toString() {
            return String.format(
                    "WindowStats{total=%d, fraud=%d(%.2f%%), recent=%d, historical=%d, baseline=%d, fraudBuf=%d}",
                    totalProcessed, totalFraud, getFraudRate() * 100,
                    recentSize, historicalSize, baselineSize, fraudBufferSize
            );
        }
    }
}
