package com.fraud.detection.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 用户行为序列类 - ML模型输入
按账户聚合交易记录,构建30维特征向量
包含:16维统计特征+4维时序特征+3维基础图特征+7维深度图特征
 */
public class UserBehaviorSequence implements Serializable {

    public String accountId;
    public List<Transaction> transactions;
    public String behaviorPath;
    public double[] features;
    public long sequenceStartTime;
    public long sequenceEndTime;
    public int sequenceLength;

    public double avgAmount;
    public double stdAmount;
    public double maxAmount;
    public double totalAmount;
    public int cityChangeCount;
    public int deviceChangeCount;
    public int channelChangeCount;
    public int nightTxCount;
    public int highRiskDeviceCount;
    public int abroadTxCount;
    public int distinctCityCount;
    public int distinctDeviceCount;
    public int distinctChannelCount;
    public double amountBalanceRatio;
    public double cashOutTransferRatio;
    public double velocityScore;

    // 时序特征
    public double intervalMean;          // 交易间隔均值（秒）
    public double intervalVariance;      // 交易间隔方差
    public double amountTrendSlope;      // 金额趋势斜率（正=递增，负=递减）
    public double frequencyAcceleration; // 频率加速度（后半段频率 / 前半段频率）

    // 图特征
    public int outDegree;                // 出度（向多少不同账户转出）
    public int inDegree;                 // 入度（从多少不同账户接收）
    public double maxSingleTransferRatio; // 最大单笔转出占总金额比例
    public int distinctDestCount;        // 不同目标账户数
    
    // 新增：欺诈标签（用于有监督训练）
    public int isFraud;                  // 1=欺诈，0=正常
    public String fraudType;             // 欺诈类型

    // 图特征
    public int graphCycleCount;          // 关联环路数
    public int graphMaxChainDepth;       // 最大资金链深度
    public int graphCommunitySize;       // 同IP/设备关联账户数
    public double graphInAmount;         // 图上的入金总额
    public double graphOutAmount;        // 图上的出金总额
    public double graphFlowBalance;      // 流入/流出比
    public int graphTotalDegree;         // 图总度数

    // 方向性特征（来自GraphAnalyzer方向分析）
    public double directionAsymmetry;    // 方向不对称度 abs(in-out)/(in+out)
    public double inOutFreqRatio;        // 入/出频率比 outDegree/inDegree
    public double netFlowAsymmetry;      // 净流出不对称 (out-in)/(in+out)
    public double timeConcentration;     // 时间集中度（单位时间交易密度）

    // 时间演化特征
    public double timeDensityRatio;      // 近5分钟交易数/近30分钟交易数
    public double hourRiskScore;         // 交易时间风险分（凌晨=高）

    public UserBehaviorSequence() {
        this.transactions = new ArrayList<>();
        this.features = new double[16];
        this.sequenceStartTime = System.currentTimeMillis();
        this.behaviorPath = "";
    }

/**
     * 构建行为路径字符串
     * 编码交易类型和切换点,如T->T[CITY][DEV]->C
     * 用于描述用户行为序列的模式
     */
    public void buildBehaviorPath() {
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < transactions.size(); i++) {
            Transaction tx = transactions.get(i);
            switch (tx.type) {
                case "TRANSFER":
                    path.append("T");
                    break;
                case "CASH_OUT":
                    path.append("C");
                    break;
                case "PAYMENT":
                    path.append("P");
                    break;
                case "DEBIT":
                    path.append("D");
                    break;
                default:
                    path.append("?");
            }

            if (i < transactions.size() - 1) {
                Transaction next = transactions.get(i + 1);
                if (!Objects.equals(tx.city, next.city)) {
                    path.append("[CITY]");
                }
                if (!Objects.equals(tx.deviceId, next.deviceId)) {
                    path.append("[DEV]");
                }
                if (!Objects.equals(tx.payChannel, next.payChannel)) {
                    path.append("[CH]");
                }
                path.append("->");
            }
        }
        this.behaviorPath = path.toString();
    }

/**
     * 计算23维特征向量(不含图特征)
     * 包含:16维统计特征+4维时序特征+3维基础图特征
     * 在BehaviorSequenceBuilder中调用
     */
    public void computeFeatures() {
        if (transactions.isEmpty()) {
            return;
        }

        int size = transactions.size();
        double sum = 0;
        double sumSq = 0;
        double max = 0;
        String lastCity = transactions.get(0).city;
        String lastDevice = transactions.get(0).deviceId;
        String lastChannel = transactions.get(0).payChannel;
        int cityChanges = 0;
        int deviceChanges = 0;
        int channelChanges = 0;
        int nightCount = 0;
        int highRiskCount = 0;
        int abroadCount = 0;
        int cashOutTransferCount = 0;
        double maxBalanceRatio = 0;
        Set<String> cities = new HashSet<>();
        Set<String> devices = new HashSet<>();
        Set<String> channels = new HashSet<>();

        // 图特征变量
        Set<String> destAccounts = new HashSet<>();
        Set<String> origAccounts = new HashSet<>();
        double maxSingleTransfer = 0;

        for (Transaction tx : transactions) {
            sum += tx.amount;
            sumSq += tx.amount * tx.amount;
            cities.add(tx.city);
            devices.add(tx.deviceId);
            channels.add(tx.payChannel);
            destAccounts.add(tx.nameDest);
            origAccounts.add(tx.nameOrig);

            if (tx.amount > max) {
                max = tx.amount;
            }

            if (!Objects.equals(tx.city, lastCity)) {
                cityChanges++;
                lastCity = tx.city;
            }

            if (!Objects.equals(tx.deviceId, lastDevice)) {
                deviceChanges++;
                lastDevice = tx.deviceId;
            }

            if (!Objects.equals(tx.payChannel, lastChannel)) {
                channelChanges++;
                lastChannel = tx.payChannel;
            }

            if (tx.transactionHour >= 22 || tx.transactionHour <= 6) {
                nightCount++;
            }

            if ("HIGH".equals(tx.deviceRiskLevel)) {
                highRiskCount++;
            }

            if ("ABROAD".equals(tx.isAbroad) || "YES".equals(tx.isAbroad) || "1".equals(tx.isAbroad)) {
                abroadCount++;
            }

            if ("TRANSFER".equals(tx.type) || "CASH_OUT".equals(tx.type)) {
                cashOutTransferCount++;
                if (tx.amount > maxSingleTransfer) {
                    maxSingleTransfer = tx.amount;
                }
            }

            double ratio = tx.oldbalanceOrg > 0 ? tx.amount / tx.oldbalanceOrg : 0;
            if (ratio > maxBalanceRatio) {
                maxBalanceRatio = ratio;
            }
        }

        this.avgAmount = sum / size;
        this.stdAmount = Math.sqrt(Math.max(sumSq / size - avgAmount * avgAmount, 0));
        this.maxAmount = max;
        this.totalAmount = sum;
        this.cityChangeCount = cityChanges;
        this.deviceChangeCount = deviceChanges;
        this.channelChangeCount = channelChanges;
        this.nightTxCount = nightCount;
        this.highRiskDeviceCount = highRiskCount;
        this.abroadTxCount = abroadCount;
        this.distinctCityCount = cities.size();
        this.distinctDeviceCount = devices.size();
        this.distinctChannelCount = channels.size();
        this.amountBalanceRatio = maxBalanceRatio;
        this.cashOutTransferRatio = size > 0 ? (double) cashOutTransferCount / size : 0;
        this.sequenceLength = size;
        this.velocityScore = computeVelocityScore(size);

        // 计算时序特征
        computeTimeSeriesFeatures();

        // 计算图特征
        this.outDegree = destAccounts.size();
        this.inDegree = origAccounts.size();
        this.maxSingleTransferRatio = totalAmount > 0 ? maxSingleTransfer / totalAmount : 0;
        this.distinctDestCount = destAccounts.size();

        // 构建完整的36维特征向量
        this.features = new double[]{
                avgAmount,              // 0
                stdAmount,              // 1
                maxAmount,              // 2
                totalAmount,            // 3
                cityChanges,            // 4
                deviceChanges,          // 5
                channelChanges,         // 6
                nightCount,             // 7
                highRiskCount,          // 8
                abroadCount,            // 9
                distinctCityCount,      // 10
                distinctDeviceCount,    // 11
                distinctChannelCount,   // 12
                amountBalanceRatio,     // 13
                cashOutTransferRatio,   // 14
                velocityScore,          // 15
                // 时序特征
                intervalMean,           // 16
                intervalVariance,       // 17
                amountTrendSlope,       // 18
                frequencyAcceleration,  // 19
                // 基础图特征
                (double) outDegree,     // 20
                maxSingleTransferRatio, // 21
                (double) distinctDestCount, // 22
                // 深度图特征（初始化为0，由GraphFeatureExtractor填充）
                (double) graphCycleCount,       // 23
                (double) graphMaxChainDepth,    // 24
                (double) graphCommunitySize,    // 25
                graphInAmount,                  // 26
                graphOutAmount,                 // 27
                graphFlowBalance,               // 28
                (double) graphTotalDegree,      // 29
                // 方向性特征（初始化为0，由GraphAnalyzer填充）
                directionAsymmetry,             // 30
                inOutFreqRatio,                 // 31
                netFlowAsymmetry,               // 32
                timeConcentration,              // 33
                // 时间演化特征
                timeDensityRatio,               // 34
                hourRiskScore                   // 35
        };
    }

/**
     * 计算4维时序特征
     * 包含:交易间隔均值/方差、金额趋势斜率、频率加速度
     * 时序特征能捕捉交易行为的动态变化模式
     */
    private void computeTimeSeriesFeatures() {
        if (transactions.size() < 2) {
            this.intervalMean = 0;
            this.intervalVariance = 0;
            this.amountTrendSlope = 0;
            this.frequencyAcceleration = 0;
            return;
        }

        // 计算交易间隔
        double[] intervals = new double[transactions.size() - 1];
        double intervalSum = 0;
        for (int i = 1; i < transactions.size(); i++) {
            long diff = transactions.get(i).eventTime - transactions.get(i - 1).eventTime;
            intervals[i - 1] = Math.max(diff / 1000.0, 0.1); // 转为秒，最小0.1秒
            intervalSum += intervals[i - 1];
        }
        this.intervalMean = intervalSum / intervals.length;

        // 间隔方差
        double intervalVarSum = 0;
        for (double interval : intervals) {
            double diff = interval - intervalMean;
            intervalVarSum += diff * diff;
        }
        this.intervalVariance = intervalVarSum / intervals.length;

        // 金额趋势斜率（线性回归）
        this.amountTrendSlope = computeSlope();

        // 频率加速度：后半段频率 / 前半段频率
        int mid = transactions.size() / 2;
        if (mid >= 1) {
            long firstHalfDuration = Math.max(transactions.get(mid - 1).eventTime - transactions.get(0).eventTime, 1);
            long secondHalfDuration = Math.max(transactions.get(transactions.size() - 1).eventTime - transactions.get(mid).eventTime, 1);
            double firstFreq = mid * 1000.0 / firstHalfDuration;
            double secondFreq = (transactions.size() - mid) * 1000.0 / secondHalfDuration;
            this.frequencyAcceleration = firstFreq > 0 ? secondFreq / firstFreq : 0;
        } else {
            this.frequencyAcceleration = 0;
        }
    }

/**
     * 线性回归计算金额趋势斜率
     * 正斜率=金额递增趋势,负斜率=递减趋势
     * 递增趋势更可疑(可能是试探性攻击)
     */
    private double computeSlope() {
        int n = transactions.size();
        if (n < 2) return 0;

        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
        for (int i = 0; i < n; i++) {
            double x = i;
            double y = transactions.get(i).amount;
            sumX += x;
            sumY += y;
            sumXY += x * y;
            sumX2 += x * x;
        }
        double denominator = n * sumX2 - sumX * sumX;
        if (Math.abs(denominator) < 1e-10) return 0;
        return (n * sumXY - sumX * sumY) / denominator;
    }

/**
     * 计算交易速率评分:笔数/分钟
     * 高频交易更可疑(机器自动化操作特征)
     */
    private double computeVelocityScore(int size) {
        long duration = Math.max(sequenceEndTime - sequenceStartTime, 1);
        double minutes = duration / 60000.0;
        if (minutes <= 0.1) {
            minutes = 0.1;
        }
        return size / minutes;
    }

    /**
     * 计算包含图特征的完整特征向量（30维）
     * 在 GraphFeatureExtractor 中调用
     */
    public void computeFeaturesWithGraph() {
        computeFeatures();
        this.features = new double[]{
                avgAmount,              // 0
                stdAmount,              // 1
                maxAmount,              // 2
                totalAmount,            // 3
                (double) cityChangeCount,  // 4
                (double) deviceChangeCount, // 5
                (double) channelChangeCount, // 6
                (double) nightTxCount,     // 7
                (double) highRiskDeviceCount, // 8
                (double) abroadTxCount,    // 9
                distinctCityCount,      // 10
                distinctDeviceCount,    // 11
                distinctChannelCount,   // 12
                amountBalanceRatio,     // 13
                cashOutTransferRatio,   // 14
                velocityScore,          // 15
                intervalMean,           // 16
                intervalVariance,       // 17
                amountTrendSlope,       // 18
                frequencyAcceleration,  // 19
                (double) outDegree,     // 20
                maxSingleTransferRatio, // 21
                (double) distinctDestCount, // 22
                (double) graphCycleCount,       // 23
                (double) graphMaxChainDepth,    // 24
                (double) graphCommunitySize,    // 25
                graphInAmount,                  // 26
                graphOutAmount,                 // 27
                graphFlowBalance,               // 28
                (double) graphTotalDegree,      // 29
                directionAsymmetry,             // 30
                inOutFreqRatio,                 // 31
                netFlowAsymmetry,               // 32
                timeConcentration,              // 33
                timeDensityRatio,               // 34
                hourRiskScore                   // 35
        };
    }

    @Override
    public String toString() {
        return String.format(
                "Sequence{account=%s, path=%s, len=%d, avgAmt=%.2f, totalAmt=%.2f, cityChg=%d, devChg=%d, channelChg=%d, night=%d, highRisk=%d, abroad=%d, velocity=%.2f}",
                accountId, behaviorPath, sequenceLength, avgAmount, totalAmount,
                cityChangeCount, deviceChangeCount, channelChangeCount, nightTxCount,
                highRiskDeviceCount, abroadTxCount, velocityScore
        );
    }
}
