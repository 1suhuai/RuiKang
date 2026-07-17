package com.fraud.detection.ml;

import com.fraud.detection.explain.ExplainabilityEngine;
import com.fraud.detection.model.Alert;
import com.fraud.detection.model.AlertExplanation;
import com.fraud.detection.model.UserBehaviorSequence;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * ML异常检测器 - 核心预测组件
 * 集成6种检测能力:在线逻辑回归/GBDT树集成/行为评分/统计画像/路径频率/孤立森林
 * 使用Ensemble集成学习,动态调整各模型权重
 * 阈值0.35以上生成告警
 */
public class MLAnomalyDetector extends RichFlatMapFunction<UserBehaviorSequence, Alert> {

    // ML检测阈值：提高质量，宁缺毋滥，每条告警都要能说清类型
    private static final double ALERT_THRESHOLD = 0.55;
    private final boolean useGlobalModel;
    private transient ModelTrainer.OnlineLogisticRegressionModel classifier;
    private transient ModelTrainer.GBDTModel gbdtModel;
    private transient ModelTrainer.StatisticsModel statisticsModel;
    private transient ModelTrainer.PathFrequencyModel pathModel;
    private transient ModelTrainer.IsolationForestModel isolationForestModel;
    private transient ModelTrainer.DriftDetector driftDetector;
    private transient ModelTrainer.EnsembleWeightManager weightManager;
    private transient GlobalMLModelCache.ModelSnapshot globalModel;
    private transient ExplainabilityEngine explainabilityEngine;

    public MLAnomalyDetector() {
        this.useGlobalModel = true;
    }
    
    public MLAnomalyDetector(boolean useGlobalModel) {
        this.useGlobalModel = useGlobalModel;
    }

    @Override
    public void open(Configuration parameters) {
        // 优先从预训练文件加载模型
        if (useGlobalModel) {
            boolean loaded = GlobalMLModelCache.getInstance().loadFromPretrained();
            if (loaded) {
                System.out.println("[MLAnomalyDetector] 使用预训练模型，模型已就绪");
            } else {
                System.out.println("[MLAnomalyDetector] 未找到预训练模型，使用空模型（等待在线训练）");
            }
        }

        classifier = new ModelTrainer.OnlineLogisticRegressionModel();
        gbdtModel = new ModelTrainer.GBDTModel();
        statisticsModel = new ModelTrainer.StatisticsModel();
        pathModel = new ModelTrainer.PathFrequencyModel();
        isolationForestModel = new ModelTrainer.IsolationForestModel();
        driftDetector = new ModelTrainer.DriftDetector();
        weightManager = new ModelTrainer.EnsembleWeightManager();
        explainabilityEngine = new ExplainabilityEngine();
    }
    
/**
     * 刷新全局模型缓存
     * 当GlobalMLModelCache有新版本时,加载最新模型
     */
    private void refreshGlobalModel() {
        if (useGlobalModel && GlobalMLModelCache.getInstance().isModelReady()) {
            GlobalMLModelCache.ModelSnapshot snapshot = GlobalMLModelCache.getInstance().getModel();
            if (snapshot != null && snapshot != globalModel) {
                globalModel = snapshot;
            }
        }
    }

    @Override
/**
     * 核心处理方法:对用户行为序列进行异常检测
     * 集成5种检测模型,加权计算最终欺诈评分
     * 评分>=0.35生成告警,包含特征贡献度和可解释性信息
     */
    public void flatMap(UserBehaviorSequence sequence, Collector<Alert> out) {
        if (sequence == null || sequence.features == null || sequence.sequenceLength < 2) {
            return;
        }

        refreshGlobalModel();
        boolean useGlobal = (globalModel != null);
        ModelTrainer.Prediction prediction;
        double gbdtScore;
        double statisticalScore;
        double pathScore;
        double isolationScore;
        double wLogistic;
        double wGBDT;
        double wBehavior;
        double wStatistical;
        double wPath;
        
        if (useGlobal) {
            prediction = globalModel.classifier.predict(sequence);
            gbdtScore = globalModel.gbdtModel.predict(sequence);
            statisticalScore = globalModel.statisticsModel.detectAnomaly(sequence);
            pathScore = globalModel.pathModel.detectAnomaly(sequence.behaviorPath);
            isolationScore = globalModel.isolationForestModel.detectAnomaly(sequence);
            wLogistic = globalModel.weightManager.getLogisticWeight();
            wGBDT = globalModel.weightManager.getGBDTWeight();
            wBehavior = globalModel.weightManager.getBehaviorWeight();
            wStatistical = globalModel.weightManager.getStatisticalWeight();
            wPath = globalModel.weightManager.getPathWeight();
        } else {
            prediction = classifier.predict(sequence);
            gbdtScore = gbdtModel.predict(sequence);
            statisticalScore = statisticsModel.detectAnomaly(sequence);
            pathScore = pathModel.detectAnomaly(sequence.behaviorPath);
            isolationScore = isolationForestModel.detectAnomaly(sequence);
            wLogistic = weightManager.getLogisticWeight();
            wGBDT = weightManager.getGBDTWeight();
            wBehavior = weightManager.getBehaviorWeight();
            wStatistical = weightManager.getStatisticalWeight();
            wPath = weightManager.getPathWeight();
        }

        double behaviorScore = computeBehaviorScore(sequence);

        double isolationWeight = 0.15;
        double gbdtEnsembleWeight = 0.15;
        double totalBaseWeight = wLogistic + wGBDT + wBehavior + wStatistical + wPath;
        double probability = Math.min(1.0,
                prediction.finalProbability * wLogistic / totalBaseWeight * (1 - isolationWeight - gbdtEnsembleWeight)
                        + gbdtScore * wGBDT / totalBaseWeight * (1 - isolationWeight - gbdtEnsembleWeight)
                        + behaviorScore * wBehavior / totalBaseWeight * (1 - isolationWeight - gbdtEnsembleWeight)
                        + statisticalScore * wStatistical / totalBaseWeight * (1 - isolationWeight - gbdtEnsembleWeight)
                        + pathScore * wPath / totalBaseWeight * (1 - isolationWeight - gbdtEnsembleWeight)
                        + isolationScore * isolationWeight
                        + gbdtScore * gbdtEnsembleWeight);

        // 集成概率不再被单个子模型强制拉高，避免isolation空模型导致大量误报
        // 只有当多个子模型同时给出高分时才提升最终概率

        boolean driftDetected = driftDetector.isDriftDetected();
        double effectiveThreshold = driftDetected ? ALERT_THRESHOLD * 0.80 : ALERT_THRESHOLD;

        // 不再单独用isolationScore兜底，统一走集成概率
        boolean modelBasedAlert = probability >= effectiveThreshold
                || (useGlobal && probability >= effectiveThreshold * 0.85);

        // ML兜底层规则：仅保留强信号模式，避免空模型时大量误报
        boolean ruleBasedAlert = behaviorScore >= 0.55
                || (sequence.abroadTxCount > 0 && sequence.highRiskDeviceCount >= 1 && sequence.maxAmount >= 50000)
                || (sequence.nightTxCount >= 2 && sequence.highRiskDeviceCount >= 1 && sequence.maxAmount >= 50000)
                || (sequence.cityChangeCount >= 1 && sequence.deviceChangeCount >= 1 && sequence.maxAmount >= 50000)
                || (sequence.totalAmount >= 100000 && sequence.highRiskDeviceCount >= 1)
                || (sequence.cashOutTransferRatio >= 0.7 && sequence.maxAmount >= 50000)
                || (sequence.nightTxCount >= 2 && sequence.maxAmount >= 50000)
                || (sequence.abroadTxCount > 0 && sequence.nightTxCount >= 1 && sequence.maxAmount >= 30000)
                || (sequence.channelChangeCount >= 2 && sequence.totalAmount >= 50000)
                || (sequence.amountTrendSlope > 15000 && sequence.maxAmount >= 50000)
                || (sequence.outDegree >= 3 && sequence.maxSingleTransferRatio >= 0.4 && sequence.totalAmount >= 50000);

        boolean shouldAlert = useGlobal ? modelBasedAlert : (modelBasedAlert || ruleBasedAlert);

        if (shouldAlert) {
            String fraudType = resolveFraudType(sequence, probability, statisticalScore, pathScore, behaviorScore, isolationScore);
            // 无法判定具体类型的低置信度告警不输出，提高告警质量
            if (fraudType == null) {
                return;
            }
            Alert alert = new Alert(sequence.accountId, fraudType,
                    Math.max(probability, behaviorScore), "ML_ENSEMBLE");
            alert.amount = sequence.totalAmount;
            alert.behaviorPath = sequence.behaviorPath;
            alert.timestamp = sequence.sequenceEndTime > 0 ? sequence.sequenceEndTime : System.currentTimeMillis();
            // 从序列中获取最后一笔交易的上下文信息
            if (!sequence.transactions.isEmpty()) {
                alert.withTransactionContext(sequence.transactions.get(sequence.transactions.size() - 1));
            }
            AlertExplanation explanation = explainabilityEngine.explainAlert(alert, sequence);
            alert.explanation = explanation;
            alert.explanationSummary = explainabilityEngine.buildBriefExplanation(alert, sequence);
            alert.details = String.format(
                    "ensembleProb=%.4f,behaviorScore=%.4f,logisticProb=%.4f,gbdtScore=%.4f,reconstructErr=%.4f," +
                            "statAnomaly=%.4f,pathAnomaly=%.4f,isolationScore=%.4f,driftDetected=%b," +
                            "weights[L=%.2f,G=%.2f,B=%.2f,S=%.2f,P=%.2f]," +
                            "onlineSamples=%d,gbdtTrees=%d,pathSamples=%d,modelSource=%s," +
                            "avgAmt=%.2f,maxAmt=%.2f,totalAmt=%.2f," +
                            "cityChg=%d,devChg=%d,channelChg=%d,night=%d,highRisk=%d,abroad=%d," +
                            "balanceRatio=%.2f,cashOutRatio=%.2f," +
                            "intervalMean=%.1f,intervalVar=%.1f,amtTrend=%.1f,freqAccel=%.2f," +
                            "outDegree=%d,maxSingleRatio=%.2f,path=%s",
                    probability, behaviorScore, prediction.logisticProbability, gbdtScore, prediction.reconstructionError,
                    statisticalScore, pathScore, isolationScore, driftDetected,
                    wLogistic, wGBDT, wBehavior, wStatistical, wPath,
                    useGlobal ? globalModel.classifier.getSampleCount() : classifier.getSampleCount(),
                    useGlobal ? globalModel.gbdtModel.getTreeCount() : gbdtModel.getTreeCount(),
                    useGlobal ? globalModel.pathModel.getTotalPaths() : pathModel.getTotalPaths(),
                    useGlobal ? "GLOBAL" : "LOCAL",
                    sequence.avgAmount, sequence.maxAmount, sequence.totalAmount,
                    sequence.cityChangeCount, sequence.deviceChangeCount, sequence.channelChangeCount,
                    sequence.nightTxCount, sequence.highRiskDeviceCount, sequence.abroadTxCount,
                    sequence.amountBalanceRatio, sequence.cashOutTransferRatio,
                    sequence.intervalMean, sequence.intervalVariance, sequence.amountTrendSlope,
                    sequence.frequencyAcceleration, sequence.outDegree, sequence.maxSingleTransferRatio,
                    sequence.behaviorPath
            );
            out.collect(alert);
        }

        // 仅在训练模式下更新模型参数
        // 测试/推断阶段禁止训练，防止数据泄露和模型污染
        if (!useGlobal) {
            classifier.train(sequence, sequence.isFraud);
            gbdtModel.train(sequence, sequence.isFraud);
            statisticsModel.train(sequence);
            pathModel.train(sequence.behaviorPath);
            isolationForestModel.train(sequence);
            driftDetector.update(sequence);
        }
    }

    // 行为评分: ML兜底层，比CEP/SQL层门槛高但保留合理检测能力
    private double computeBehaviorScore(UserBehaviorSequence sequence) {
        double score = 0.0;

        // 大额异常
        if (sequence.maxAmount >= 30000) score += 0.08;
        if (sequence.maxAmount >= 50000) score += 0.06;
        if (sequence.totalAmount >= 50000) score += 0.06;
        if (sequence.totalAmount >= 100000) score += 0.06;

        // 高危设备
        if (sequence.highRiskDeviceCount >= 1) score += 0.08;
        if (sequence.highRiskDeviceCount >= 2) score += 0.06;

        // 跨境
        if (sequence.abroadTxCount >= 1) score += 0.10;

        // 夜间
        if (sequence.nightTxCount >= 1) score += 0.06;
        if (sequence.nightTxCount >= 2) score += 0.04;

        // 异地 + 设备变更
        if (sequence.cityChangeCount >= 1 && sequence.deviceChangeCount >= 1) score += 0.08;

        // 集中提现
        if (sequence.cashOutTransferRatio >= 0.5 && sequence.maxAmount >= 15000) score += 0.08;
        if (sequence.cashOutTransferRatio >= 0.7 && sequence.maxAmount >= 30000) score += 0.06;

        // 余额掏空
        if (sequence.amountBalanceRatio >= 0.5) score += 0.08;
        if (sequence.amountBalanceRatio >= 0.8) score += 0.06;

        // 速度异常 + 提现
        if (sequence.velocityScore >= 2.0 && sequence.cashOutTransferRatio >= 0.4) score += 0.06;

        // 多渠道
        if (sequence.channelChangeCount >= 2 && sequence.totalAmount >= 20000) score += 0.06;

        // 多目标分散转出
        if (sequence.outDegree >= 3 && sequence.maxSingleTransferRatio >= 0.3) score += 0.06;

        // 金额趋势
        if (sequence.amountTrendSlope > 8000) score += 0.06;
        if (sequence.frequencyAcceleration >= 2.0) score += 0.04;

        // 账户被盗急速转账
        if (sequence.nightTxCount >= 1 && sequence.abroadTxCount > 0 && sequence.highRiskDeviceCount >= 1 && sequence.maxAmount >= 20000) score += 0.10;

        // 虚假交易退款套利
        if (sequence.cashOutTransferRatio == 0 && sequence.sequenceLength >= 2 && sequence.totalAmount >= 15000 && sequence.totalAmount <= 200000) score += 0.10;

        // 养卡提额异常消费
        if (sequence.cashOutTransferRatio == 0 && sequence.sequenceLength >= 3 && sequence.nightTxCount >= 2 && sequence.maxAmount >= 20000) score += 0.10;

        return Math.min(1.0, score);
    }

    // 欺诈类型判定: ML层必须能说清具体类型，避免"未知异常"
    private String resolveFraudType(UserBehaviorSequence sequence, double probability,
                                     double statisticalScore, double pathScore,
                                     double behaviorScore, double isolationScore) {

        // 账户被盗急速转账 - 需要极强信号
        if (sequence.nightTxCount >= 2 && sequence.abroadTxCount > 0 && sequence.highRiskDeviceCount >= 1 && sequence.maxAmount >= 50000) {
            return "账户被盗急速转账";
        }

        // 虚假交易退款套利 - 需要明确的PAYMENT模式+大额
        if (sequence.cashOutTransferRatio == 0 && sequence.sequenceLength >= 3 && sequence.totalAmount >= 50000 && sequence.totalAmount <= 500000) {
            return "虚假交易退款套利";
        }

        // 养卡提额异常消费 - 需要连续PAYMENT+夜间+高额
        if (sequence.cashOutTransferRatio == 0 && sequence.sequenceLength >= 4 && sequence.nightTxCount >= 2 && sequence.maxAmount >= 50000) {
            return "养卡提额异常消费";
        }

        // 在线模型识别异地跨设备异常
        if (sequence.cityChangeCount >= 1 && sequence.deviceChangeCount >= 1 && sequence.maxAmount >= 30000) {
            return "在线模型识别异地跨设备异常";
        }

        // 在线模型识别集中提现模式
        if (sequence.cashOutTransferRatio >= 0.6 && sequence.maxAmount >= 30000) {
            return "在线模型识别集中提现模式";
        }

        // 在线模型识别高频资金转移
        if (sequence.velocityScore >= 2.0 && sequence.cashOutTransferRatio >= 0.5) {
            return "在线模型识别高频资金转移";
        }

        // 在线模型识别高风险设备大额
        if (sequence.highRiskDeviceCount >= 1 && sequence.totalAmount >= 50000) {
            return "在线模型识别高风险设备大额";
        }

        // 在线模型识别境外夜间异常
        if (sequence.abroadTxCount > 0 && sequence.nightTxCount >= 1 && sequence.maxAmount >= 30000) {
            return "在线模型识别境外夜间异常";
        }

        // 统计画像偏离欺诈
        if (statisticalScore >= 0.50 && sequence.amountBalanceRatio >= 0.5) {
            return "统计画像偏离欺诈";
        }

        // 多目标分散转出
        if (sequence.outDegree >= 3 && sequence.maxSingleTransferRatio >= 0.3) {
            return "在线模型识别多目标分散转出";
        }

        // 小额试探大额转出
        if (sequence.sequenceLength >= 2 && sequence.amountTrendSlope > 10000 && sequence.maxAmount >= 30000) {
            return "在线模型识别小额试探大额转出";
        }

        // 凌晨分批掏空
        if (sequence.nightTxCount >= 2 && sequence.amountBalanceRatio >= 0.7 && sequence.totalAmount >= 30000) {
            return "在线模型识别凌晨分批掏空";
        }

        // 兜底：高置信度时给出具体描述
        if (probability >= 0.75) {
            return "高置信集成模型欺诈";
        }

        // 不再输出"未知异常模式"，低置信度不告警
        return null;
    }
}
