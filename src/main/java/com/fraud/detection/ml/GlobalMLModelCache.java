package com.fraud.detection.ml;

import com.fraud.detection.model.UserBehaviorSequence;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 全局ML模型缓存 - 单例模式
 *
 * 功能:
 * 1. 存储训练好的ML模型实例
 * 2. 允许训练集算子更新模型
 * 3. 允许测试集算子读取训练好的模型
 * 4. 线程安全,支持并发访问
 *
 * 使用方式:
 * - 训练集算子调用: GlobalMLModelCache.getInstance().updateModel(...)
 * - 测试集算子调用: GlobalMLModelCache.getInstance().getModel()
 *
 * 供所有Task共享模型,避免每个Task独立训练
 * 支持模型版本管理和热更新
 * 包含:逻辑回归/统计模型/路径频率/孤立森林/漂移检测/集成权重
 */
public class GlobalMLModelCache implements Serializable {

    private static final AtomicReference<GlobalMLModelCache> instance = new AtomicReference<>();

    private volatile boolean modelReady = false;
    private transient ModelTrainer.OnlineLogisticRegressionModel classifier;
    private transient ModelTrainer.GBDTModel gbdtModel;
    private transient ModelTrainer.StatisticsModel statisticsModel;
    private transient ModelTrainer.PathFrequencyModel pathModel;
    private transient ModelTrainer.IsolationForestModel isolationForestModel;
    private transient ModelTrainer.DriftDetector driftDetector;
    private transient ModelTrainer.EnsembleWeightManager weightManager;

    // 统计信息
    private volatile long trainSampleCount = 0;
    private volatile long fraudTrainCount = 0;
    private volatile long normalTrainCount = 0;
    private volatile long lastUpdateTime = 0;

    public static GlobalMLModelCache getInstance() {
        GlobalMLModelCache current = instance.get();
        if (current == null) {
            GlobalMLModelCache newInstance = new GlobalMLModelCache();
            if (instance.compareAndSet(null, newInstance)) {
                return newInstance;
            } else {
                return instance.get();
            }
        }
        return current;
    }

    private GlobalMLModelCache() {
    }

    /**
     * 训练集使用:更新模型(增量训练)
     */
    public synchronized void updateModel(UserBehaviorSequence sequence, int label) {
        if (sequence == null || sequence.features == null || sequence.sequenceLength < 2) {
            return;
        }

        ensureModelInitialized();

        classifier.train(sequence, label);
        gbdtModel.train(sequence, label);
        statisticsModel.train(sequence);
        pathModel.train(sequence.behaviorPath);
        isolationForestModel.train(sequence);
        driftDetector.update(sequence);

        trainSampleCount++;
        if (label == 1) {
            fraudTrainCount++;
        } else {
            normalTrainCount++;
        }
        lastUpdateTime = System.currentTimeMillis();

        // 标记模型已准备好(至少训练了50个样本)
        if (!modelReady && trainSampleCount >= 50) {
            modelReady = true;
            System.out.println("[GlobalMLModelCache] 模型训练完成! 样本数: " + trainSampleCount);
        }
    }

    /**
     * 测试集使用:获取训练好的模型
     * 如果模型未准备好,返回null
     */
    public ModelSnapshot getModel() {
        if (!modelReady) {
            return null;
        }

        ensureModelInitialized();

        return new ModelSnapshot(
                classifier,
                gbdtModel,
                statisticsModel,
                pathModel,
                isolationForestModel,
                driftDetector,
                weightManager
        );
    }

    /**
     * 检查模型是否已训练好
     */
    public boolean isModelReady() {
        return modelReady;
    }

    /**
     * 获取训练统计信息
     */
    public String getTrainingStats() {
        return String.format(
                "训练样本总数=%d, 欺诈=%d, 正常=%d, 欺诈率=%.2f%%, 模型就绪=%b",
                trainSampleCount,
                fraudTrainCount,
                normalTrainCount,
                trainSampleCount > 0 ? (double) fraudTrainCount / trainSampleCount * 100 : 0,
                modelReady
        );
    }

    /**
     * 从预训练文件加载模型
     * 如果文件存在且加载成功，直接替换当前模型并标记为就绪
     * @return 是否加载成功
     */
    public synchronized boolean loadFromPretrained() {
        ModelPersistence.ModelState state = ModelPersistence.loadModel();
        if (state == null) {
            return false;
        }
        this.classifier = state.classifier;
        this.gbdtModel = state.gbdtModel;
        this.statisticsModel = state.statisticsModel;
        this.pathModel = state.pathModel;
        this.isolationForestModel = state.isolationForestModel;
        this.driftDetector = state.driftDetector;
        this.weightManager = state.weightManager;
        this.modelReady = true;
        this.trainSampleCount = (classifier != null) ? classifier.getSampleCount() : 0;
        this.lastUpdateTime = System.currentTimeMillis();
        System.out.println("[GlobalMLModelCache] 从预训练文件加载模型成功 | " + state.trainingStats);
        return true;
    }

    /**
     * 重置模型(用于调试)
     */
    public synchronized void reset() {
        modelReady = false;
        trainSampleCount = 0;
        fraudTrainCount = 0;
        normalTrainCount = 0;
        lastUpdateTime = 0;
        classifier = null;
        gbdtModel = null;
        statisticsModel = null;
        pathModel = null;
        isolationForestModel = null;
        driftDetector = null;
        weightManager = null;
    }

    /**
     * 懒加载模型实例
     */
    private void ensureModelInitialized() {
        if (classifier == null) {
            classifier = new ModelTrainer.OnlineLogisticRegressionModel();
        }
        if (gbdtModel == null) {
            gbdtModel = new ModelTrainer.GBDTModel();
        }
        if (statisticsModel == null) {
            statisticsModel = new ModelTrainer.StatisticsModel();
        }
        if (pathModel == null) {
            pathModel = new ModelTrainer.PathFrequencyModel();
        }
        if (isolationForestModel == null) {
            isolationForestModel = new ModelTrainer.IsolationForestModel();
        }
        if (driftDetector == null) {
            driftDetector = new ModelTrainer.DriftDetector();
        }
        if (weightManager == null) {
            weightManager = new ModelTrainer.EnsembleWeightManager();
        }
    }

    /**
     * 模型快照(供测试集使用)
     */
    public static class ModelSnapshot implements Serializable {
        public final ModelTrainer.OnlineLogisticRegressionModel classifier;
        public final ModelTrainer.GBDTModel gbdtModel;
        public final ModelTrainer.StatisticsModel statisticsModel;
        public final ModelTrainer.PathFrequencyModel pathModel;
        public final ModelTrainer.IsolationForestModel isolationForestModel;
        public final ModelTrainer.DriftDetector driftDetector;
        public final ModelTrainer.EnsembleWeightManager weightManager;

        public ModelSnapshot(
                ModelTrainer.OnlineLogisticRegressionModel classifier,
                ModelTrainer.GBDTModel gbdtModel,
                ModelTrainer.StatisticsModel statisticsModel,
                ModelTrainer.PathFrequencyModel pathModel,
                ModelTrainer.IsolationForestModel isolationForestModel,
                ModelTrainer.DriftDetector driftDetector,
                ModelTrainer.EnsembleWeightManager weightManager
        ) {
            this.classifier = classifier;
            this.gbdtModel = gbdtModel;
            this.statisticsModel = statisticsModel;
            this.pathModel = pathModel;
            this.isolationForestModel = isolationForestModel;
            this.driftDetector = driftDetector;
            this.weightManager = weightManager;
        }
    }
}
