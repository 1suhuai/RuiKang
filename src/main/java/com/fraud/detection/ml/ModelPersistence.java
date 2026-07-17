package com.fraud.detection.ml;

import java.io.*;

/**
 * 模型持久化工具: 将训练好的模型序列化保存到文件，运行时加载
 *
 * 流程:
 * 1. 预训练阶段: 训练集数据训练模型 → saveModel() 保存到文件
 * 2. 运行阶段: loadModel() 从文件加载 → 在线增量学习更新
 */
public class ModelPersistence {

    private static final String DEFAULT_MODEL_DIR = "models";
    private static final String MODEL_FILE = "fraud_model.ser";

    /**
     * 保存全局模型缓存到文件
     */
    public static boolean saveModel(GlobalMLModelCache cache) {
        return saveModel(cache, DEFAULT_MODEL_DIR, MODEL_FILE);
    }

    public static boolean saveModel(GlobalMLModelCache cache, String dir, String filename) {
        if (cache == null || !cache.isModelReady()) {
            System.out.println("[ModelPersistence] 模型未就绪，跳过保存");
            return false;
        }

        File modelDir = new File(dir);
        if (!modelDir.exists()) {
            modelDir.mkdirs();
        }

        File modelFile = new File(modelDir, filename);
        File tempFile = new File(modelDir, filename + ".tmp");

        try {
            // 先写临时文件，写完再重命名，避免写到一半断电导致模型损坏
            GlobalMLModelCache.ModelSnapshot snapshot = cache.getModel();
            if (snapshot == null) {
                System.out.println("[ModelPersistence] 模型快照为空，跳过保存");
                return false;
            }

            // 保存完整模型状态
            ModelState state = new ModelState(
                    snapshot.classifier,
                    snapshot.gbdtModel,
                    snapshot.statisticsModel,
                    snapshot.pathModel,
                    snapshot.isolationForestModel,
                    snapshot.driftDetector,
                    snapshot.weightManager,
                    cache.getTrainingStats()
            );

            try (ObjectOutputStream oos = new ObjectOutputStream(
                    new BufferedOutputStream(new FileOutputStream(tempFile)))) {
                oos.writeObject(state);
                oos.flush();
            }

            // 原子性替换
            if (modelFile.exists()) {
                modelFile.delete();
            }
            if (!tempFile.renameTo(modelFile)) {
                // renameTo可能跨文件系统失败，用复制代替
                copyFile(tempFile, modelFile);
                tempFile.delete();
            }

            System.out.println("[ModelPersistence] 模型保存成功: " + modelFile.getAbsolutePath()
                    + " (" + modelFile.length() / 1024 + "KB) | " + cache.getTrainingStats());
            return true;

        } catch (Exception e) {
            System.err.println("[ModelPersistence] 模型保存失败: " + e.getMessage());
            e.printStackTrace();
            if (tempFile.exists()) tempFile.delete();
            return false;
        }
    }

    /**
     * 从文件加载模型
     * @return ModelState 或 null
     */
    public static ModelState loadModel() {
        return loadModel(DEFAULT_MODEL_DIR, MODEL_FILE);
    }

    public static ModelState loadModel(String dir, String filename) {
        File modelFile = new File(dir, filename);
        if (!modelFile.exists()) {
            System.out.println("[ModelPersistence] 模型文件不存在: " + modelFile.getAbsolutePath());
            return null;
        }

        try (ObjectInputStream ois = new ObjectInputStream(
                new BufferedInputStream(new FileInputStream(modelFile)))) {
            ModelState state = (ModelState) ois.readObject();
            System.out.println("[ModelPersistence] 模型加载成功: " + modelFile.getAbsolutePath()
                    + " (" + modelFile.length() / 1024 + "KB) | " + state.trainingStats);
            return state;
        } catch (InvalidClassException e) {
            System.err.println("[ModelPersistence] 模型版本不兼容(类结构已变更)，将重新训练: " + e.getMessage());
            modelFile.delete();
            return null;
        } catch (Exception e) {
            System.err.println("[ModelPersistence] 模型加载失败: " + e.getMessage());
            return null;
        }
    }

    /**
     * 检查模型文件是否存在
     */
    public static boolean modelExists() {
        return new File(DEFAULT_MODEL_DIR, MODEL_FILE).exists();
    }

    private static void copyFile(File src, File dst) throws IOException {
        try (InputStream in = new BufferedInputStream(new FileInputStream(src));
             OutputStream out = new BufferedOutputStream(new FileOutputStream(dst))) {
            byte[] buffer = new byte[8192];
            int len;
            while ((len = in.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }
        }
    }

    /**
     * 模型状态容器: 包含所有子模型，可序列化保存
     */
    public static class ModelState implements Serializable {
        private static final long serialVersionUID = 1L;

        public final ModelTrainer.OnlineLogisticRegressionModel classifier;
        public final ModelTrainer.GBDTModel gbdtModel;
        public final ModelTrainer.StatisticsModel statisticsModel;
        public final ModelTrainer.PathFrequencyModel pathModel;
        public final ModelTrainer.IsolationForestModel isolationForestModel;
        public final ModelTrainer.DriftDetector driftDetector;
        public final ModelTrainer.EnsembleWeightManager weightManager;
        public final String trainingStats;

        public ModelState(
                ModelTrainer.OnlineLogisticRegressionModel classifier,
                ModelTrainer.GBDTModel gbdtModel,
                ModelTrainer.StatisticsModel statisticsModel,
                ModelTrainer.PathFrequencyModel pathModel,
                ModelTrainer.IsolationForestModel isolationForestModel,
                ModelTrainer.DriftDetector driftDetector,
                ModelTrainer.EnsembleWeightManager weightManager,
                String trainingStats) {
            this.classifier = classifier;
            this.gbdtModel = gbdtModel;
            this.statisticsModel = statisticsModel;
            this.pathModel = pathModel;
            this.isolationForestModel = isolationForestModel;
            this.driftDetector = driftDetector;
            this.weightManager = weightManager;
            this.trainingStats = trainingStats;
        }
    }
}
