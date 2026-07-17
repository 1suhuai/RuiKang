package com.fraud.detection.ml;

import com.fraud.detection.model.UserBehaviorSequence;

import java.io.Serializable;
import java.util.*;

/**
 * ML模型训练器 - 在线学习核心组件
 * 包含6个内部模型类:
 * 1. OnlineLogisticRegressionModel: 在线逻辑回归(梯度下降法)
 * 2. GBDTModel: 梯度提升决策树(树集成学习)
 * 3. StatisticsModel: 统计画像模型(均值/方差/分布)
 * 4. PathFrequencyModel: 路径频率模型(交易路径统计)
 * 5. IsolationForestModel: 孤立森林异常检测
 * 6. DriftDetector: 数据漂移检测
 * 7. EnsembleWeightManager: 集成权重管理
 */
public class ModelTrainer {

    // 在线逻辑回归: Welford算法更新统计量，SGD更新权重
    public static class OnlineLogisticRegressionModel implements Serializable {

        private static final int FEATURE_COUNT = 36;
        private static final double LEARNING_RATE = 0.12;
        private static final double L2 = 0.0003;
        private static final double MIN_STD = 1.0E-6;
        private static final double FRAUD_CLASS_WEIGHT = 5.0;

        private final double[] weights = new double[FEATURE_COUNT];
        private final double[] means = new double[FEATURE_COUNT];
        private final double[] m2 = new double[FEATURE_COUNT];
        private long sampleCount;
        private long fraudSampleCount;
        private double bias = -0.8;

        public OnlineLogisticRegressionModel() {
            weights[0] = 0.15; weights[1] = 0.10; weights[2] = 0.20; weights[3] = 0.18;
            weights[4] = 0.14; weights[5] = 0.18; weights[6] = 0.10; weights[7] = 0.14;
            weights[8] = 0.22; weights[9] = 0.22; weights[10] = 0.12; weights[11] = 0.12;
            weights[12] = 0.08; weights[13] = 0.25; weights[14] = 0.16; weights[15] = 0.13;
            weights[16] = 0.18; weights[17] = 0.20; weights[18] = 0.23; weights[19] = 0.16;
            weights[20] = 0.13; weights[21] = 0.20; weights[22] = 0.10;
            weights[23] = 0.18; weights[24] = 0.15; weights[25] = 0.20;
            weights[26] = 0.12; weights[27] = 0.12; weights[28] = 0.15; weights[29] = 0.10;
            weights[30] = 0.25; weights[31] = 0.18; weights[32] = 0.22; weights[33] = 0.20;
            weights[34] = 0.18; weights[35] = 0.22;
        }

        public Prediction predict(UserBehaviorSequence sequence) {
            double[] normalized = normalize(sequence.features);
            double logit = bias + dot(weights, normalized);
            double probability = sigmoid(logit);
            double reconstructionError = reconstructionError(normalized);
            double finalProbability = clamp(probability * 0.72 + reconstructionError * 0.28, 0.0, 1.0);
            return new Prediction(finalProbability, probability, reconstructionError, normalized);
        }

        public void train(UserBehaviorSequence sequence, int label) {
            if (sequence == null || sequence.features == null || sequence.features.length == 0) return;
            updateStatistics(sequence.features);
            double[] normalized = normalize(sequence.features);
            double prediction = sigmoid(bias + dot(weights, normalized));
            double error = label - prediction;
            double effectiveLearningRate = (label == 1) ? LEARNING_RATE * FRAUD_CLASS_WEIGHT : LEARNING_RATE;
            fraudSampleCount += label;
            for (int i = 0; i < weights.length; i++) {
                weights[i] += effectiveLearningRate * (error * normalized[i] - L2 * weights[i]);
            }
            bias += effectiveLearningRate * error;
        }

        public long getSampleCount() { return sampleCount; }

        private void updateStatistics(double[] features) {
            sampleCount++;
            int limit = Math.min(FEATURE_COUNT, features.length);
            for (int i = 0; i < limit; i++) {
                double value = sanitize(features[i]);
                double delta = value - means[i];
                means[i] += delta / sampleCount;
                double delta2 = value - means[i];
                m2[i] += delta * delta2;
            }
        }

        private double[] normalize(double[] features) {
            double[] normalized = new double[FEATURE_COUNT];
            int limit = Math.min(FEATURE_COUNT, features.length);
            for (int i = 0; i < limit; i++) {
                double value = sanitize(features[i]);
                double std = sampleCount > 1 ? Math.sqrt(Math.max(m2[i] / (sampleCount - 1), MIN_STD)) : bootstrapStd(i);
                double mean = sampleCount > 0 ? means[i] : bootstrapMean(i);
                normalized[i] = clamp((value - mean) / (3.0 * std), -3.0, 3.0) / 3.0;
            }
            return normalized;
        }

        private double reconstructionError(double[] normalized) {
            double sum = 0.0;
            for (double value : normalized) sum += Math.abs(value);
            return clamp(sum / normalized.length, 0.0, 1.0);
        }

        private double bootstrapMean(int index) {
            double[] defaults = {
                    80000, 30000, 180000, 260000, 0.5, 0.5, 0.4, 0.3,
                    0.2, 0.1, 1.4, 1.4, 1.3, 0.35, 0.55, 1.2,
                    30.0, 500.0, 1000.0, 1.0, 2.0, 0.4, 2.0,
                    0.5, 1.5, 3.0, 50000.0, 50000.0, 1.0, 4.0,
                    0.5, 1.5, 0.3, 0.5, 0.3, 0.4
            };
            return index < defaults.length ? defaults[index] : 0;
        }

        private double bootstrapStd(int index) {
            double[] defaults = {
                    90000, 50000, 180000, 300000, 1.2, 1.2, 1.0, 1.0,
                    0.8, 0.6, 1.0, 1.0, 0.8, 0.35, 0.35, 1.5,
                    60.0, 800.0, 5000.0, 1.5, 3.0, 0.3, 3.0,
                    2.0, 2.0, 5.0, 80000.0, 80000.0, 1.5, 6.0,
                    0.5, 1.0, 0.5, 0.4, 0.3, 0.3
            };
            return index < defaults.length ? defaults[index] : 1.0;
        }

        private double dot(double[] left, double[] right) {
            double sum = 0.0;
            for (int i = 0; i < left.length; i++) sum += left[i] * right[i];
            return sum;
        }

        private double sigmoid(double value) {
            if (value > 35) return 1.0;
            if (value < -35) return 0.0;
            return 1.0 / (1.0 + Math.exp(-value));
        }

        private double sanitize(double value) {
            if (Double.isNaN(value) || Double.isInfinite(value)) return 0.0;
            return value;
        }

        private double clamp(double value, double min, double max) {
            return Math.max(min, Math.min(max, value));
        }
    }

    // 路径频率模型: 记录常见行为路径频率，罕见路径视为异常
    public static class PathFrequencyModel implements Serializable {

        private static final int MIN_PATH_COUNT_FOR_NORMAL = 5;
        private final Map<String, Integer> pathCounts = new HashMap<>();
        private int totalPaths;

        public void train(String behaviorPath) {
            if (behaviorPath == null || behaviorPath.isEmpty()) return;
            totalPaths++;
            pathCounts.merge(behaviorPath, 1, Integer::sum);
        }

        public double detectAnomaly(String behaviorPath) {
            if (behaviorPath == null || behaviorPath.isEmpty() || totalPaths < 2) return 0.0;
            Integer count = pathCounts.getOrDefault(behaviorPath, 0);
            double frequency = (double) count / totalPaths;
            if (frequency < 1.0 / (totalPaths * 0.5)) return 0.9;
            if (count < MIN_PATH_COUNT_FOR_NORMAL) return 0.5 + 0.4 * (1.0 - (double) count / MIN_PATH_COUNT_FOR_NORMAL);
            return Math.max(0.0, 0.3 * (1.0 - frequency * 10));
        }

        public int getTotalPaths() { return totalPaths; }
    }

    // 统计画像模型: Welford算法计算Z-Score检测偏离
    public static class StatisticsModel implements Serializable {

        private static final int FEATURE_COUNT = 36;
        private final double[] means = new double[FEATURE_COUNT];
        private final double[] m2 = new double[FEATURE_COUNT];
        private long sampleCount;

        public void train(UserBehaviorSequence sequence) {
            if (sequence == null || sequence.features == null) return;
            sampleCount++;
            int limit = Math.min(FEATURE_COUNT, sequence.features.length);
            for (int i = 0; i < limit; i++) {
                double value = sanitize(sequence.features[i]);
                double delta = value - means[i];
                means[i] += delta / sampleCount;
                double delta2 = value - means[i];
                m2[i] += delta * delta2;
            }
        }

        public double detectAnomaly(UserBehaviorSequence sequence) {
            if (sequence == null || sequence.features == null || sampleCount < 2) return 0.0;
            double score = 0.0;
            int limit = Math.min(FEATURE_COUNT, sequence.features.length);
            for (int i = 0; i < limit; i++) {
                double std = Math.sqrt(Math.max(m2[i] / Math.max(1, sampleCount - 1), 1.0E-6));
                double z = Math.abs(sanitize(sequence.features[i]) - means[i]) / std;
                score += Math.min(1.0, z / 3.0);
            }
            return limit == 0 ? 0.0 : score / limit;
        }

        public long getSampleCount() { return sampleCount; }

        private double sanitize(double value) {
            if (Double.isNaN(value) || Double.isInfinite(value)) return 0.0;
            return value;
        }
    }

    // IsolationForest模型: 蓄水池采样维护子样本，构建真正的隔离树计算异常分数
    public static class IsolationForestModel implements Serializable {

        private static final int FEATURE_COUNT = 36;
        private static final int NUM_TREES = 50;
        private static final int SUBSAMPLE_SIZE = 64;
        private static final int MAX_BUFFER_SIZE = 500;

        private final Random random = new Random(42);
        private long sampleCount;
        private final List<double[]> sampleBuffer = new ArrayList<>();
        private List<IsolationTree> trees = new ArrayList<>();

        /**
         * 隔离树节点
         */
        public static class TreeNode implements Serializable {
            public int featureIndex;       // 分割特征索引
            public double splitValue;      // 分割值
            public TreeNode left;          // 左子树 (value <= splitValue)
            public TreeNode right;         // 右子树 (value > splitValue)
            public int height;             // 节点高度

            public TreeNode(int featureIndex, double splitValue, int height) {
                this.featureIndex = featureIndex;
                this.splitValue = splitValue;
                this.height = height;
            }

            public static TreeNode leaf(int height) {
                TreeNode node = new TreeNode(-1, 0, height);
                node.left = null;
                node.right = null;
                return node;
            }

            public boolean isLeaf() {
                return left == null && right == null;
            }
        }

        /**
         * 隔离树
         */
        public static class IsolationTree implements Serializable {
            public TreeNode root;

            public IsolationTree(TreeNode root) {
                this.root = root;
            }
        }

        public void train(UserBehaviorSequence sequence) {
            if (sequence == null || sequence.features == null) return;
            sampleCount++;
            double[] features = sequence.features.clone();

            if (sampleBuffer.size() < MAX_BUFFER_SIZE) {
                sampleBuffer.add(features);
            } else {
                if (random.nextDouble() < (double) MAX_BUFFER_SIZE / sampleCount) {
                    sampleBuffer.set(random.nextInt(MAX_BUFFER_SIZE), features);
                }
            }

            // 每100条数据重建一次树
            if (sampleCount % 100 == 0 && sampleBuffer.size() >= SUBSAMPLE_SIZE) {
                rebuildTrees();
            }
        }

        /**
         * 重建所有隔离树
         * 每棵树从蓄水池中随机抽取SUBSAMPLE_SIZE个样本构建
         */
        private void rebuildTrees() {
            trees.clear();
            int n = sampleBuffer.size();
            if (n < SUBSAMPLE_SIZE) return;

            // 找出最小特征长度
            int minFeatureLength = Integer.MAX_VALUE;
            for (double[] sample : sampleBuffer) {
                if (sample != null && sample.length < minFeatureLength) {
                    minFeatureLength = sample.length;
                }
            }
            if (minFeatureLength <= 0 || minFeatureLength == Integer.MAX_VALUE) return;

            for (int t = 0; t < NUM_TREES; t++) {
                // 随机抽取子样本
                List<double[]> subsample = new ArrayList<>();
                List<Integer> indices = new ArrayList<>();
                for (int i = 0; i < n; i++) indices.add(i);
                Collections.shuffle(indices, random);
                for (int i = 0; i < Math.min(SUBSAMPLE_SIZE, indices.size()); i++) {
                    subsample.add(sampleBuffer.get(indices.get(i)));
                }

                // 构建树
                TreeNode root = buildTree(subsample, 0, minFeatureLength);
                trees.add(new IsolationTree(root));
            }
        }

        /**
         * 递归构建隔离树
         * @param samples 子样本
         * @param currentHeight 当前高度
         * @param maxFeatureLength 最大特征数
         * @return 树节点
         */
        private TreeNode buildTree(List<double[]> samples, int currentHeight, int maxFeatureLength) {
            int limit = (int) Math.ceil(Math.log(Math.max(2, samples.size())) / Math.log(2));
            if (currentHeight >= limit || samples.size() <= 1) {
                return TreeNode.leaf(currentHeight);
            }

            // 随机选择特征
            int featureIdx = random.nextInt(maxFeatureLength);

            // 找出该特征的最小最大值
            double minVal = Double.MAX_VALUE;
            double maxVal = -Double.MAX_VALUE;
            for (double[] sample : samples) {
                if (featureIdx < sample.length) {
                    double v = sanitize(sample[featureIdx]);
                    if (v < minVal) minVal = v;
                    if (v > maxVal) maxVal = v;
                }
            }

            // 如果所有值相同，无法分割
            if (minVal == maxVal) {
                return TreeNode.leaf(currentHeight);
            }

            // 在[minVal, maxVal]之间随机选择分割值
            double splitValue = minVal + random.nextDouble() * (maxVal - minVal);

            // 分割样本
            List<double[]> leftSamples = new ArrayList<>();
            List<double[]> rightSamples = new ArrayList<>();
            for (double[] sample : samples) {
                if (featureIdx < sample.length && sanitize(sample[featureIdx]) <= splitValue) {
                    leftSamples.add(sample);
                } else {
                    rightSamples.add(sample);
                }
            }

            TreeNode node = new TreeNode(featureIdx, splitValue, currentHeight);
            node.left = buildTree(leftSamples, currentHeight + 1, maxFeatureLength);
            node.right = buildTree(rightSamples, currentHeight + 1, maxFeatureLength);
            return node;
        }

        public double detectAnomaly(UserBehaviorSequence sequence) {
            if (sequence == null || sequence.features == null || trees.isEmpty()) return 0.0;

            // 创建快照副本避免并发修改异常
            List<IsolationTree> snapshot = new ArrayList<>(trees);
            if (snapshot.isEmpty()) return 0.0;

            double totalPathLength = 0;
            for (IsolationTree tree : snapshot) {
                totalPathLength += computePathLength(tree.root, sequence.features, 0);
            }
            double avgPathLength = totalPathLength / snapshot.size();

            // 计算c(n)：二叉搜索树的平均路径长度
            double n = SUBSAMPLE_SIZE;
            double c = 2.0 * (Math.log(n - 1) + 0.5772156649) - 2.0 * (n - 1) / n;

            // 异常分数: s(x,n) = 2^(-E(h(x))/c(n))
            double anomalyScore = Math.pow(2, -avgPathLength / c);
            return clamp(anomalyScore, 0.0, 1.0);
        }

        /**
         * 计算样本在树中的路径长度
         */
        private double computePathLength(TreeNode node, double[] features, int currentDepth) {
            if (node.isLeaf()) {
                // 叶子节点：需要加上未分割样本的平均深度修正
                return currentDepth + c(features.length > SUBSAMPLE_SIZE ? SUBSAMPLE_SIZE : Math.max(2, features.length));
            }

            double value = (node.featureIndex < features.length) ? sanitize(features[node.featureIndex]) : 0;
            if (value <= node.splitValue) {
                return computePathLength(node.left, features, currentDepth + 1);
            } else {
                return computePathLength(node.right, features, currentDepth + 1);
            }
        }

        /**
         * 计算n个样本的平均二叉搜索树路径长度
         */
        private double c(double n) {
            if (n <= 1) return 0;
            if (n == 2) return 1;
            return 2.0 * (Math.log(n - 1) + 0.5772156649) - 2.0 * (n - 1) / n;
        }

        private double sanitize(double value) {
            if (Double.isNaN(value) || Double.isInfinite(value)) return 0.0;
            return value;
        }

        private double clamp(double value, double min, double max) {
            return Math.max(min, Math.min(max, value));
        }
    }

    // GBDT(梯度提升决策树)模型: 轻量级树集成，在线训练+预测
    public static class GBDTModel implements Serializable {

        private static final int FEATURE_COUNT = 36;
        private static final int NUM_TREES = 30;
        private static final int MAX_DEPTH = 4;
        private static final double LEARNING_RATE = 0.1;
        private static final double MIN_SAMPLE_SPLIT = 10;
        private static final double SUBSAMPLE_RATIO = 0.8;
        private static final int MAX_BUFFER = 500;

        private final Random random = new Random(42);
        private final List<double[]> sampleBuffer = new ArrayList<>();
        private final List<Integer> labelBuffer = new ArrayList<>();
        private long sampleCount;
        private List<DecisionTree> trees = new ArrayList<>();
        private double baseScore = 0.0;

        /**
         * 决策树节点
         */
        public static class TreeNode implements Serializable {
            public int featureIndex;
            public double splitValue;
            public double prediction;  // 叶子节点预测值
            public TreeNode left;
            public TreeNode right;

            public static TreeNode leaf(double prediction) {
                TreeNode node = new TreeNode();
                node.featureIndex = -1;
                node.splitValue = 0;
                node.prediction = prediction;
                node.left = null;
                node.right = null;
                return node;
            }

            public boolean isLeaf() {
                return left == null && right == null;
            }
        }

        /**
         * 决策树
         */
        public static class DecisionTree implements Serializable {
            public TreeNode root;
            public double treeWeight;

            public DecisionTree(TreeNode root, double weight) {
                this.root = root;
                this.treeWeight = weight;
            }
        }

        public void train(UserBehaviorSequence sequence, int label) {
            if (sequence == null || sequence.features == null) return;
            sampleCount++;

            double[] features = sequence.features.clone();
            // 确保特征长度
            if (features.length < FEATURE_COUNT) {
                features = Arrays.copyOf(features, FEATURE_COUNT);
            }
            sampleBuffer.add(features);
            labelBuffer.add(label);

            // 限制缓冲区大小
            if (sampleBuffer.size() > MAX_BUFFER) {
                sampleBuffer.remove(0);
                labelBuffer.remove(0);
            }

            // 每50条样本训练一棵新树
            if (sampleCount % 50 == 0 && sampleBuffer.size() >= MIN_SAMPLE_SPLIT * 2) {
                trainOneTree();
            }
        }

        /**
         * 训练一棵树拟合当前残差
         */
        private void trainOneTree() {
            int n = sampleBuffer.size();
            if (n < MIN_SAMPLE_SPLIT * 2) return;

            // 计算当前预测残差
            double[] residuals = new double[n];
            for (int i = 0; i < n; i++) {
                double currentPred = predictRaw(sampleBuffer.get(i));
                residuals[i] = labelBuffer.get(i) - sigmoid(currentPred);
            }

            // 子采样
            int subSize = (int) (n * SUBSAMPLE_RATIO);
            List<Integer> indices = new ArrayList<>();
            for (int i = 0; i < n; i++) indices.add(i);
            Collections.shuffle(indices, random);
            List<Integer> subIndices = indices.subList(0, subSize);

            // 构建树
            TreeNode root = buildTree(residuals, subIndices, 0);
            double weight = computeTreeWeight(root, residuals, subIndices);
            trees.add(new DecisionTree(root, weight * LEARNING_RATE));

            // 更新baseScore
            if (trees.size() == 1) {
                double sumLabels = 0;
                for (int l : labelBuffer) sumLabels += l;
                double mean = sumLabels / n;
                baseScore = Math.log(Math.max(mean, 0.01) / Math.max(1 - mean, 0.01));
            }
        }

        /**
         * 递归构建树，找到最优分割点
         */
        private TreeNode buildTree(double[] residuals, List<Integer> indices, int depth) {
            if (depth >= MAX_DEPTH || indices.size() < MIN_SAMPLE_SPLIT) {
                double sum = 0;
                for (int idx : indices) sum += residuals[idx];
                return TreeNode.leaf(sum / Math.max(1, indices.size()));
            }

            int n = indices.size();
            int featureIdx = random.nextInt(FEATURE_COUNT);

            // 收集该特征的值
            double[] featureValues = new double[n];
            for (int i = 0; i < n; i++) {
                double[] features = sampleBuffer.get(indices.get(i));
                featureValues[i] = featureIdx < features.length ? sanitize(features[featureIdx]) : 0;
            }

            // 找出最优分割点（用分位数候选点）
            double[] sortedVals = featureValues.clone();
            Arrays.sort(sortedVals);
            int numCandidates = Math.min(10, n - 1);
            double bestGain = 0;
            double bestSplit = 0;

            for (int c = 1; c <= numCandidates; c++) {
                int splitIdx = n * c / (numCandidates + 1);
                double splitVal = sortedVals[splitIdx];

                double leftSum = 0, rightSum = 0;
                int leftCount = 0, rightCount = 0;
                for (int i = 0; i < n; i++) {
                    if (featureValues[i] <= splitVal) {
                        leftSum += residuals[indices.get(i)];
                        leftCount++;
                    } else {
                        rightSum += residuals[indices.get(i)];
                        rightCount++;
                    }
                }

                if (leftCount < 2 || rightCount < 2) continue;

                // 增益 = 左方差减少 + 右方差减少
                double leftMean = leftSum / leftCount;
                double rightMean = rightSum / rightCount;
                double gain = leftCount * leftMean * leftMean + rightCount * rightMean * rightMean;

                if (gain > bestGain) {
                    bestGain = gain;
                    bestSplit = splitVal;
                }
            }

            if (bestGain <= 0) {
                double sum = 0;
                for (int idx : indices) sum += residuals[idx];
                return TreeNode.leaf(sum / Math.max(1, indices.size()));
            }

            // 分割
            List<Integer> leftIndices = new ArrayList<>();
            List<Integer> rightIndices = new ArrayList<>();
            for (int idx : indices) {
                double[] features = sampleBuffer.get(idx);
                double v = featureIdx < features.length ? sanitize(features[featureIdx]) : 0;
                if (v <= bestSplit) {
                    leftIndices.add(idx);
                } else {
                    rightIndices.add(idx);
                }
            }

            TreeNode node = new TreeNode();
            node.featureIndex = featureIdx;
            node.splitValue = bestSplit;
            node.left = buildTree(residuals, leftIndices, depth + 1);
            node.right = buildTree(residuals, rightIndices, depth + 1);
            return node;
        }

        /**
         * 计算树的权重（简化：返回根节点平均预测值）
         */
        private double computeTreeWeight(TreeNode root, double[] residuals, List<Integer> indices) {
            return 1.0;
        }

        public double predict(UserBehaviorSequence sequence) {
            if (sequence == null || sequence.features == null) return 0.5;
            double[] features = sequence.features.clone();
            if (features.length < FEATURE_COUNT) {
                features = Arrays.copyOf(features, FEATURE_COUNT);
            }
            double rawScore = predictRaw(features);
            return sigmoid(rawScore);
        }

        private double predictRaw(double[] features) {
            double score = baseScore;
            for (DecisionTree tree : trees) {
                score += tree.treeWeight * predictTreeNode(tree.root, features);
            }
            return score;
        }

        private double predictTreeNode(TreeNode node, double[] features) {
            if (node.isLeaf()) {
                return node.prediction;
            }
            double v = node.featureIndex < features.length ? sanitize(features[node.featureIndex]) : 0;
            if (v <= node.splitValue) {
                return predictTreeNode(node.left, features);
            } else {
                return predictTreeNode(node.right, features);
            }
        }

        public int getTreeCount() {
            return trees.size();
        }

        private double sigmoid(double x) {
            if (x > 35) return 1.0;
            if (x < -35) return 0.0;
            return 1.0 / (1.0 + Math.exp(-x));
        }

        private double sanitize(double value) {
            if (Double.isNaN(value) || Double.isInfinite(value)) return 0.0;
            return value;
        }
    }

    // 漂移检测器: 滑动窗口对比基线分布，EMA更新基线
    public static class DriftDetector implements Serializable {

        private static final int FEATURE_COUNT = 36;
        private static final double DRIFT_THRESHOLD = 0.3;
        private static final long WINDOW_SIZE = 500;

        private final double[] recentMeans = new double[FEATURE_COUNT];
        private final double[] recentM2 = new double[FEATURE_COUNT];
        private long recentCount;
        private long windowStartCount;
        private final double[] baselineMeans = new double[FEATURE_COUNT];
        private final double[] baselineM2 = new double[FEATURE_COUNT];
        private long baselineCount;
        private boolean baselineEstablished;
        private double lastDriftScore;
        private long lastDriftCheckTime;

        public void update(UserBehaviorSequence sequence) {
            if (sequence == null || sequence.features == null) return;
            if (recentCount - windowStartCount >= WINDOW_SIZE) resetWindow();
            recentCount++;
            long countInWindow = recentCount - windowStartCount;
            int limit = Math.min(FEATURE_COUNT, sequence.features.length);
            for (int i = 0; i < limit; i++) {
                double value = sanitize(sequence.features[i]);
                double delta = value - recentMeans[i];
                recentMeans[i] += delta / countInWindow;
                double delta2 = value - recentMeans[i];
                recentM2[i] += delta * delta2;
            }
            if (countInWindow % 200 == 0 && countInWindow > 0) updateBaseline();
        }

        private void resetWindow() {
            System.arraycopy(recentMeans, 0, baselineMeans, 0, FEATURE_COUNT);
            System.arraycopy(recentM2, 0, baselineM2, 0, FEATURE_COUNT);
            baselineCount = recentCount;
            baselineEstablished = true;
            windowStartCount = recentCount;
            java.util.Arrays.fill(recentMeans, 0.0);
            java.util.Arrays.fill(recentM2, 0.0);
        }

        private void updateBaseline() {
            if (!baselineEstablished) {
                System.arraycopy(recentMeans, 0, baselineMeans, 0, FEATURE_COUNT);
                System.arraycopy(recentM2, 0, baselineM2, 0, FEATURE_COUNT);
                baselineCount = recentCount;
                baselineEstablished = true;
                return;
            }
            double alpha = 0.1;
            for (int i = 0; i < FEATURE_COUNT; i++) {
                baselineMeans[i] = alpha * recentMeans[i] + (1 - alpha) * baselineMeans[i];
                baselineM2[i] = alpha * recentM2[i] + (1 - alpha) * baselineM2[i];
            }
            baselineCount = recentCount;
        }

        public double detectDrift() {
            if (!baselineEstablished || (recentCount - windowStartCount) < 50) return 0.0;
            double totalDrift = 0;
            int validFeatures = 0;
            for (int i = 0; i < FEATURE_COUNT; i++) {
                double baselineStd = Math.sqrt(Math.max(baselineM2[i] / Math.max(1, baselineCount - 1), 1e-6));
                if (baselineStd > 1e-6) {
                    double drift = Math.abs(recentMeans[i] - baselineMeans[i]) / baselineStd;
                    totalDrift += Math.min(drift, 3.0);
                    validFeatures++;
                }
            }
            lastDriftScore = validFeatures > 0 ? totalDrift / validFeatures : 0;
            lastDriftCheckTime = System.currentTimeMillis();
            return lastDriftScore;
        }

        public boolean isDriftDetected() { return detectDrift() > DRIFT_THRESHOLD; }
        public double getLastDriftScore() { return lastDriftScore; }

        private double sanitize(double value) {
            if (Double.isNaN(value) || Double.isInfinite(value)) return 0.0;
            return value;
        }
    }

    // 动态权重管理器: 根据各模型近期准确率使用Softmax调整集成权重
    public static class EnsembleWeightManager implements Serializable {

        private double logisticWeight = 0.28;
        private double gbdtWeight = 0.22;
        private double behaviorWeight = 0.28;
        private double statisticalWeight = 0.12;
        private double pathWeight = 0.10;
        private final double[] logisticCorrect = new double[100];
        private final double[] gbdtCorrect = new double[100];
        private final double[] behaviorCorrect = new double[100];
        private final double[] statisticalCorrect = new double[100];
        private final double[] pathCorrect = new double[100];
        private int windowIndex;
        private long totalPredictions;

        public void updateWeights(boolean logisticCorrect, boolean gbdtCorrect, boolean behaviorCorrect,
                                  boolean statisticalCorrect, boolean pathCorrect, boolean isActuallyFraud) {
            int idx = (int) (windowIndex % 100);
            this.logisticCorrect[idx] = (logisticCorrect == isActuallyFraud) ? 1.0 : 0.0;
            this.gbdtCorrect[idx] = (gbdtCorrect == isActuallyFraud) ? 1.0 : 0.0;
            this.behaviorCorrect[idx] = (behaviorCorrect == isActuallyFraud) ? 1.0 : 0.0;
            this.statisticalCorrect[idx] = (statisticalCorrect == isActuallyFraud) ? 1.0 : 0.0;
            this.pathCorrect[idx] = (pathCorrect == isActuallyFraud) ? 1.0 : 0.0;
            windowIndex++;
            totalPredictions++;
            if (totalPredictions % 50 == 0) recalculateWeights();
        }

        private void recalculateWeights() {
            int windowSize = (int) Math.min(100, totalPredictions);
            double lAcc = average(logisticCorrect, windowSize);
            double gAcc = average(gbdtCorrect, windowSize);
            double bAcc = average(behaviorCorrect, windowSize);
            double sAcc = average(statisticalCorrect, windowSize);
            double pAcc = average(pathCorrect, windowSize);
            double total = lAcc + gAcc + bAcc + sAcc + pAcc;
            if (total > 0) {
                double temperature = 2.0;
                double lExp = Math.exp(lAcc * temperature);
                double gExp = Math.exp(gAcc * temperature);
                double bExp = Math.exp(bAcc * temperature);
                double sExp = Math.exp(sAcc * temperature);
                double pExp = Math.exp(pAcc * temperature);
                double expTotal = lExp + gExp + bExp + sExp + pExp;
                logisticWeight = 0.10 + 0.60 * (lExp / expTotal);
                gbdtWeight = 0.10 + 0.60 * (gExp / expTotal);
                behaviorWeight = 0.10 + 0.60 * (bExp / expTotal);
                statisticalWeight = 0.05 + 0.25 * (sExp / expTotal);
                pathWeight = 0.05 + 0.25 * (pExp / expTotal);
                double wTotal = logisticWeight + gbdtWeight + behaviorWeight + statisticalWeight + pathWeight;
                logisticWeight /= wTotal;
                gbdtWeight /= wTotal;
                behaviorWeight /= wTotal;
                statisticalWeight /= wTotal;
                pathWeight /= wTotal;
            }
        }

        private double average(double[] arr, int size) {
            double sum = 0;
            for (int i = 0; i < Math.min(size, arr.length); i++) sum += arr[i];
            return size > 0 ? sum / size : 0;
        }

        public double getLogisticWeight() { return logisticWeight; }
        public double getGBDTWeight() { return gbdtWeight; }
        public double getBehaviorWeight() { return behaviorWeight; }
        public double getStatisticalWeight() { return statisticalWeight; }
        public double getPathWeight() { return pathWeight; }
    }

    public static class Prediction implements Serializable {
        public final double finalProbability;
        public final double logisticProbability;
        public final double reconstructionError;
        public final double[] normalizedFeatures;

        public Prediction(double finalProbability, double logisticProbability, double reconstructionError, double[] normalizedFeatures) {
            this.finalProbability = finalProbability;
            this.logisticProbability = logisticProbability;
            this.reconstructionError = reconstructionError;
            this.normalizedFeatures = Arrays.copyOf(normalizedFeatures, normalizedFeatures.length);
        }
    }
}
