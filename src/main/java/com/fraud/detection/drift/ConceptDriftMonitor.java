package com.fraud.detection.drift;

import com.fraud.detection.model.UserBehaviorSequence;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

/**
 * 概念漂移实时监控器
 * 作为RichFlatMapFunction嵌入Flink管线，实时监测数据分布变化
 * 
 * 实现4种漂移检测算法：
 * 1. ADWIN (Adaptive Windowing) - 自适应窗口，检测数据流中的变化点
 * 2. Page-Hinkley test - 序列分析，检测均值偏移
 * 3. KS-test (Kolmogorov-Smirnov) - 比较当前与历史分布差异
 * 4. PSI (Population Stability Index) - 衡量特征分布稳定性
 * 
 * 对36+维特征进行逐特征漂移追踪，输出带严重程度的漂移事件
 */
public class ConceptDriftMonitor extends RichFlatMapFunction<UserBehaviorSequence, ConceptDriftMonitor.DriftEvent> {

    private static final int FEATURE_COUNT = 36;
    private static final int CHECK_INTERVAL = 200; // 每200条检查一次

    // 各算法实例
    private transient ADWINDetector[] adwinDetectors;      // 每特征一个ADWIN
    private transient PageHinkleyTest[] pageHinkleyTests;  // 每特征一个PH检验
    private transient KSTestTracker ksTracker;              // 全局KS检验
    private transient PSITracker psiTracker;                // PSI追踪
    private transient SampleCounter counter;

    @Override
    public void open(Configuration parameters) {
        // 初始化每特征ADWIN检测器
        adwinDetectors = new ADWINDetector[FEATURE_COUNT];
        for (int i = 0; i < FEATURE_COUNT; i++) {
            adwinDetectors[i] = new ADWINDetector(0.002); // delta控制敏感度
        }

        // 初始化每特征Page-Hinkley检验
        pageHinkleyTests = new PageHinkleyTest[FEATURE_COUNT];
        for (int i = 0; i < FEATURE_COUNT; i++) {
            pageHinkleyTests[i] = new PageHinkleyTest(50.0, 0.005); // threshold=50, alpha=0.005
        }

        ksTracker = new KSTestTracker();
        psiTracker = new PSITracker();
        counter = new SampleCounter();
    }

    @Override
    public void flatMap(UserBehaviorSequence sequence, Collector<DriftEvent> out) {
        if (sequence == null || sequence.features == null) return;

        counter.totalCount++;
        int featLen = Math.min(FEATURE_COUNT, sequence.features.length);

        // 更新各算法
        for (int i = 0; i < featLen; i++) {
            double value = sanitize(sequence.features[i]);
            adwinDetectors[i].add(value);
            pageHinkleyTests[i].add(value);
        }

        // 累积样本用于KS和PSI
        ksTracker.addSample(sequence);
        psiTracker.addSample(sequence);

        // 周期性检查漂移
        if (counter.totalCount % CHECK_INTERVAL == 0 && counter.totalCount >= 100) {
            DriftEvent event = checkDrift(featLen);
            if (event != null && event.severity != DriftSeverity.NONE) {
                out.collect(event);
            }
        }
    }

    /**
     * 综合4种算法结果判定漂移严重程度
     */
    private DriftEvent checkDrift(int featLen) {
        int adwinDriftCount = 0;
        int phDriftCount = 0;
        List<Integer> adwinDriftFeatures = new ArrayList<>();
        List<Integer> phDriftFeatures = new ArrayList<>();

        // ADWIN逐特征检测
        for (int i = 0; i < featLen; i++) {
            if (adwinDetectors[i].detected()) {
                adwinDriftCount++;
                adwinDriftFeatures.add(i);
                adwinDetectors[i].reset(); // 检测后重置
            }
        }

        // Page-Hinkley逐特征检测
        for (int i = 0; i < featLen; i++) {
            if (pageHinkleyTests[i].detected()) {
                phDriftCount++;
                phDriftFeatures.add(i);
                pageHinkleyTests[i].reset();
            }
        }

        // KS检验全局分布
        double ksScore = ksTracker.computeKSScore();
        boolean ksDrift = ksScore > 0.15; // KS统计量阈值

        // PSI分布稳定性
        double psiScore = psiTracker.computePSI();
        boolean psiDrift = psiScore > 0.2; // PSI > 0.2 表示显著漂移

        // 综合评分
        double driftRatio = (adwinDriftCount + phDriftCount) / (2.0 * featLen);
        double algorithmVote = 0;
        if (ksDrift) algorithmVote += 0.25;
        if (psiDrift) algorithmVote += 0.25;
        algorithmVote += driftRatio * 0.5;

        // 确定严重程度
        DriftSeverity severity;
        if (algorithmVote >= 0.7 || (adwinDriftCount >= featLen * 0.6)) {
            severity = DriftSeverity.CRITICAL;
        } else if (algorithmVote >= 0.5 || (adwinDriftCount >= featLen * 0.4)) {
            severity = DriftSeverity.SEVERE;
        } else if (algorithmVote >= 0.3 || (adwinDriftCount >= featLen * 0.2)) {
            severity = DriftSeverity.MODERATE;
        } else if (algorithmVote >= 0.15 || adwinDriftCount > 0 || phDriftCount > 0) {
            severity = DriftSeverity.MINOR;
        } else {
            severity = DriftSeverity.NONE;
        }

        // 构建漂移事件
        if (severity != DriftSeverity.NONE) {
            Map<String, Object> details = new HashMap<>();
            details.put("adwin_drift_count", adwinDriftCount);
            details.put("ph_drift_count", phDriftCount);
            details.put("ks_score", ksScore);
            details.put("psi_score", psiScore);
            details.put("algorithm_vote", algorithmVote);
            details.put("adwin_drift_features", adwinDriftFeatures);
            details.put("ph_drift_features", phDriftFeatures);

            return new DriftEvent(
                    severity,
                    algorithmVote,
                    System.currentTimeMillis(),
                    counter.totalCount,
                    details
            );
        }
        return null;
    }

    private double sanitize(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) return 0.0;
        return value;
    }

    /**
     * 漂移事件输出类
     */
    public static class DriftEvent implements Serializable {
        public final DriftSeverity severity;      // 严重程度
        public final double driftScore;            // 综合漂移评分[0,1]
        public final long timestamp;               // 检测时间戳
        public final long sampleCount;             // 累计样本数
        public final Map<String, Object> details;  // 各算法详细信息

        public DriftEvent(DriftSeverity severity, double driftScore,
                          long timestamp, long sampleCount, Map<String, Object> details) {
            this.severity = severity;
            this.driftScore = driftScore;
            this.timestamp = timestamp;
            this.sampleCount = sampleCount;
            this.details = details;
        }

        @Override
        public String toString() {
            return String.format("DriftEvent{severity=%s, score=%.4f, samples=%d, details=%s}",
                    severity, driftScore, sampleCount, details);
        }
    }

    /**
     * 漂移严重程度枚举
     */
    public enum DriftSeverity implements Serializable {
        NONE,       // 无漂移
        MINOR,      // 轻微漂移：微调阈值
        MODERATE,   // 中度漂移：使用近期窗口重训
        SEVERE,     // 严重漂移：全量重训+阈值校准
        CRITICAL    // 紧急漂移：回退规则引擎+紧急重训
    }

    // ========== 内部算法实现 ==========

    /**
     * ADWIN (Adaptive Windowing) 漂移检测器
     * 核心思想：维护一个自适应大小的滑动窗口，
     * 当窗口内子段的均值差异超过统计学阈值时，判定漂移发生并截断窗口
     */
    public static class ADWINDetector implements Serializable {
        private final double delta;           // 置信度参数，越小越敏感
        private final Deque<Double> window;   // 自适应窗口
        private int maxLength = 500;          // 窗口最大长度

        private boolean driftFlag = false;

        public ADWINDetector(double delta) {
            this.delta = delta;
            this.window = new ArrayDeque<>();
        }

        public void add(double value) {
            window.addLast(value);
            if (window.size() > maxLength) {
                window.removeFirst();
            }
            // 检测窗口内是否有变化点
            if (window.size() >= 10) {
                detectChange();
            }
        }

        /**
         * 在窗口中搜索最优分割点，
         * 如果某分割点前后子段均值差异超过阈值，则判定漂移
         */
        private void detectChange() {
            List<Double> list = new ArrayList<>(window);
            int n = list.size();
            double cutPoint = -1;
            double maxDiff = 0;

            // 遍历可能的分割点
            for (int i = 1; i < n - 1; i++) {
                double leftMean = mean(list, 0, i);
                double rightMean = mean(list, i, n);
                double diff = Math.abs(leftMean - rightMean);

                // ADWIN理论阈值：sqrt((1/(2*m_left) + 1/(2*m_right)) * ln(4/delta))
                double mLeft = i;
                double mRight = n - i;
                double threshold = Math.sqrt(
                        (1.0 / (2.0 * mLeft) + 1.0 / (2.0 * mRight)) * Math.log(4.0 / delta)
                );

                if (diff > threshold && diff > maxDiff) {
                    maxDiff = diff;
                    cutPoint = i;
                }
            }

            if (cutPoint > 0) {
                driftFlag = true;
                // 截断窗口，保留变化点之后的数据（适应新分布）
                while (window.size() > n - cutPoint) {
                    window.removeFirst();
                }
            }
        }

        private double mean(List<Double> list, int from, int to) {
            double sum = 0;
            for (int i = from; i < to; i++) sum += list.get(i);
            return sum / (to - from);
        }

        public boolean detected() {
            return driftFlag;
        }

        public void reset() {
            driftFlag = false;
        }
    }

    /**
     * Page-Hinkley 漂移检测器
     * 核心思想：累积偏差和，当偏差超过阈值时判定均值发生偏移
     * 适合检测数据流中的渐进漂移和突变漂移
     */
    public static class PageHinkleyTest implements Serializable {
        private final double threshold;   // 漂移判定阈值
        private final double alpha;       // 最小允许的平均变化量

        private double sum;               // 累积偏差
        private double xMean;             // 当前均值
        private double minSum;            // 最小累积偏差
        private long count;

        private boolean driftFlag = false;

        public PageHinkleyTest(double threshold, double alpha) {
            this.threshold = threshold;
            this.alpha = alpha;
            this.sum = 0;
            this.xMean = 0;
            this.minSum = Double.MAX_VALUE;
            this.count = 0;
        }

        public void add(double value) {
            count++;
            // 增量更新均值
            xMean += (value - xMean) / count;
            // 累积偏差
            sum += (value - xMean - alpha);
            // 跟踪最小累积偏差
            minSum = Math.min(minSum, sum);

            // PH检验：如果当前累积偏差与最小值之差超过阈值，判定漂移
            if (sum - minSum > threshold) {
                driftFlag = true;
            }
        }

        public boolean detected() {
            return driftFlag;
        }

        public void reset() {
            driftFlag = false;
            sum = 0;
            xMean = 0;
            minSum = Double.MAX_VALUE;
            count = 0;
        }
    }

    /**
     * KS-test (Kolmogorov-Smirnov) 分布检验
     * 核心思想：比较当前样本与历史样本的经验累积分布函数(ECDF)，
     * 最大差值(D统计量)即为KS统计量
     */
    public static class KSTestTracker implements Serializable {
        private static final int MAX_HISTORY = 5000;
        private static final int RECENT_WINDOW = 500;

        private final List<double[]> historySamples = new ArrayList<>();
        private final List<double[]> recentSamples = new ArrayList<>();

        public void addSample(UserBehaviorSequence seq) {
            if (seq == null || seq.features == null) return;
            double[] copy = Arrays.copyOf(seq.features, Math.min(36, seq.features.length));
            recentSamples.add(copy);
            historySamples.add(copy);

            // 限制历史窗口大小
            if (historySamples.size() > MAX_HISTORY) {
                historySamples.subList(0, historySamples.size() - MAX_HISTORY).clear();
            }
            // 限制近期窗口
            if (recentSamples.size() > RECENT_WINDOW) {
                recentSamples.subList(0, recentSamples.size() - RECENT_WINDOW).clear();
            }
        }

        /**
         * 计算KS统计量
         * 对每个特征计算近期vs历史的ECDF最大差异，取平均
         */
        public double computeKSScore() {
            if (recentSamples.size() < 30 || historySamples.size() < 100) {
                return 0.0; // 样本不足
            }

            int featCount = recentSamples.get(0).length;
            double totalKS = 0;
            int validFeatures = 0;

            for (int f = 0; f < featCount; f++) {
                // 提取该特征的值
                double[] recent = extractFeature(recentSamples, f);
                double[] history = extractFeature(historySamples, f);

                double ks = computeKSStatistic(recent, history);
                totalKS += ks;
                validFeatures++;
            }

            return validFeatures > 0 ? totalKS / validFeatures : 0.0;
        }

        private double[] extractFeature(List<double[]> samples, int featIdx) {
            double[] vals = new double[samples.size()];
            for (int i = 0; i < samples.size(); i++) {
                vals[i] = featIdx < samples.get(i).length ? samples.get(i)[featIdx] : 0.0;
            }
            return vals;
        }

        /**
         * 计算两组样本的KS统计量
         * 即两个ECDF之间的最大垂直距离
         */
        private double computeKSStatistic(double[] sample1, double[] sample2) {
            Arrays.sort(sample1);
            Arrays.sort(sample2);

            double maxDiff = 0;
            int n1 = sample1.length;
            int n2 = sample2.length;
            int i1 = 0, i2 = 0;

            while (i1 < n1 && i2 < n2) {
                double d1 = sample1[i1];
                double d2 = sample2[i2];
                double v = Math.min(d1, d2);

                // 计算ECDF值
                int c1 = countLessEqual(sample1, v);
                int c2 = countLessEqual(sample2, v);
                double ecdf1 = (double) c1 / n1;
                double ecdf2 = (double) c2 / n2;

                maxDiff = Math.max(maxDiff, Math.abs(ecdf1 - ecdf2));

                if (d1 <= d2) i1++;
                else i2++;
            }

            return maxDiff;
        }

        private int countLessEqual(double[] sorted, double v) {
            int lo = 0, hi = sorted.length;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (sorted[mid] <= v) lo = mid + 1;
                else hi = mid;
            }
            return lo;
        }
    }

    /**
     * PSI (Population Stability Index) 人口稳定性指数
     * 核心思想：将特征值分桶，比较预期分布与实际分布的差异
     * PSI < 0.1: 无显著变化
     * 0.1 <= PSI < 0.2: 轻微变化
     * 0.2 <= PSI < 0.25: 中等变化
     * PSI >= 0.25: 显著变化
     */
    public static class PSITracker implements Serializable {
        private static final int NUM_BUCKETS = 10;      // 分桶数
        private static final int MAX_HISTORY = 10000;
        private static final int RECENT_SIZE = 1000;

        private final List<double[]> baselineSamples = new ArrayList<>();
        private final List<double[]> recentSamples = new ArrayList<>();
        private boolean baselineEstablished = false;

        // 基线分桶边界和比例
        private double[][] bucketBoundaries;  // [feature][bucket]
        private double[][] bucketProportions;  // [feature][bucket]

        public void addSample(UserBehaviorSequence seq) {
            if (seq == null || seq.features == null) return;
            double[] copy = Arrays.copyOf(seq.features, Math.min(36, seq.features.length));

            if (!baselineEstablished) {
                baselineSamples.add(copy);
                if (baselineSamples.size() >= 1000) {
                    establishBaseline();
                }
            }

            recentSamples.add(copy);
            if (recentSamples.size() > RECENT_SIZE) {
                recentSamples.remove(0);
            }

            // 限制基线样本存储
            if (baselineSamples.size() > MAX_HISTORY) {
                baselineSamples.subList(0, baselineSamples.size() - MAX_HISTORY).clear();
            }
        }

        /**
         * 建立基线分布
         */
        private void establishBaseline() {
            int featCount = baselineSamples.get(0).length;
            bucketBoundaries = new double[featCount][NUM_BUCKETS + 1];
            bucketProportions = new double[featCount][NUM_BUCKETS];

            for (int f = 0; f < featCount; f++) {
                // 提取特征值并排序
                double[] values = new double[baselineSamples.size()];
                for (int i = 0; i < baselineSamples.size(); i++) {
                    values[i] = f < baselineSamples.get(i).length ? baselineSamples.get(i)[f] : 0.0;
                }
                Arrays.sort(values);

                // 计算分位数边界
                for (int b = 0; b <= NUM_BUCKETS; b++) {
                    int idx = (int) Math.round((double) b / NUM_BUCKETS * (values.length - 1));
                    idx = Math.min(idx, values.length - 1);
                    bucketBoundaries[f][b] = values[idx];
                }

                // 计算各桶比例（每桶10%）
                for (int b = 0; b < NUM_BUCKETS; b++) {
                    bucketProportions[f][b] = 1.0 / NUM_BUCKETS;
                }
            }

            baselineEstablished = true;
        }

        /**
         * 计算PSI值
         * PSI = Σ(actual% - expected%) * ln(actual% / expected%)
         */
        public double computePSI() {
            if (!baselineEstablished || recentSamples.size() < 100) {
                return 0.0;
            }

            int featCount = bucketProportions.length;
            double totalPSI = 0;
            int validFeatures = 0;

            for (int f = 0; f < featCount; f++) {
                // 统计近期样本在各桶中的分布
                int[] bucketCounts = new int[NUM_BUCKETS];
                for (double[] sample : recentSamples) {
                    double value = f < sample.length ? sample[f] : 0.0;
                    int bucket = findBucket(f, value);
                    bucketCounts[bucket]++;
                }

                // 计算PSI
                double featurePSI = 0;
                int n = recentSamples.size();
                for (int b = 0; b < NUM_BUCKETS; b++) {
                    double expectedPct = bucketProportions[f][b];
                    double actualPct = (double) bucketCounts[b] / n;

                    // 防止log(0)，加平滑项
                    expectedPct = Math.max(expectedPct, 0.0001);
                    actualPct = Math.max(actualPct, 0.0001);

                    featurePSI += (actualPct - expectedPct) * Math.log(actualPct / expectedPct);
                }

                totalPSI += Math.abs(featurePSI); // PSI取绝对值
                validFeatures++;
            }

            return validFeatures > 0 ? totalPSI / validFeatures : 0.0;
        }

        /**
         * 查找值所属的桶
         */
        private int findBucket(int featureIdx, double value) {
            for (int b = 0; b < NUM_BUCKETS; b++) {
                if (value <= bucketBoundaries[featureIdx][b + 1]) {
                    return b;
                }
            }
            return NUM_BUCKETS - 1;
        }
    }

    /**
     * 样本计数器
     */
    private static class SampleCounter implements Serializable {
        long totalCount = 0;
    }
}
