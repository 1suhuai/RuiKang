package com.fraud.detection.graph;

import com.fraud.detection.model.Alert;

import java.util.*;

/**
 * 图分析引擎
对TransactionGraph进行深度分析
检测:
- 关联环路数(洗钱环路指标)
- 最大资金链深度
- 同IP/设备关联账户数(社区大小)
- 入金额/出金额/流平衡
- 总度数(连接数)
 */
public class GraphAnalyzer implements java.io.Serializable {

    private static final double CHAIN_ALERT_THRESHOLD = 35.0;
    private static final int FEATURE_COUNT = 8;
    private static final int DIRECTION_FEATURE_COUNT = 4;

    public List<Alert> analyzeAccount(TransactionGraph graph, String accountId, long timestamp) {
        List<Alert> alerts = new ArrayList<>();
        if (graph == null || accountId == null) {
            return alerts;
        }

        int outDegree = graph.getOutDegree(accountId);
        int inDegree = graph.getInDegree(accountId);
        double outAmount = graph.getOutAmount(accountId);
        double inAmount = graph.getInAmount(accountId);
        int distinctDestCount = graph.getOutNeighbors(accountId).size();
        boolean hasCycle = !graph.detectCycles(accountId).isEmpty();
        int chainDepth = getMaxChainDepth(graph, accountId);
        int communitySize = graph.detectCommunity(accountId).size();

        double cashOutRatio = inAmount > 0 ? outAmount / inAmount : 0;
        double directionAsymmetry = calcDirectionAsymmetry(inAmount, outAmount);
        double netFlowAsymmetry = calcInOutAmountAsymmetry(inAmount, outAmount);
        double timeConcentration = calcTimeConcentration(graph, accountId);

        boolean isCashOutHeavy = outAmount > 5000 && cashOutRatio > 0.7;
        boolean isChainNode = outDegree >= 1 && inDegree >= 1;
        boolean isCircleNode = hasCycle && outDegree >= 1;
        boolean isFanOut = outDegree >= 3 && outAmount >= 15000;
        boolean isFanIn = inDegree >= 2 && inAmount >= 10000;

        double totalScore = 0;
        StringBuilder descBuilder = new StringBuilder("Graph:[");

        if (isChainNode) {
            totalScore += 25;
            if (chainDepth >= 2) totalScore += 10;
            descBuilder.append("链深度=").append(chainDepth).append(",");
        }
        if (isCircleNode) {
            totalScore += 30;
            descBuilder.append("环路,");
        }
        if (isFanOut) {
            totalScore += 20;
            descBuilder.append("分散出(").append(outDegree).append("路),");
        }
        if (isFanIn) {
            totalScore += 20;
            descBuilder.append("集中入(").append(inDegree).append("路),");
        }
        if (isCashOutHeavy) {
            totalScore += 20;
            descBuilder.append("高提现比(").append(String.format("%.0f", cashOutRatio * 100)).append("%),");
        }
        if (directionAsymmetry > 0.5 && outAmount > inAmount && outAmount > 3000) {
            totalScore += 15 * directionAsymmetry;
            descBuilder.append("方向不对称(出远大于入),");
        }
        if (netFlowAsymmetry > 0.4 && outAmount > 5000) {
            totalScore += 10 * netFlowAsymmetry;
            descBuilder.append("净流出不对称,");
        }
        if (timeConcentration > 0.5) {
            totalScore += 12 * timeConcentration;
            descBuilder.append("时间高密集(").append(String.format("%.1f", timeConcentration)).append("),");
        }

        if (descBuilder.charAt(descBuilder.length() - 1) == ',') {
            descBuilder.setLength(descBuilder.length() - 1);
        }
        descBuilder.append("]");

        if (totalScore >= CHAIN_ALERT_THRESHOLD) {
            double confidence = Math.min(0.95, totalScore / 100.0);

            // 根据图特征判定具体欺诈类型
            String fraudType = resolveGraphFraudType(
                    isChainNode, isCircleNode, isFanOut, isFanIn, isCashOutHeavy,
                    chainDepth, outDegree, inDegree, communitySize,
                    cashOutRatio, directionAsymmetry, netFlowAsymmetry);

            Alert alert = new Alert(accountId, fraudType, confidence, "GRAPH_ANALYZER");
            alert.amount = outAmount + inAmount;
            alert.timestamp = timestamp;
            alert.behaviorPath = accountId;

            // 构建图路径信息，增强可解释性与溯源
            String graphPath = buildGraphPath(graph, accountId, isChainNode, isFanOut, isFanIn, hasCycle);

            alert.details = descBuilder.toString() + String.format(
                    ",outDeg=%d,inDeg=%d,outAmt=%.0f,inAmt=%.0f,chain=%d,cycle=%b,community=%d,path=%s",
                    outDegree, inDegree, outAmount, inAmount, chainDepth, hasCycle, communitySize, graphPath);
            alerts.add(alert);
        }

        return alerts;
    }

    /**
     * 根据图拓扑特征判定具体欺诈类型
     * 图分析的核心价值：通过资金流向拓扑识别CEP/SQL无法发现的模式
     */
    private String resolveGraphFraudType(boolean isChainNode, boolean isCircleNode,
                                          boolean isFanOut, boolean isFanIn, boolean isCashOutHeavy,
                                          int chainDepth, int outDegree, int inDegree, int communitySize,
                                          double cashOutRatio, double directionAsymmetry, double netFlowAsymmetry) {

        // 洗钱环路: 有环路（最高优先级，图分析独有模式）
        if (isCircleNode) {
            return "图分析-洗钱环路";
        }

        // 多层链式洗钱: A→B→C 有入有出即链式节点
        if (isChainNode && chainDepth >= 2) {
            if (isCashOutHeavy) return "图分析-链式洗钱网络";
            return "图分析-链式中转节点";
        }

        // 分散转入集中提现: 扇入+高提现比
        if (isFanIn && isCashOutHeavy) {
            return "图分析-分散转入集中提现";
        }

        // 分散转入: 扇入但不一定提现
        if (isFanIn) {
            return "图分析-资金汇聚节点";
        }

        // 团伙作案: 社区规模>=3 + 扇出（需要足够的关联账户）
        if (communitySize >= 3 && isFanOut) {
            return "图分析-团伙协作网络";
        }

        // 多目标分散转出: 扇出（出度>=3才判定）
        if (isFanOut && outDegree >= 3) {
            return "图分析-多目标分散转出";
        }

        // 账户被盗: 方向不对称(出远大于入) + 高提现
        if (directionAsymmetry > 0.5 && isCashOutHeavy && outDegree >= 1) {
            return "图分析-账户被盗资金转移";
        }

        // 净流出异常
        if (netFlowAsymmetry > 0.4 && isCashOutHeavy) {
            return "图分析-异常资金流出";
        }

        // 链式节点（入+出但链深不够2）
        if (isChainNode) {
            return "图分析-链式中转节点";
        }

        return "图分析-异常交易网络";
    }

    private int getMaxChainDepth(TransactionGraph graph, String accountId) {
        List<List<TransactionGraph.Edge>> chains = graph.traceFundChain(accountId, 10);
        int maxDepth = 0;
        for (List<TransactionGraph.Edge> chain : chains) {
            maxDepth = Math.max(maxDepth, chain.size());
        }
        return maxDepth;
    }

    /**
     * 构建图路径描述，用于可解释性与溯源
     * 格式: A→B(10万)→C(9.8万) 或 A←D(2万)←E(3万) A→F(5万)
     */
    private String buildGraphPath(TransactionGraph graph, String accountId,
                                   boolean isChainNode, boolean isFanOut, boolean isFanIn,
                                   boolean hasCycle) {
        StringBuilder path = new StringBuilder();
        String shortId = accountId.length() > 8 ? accountId.substring(0, 8) : accountId;

        // 出边路径
        List<TransactionGraph.Edge> outEdges = graph.getOutEdges(accountId);
        if (!outEdges.isEmpty()) {
            for (TransactionGraph.Edge e : outEdges.subList(0, Math.min(3, outEdges.size()))) {
                String dest = e.toAccount.length() > 8 ? e.toAccount.substring(0, 8) : e.toAccount;
                path.append(shortId).append("→").append(dest)
                    .append("(").append(String.format("%.0f", e.amount)).append(")");
                if (outEdges.size() > 3) { path.append("..."); break; }
            }
        }

        // 入边路径
        List<TransactionGraph.Edge> inEdges = graph.getInEdges(accountId);
        if (!inEdges.isEmpty()) {
            if (path.length() > 0) path.append(" ");
            for (TransactionGraph.Edge e : inEdges.subList(0, Math.min(3, inEdges.size()))) {
                String src = e.fromAccount.length() > 8 ? e.fromAccount.substring(0, 8) : e.fromAccount;
                path.append(src).append("→").append(shortId)
                    .append("(").append(String.format("%.0f", e.amount)).append(")");
                if (inEdges.size() > 3) { path.append("..."); break; }
            }
        }

        // 链式路径
        if (isChainNode) {
            List<List<TransactionGraph.Edge>> chains = graph.traceFundChain(accountId, 3);
            if (!chains.isEmpty() && path.length() > 0) path.append(" | ");
            List<TransactionGraph.Edge> longestChain = chains.get(0);
            for (TransactionGraph.Edge e : longestChain) {
                String src = e.fromAccount.length() > 8 ? e.fromAccount.substring(0, 8) : e.fromAccount;
                String dest = e.toAccount.length() > 8 ? e.toAccount.substring(0, 8) : e.toAccount;
                path.append(src).append("→").append(dest)
                    .append("(").append(String.format("%.0f", e.amount)).append(")→");
            }
            if (path.length() > 2) path.setLength(path.length() - 2); // 去掉最后的→
        }

        // 环路标记
        if (hasCycle) {
            if (path.length() > 0) path.append(" | ");
            path.append("CYCLE detected");
        }

        return path.length() > 0 ? path.toString() : shortId;
    }

    public double[] extractGraphFeatures(TransactionGraph graph, String accountId) {
        double[] features = new double[FEATURE_COUNT];
        if (graph == null || accountId == null) {
            Arrays.fill(features, 0.0);
            return features;
        }

        features[0] = graph.getOutDegree(accountId);
        features[1] = graph.getInDegree(accountId);
        features[2] = graph.getOutNeighbors(accountId).size();
        features[3] = graph.detectCycles(accountId).isEmpty() ? 0.0 : 1.0;
        features[4] = getMaxChainDepth(graph, accountId);
        features[5] = graph.getOutDegree(accountId) + graph.getInDegree(accountId);
        features[6] = graph.getInAmount(accountId);
        features[7] = graph.getOutAmount(accountId);

        return features;
    }

    public double[] extractDirectionFeatures(TransactionGraph graph, String accountId) {
        double[] features = new double[DIRECTION_FEATURE_COUNT];
        if (graph == null || accountId == null) {
            Arrays.fill(features, 0.0);
            return features;
        }

        double inAmount = graph.getInAmount(accountId);
        double outAmount = graph.getOutAmount(accountId);
        int inDegree = graph.getInDegree(accountId);
        int outDegree = graph.getOutDegree(accountId);

        features[0] = calcDirectionAsymmetry(inAmount, outAmount);
        features[1] = calcInOutFrequencyRatio(inDegree, outDegree);
        features[2] = calcInOutAmountAsymmetry(inAmount, outAmount);
        features[3] = calcTimeConcentration(graph, accountId);

        return features;
    }

    private double calcDirectionAsymmetry(double inAmount, double outAmount) {
        double total = inAmount + outAmount;
        if (total <= 0) return 0.0;
        return Math.abs(inAmount - outAmount) / total;
    }

    private double calcInOutFrequencyRatio(int inDegree, int outDegree) {
        if (inDegree == 0 && outDegree == 0) return 0.0;
        if (inDegree == 0) return Double.MAX_VALUE;
        if (outDegree == 0) return 0.0;
        return (double) outDegree / inDegree;
    }

    private double calcInOutAmountAsymmetry(double inAmount, double outAmount) {
        if (inAmount <= 0 && outAmount <= 0) return 0.0;
        double netFlow = outAmount - inAmount;
        return netFlow / (inAmount + outAmount);
    }

    private double calcTimeConcentration(TransactionGraph graph, String accountId) {
        List<TransactionGraph.Edge> allEdges = new ArrayList<>();
        allEdges.addAll(graph.getOutEdges(accountId));
        allEdges.addAll(graph.getInEdges(accountId));
        if (allEdges.size() < 2) return 0.0;

        long minTs = Long.MAX_VALUE;
        long maxTs = Long.MIN_VALUE;
        for (TransactionGraph.Edge e : allEdges) {
            if (e.timestamp < minTs) minTs = e.timestamp;
            if (e.timestamp > maxTs) maxTs = e.timestamp;
        }

        long span = maxTs - minTs;
        if (span <= 0) return 1.0;
        double density = (double) allEdges.size() / (span / 1000.0);
        return Math.min(density * 10.0, 1.0);
    }

    private double getMaxSingleTransferRatio(TransactionGraph graph, String accountId) {
        List<TransactionGraph.Edge> outEdges = graph.getOutEdges(accountId);
        double totalOut = graph.getOutAmount(accountId);
        if (totalOut <= 0 || outEdges.isEmpty()) return 0;
        double maxSingle = 0;
        for (TransactionGraph.Edge e : outEdges) {
            maxSingle = Math.max(maxSingle, e.amount);
        }
        return maxSingle / totalOut;
    }
}
