package com.fraud.detection.graph;

import java.io.Serializable;
import java.util.*;

/**
 * 交易图数据结构 - 有向图
 * 节点=账户,边=转账关系
 * 维护账户间的资金流向关系
 * 用于检测洗钱环路、资金链深度、关联社区等图特征
 * 邻接表存储，支持资金链路追踪和团伙识别
 */
public class TransactionGraph implements Serializable {

    public static class Edge implements Serializable {
        public String fromAccount;
        public String toAccount;
        public double amount;
        public long timestamp;
        public String txType;

        public Edge() {}

        public Edge(String from, String to, double amount, long timestamp, String txType) {
            this.fromAccount = from;
            this.toAccount = to;
            this.amount = amount;
            this.timestamp = timestamp;
            this.txType = txType;
        }

        @Override
        public String toString() {
            return String.format("%s->%s(%.0f,%s)", fromAccount, toAccount, amount, txType);
        }
    }

    // 出边：accountId → 该账户转出的目标列表
    private final Map<String, List<Edge>> outEdges = new HashMap<>();
    // 入边：accountId → 转入该账户的来源列表
    private final Map<String, List<Edge>> inEdges = new HashMap<>();
    // IP → 账户集合（用于团伙识别）
    private final Map<String, Set<String>> ipAccountMap = new HashMap<>();
    // 设备 → 账户集合
    private final Map<String, Set<String>> deviceAccountMap = new HashMap<>();

    private int totalEdges;

    public void addEdge(String from, String to, double amount, long timestamp, String txType) {
        if (from == null || to == null || from.equals(to)) return;

        Edge edge = new Edge(from, to, amount, timestamp, txType);
        outEdges.computeIfAbsent(from, k -> new ArrayList<>()).add(edge);
        inEdges.computeIfAbsent(to, k -> new ArrayList<>()).add(edge);
        totalEdges++;
    }

    public void addAccountAttributes(String accountId, String ipSegment, String deviceId) {
        if (accountId != null && ipSegment != null) {
            ipAccountMap.computeIfAbsent(ipSegment, k -> new HashSet<>()).add(accountId);
        }
        if (accountId != null && deviceId != null) {
            deviceAccountMap.computeIfAbsent(deviceId, k -> new HashSet<>()).add(accountId);
        }
    }

    public List<Edge> getOutEdges(String accountId) {
        return outEdges.getOrDefault(accountId, Collections.emptyList());
    }

    public List<Edge> getInEdges(String accountId) {
        return inEdges.getOrDefault(accountId, Collections.emptyList());
    }

    public int getOutDegree(String accountId) {
        return getOutEdges(accountId).size();
    }

    public int getInDegree(String accountId) {
        return getInEdges(accountId).size();
    }

    public double getOutAmount(String accountId) {
        double sum = 0;
        for (Edge e : getOutEdges(accountId)) sum += e.amount;
        return sum;
    }

    public double getInAmount(String accountId) {
        double sum = 0;
        for (Edge e : getInEdges(accountId)) sum += e.amount;
        return sum;
    }

    public Set<String> getOutNeighbors(String accountId) {
        Set<String> neighbors = new HashSet<>();
        for (Edge e : getOutEdges(accountId)) neighbors.add(e.toAccount);
        return neighbors;
    }

    public Set<String> getInNeighbors(String accountId) {
        Set<String> neighbors = new HashSet<>();
        for (Edge e : getInEdges(accountId)) neighbors.add(e.fromAccount);
        return neighbors;
    }

    // BFS 资金链路追踪
    public List<List<Edge>> traceFundChain(String accountId, int maxDepth) {
        List<List<Edge>> chains = new ArrayList<>();
        Queue<List<Edge>> queue = new LinkedList<>();

        // 初始化：从起始账户的出边开始
        for (Edge edge : getOutEdges(accountId)) {
            List<Edge> chain = new ArrayList<>();
            chain.add(edge);
            queue.offer(chain);
        }

        while (!queue.isEmpty()) {
            List<Edge> current = queue.poll();
            if (current.size() >= maxDepth) {
                chains.add(current);
                continue;
            }

            String lastAccount = current.get(current.size() - 1).toAccount;
            List<Edge> nextEdges = getOutEdges(lastAccount);

            if (nextEdges.isEmpty()) {
                chains.add(current); // 叶子节点
            } else {
                for (Edge next : nextEdges) {
                    // 避免环路
                    boolean isCycle = false;
                    for (Edge e : current) {
                        if (e.fromAccount.equals(next.toAccount) || e.toAccount.equals(next.toAccount)) {
                            isCycle = true;
                            break;
                        }
                    }
                    if (!isCycle) {
                        List<Edge> newChain = new ArrayList<>(current);
                        newChain.add(next);
                        queue.offer(newChain);
                    } else {
                        chains.add(current); // 检测到环路，记录当前链
                    }
                }
            }
        }

        if (chains.isEmpty() && !getOutEdges(accountId).isEmpty()) {
            chains.add(getOutEdges(accountId));
        }

        return chains;
    }

    // DFS 环路检测
    public List<List<String>> detectCycles(String accountId) {
        List<List<String>> cycles = new ArrayList<>();
        Set<String> visited = new HashSet<>();
        Deque<String> path = new ArrayDeque<>();
        detectCyclesDFS(accountId, accountId, visited, path, cycles, 0, 5);
        return cycles;
    }

    private void detectCyclesDFS(String current, String target, Set<String> visited,
                                  Deque<String> path, List<List<String>> cycles,
                                  int depth, int maxDepth) {
        if (depth > maxDepth) return;

        visited.add(current);
        path.addLast(current);

        for (Edge edge : getOutEdges(current)) {
            if (edge.toAccount.equals(target) && path.size() >= 2) {
                // 找到环路
                List<String> cycle = new ArrayList<>(path);
                cycle.add(target);
                cycles.add(cycle);
            } else if (!visited.contains(edge.toAccount)) {
                detectCyclesDFS(edge.toAccount, target, visited, path, cycles, depth + 1, maxDepth);
            }
        }

        path.removeLast();
        visited.remove(current);
    }

    // 基于共享 IP/设备的团伙识别
    public Set<String> detectCommunity(String accountId) {
        Set<String> community = new HashSet<>();
        community.add(accountId);

        // 通过共享 IP 扩展
        for (Map.Entry<String, Set<String>> entry : ipAccountMap.entrySet()) {
            if (entry.getValue().contains(accountId)) {
                community.addAll(entry.getValue());
            }
        }

        // 通过共享设备扩展
        for (Map.Entry<String, Set<String>> entry : deviceAccountMap.entrySet()) {
            if (entry.getValue().contains(accountId)) {
                community.addAll(entry.getValue());
            }
        }

        // 通过直接交易关系扩展一层
        Set<String> directNeighbors = new HashSet<>();
        for (String member : community) {
            directNeighbors.addAll(getOutNeighbors(member));
            directNeighbors.addAll(getInNeighbors(member));
        }
        community.addAll(directNeighbors);

        return community;
    }

    // 分散转入检测：多个不同账户向同一目标转入
    public ScatterPattern detectScatterInPattern(String targetAccount) {
        List<Edge> inEdgesList = getInEdges(targetAccount);
        if (inEdgesList.size() < 2) return null;

        Set<String> uniqueSenders = new HashSet<>();
        double totalInAmount = 0;
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;

        for (Edge e : inEdgesList) {
            uniqueSenders.add(e.fromAccount);
            totalInAmount += e.amount;
            minTime = Math.min(minTime, e.timestamp);
            maxTime = Math.max(maxTime, e.timestamp);
        }

        if (uniqueSenders.size() >= 2) {
            ScatterPattern pattern = new ScatterPattern();
            pattern.targetAccount = targetAccount;
            pattern.senderCount = uniqueSenders.size();
            pattern.totalAmount = totalInAmount;
            pattern.timeWindowMs = maxTime - minTime;
            pattern.senders = uniqueSenders;
            return pattern;
        }
        return null;
    }

    // 集中转出检测：一个账户向多个不同目标转出
    public ScatterPattern detectScatterOutPattern(String sourceAccount) {
        List<Edge> outEdgesList = getOutEdges(sourceAccount);
        if (outEdgesList.size() < 2) return null;

        Set<String> uniqueReceivers = new HashSet<>();
        double totalOutAmount = 0;
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;

        for (Edge e : outEdgesList) {
            uniqueReceivers.add(e.toAccount);
            totalOutAmount += e.amount;
            minTime = Math.min(minTime, e.timestamp);
            maxTime = Math.max(maxTime, e.timestamp);
        }

        if (uniqueReceivers.size() >= 2) {
            ScatterPattern pattern = new ScatterPattern();
            pattern.targetAccount = sourceAccount;
            pattern.senderCount = uniqueReceivers.size();
            pattern.totalAmount = totalOutAmount;
            pattern.timeWindowMs = maxTime - minTime;
            pattern.senders = uniqueReceivers;
            pattern.isOutPattern = true;
            return pattern;
        }
        return null;
    }

    // 综合图风险评分
    public double computeGraphRiskScore(String accountId) {
        double score = 0.0;

        int outDeg = getOutDegree(accountId);
        int inDeg = getInDegree(accountId);
        double outAmt = getOutAmount(accountId);
        double inAmt = getInAmount(accountId);

        // 出度风险
        if (outDeg >= 3) score += 0.15;
        if (outDeg >= 5) score += 0.10;

        // 入度风险（分散转入）
        if (inDeg >= 3) score += 0.15;
        if (inDeg >= 5) score += 0.10;

        // 大额资金流动
        if (outAmt > 100000) score += 0.15;
        if (inAmt > 100000) score += 0.10;

        // 流入流出比异常
        if (inAmt > 0 && outAmt > 0) {
            double ratio = outAmt / inAmt;
            if (ratio > 0.9 && ratio < 1.1) score += 0.10; // 近似等额转出
        }

        // 环路检测
        List<List<String>> cycles = detectCycles(accountId);
        if (!cycles.isEmpty()) score += 0.20;

        // 团伙关联
        Set<String> community = detectCommunity(accountId);
        if (community.size() >= 3) score += 0.10;
        if (community.size() >= 5) score += 0.10;

        return Math.min(1.0, score);
    }

    public int getNodeCount() {
        Set<String> allNodes = new HashSet<>(outEdges.keySet());
        allNodes.addAll(inEdges.keySet());
        return allNodes.size();
    }

    public int getTotalEdges() {
        return totalEdges;
    }

    public static class ScatterPattern implements Serializable {
        public String targetAccount;
        public int senderCount;
        public double totalAmount;
        public long timeWindowMs;
        public Set<String> senders;
        public boolean isOutPattern;

        @Override
        public String toString() {
            return String.format("ScatterPattern{target=%s, senders=%d, amount=%.0f, window=%ds}",
                    targetAccount, senderCount, totalAmount, timeWindowMs / 1000);
        }
    }
}
