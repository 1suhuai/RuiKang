"""
GNN图神经网络检测服务 - GraphSAGE实现
实现基于图结构的关系型欺诈检测
"""
import json
import math
import time
import logging
import uuid
from datetime import datetime
from collections import defaultdict, deque
from typing import Dict, List, Optional, Any, Set, Tuple

logger = logging.getLogger("gnn_service")


class GraphNode:
    """图节点"""

    def __init__(self, node_id: str, node_type: str = "account"):
        self.node_id = node_id
        self.node_type = node_type
        self.features: Dict[str, float] = {}
        self.neighbors: List[Tuple[str, float]] = []  # (neighbor_id, edge_weight)
        self.embedding: List[float] = []

    def add_neighbor(self, neighbor_id: str, weight: float = 1.0):
        self.neighbors.append((neighbor_id, weight))

    def set_feature(self, key: str, value: float):
        self.features[key] = value

    def to_dict(self) -> Dict:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "features": self.features,
            "degree": len(self.neighbors),
            "has_embedding": len(self.embedding) > 0,
        }


class GraphEdge:
    """图边"""

    def __init__(self, source: str, target: str, edge_type: str = "transfer", weight: float = 1.0):
        self.source = source
        self.target = target
        self.edge_type = edge_type
        self.weight = weight
        self.timestamp = time.time()
        self.amount = 0.0


class TransactionGraph:
    """交易图结构"""

    def __init__(self, max_nodes: int = 10000):
        self.nodes: Dict[str, GraphNode] = {}
        self.edges: List[GraphEdge] = []
        self.max_nodes = max_nodes
        self._neighbor_cache: Dict[str, List[str]] = {}

    def add_node(self, node: GraphNode):
        if len(self.nodes) >= self.max_nodes:
            self._evict_oldest()
        self.nodes[node.node_id] = node
        self._neighbor_cache.clear()

    def add_edge(self, edge: GraphEdge):
        self.edges.append(edge)
        if edge.source in self.nodes:
            self.nodes[edge.source].add_neighbor(edge.target, edge.weight)
        if edge.target in self.nodes:
            self.nodes[edge.target].add_neighbor(edge.source, edge.weight)
        self._neighbor_cache.clear()

    def get_node(self, node_id: str) -> Optional[GraphNode]:
        return self.nodes.get(node_id)

    def get_neighbors(self, node_id: str, max_depth: int = 2, sample_size: int = 10) -> Dict[str, List[str]]:
        """获取多跳邻居（带采样）"""
        if node_id in self._neighbor_cache:
            return self._neighbor_cache[node_id]

        visited: Set[str] = {node_id}
        current_level: List[str] = [node_id]
        neighbors_by_hop: Dict[str, List[str]] = {}

        for hop in range(1, max_depth + 1):
            next_level: List[str] = []
            for nid in current_level:
                if nid in self.nodes:
                    for neighbor_id, _ in self.nodes[nid].neighbors:
                        if neighbor_id not in visited:
                            visited.add(neighbor_id)
                            next_level.append(neighbor_id)

            # 采样
            if len(next_level) > sample_size:
                import random
                next_level = random.sample(next_level, sample_size)

            neighbors_by_hop[f"hop_{hop}"] = next_level
            current_level = next_level

            if not current_level:
                break

        self._neighbor_cache[node_id] = neighbors_by_hop
        return neighbors_by_hop

    def _evict_oldest(self):
        """淘汰最老的节点"""
        if self.nodes:
            oldest_id = next(iter(self.nodes))
            del self.nodes[oldest_id]

    @property
    def node_count(self) -> int:
        return len(self.nodes)

    @property
    def edge_count(self) -> int:
        return len(self.edges)


class GraphSAGEModel:
    """简化的GraphSAGE模型实现"""

    def __init__(self, input_dim: int = 16, hidden_dim: int = 32, output_dim: int = 1):
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.output_dim = output_dim

        # 简化的权重矩阵（实际应用中应使用PyTorch/DeepGraphLibrary训练）
        self.w_agg: List[List[float]] = self._init_weights(hidden_dim, input_dim)
        self.w_self: List[List[float]] = self._init_weights(hidden_dim, input_dim)
        self.w_out: List[List[float]] = self._init_weights(output_dim, hidden_dim)

        # 训练历史
        self.training_epochs: int = 0
        self.loss_history: List[float] = []

    def _init_weights(self, rows: int, cols: int) -> List[List[float]]:
        """Xavier初始化"""
        import random
        scale = math.sqrt(2.0 / (rows + cols))
        return [[random.gauss(0, scale) for _ in range(cols)] for _ in range(rows)]

    def forward(self, node: GraphNode, graph: TransactionGraph) -> float:
        """
        GraphSAGE前向传播
        1. 邻居聚合（Mean Aggregator）
        2. 节点表示更新
        3. 异常评分计算
        """
        # 1. 获取2跳邻居
        neighbors = graph.get_neighbors(node.node_id, max_depth=2, sample_size=10)

        # 2. 收集邻居特征
        neighbor_features = self._aggregate_neighbor_features(neighbors, graph)

        # 3. 节点自身特征
        self_features = self._extract_node_features(node)

        # 4. 邻居聚合（Mean）
        agg_features = self._mean_aggregate(neighbor_features)

        # 5. 拼接+变换 [self || agg] -> hidden
        combined = self_features[:len(self_features) // 2] + agg_features[:len(agg_features) // 2]
        hidden = self._matmul(self.w_self, self_features)
        agg_transformed = self._matmul(self.w_agg, agg_features)
        hidden = [(h + a) / 2 for h, a in zip(hidden, agg_transformed)]

        # 6. ReLU激活
        hidden = [max(0, h) for h in hidden]

        # 7. 输出层 -> 异常评分
        output = self._matmul(self.w_out, hidden)
        score = output[0] if output else 0.0

        # Sigmoid归一化
        fraud_score = 1.0 / (1.0 + math.exp(-max(-500, min(500, score))))

        return fraud_score

    def _aggregate_neighbor_features(self, neighbors: Dict, graph: TransactionGraph) -> List[List[float]]:
        """聚合邻居特征"""
        features = []
        for hop, node_ids in neighbors.items():
            for nid in node_ids:
                neighbor = graph.get_node(nid)
                if neighbor:
                    feat = self._extract_node_features(neighbor)
                    features.append(feat)
        return features

    def _extract_node_features(self, node: GraphNode) -> List[float]:
        """提取节点特征向量"""
        feat = node.features.get("transaction_count", 0)
        amount = node.features.get("avg_amount", 0)
        risk = node.features.get("risk_score", 0)
        degree = node.features.get("degree", 0)
        return [feat / 100, amount / 500000, risk, degree / 50] + [0] * 12

    def _mean_aggregate(self, features: List[List[float]]) -> List[float]:
        """Mean聚合器"""
        if not features:
            return [0.0] * self.hidden_dim
        dim = len(features[0])
        result = [0.0] * dim
        for feat in features:
            for i in range(dim):
                result[i] += feat[i]
        return [x / len(features) for x in result]

    def _matmul(self, weights: List[List[float]], features: List[float]) -> List[float]:
        """矩阵乘法"""
        if not features or not weights:
            return []
        rows = len(weights)
        cols = len(weights[0]) if weights else 0
        result = []
        for i in range(rows):
            val = 0.0
            for j in range(min(cols, len(features))):
                val += weights[i][j] * features[j]
            result.append(val)
        return result


class GNNDetectionService:
    """GNN图神经网络检测服务"""

    def __init__(self):
        self.graph = TransactionGraph(max_nodes=10000)
        self.model = GraphSAGEModel(input_dim=16, hidden_dim=32, output_dim=1)

        # 历史统计
        self.total_detections = 0
        self.fraud_detections = 0
        self.detection_history: deque = deque(maxlen=1000)

        # 账户画像
        self.account_profiles: Dict[str, Dict] = defaultdict(lambda: {
            "transaction_count": 0,
            "total_amount": 0.0,
            "avg_amount": 0.0,
            "risk_score": 0.0,
            "last_update": time.time(),
        })

        logger.info("GNN检测服务已初始化")

    def update_account_profile(self, account_id: str, transaction: Dict):
        """更新账户画像"""
        profile = self.account_profiles[account_id]
        profile["transaction_count"] += 1
        profile["total_amount"] += transaction.get("amount", 0)
        profile["avg_amount"] = profile["total_amount"] / profile["transaction_count"]
        profile["last_update"] = time.time()

        # 更新图节点
        node = self.graph.get_node(account_id)
        if node:
            node.set_feature("transaction_count", profile["transaction_count"])
            node.set_feature("avg_amount", profile["avg_amount"])
            node.set_feature("degree", len(node.neighbors))
        else:
            node = GraphNode(account_id, "account")
            node.set_feature("transaction_count", profile["transaction_count"])
            node.set_feature("avg_amount", profile["avg_amount"])
            node.set_feature("risk_score", transaction.get("deviceRiskLevel", "LOW") == "HIGH" and 0.8 or 0.1)
            node.set_feature("degree", 0)
            self.graph.add_node(node)

        # 添加边（转账关系）
        target = transaction.get("nameDest", "")
        if target:
            edge = GraphEdge(
                source=account_id,
                target=target,
                edge_type=transaction.get("type", "TRANSFER"),
                weight=transaction.get("amount", 0),
            )
            edge.amount = transaction.get("amount", 0)
            self.graph.add_edge(edge)

            # 更新目标账户画像
            if target not in self.account_profiles:
                target_node = GraphNode(target, "account")
                target_node.set_feature("transaction_count", 0)
                target_node.set_feature("avg_amount", 0)
                target_node.set_feature("risk_score", 0.1)
                target_node.set_feature("degree", 1)
                self.graph.add_node(target_node)

    def detect(self, request_data: Dict) -> Dict:
        """执行GNN异常检测"""
        start_time = time.monotonic()
        self.total_detections += 1

        account_id = request_data.get("account_id", request_data.get("nameOrig", ""))
        amount = request_data.get("amount", 0)

        # 更新图结构
        if account_id:
            self.update_account_profile(account_id, request_data)

        # 执行GNN检测
        fraud_score = 0.0
        if account_id and account_id in self.graph.nodes:
            node = self.graph.nodes[account_id]
            fraud_score = self.model.forward(node, self.graph)

        # 结合规则增强
        rule_boost = 0.0
        if amount > 50000:
            rule_boost += 0.15
        if request_data.get("deviceRiskLevel") == "HIGH":
            rule_boost += 0.1
        if request_data.get("isAbroad") == "ABROAD":
            rule_boost += 0.1

        final_score = min(1.0, fraud_score + rule_boost)
        is_fraud = final_score > 0.65

        if is_fraud:
            self.fraud_detections += 1

        processing_time = (time.monotonic() - start_time) * 1000

        result = {
            "request_id": f"GNN_{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}",
            "is_fraud": is_fraud,
            "fraud_probability": round(final_score, 4),
            "gnn_score": round(fraud_score, 4),
            "rule_boost": round(rule_boost, 4),
            "risk_level": self._classify_risk(final_score),
            "graph_info": {
                "node_count": self.graph.node_count,
                "edge_count": self.graph.edge_count,
                "account_neighbors": len(self.graph.get_node(account_id).neighbors) if account_id in self.graph.nodes else 0,
            },
            "processing_time_ms": round(processing_time, 3),
            "timestamp": datetime.now().isoformat(),
        }

        self.detection_history.append(result)
        return result

    def _classify_risk(self, score: float) -> str:
        if score >= 0.85:
            return "CRITICAL"
        elif score >= 0.65:
            return "HIGH"
        elif score >= 0.35:
            return "MEDIUM"
        return "LOW"

    def get_graph_stats(self) -> Dict:
        """获取图统计信息"""
        return {
            "node_count": self.graph.node_count,
            "edge_count": self.graph.edge_count,
            "total_detections": self.total_detections,
            "fraud_detections": self.fraud_detections,
            "fraud_rate": round(self.fraud_detections / max(self.total_detections, 1), 4),
            "account_profiles": len(self.account_profiles),
        }


# 全局实例
gnn_detection_service = GNNDetectionService()
