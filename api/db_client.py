"""
Doris数据库客户端 - 提供所有Doris查询方法
使用pymysql连接Doris(兼容MySQL协议)
"""
import pymysql
import re
import logging
import threading
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta as _td

from config import settings

logger = logging.getLogger(__name__)


def _time_to_str(val) -> str:
    """将 Doris TIME 类型（pymysql 返回 timedelta）转为 'HH:MM:SS' 字符串"""
    if val is None:
        return ""
    if isinstance(val, _td):
        total_seconds = int(val.total_seconds())
        h = total_seconds // 3600
        m = (total_seconds % 3600) // 60
        s = total_seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d}"
    return str(val)


class DorisClient:
    """Doris数据库查询客户端 - 线程安全，自动重连"""

    def __init__(self):
        self.host = settings.DORIS_HOST
        self.port = settings.DORIS_PORT
        self.user = settings.DORIS_USER
        self.password = settings.DORIS_PASSWORD
        self.database = settings.DORIS_DATABASE
        self._local = threading.local()

    def _get_connection(self):
        """获取线程本地连接，自动重连"""
        conn = getattr(self._local, 'connection', None)
        if conn is not None and conn.open:
            return conn
        try:
            conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=5,
                read_timeout=10,
                write_timeout=10,
                init_command="SET NAMES utf8mb4 COLLATE utf8mb4_general_ci",
                binary_prefix=True,
            )
            self._local.connection = conn
            return conn
        except Exception as e:
            logger.error(f"Doris连接失败: {e}")
            return None

    def execute(self, sql: str, params: tuple = None) -> List[Dict]:
        """执行查询SQL，返回结果列表，失败自动重试一次"""
        conn = self._get_connection()
        if not conn:
            return []
        try:
            with conn.cursor() as cursor:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                return cursor.fetchall()
        except Exception as e:
            logger.warning(f"Doris查询失败(重试): {e}")
            # 连接可能已断开，清除后重试一次
            try:
                conn.close()
            except:
                pass
            self._local.connection = None
            conn = self._get_connection()
            if not conn:
                return []
            try:
                with conn.cursor() as cursor:
                    if params:
                        cursor.execute(sql, params)
                    else:
                        cursor.execute(sql)
                    return cursor.fetchall()
            except Exception as e2:
                logger.error(f"Doris查询重试失败: {e2}")
                return []

    def execute_write(self, sql: str, params: tuple = None) -> bool:
        """执行写入SQL（INSERT/UPDATE/DELETE），返回是否成功"""
        conn = self._get_connection()
        if not conn:
            return False
        try:
            with conn.cursor() as cursor:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Doris写入失败: {e}\nSQL: {sql}")
            try:
                conn.rollback()
            except:
                pass
            self._local.connection = None
            return False

    def save_feedback(self, alert_data: Dict, feedback_type: str, comment: str = "") -> bool:
        """保存人工反馈到Doris
        feedback_type: 'confirmed' 或 'false_positive'
        当city/device_id为空或UNKNOWN时，从fraud_alert_result表补查
        """
        import datetime

        feedback_id = f"FB_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_{alert_data.get('alert_id', 'unknown')}"
        feedback_time = datetime.datetime.now().strftime("%H:%M:%S.%f")[:12]

        table_name = (
            "fraud_feedback_confirmed"
            if feedback_type == "confirmed"
            else "fraud_feedback_false_positive"
        )

        # 补查city/device_id：当alert_data中缺失时，从fraud_alert_result表重新查询
        city = alert_data.get("city")
        device_id = alert_data.get("device_id")
        if (not city or city == "UNKNOWN") or (not device_id or device_id == "UNKNOWN"):
            alert_id = alert_data.get("alert_id")
            if alert_id:
                fresh = self.get_alert_by_id(alert_id)
                if fresh:
                    if not city or city == "UNKNOWN":
                        city = fresh.get("city")
                    if not device_id or device_id == "UNKNOWN":
                        device_id = fresh.get("device_id")

        sql = (
            f"INSERT INTO {table_name} "
            "(feedback_id, alert_id, account_id, fraud_type, confidence, source, "
            "amount, city, device_id, alert_time, feedback_time, operator, comment) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )

        params = (
            feedback_id,
            alert_data.get("alert_id"),
            alert_data.get("account_id"),
            alert_data.get("fraud_type"),
            float(alert_data.get("confidence", 0)),
            alert_data.get("source", "unknown"),
            float(alert_data.get("amount", 0)),
            city,
            device_id,
            alert_data.get("alert_time"),
            feedback_time,
            "人工审核",
            comment,
        )

        return self.execute_write(sql, params)

    def get_feedback_data(self, feedback_type: str = "confirmed", limit: int = 1000) -> List[Dict]:
        """从Doris读取反馈数据用于重训练
        feedback_type: 'confirmed' 或 'false_positive'
        """
        table = (
            "fraud_feedback_confirmed"
            if feedback_type == "confirmed"
            else "fraud_feedback_false_positive"
        )
        sql = (
            f"SELECT alert_id, account_id, fraud_type, confidence, source, "
            f"amount, city, alert_time, feedback_time "
            f"FROM {table} ORDER BY feedback_time DESC LIMIT {limit}"
        )
        return self.execute(sql)

    def close(self):
        """关闭当前线程的连接"""
        conn = getattr(self._local, 'connection', None)
        if conn and conn.open:
            conn.close()
        self._local.connection = None

    def _parse_amount(self, raw) -> float:
        """解析金额字符串为数字。支持: '¥10K+', '¥67,316', '12345.67' 等格式"""
        if raw is None:
            return 0.0
        if isinstance(raw, (int, float)):
            return float(raw)
        s = str(raw).strip()
        # 去掉 ¥ 符号
        s = s.replace("¥", "").replace("￥", "").replace(",", "")
        # 处理 K/M 后缀
        multiplier = 1.0
        if s.upper().endswith("K+"):
            s = s[:-2]
            multiplier = 1000
        elif s.upper().endswith("K"):
            s = s[:-1]
            multiplier = 1000
        elif s.upper().endswith("M+"):
            s = s[:-2]
            multiplier = 1000000
        elif s.upper().endswith("M"):
            s = s[:-1]
            multiplier = 1000000
        try:
            return float(s) * multiplier
        except (ValueError, TypeError):
            return 0.0

    def _hour_filter(self, table: str, before_hour: Optional[int]) -> str:
        """构建时间小时过滤条件，使用 transaction_hour 字段过滤
        before_hour 为 None 时不加过滤条件，保持向后兼容
        """
        if before_hour is None:
            return ""
        return f" AND transaction_hour <= {before_hour}"

    def get_alert_total_count(self, before_hour: Optional[int] = None) -> int:
        """从 fraud_alert_result 表查询告警总数"""
        rows = self.execute(
            "SELECT COUNT(*) as cnt FROM fraud_alert_result "
            "WHERE source IS NOT NULL AND source != ''"
            + self._hour_filter("fraud_alert_result", before_hour)
        )
        return int(rows[0]["cnt"]) if rows else 0

    # ============================================================
    # KPI & Metrics
    # ============================================================

    def get_fraud_types(self) -> List[str]:
        """获取所有欺诈类型"""
        rows = self.execute("SELECT DISTINCT fraud_type FROM fraud_alert_result ORDER BY fraud_type")
        return [r["fraud_type"] for r in rows if r.get("fraud_type")]

    def get_detection_sources(self) -> List[str]:
        """获取所有检测来源"""
        rows = self.execute("SELECT DISTINCT source FROM fraud_alert_result ORDER BY source")
        return [r["source"] for r in rows if r.get("source")]

    def get_ml_anomaly_count(self) -> int:
        """获取ML集成检测到的异常数据数量"""
        rows = self.execute("SELECT COUNT(*) as cnt FROM fraud_alert_result WHERE source LIKE 'ML%'")
        return rows[0]["cnt"] if rows else 0

    def get_latest_metrics(self) -> Optional[Dict]:
        """获取最新评估指标"""
        rows = self.execute("SELECT * FROM fraud_evaluation_metrics ORDER BY metric_time DESC LIMIT 1")
        return rows[0] if rows else None

    def get_metrics_history(self, limit: int = 30) -> List[Dict]:
        """获取最近N条指标历史"""
        return self.execute(
            "SELECT * FROM fraud_evaluation_metrics ORDER BY metric_time DESC LIMIT %s",
            (limit,)
        )

    def get_validation_latest(self) -> Optional[Dict]:
        """获取最新ML验证结果(用于P/R/F1等)"""
        rows = self.execute("SELECT * FROM fraud_validation_result ORDER BY sample_id DESC LIMIT 1")
        return rows[0] if rows else None

    def get_total_amounts(self, before_hour: Optional[int] = None) -> Dict[str, float]:
        """从Doris查询真实累计金额: 正常交易总额、欺诈交易总额"""
        # 欺诈告警总金额 (amount 可能是字符串如 "¥10K+"，需在Python端解析)
        fraud_rows = self.execute(
            "SELECT amount FROM fraud_alert_result "
            "WHERE amount IS NOT NULL AND amount != ''"
            + self._hour_filter("fraud_alert_result", before_hour)
        )
        fraud_total = sum(self._parse_amount(r.get("amount")) for r in fraud_rows)

        # 正常交易总金额
        normal_rows = self.execute(
            "SELECT amount FROM fraud_normal_transaction "
            "WHERE amount IS NOT NULL AND amount != ''"
            + self._hour_filter("fraud_normal_transaction", before_hour)
        )
        normal_total = sum(self._parse_amount(r.get("amount")) for r in normal_rows)

        return {
            "fraud_total": round(fraud_total, 2),
            "normal_total": round(normal_total, 2),
            "grand_total": round(fraud_total + normal_total, 2),
            "total_fraud_amount": round(fraud_total, 2),
        }

    def get_rule_hit_counts(self) -> Dict[str, int]:
        """从Doris查询各规则命中次数 — 按fraud_type分组统计"""
        rows = self.execute(
            "SELECT fraud_type as name, COUNT(*) as hits "
            "FROM fraud_alert_result "
            "GROUP BY fraud_type"
        )
        return {r["name"]: int(r["hits"]) for r in rows}

    # ============================================================
    # Alert & Traffic Data
    # ============================================================

    def get_recent_alerts(self, limit: int = 200, before_hour: Optional[int] = None) -> List[Dict]:
        """获取最近告警"""
        rows = self.execute(
            "SELECT alert_id, account_id, fraud_type, confidence, source, "
            "amount, city, alert_time, risk_level, explanation_summary, "
            "transaction_hour "
            "FROM fraud_alert_result WHERE 1=1"
            + self._hour_filter("fraud_alert_result", before_hour)
            + " ORDER BY alert_time DESC"
        )
        # 格式化时间字符串和数值字段
        today = datetime.now().strftime("%Y-%m-%d")
        for r in rows:
            # 将 alert_time (timedelta from Doris TIME) 转换为字符串
            alert_time = _time_to_str(r.get("alert_time"))
            r["alert_time"] = alert_time
            if alert_time:
                r["timestamp"] = f"{today}T{alert_time}"
                r["alert_time_formatted"] = alert_time
            else:
                r["timestamp"] = ""
                r["alert_time_formatted"] = ""
            if r.get("confidence") is not None:
                r["confidence"] = float(r["confidence"])
            # 解析金额: Doris中可能是 "¥10K+" 或 "¥67,316" 等格式
            raw_amount = r.get("amount")
            if raw_amount is not None:
                r["amount"] = self._parse_amount(raw_amount)
            # 将 detection_source 映射（Doris 存的是 source，前端期望 detection_source）
            if r.get("source"):
                r["detection_source"] = r["source"]
        return rows

    def get_alert_by_id(self, alert_id: str) -> Optional[Dict]:
        """获取单个告警详情"""
        rows = self.execute(
            "SELECT alert_id, account_id, fraud_type, confidence, source, "
            "amount, city, alert_time, risk_level, details, "
            "explanation_summary, top_features, graph_path, behavior_path "
            "FROM fraud_alert_result WHERE alert_id = %s LIMIT 1",
            (alert_id,)
        )
        if not rows:
            return None
        r = rows[0]
        r["alert_time"] = _time_to_str(r.get("alert_time"))
        if r.get("confidence") is not None:
            r["confidence"] = float(r["confidence"])
        if r.get("amount") is not None:
            r["amount"] = self._parse_amount(r["amount"])
        return r

    def get_total_count(self) -> Dict[str, int]:
        """从Doris查询从0点到现在的累计交易总数（正常+欺诈）"""
        import datetime
        now = datetime.datetime.now()
        current_time_str = now.strftime('%H:%M:%S')

        # 正常交易总数（按tx_time过滤到当前时间）
        normal_rows = self.execute(
            f"SELECT COUNT(*) AS cnt FROM fraud_normal_transaction "
            f"WHERE tx_time IS NOT NULL AND tx_time <= '{current_time_str}'"
        )
        normal_count = int(normal_rows[0]["cnt"]) if normal_rows else 0

        # 欺诈告警总数（按alert_time过滤到当前时间）
        fraud_rows = self.execute(
            f"SELECT COUNT(*) AS cnt FROM fraud_alert_result "
            f"WHERE alert_time IS NOT NULL AND alert_time <= '{current_time_str}'"
        )
        fraud_count = int(fraud_rows[0]["cnt"]) if fraud_rows else 0

        return {
            "normal_count": normal_count,
            "fraud_count": fraud_count,
            "total_count": normal_count + fraud_count,
        }

    def get_traffic_history(self, seconds: int = 120, before_hour: Optional[int] = None) -> Dict[str, List]:
        """获取流量数据(正常 vs 欺诈) — 按秒聚合，显示系统当前时间前120秒"""
        import datetime

        # 系统当前时间的秒数（从0点开始）
        now = datetime.datetime.now()
        current_second = now.hour * 3600 + now.minute * 60 + now.second

        # 按秒聚合欺诈告警（从 alert_time 解析 HH:MM:SS）
        fraud_rows = self.execute(
            "SELECT "
            "  CAST(SUBSTR(alert_time, 1, 2) AS INT) * 3600 "
            "  + CAST(SUBSTR(alert_time, 4, 2) AS INT) * 60 "
            "  + CAST(SUBSTR(alert_time, 7, 2) AS INT) AS second_of_day, "
            "  COUNT(*) AS cnt "
            "FROM fraud_alert_result "
            "WHERE alert_time IS NOT NULL AND alert_time != '' "
            "GROUP BY "
            "  CAST(SUBSTR(alert_time, 1, 2) AS INT) * 3600 "
            "  + CAST(SUBSTR(alert_time, 4, 2) AS INT) * 60 "
            "  + CAST(SUBSTR(alert_time, 7, 2) AS INT) "
            "ORDER BY second_of_day"
        )

        # 按秒聚合正常交易（从 tx_time 解析 HH:MM:SS）
        normal_rows = self.execute(
            "SELECT "
            "  CAST(SUBSTR(tx_time, 1, 2) AS INT) * 3600 "
            "  + CAST(SUBSTR(tx_time, 4, 2) AS INT) * 60 "
            "  + CAST(SUBSTR(tx_time, 7, 2) AS INT) AS second_of_day, "
            "  COUNT(*) AS cnt "
            "FROM fraud_normal_transaction "
            "WHERE tx_time IS NOT NULL AND tx_time != '' "
            "GROUP BY "
            "  CAST(SUBSTR(tx_time, 1, 2) AS INT) * 3600 "
            "  + CAST(SUBSTR(tx_time, 4, 2) AS INT) * 60 "
            "  + CAST(SUBSTR(tx_time, 7, 2) AS INT) "
            "ORDER BY second_of_day"
        )

        fraud_buckets = {r["second_of_day"]: r["cnt"] for r in fraud_rows}
        normal_buckets = {r["second_of_day"]: r["cnt"] for r in normal_rows}

        # 生成最近120秒的时间序列
        timestamps = []
        normal_counts = []
        fraud_counts = []

        for i in range(119, -1, -1):
            second = (current_second - i) % (24 * 3600)
            h = second // 3600
            m = (second % 3600) // 60
            s = second % 60
            timestamps.append(f"{h:02d}:{m:02d}:{s:02d}")
            normal_counts.append(normal_buckets.get(second, 0))
            fraud_counts.append(fraud_buckets.get(second, 0))

        return {
            "timestamps": timestamps,
            "normal": normal_counts,
            "fraud": fraud_counts,
        }

    # ============================================================
    # Distribution Data
    # ============================================================

    def get_fraud_type_distribution(self, before_hour: Optional[int] = None) -> List[Dict]:
        """告警类型分布 — 合并同类项，避免类型过多"""
        return self.execute(
            "SELECT "
            "  CASE "
            "    WHEN fraud_type LIKE '在线模型识别%%' THEN '在线模型识别' "
            "    WHEN fraud_type LIKE '集成模型%%' THEN '集成模型未知异常' "
            "    WHEN fraud_type LIKE '路径频率%%' THEN '路径频率模型识别' "
            "    WHEN fraud_type LIKE '高置信%%' THEN '高置信集成模型欺诈' "
            "    WHEN fraud_type LIKE '图模型%%' THEN '图模型多目标分散' "
            "    WHEN fraud_type LIKE '时序模型%%' THEN '时序模型夜间不规律' "
            "    WHEN fraud_type LIKE '统计画像%%' THEN '统计画像偏离欺诈' "
            "    ELSE fraud_type "
            "  END as name, "
            "  COUNT(*) as value "
            "FROM fraud_alert_result WHERE 1=1"
            + self._hour_filter("fraud_alert_result", before_hour)
            + " GROUP BY name ORDER BY value DESC"
        )

    def get_source_distribution(self, before_hour: Optional[int] = None) -> List[Dict]:
        """检测来源分布 — 按原始source归类，真实展示Doris数据"""
        rows = self.execute(
            "SELECT source, COUNT(*) AS value FROM fraud_alert_result "
            "WHERE source IS NOT NULL AND source != ''"
            + self._hour_filter("fraud_alert_result", before_hour)
            + " GROUP BY source ORDER BY value DESC"
        )
        classified = {}
        for r in rows:
            source_raw = r.get("source", "")
            source = str(source_raw).strip()
            value = r.get("value", 0)
            if source.startswith("CEP") or source.startswith("C"):
                name = "CEP规则"
            elif source.startswith("SQL") or source.startswith("S"):
                name = "SQL引擎"
            elif source.startswith("GRAPH") or source.startswith("GNA"):
                name = "图分析"
            elif source.startswith("GNN") or source.startswith("GN"):
                name = "GNN检测"
            elif source.startswith("ML") or source.startswith("M"):
                name = "ML集成"
            else:
                name = "其他"
            classified[name] = classified.get(name, 0) + value
        
        return [{"name": k, "value": v} for k, v in sorted(classified.items(), key=lambda x: -x[1])]

    def get_confidence_distribution(self, before_hour: Optional[int] = None) -> List[Dict]:
        """置信度分桶分布"""
        return self.execute(
            "SELECT "
            "  CASE WHEN confidence >= 0.9 THEN '高危(>90%)' "
            "       WHEN confidence >= 0.7 THEN '中危(70-90%)' "
            "       ELSE '低危(<70%)' END as name, "
            "  COUNT(*) as value "
            "FROM fraud_alert_result WHERE 1=1"
            + self._hour_filter("fraud_alert_result", before_hour)
            + " GROUP BY name ORDER BY value DESC"
        )

    def get_geo_distribution(self, before_hour: Optional[int] = None) -> List[Dict]:
        """地域分布"""
        return self.execute(
            "SELECT COALESCE(city, '未知') as name, COUNT(*) as value "
            "FROM fraud_alert_result WHERE 1=1"
            + self._hour_filter("fraud_alert_result", before_hour)
            + " GROUP BY name ORDER BY value DESC"
        )

    def get_amount_distribution(self) -> List[Dict]:
        """金额区间分布"""
        return self.execute(
            "SELECT "
            "  CASE WHEN amount >= 100000 THEN '10万以上' "
            "       WHEN amount >= 50000 THEN '5-10万' "
            "       WHEN amount >= 10000 THEN '1-5万' "
            "       WHEN amount >= 1000 THEN '1千-1万' "
            "       ELSE '1千以下' END as name, "
            "  COUNT(*) as value "
            "FROM fraud_alert_result "
            "GROUP BY name ORDER BY value DESC"
        )

    # ============================================================
    # Model Drift Data
    # ============================================================

    def get_drift_history(self, limit: int = 30) -> List[Dict]:
        """获取模型漂移历史(基于F1变化)"""
        return self.execute(
            "SELECT sample_id, f1_score, precision, recall, accuracy, "
            "total_samples, fraud_samples "
            "FROM fraud_validation_result ORDER BY sample_id DESC LIMIT %s",
            (limit,)
        )

    def get_drift_events(self, limit: int = 30) -> List[Dict]:
        """从fraud_drift_event表获取概念漂移事件"""
        rows = self.execute(
            "SELECT event_id, severity, drift_score, event_timestamp, "
            "sample_count, details, dt "
            "FROM fraud_drift_event ORDER BY event_timestamp DESC LIMIT %s",
            (limit,)
        )
        for r in rows:
            if r.get("drift_score") is not None:
                r["drift_score"] = float(r["drift_score"])
            if r.get("sample_count") is not None:
                r["sample_count"] = int(r["sample_count"])
        return rows

    def get_feedback_stats(self, limit: int = 30) -> List[Dict]:
        """从fraud_feedback_confirmed和fraud_feedback_false_positive表聚合反馈统计"""
        # 查询确认欺诈数量
        confirmed_rows = self.execute(
            "SELECT COUNT(*) as cnt FROM fraud_feedback_confirmed"
        )
        confirmed_count = int(confirmed_rows[0]["cnt"]) if confirmed_rows else 0

        # 查询误报数量
        fp_rows = self.execute(
            "SELECT COUNT(*) as cnt FROM fraud_feedback_false_positive"
        )
        fp_count = int(fp_rows[0]["cnt"]) if fp_rows else 0

        total = confirmed_count + fp_count
        fpr = (fp_count / total) if total > 0 else 0.0

        # 返回聚合结果（单条记录代表当前快照）
        return [{
            "stat_id": "AGGREGATE_SNAPSHOT",
            "type": "STATS",
            "total_feedback": total,
            "confirmed_fraud": confirmed_count,
            "false_positive": fp_count,
            "incorrect_type": 0,
            "false_positive_rate": fpr,
            "report_time": int(datetime.now().timestamp() * 1000),
            "dt": datetime.now().strftime("%Y%m%d"),
        }]

    def get_model_status(self) -> Dict:
        """获取模型迭代状态：模型文件信息 + 训练历史 + 最新指标"""
        import os

        result = {
            "model_file": None,
            "training_history": [],
            "latest_metrics": None,
            "iteration_count": 0
        }

        # 1. 模型文件信息
        model_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "models", "fraud_model.ser")
        if os.path.exists(model_path):
            stat = os.stat(model_path)
            result["model_file"] = {
                "path": "models/fraud_model.ser",
                "size_kb": round(stat.st_size / 1024, 1),
                "last_modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                "exists": True
            }
        else:
            result["model_file"] = {"exists": False}

        # 2. 训练历史（F1随训练样本数变化）— 取所有数据点
        history = self.execute(
            "SELECT total_samples, f1_score, precision, recall, accuracy, "
            "fraud_samples, normal_samples "
            "FROM fraud_validation_result "
            "ORDER BY total_samples ASC LIMIT 100"
        )
        if history:
            result["training_history"] = [
                {
                    "samples": r.get("total_samples", 0),
                    "f1": r.get("f1_score", 0),
                    "precision": r.get("precision", 0),
                    "recall": r.get("recall", 0),
                    "accuracy": r.get("accuracy", 0),
                    "fraud_samples": r.get("fraud_samples", 0),
                    "normal_samples": r.get("normal_samples", 0)
                }
                for r in history
            ]
            result["iteration_count"] = len(history)

        # 3. 最新指标
        latest = self.execute(
            "SELECT f1_score, precision, recall, accuracy, "
            "total_samples, fraud_samples, normal_samples "
            "FROM fraud_validation_result ORDER BY sample_id DESC LIMIT 1"
        )
        if latest:
            result["latest_metrics"] = latest[0]

        return result


# 全局实例
doris_client = DorisClient()
