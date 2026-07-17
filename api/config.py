"""
配置文件 - 欺诈检测系统REST API
包含数据库连接、模型路径、检测阈值、服务器设置、JWT认证等
"""
import os
import secrets
from typing import List, Optional

# 加载.env环境变量
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
except ImportError:
    pass


class Settings:
    """系统配置类 - 支持环境变量覆盖"""

    # ========== 服务器设置 ==========
    # 服务监听地址和端口
    HOST: str = os.getenv("API_HOST", "0.0.0.0")
    PORT: int = int(os.getenv("API_PORT", "8000"))
    DEBUG: bool = os.getenv("API_DEBUG", "false").lower() == "true"

    # ========== 数据库配置(Doris/MySQL) ==========
    # Doris连接信息(用于查询告警历史和统计数据)
    DORIS_HOST: str = os.getenv("DORIS_HOST", "192.168.10.10")
    DORIS_PORT: int = int(os.getenv("DORIS_PORT", "9030"))
    DORIS_USER: str = os.getenv("DORIS_USER", "root")
    DORIS_PASSWORD: str = os.getenv("DORIS_PASSWORD", "123456")
    DORIS_DATABASE: str = os.getenv("DORIS_DATABASE", "final")

    # MySQL连接信息(备用数据库)
    MYSQL_HOST: str = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_PORT: int = int(os.getenv("MYSQL_PORT", "3306"))
    MYSQL_USER: str = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD: str = os.getenv("MYSQL_PASSWORD", "")
    MYSQL_DATABASE: str = os.getenv("MYSQL_DATABASE", "fraud_detection")

    # 数据库连接池设置
    DB_POOL_SIZE: int = int(os.getenv("DB_POOL_SIZE", "5"))
    DB_MAX_OVERFLOW: int = int(os.getenv("DB_MAX_OVERFLOW", "10"))

    # ========== 模型文件路径 ==========
    # 预训练模型权重文件(JSON格式)
    MODEL_WEIGHTS_DIR: str = os.getenv("MODEL_WEIGHTS_DIR", "./models")
    LOGISTIC_REGRESSION_WEIGHTS: str = os.path.join(
        MODEL_WEIGHTS_DIR, "logistic_regression_weights.json"
    )
    GBDT_WEIGHTS: str = os.path.join(MODEL_WEIGHTS_DIR, "gbdt_weights.json")
    ENSEMBLE_WEIGHTS: str = os.path.join(MODEL_WEIGHTS_DIR, "ensemble_weights.json")

    # 性能评估数据路径
    PERFORMANCE_SUMMARY_FILE: str = os.getenv(
        "PERFORMANCE_SUMMARY_FILE", "./performance_summary.json"
    )
    EVALUATION_METRICS_FILE: str = os.getenv(
        "EVALUATION_METRICS_FILE", "./evaluation_metrics.json"
    )

    # ========== 检测阈值配置 ==========
    # 默认欺诈检测阈值
    DEFAULT_FRAUD_THRESHOLD: float = float(os.getenv("DEFAULT_FRAUD_THRESHOLD", "0.35"))

    # 各层检测权重(与Java端MLAnomalyDetector保持一致)
    CEP_CONFIDENCE_THRESHOLDS: dict = {
        "小额试探大额转出": 0.85,
        "多层链式洗钱": 0.90,
        "异地跨设备突发大额": 0.85,
        "分散转入集中提现": 0.80,
        "多渠道轮番转账": 0.80,
        "凌晨分批掏空": 0.80,
        "小额掩护大额跑路": 0.85,
        "团伙同IP批量作案": 0.85,
    }

    # 行为评分阈值
    BEHAVIOR_SCORE_THRESHOLD: float = float(os.getenv("BEHAVIOR_SCORE_THRESHOLD", "0.30"))

    # 高风险等级映射
    RISK_LEVEL_THRESHOLDS: dict = {
        "LOW": 0.0,       # 低风险: 0.0 - 0.35
        "MEDIUM": 0.35,   # 中风险: 0.35 - 0.65
        "HIGH": 0.65,     # 高风险: 0.65 - 0.85
        "CRITICAL": 0.85, # 极高风险: > 0.85
    }

    # ========== 限流配置 ==========
    # 每分钟最大请求数
    RATE_LIMIT_PER_MINUTE: int = int(os.getenv("RATE_LIMIT_PER_MINUTE", "100"))
    # 批量检测最大交易数
    MAX_BATCH_SIZE: int = int(os.getenv("MAX_BATCH_SIZE", "100"))

    # ========== CORS配置 ==========
    # 允许的跨域来源
    CORS_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:5500",
        "http://localhost:8080",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:5500",
        "http://127.0.0.1:8080",
        "http://localhost:8002",
        "http://127.0.0.1:8002",
    ]

    # ========== 日志配置 ==========
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT: str = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"

    # ========== JWT认证配置 ==========
    SECRET_KEY: str = os.getenv("JWT_SECRET_KEY", secrets.token_hex(32))
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "120"))

    # 演示用硬编码凭据
    DEMO_USERNAME: str = os.getenv("DEMO_USERNAME", "admin")
    DEMO_PASSWORD: str = os.getenv("DEMO_PASSWORD", "admin123")


# 全局配置实例
settings = Settings()
