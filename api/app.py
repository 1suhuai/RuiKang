"""
FastAPI主应用 - 欺诈检测系统REST API v2.0
提供交易检测、告警查询、人工反馈、性能指标、规则管理、漂移监控、GNN检测等接口
"""
import time
import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict

from fastapi import FastAPI, HTTPException, Query, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from config import settings
from auth import (
    create_token, verify_token, require_auth, optional_auth,
    verify_password, encrypt_password,
    sanitize_alert, sanitize_alert_list,
)
from models import (
    TransactionRequest,
    BatchDetectRequest,
    DetectResponse,
    BatchDetectResponse,
    AlertResponse,
    AlertListResponse,
    FeedbackRequest,
    FeedbackResponse,
    MetricsResponse,
    LatencyResponse,
    RuleResponse,
    RuleUpdateRequest,
    RuleUpdateResponse,
    ModelInfoResponse,
    RetrainRequest,
    RetrainResponse,
    HealthResponse,
    GnnDetectRequest,
)
from services import (
    fraud_detection_service,
    alert_service,
    metrics_service,
    rule_service,
    doris_data_service,
    category_service,
)
from drift_monitor import drift_monitor_service
from gnn_service import gnn_detection_service
from retrain_service import retrain_service, _model_state

# ============================================================
# 日志配置
# ============================================================
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL, logging.INFO),
    format=settings.LOG_FORMAT,
)
logger = logging.getLogger("fraud_detection_api")

# ============================================================
# FastAPI应用初始化
# ============================================================
app = FastAPI(
    title="金融反诈智能守护系统 REST API v2.0",
    description="""
## 系统简介

基于 Flink + CEP/SQL/Graph/ML/GNN 五层架构的实时金融反欺诈检测系统 REST 接口。

### 核心功能
- **实时检测**: 单笔/批量交易欺诈检测, 返回欺诈概率、风险等级、可解释性说明
- **告警管理**: 告警历史查询、详情查看、人工反馈提交
- **性能监控**: 系统延迟、吞吐量、模型评估指标
- **规则管理**: CEP检测规则查看和阈值调整
- **模型管理**: 模型版本查询、重训练触发
- **漂移监控**: 概念漂移检测、自动重训练触发
- **GNN检测**: 图神经网络异常检测

### 架构说明
- CEP规则引擎: 8种已知欺诈模式匹配
- SQL关联检测: 跨账户关联分析
- 图分析引擎: 资金流向图谱异常检测
- ML集成模型: LightGBM + GBDT + Logistic Regression + Isolation Forest + Behavior Scoring
- GNN图神经网络: GraphSAGE异常节点检测
    """,
    version="2.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# ============================================================
# 中间件配置
# ============================================================

# CORS配置 - 支持Dashboard集成
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 安全头中间件 + JWT鉴权中间件
PUBLIC_PATHS = {"/", "/api/v1/auth/login", "/api/v1/health", "/api/docs", "/api/redoc", "/api/openapi.json",
    "/api/v1/doris/", "/api/v1/metrics/", "/api/v1/drift/", "/api/v1/rules", "/api/v1/models",
    "/api/v1/feedback", "/api/v1/experiments", "/api/v1/describe/", "/api/v1/categories"}

@app.middleware("http")
async def security_and_auth_middleware(request: Request, call_next):
    # 安全头
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    response.headers["Pragma"] = "no-cache"

    # JWT鉴权：非公开端点需要验证token
    path = request.url.path
    if not any(path.startswith(p) for p in PUBLIC_PATHS):
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return JSONResponse(status_code=401, content={"error": True, "detail": "未登录，请先登录"})
        token = auth_header[7:]
        payload = verify_token(token)
        if payload is None:
            return JSONResponse(status_code=401, content={"error": True, "detail": "Token无效或已过期，请重新登录"})

    return response

# 限流器
limiter = Limiter(key_func=get_remote_address, default_limits=[f"{settings.RATE_LIMIT_PER_MINUTE}/minute"])
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# 请求日志中间件
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.monotonic()
    response = await call_next(request)
    duration = (time.monotonic() - start) * 1000
    logger.info(f"{request.method} {request.url.path} -> {response.status_code} ({duration:.1f}ms)")
    return response

# ============================================================
# 全局异常处理
# ============================================================
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "status_code": exc.status_code,
            "detail": exc.detail,
            "timestamp": datetime.now().isoformat(),
        },
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(f"未处理异常: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": True,
            "status_code": 500,
            "detail": "内部服务错误",
            "timestamp": datetime.now().isoformat(),
        },
    )

# ============================================================
# 启动/关闭事件
# ============================================================
_start_time = time.time()

@app.on_event("startup")
async def startup_event():
    logger.info("=" * 60)
    logger.info("金融反诈智能守护系统 REST API v2.0 启动中...")
    logger.info(f"服务地址: http://{settings.HOST}:{settings.PORT}")
    logger.info(f"检测阈值: {settings.DEFAULT_FRAUD_THRESHOLD}")
    logger.info(f"限流配置: {settings.RATE_LIMIT_PER_MINUTE} 请求/分钟")
    logger.info(f"JWT鉴权: 已启用")
    logger.info(f"数据脱敏: 已启用")
    logger.info(f"安全头: 已启用")
    logger.info(f"漂移监控: {'已启用' if drift_monitor_service else '未启用'}")
    logger.info(f"GNN检测: {'已启用' if gnn_detection_service else '未启用'}")
    logger.info("=" * 60)

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("金融反诈智能守护系统 REST API 关闭")

# ============================================================
# 认证接口
# ============================================================

# 预计算密码哈希（每次调用 encrypt_password 会生成不同盐，所以必须预计算）
_DEMO_USERS = {
    "admin": encrypt_password("admin123"),
    "analyst": encrypt_password("analyst123"),
}

@app.post("/api/v1/auth/login", summary="用户登录获取Token", tags=["认证"])
async def login(username: str = Body(...), password: str = Body(...)):
    """
    用户登录，返回JWT Token。
    演示模式：用户名 admin，密码 admin123
    """
    stored = _DEMO_USERS.get(username)
    if not stored or not verify_password(password, stored):
        raise HTTPException(status_code=401, detail="用户名或密码错误")
    
    role = "admin" if username == "admin" else "analyst"
    token = create_token(user_id=username, role=role)
    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_in": 86400,
        "user": {"id": username, "role": role},
    }

@app.get("/api/v1/auth/verify", summary="验证Token", tags=["认证"])
async def verify_auth(request: Request):
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return {"valid": False, "message": "缺少有效token"}
    payload = verify_token(auth_header[7:])
    if payload:
        return {"valid": True, "user": payload}
    return {"valid": False, "message": "Token无效或已过期"}


# ============================================================
# 健康检查
# ============================================================
@app.get(
    "/api/v1/health",
    response_model=HealthResponse,
    summary="健康检查",
    tags=["系统"],
)
async def health_check():
    checks = {
        "fraud_detection_service": "healthy" if fraud_detection_service else "unhealthy",
        "alert_service": "healthy" if alert_service else "unhealthy",
        "metrics_service": "healthy" if metrics_service else "unhealthy",
        "rule_service": "healthy" if rule_service else "unhealthy",
        "drift_monitor": "healthy" if drift_monitor_service else "unhealthy",
        "gnn_service": "healthy" if gnn_detection_service else "unhealthy",
    }
    all_healthy = all(v == "healthy" for v in checks.values())
    return HealthResponse(
        status="healthy" if all_healthy else "degraded",
        version="2.0.0",
        timestamp=datetime.now().isoformat(),
        uptime_seconds=round(time.time() - _start_time, 1),
        checks=checks,
    )


# ============================================================
# 交易检测接口
# ============================================================
@app.post(
    "/api/v1/detect",
    response_model=DetectResponse,
    summary="单笔欺诈检测",
    tags=["检测"],
)
@limiter.limit(f"{settings.RATE_LIMIT_PER_MINUTE}/minute")
async def detect_fraud(request: Request, tx: TransactionRequest):
    tx_dict = tx.model_dump(exclude_none=True)
    if tx_dict.get("transactionHour") is None and tx_dict.get("step") is not None:
        tx_dict["transactionHour"] = tx_dict["step"] % 24

    result = fraud_detection_service.detect(tx_dict)

    if result["is_fraud"]:
        alert_id = alert_service.add_alert(result)
        result["_alert_id"] = alert_id

    # 更新漂移监控
    if drift_monitor_service:
        drift_monitor_service.record_prediction(tx_dict, result)

    return DetectResponse(**result)


@app.post(
    "/api/v1/detect/batch",
    response_model=BatchDetectResponse,
    summary="批量欺诈检测",
    tags=["检测"],
)
@limiter.limit(f"{settings.RATE_LIMIT_PER_MINUTE}/minute")
async def batch_detect_fraud(request: Request, batch: BatchDetectRequest):
    start_time = time.monotonic()
    results = []
    fraud_count = 0

    for tx in batch.transactions:
        tx_dict = tx.model_dump(exclude_none=True)
        if tx_dict.get("transactionHour") is None and tx_dict.get("step") is not None:
            tx_dict["transactionHour"] = tx_dict["step"] % 24

        result = fraud_detection_service.detect(tx_dict)

        if result["is_fraud"]:
            alert_service.add_alert(result)
            fraud_count += 1

        results.append(DetectResponse(**result))

    processing_time = (time.monotonic() - start_time) * 1000

    return BatchDetectResponse(
        request_id=f"BATCH_{int(time.time()*1000)}",
        total_transactions=len(batch.transactions),
        fraud_count=fraud_count,
        normal_count=len(batch.transactions) - fraud_count,
        processing_time_ms=round(processing_time, 3),
        results=results,
    )


# ============================================================
# GNN检测接口
# ============================================================
@app.post(
    "/api/v1/gnn/detect",
    summary="GNN图神经网络检测",
    tags=["GNN"],
)
async def gnn_detect(request: GnnDetectRequest = None):
    if not gnn_detection_service:
        raise HTTPException(status_code=503, detail="GNN服务未启用")

    result = gnn_detection_service.detect(request.dict() if request else {})
    return result


# ============================================================
# 告警查询接口
# ============================================================
@app.get(
    "/api/v1/alerts",
    response_model=AlertListResponse,
    summary="查询告警列表",
    tags=["告警"],
)
async def query_alerts(
    account_id: Optional[str] = Query(None, description="账户ID"),
    fraud_type: Optional[str] = Query(None, description="欺诈类型"),
    start_date: Optional[str] = Query(None, description="开始日期"),
    end_date: Optional[str] = Query(None, description="结束日期"),
    min_confidence: Optional[float] = Query(None, ge=0, le=1),
    max_confidence: Optional[float] = Query(None, ge=0, le=1),
    source: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    result = alert_service.query_alerts(
        account_id=account_id, fraud_type=fraud_type,
        start_date=start_date, end_date=end_date,
        min_confidence=min_confidence, max_confidence=max_confidence,
        source=source, page=page, page_size=page_size,
    )
    # 数据脱敏
    if "alerts" in result and result["alerts"]:
        result["alerts"] = sanitize_alert_list(result["alerts"])
    return AlertListResponse(**result)


@app.get(
    "/api/v1/alerts/{alert_id}",
    response_model=AlertResponse,
    summary="获取告警详情",
    tags=["告警"],
)
async def get_alert(alert_id: str):
    alert = alert_service.get_alert(alert_id)
    if not alert:
        raise HTTPException(status_code=404, detail=f"告警 {alert_id} 不存在")
    # 数据脱敏
    return AlertResponse(**sanitize_alert(alert))


# ============================================================
# 人工反馈接口
# ============================================================
@app.post(
    "/api/v1/feedback",
    response_model=FeedbackResponse,
    summary="提交人工反馈",
    tags=["反馈"],
)
async def submit_feedback(feedback: FeedbackRequest):
    result = alert_service.submit_feedback(
        alert_id=feedback.alert_id,
        feedback=feedback.feedback,
        corrected_type=feedback.corrected_fraud_type,
        confidence_adj=feedback.confidence_adjustment,
        comment=feedback.comment,
    )

    if not result["success"]:
        raise HTTPException(status_code=404, detail=result["message"])

    # 触发漂移检查
    if drift_monitor_service:
        drift_monitor_service.check_drift()

    return FeedbackResponse(**result)


# ============================================================
# 性能指标接口
# ============================================================
@app.get(
    "/api/v1/metrics",
    summary="系统性能指标",
    tags=["指标"],
)
async def get_metrics():
    data = metrics_service.get_metrics()
    # 添加基础指标用于前端KPI
    stats = fraud_detection_service.stats
    api_latency = fraud_detection_service.get_api_latency_stats()
    data["total_processed"] = stats["total_transactions"]
    data["total_fraud"] = stats["total_fraud_detected"]
    data["avg_latency_ms"] = round(api_latency.get("avg_ms", 70))
    return data


@app.get(
    "/api/v1/metrics/latency",
    response_model=LatencyResponse,
    summary="延迟详细分解",
    tags=["指标"],
)
async def get_latency():
    data = metrics_service.get_latency_breakdown()
    return LatencyResponse(**data)


# ============================================================
# 规则管理接口
# ============================================================
@app.get(
    "/api/v1/rules",
    response_model=list[RuleResponse],
    summary="列出检测规则",
    tags=["规则"],
)
async def list_rules():
    rules = rule_service.list_rules()
    return [RuleResponse(**r) for r in rules]


@app.put(
    "/api/v1/rules/{rule_id}",
    response_model=RuleUpdateResponse,
    summary="更新规则阈值",
    tags=["规则"],
)
async def update_rule(rule_id: str, updates: RuleUpdateRequest):
    update_dict = updates.model_dump(exclude_none=True)
    if not update_dict:
        raise HTTPException(status_code=400, detail="至少需要一个更新字段")

    result = rule_service.update_rule(rule_id, update_dict)
    if not result["success"]:
        raise HTTPException(status_code=404, detail=result["message"])

    return RuleUpdateResponse(**result)


# ============================================================
# 检测分类开关接口
# ============================================================
@app.get(
    "/api/v1/categories",
    summary="获取检测分类开关状态",
    tags=["规则"],
)
async def get_categories():
    return category_service.get_categories()


@app.put(
    "/api/v1/categories/{name}",
    summary="切换检测分类开关",
    tags=["规则"],
)
async def toggle_category(name: str, body: Dict = Body(...)):
    enabled = body.get("enabled", True)
    result = category_service.toggle_category(name, enabled)
    if not result["success"]:
        raise HTTPException(status_code=404, detail=result["message"])
    return result

@app.get("/api/v1/ml-anomaly-count", summary="ML异常数量", tags=["规则"])
async def ml_anomaly_count():
    return {"count": doris_data_service.get_ml_anomaly_count()}


# ============================================================
# 模型管理接口
# ============================================================
@app.get(
    "/api/v1/models",
    summary="模型版本和状态",
    tags=["模型"],
)
async def get_models():
    models_list = [
        {"name": "LightGBM", "version": "v2.1.0", "status": "active", "weight": 0.25},
        {"name": "GBDT", "version": "v1.8.3", "status": "active", "weight": 0.25},
        {"name": "Logistic Regression", "version": "v1.5.2", "status": "active", "weight": 0.15},
        {"name": "Isolation Forest", "version": "v1.2.0", "status": "active", "weight": 0.10},
        {"name": "Behavior Scoring", "version": "v2.0.1", "status": "active", "weight": 0.15},
        {"name": "GNN GraphSAGE", "version": "v1.0.0", "status": "active", "weight": 0.10},
    ]

    return {
        "models": models_list,
        "ensemble_weights": fraud_detection_service._ensemble_weights,
        "current_threshold": settings.DEFAULT_FRAUD_THRESHOLD,
        "ensemble_strategy": "dynamic_weighted",
    }


@app.post(
    "/api/v1/models/retrain",
    summary="触发模型重训练",
    tags=["模型"],
)
async def trigger_retrain(request: RetrainRequest):
    result = retrain_service.trigger_retrain(reason=request.reason or "manual")
    return RetrainResponse(**result)


@app.get(
    "/api/v1/models/info",
    summary="当前模型信息",
    tags=["模型"],
)
async def get_model_info():
    return retrain_service.get_model_info()


@app.get(
    "/api/v1/models/history",
    summary="重训练历史",
    tags=["模型"],
)
async def get_retrain_history():
    return {"history": retrain_service.get_retrain_history()}


@app.get(
    "/api/v1/models/retrain/progress",
    summary="重训练进度",
    tags=["模型"],
)
async def get_retrain_progress():
    """获取当前重训练进度，返回步骤、百分比和详情"""
    progress = retrain_service.get_progress()
    status = "idle"
    if progress:
        status = "training"
    elif _model_state.get("status") == "error":
        status = "error"
    return {
        "status": status,
        "progress": progress,
        "model_info": retrain_service.get_model_info(),
    }


# ============================================================
# 漂移监控接口
# ============================================================
@app.get(
    "/api/v1/drift/status",
    summary="漂移检测状态",
    tags=["漂移"],
)
async def get_drift_status():
    if not drift_monitor_service:
        return {"status": "unavailable", "message": "漂移监控服务未启用"}
    return drift_monitor_service.get_status()


@app.get(
    "/api/v1/drift/history",
    summary="漂移检测历史",
    tags=["漂移"],
)
async def get_drift_history():
    if not drift_monitor_service:
        return {"history": []}
    return drift_monitor_service.get_history()


# ============================================================
# Doris真实数据接口
# ============================================================
@app.get("/api/v1/doris/metrics", summary="Doris KPI指标", tags=["Doris"])
async def doris_metrics(before_hour: Optional[int] = Query(None, ge=0, le=23, description="模拟当前小时(0-23)，只返回该小时之前的数据")):
    return doris_data_service.get_kpi_metrics(before_hour)

@app.get("/api/v1/doris/total-count", summary="从0点到现在的累计交易总数", tags=["Doris"])
async def doris_total_count():
    return doris_data_service.get_total_count()

@app.get("/api/v1/doris/traffic", summary="Doris流量趋势", tags=["Doris"])
async def doris_traffic(
    seconds: int = Query(60, ge=1, le=3600),
    before_hour: Optional[int] = Query(None, ge=0, le=23, description="模拟当前小时(0-23)，只返回该小时之前的数据"),
):
    return doris_data_service.get_traffic_history(seconds, before_hour)

@app.get("/api/v1/doris/fraud-types", summary="Doris欺诈类型列表", tags=["Doris"])
async def doris_fraud_types():
    return doris_data_service.get_fraud_types()

@app.get("/api/v1/doris/detection-sources", summary="Doris检测来源列表", tags=["Doris"])
async def doris_detection_sources():
    return doris_data_service.get_detection_sources()

@app.get("/api/v1/doris/alerts", summary="Doris最近告警", tags=["Doris"])
async def doris_alerts(
    limit: int = Query(200, ge=1, le=10000),
    before_hour: Optional[int] = Query(None, ge=0, le=23, description="模拟当前小时(0-23)，只返回该小时之前的数据"),
):
    alerts = doris_data_service.get_recent_alerts(limit, before_hour)
    # 数据脱敏
    if isinstance(alerts, list):
        alerts = sanitize_alert_list(alerts)
    return alerts

@app.get("/api/v1/doris/alerts/{alert_id}", summary="Doris告警详情", tags=["Doris"])
async def doris_alert_detail(alert_id: str):
    alert = doris_data_service.get_alert_detail(alert_id)
    if not alert:
        raise HTTPException(status_code=404, detail=f"告警 {alert_id} 不存在")
    # 数据脱敏
    return sanitize_alert(alert)

@app.get("/api/v1/doris/fraud-types", summary="告警类型分布", tags=["Doris"])
async def doris_fraud_types(before_hour: Optional[int] = Query(None, ge=0, le=23, description="模拟当前小时(0-23)，只返回该小时之前的数据")):
    return doris_data_service.get_fraud_type_distribution(before_hour)

@app.get("/api/v1/doris/sources", summary="检测来源分布", tags=["Doris"])
async def doris_sources(before_hour: Optional[int] = Query(None, ge=0, le=23, description="模拟当前小时(0-23)，只返回该小时之前的数据")):
    return doris_data_service.get_source_distribution(before_hour)

@app.get("/api/v1/doris/confidence", summary="置信度分布", tags=["Doris"])
async def doris_confidence(before_hour: Optional[int] = Query(None, ge=0, le=23, description="模拟当前小时(0-23)，只返回该小时之前的数据")):
    return doris_data_service.get_confidence_distribution(before_hour)

@app.get("/api/v1/doris/geo", summary="地域分布", tags=["Doris"])
async def doris_geo(before_hour: Optional[int] = Query(None, ge=0, le=23, description="模拟当前小时(0-23)，只返回该小时之前的数据")):
    return doris_data_service.get_geo_distribution(before_hour)

@app.get("/api/v1/doris/amounts", summary="金额分布", tags=["Doris"])
async def doris_amounts():
    return doris_data_service.get_amount_distribution()

@app.get("/api/v1/doris/drift", summary="模型漂移历史", tags=["Doris"])
async def doris_drift(limit: int = Query(30, ge=1, le=100)):
    return doris_data_service.get_drift_history(limit)

@app.get("/api/v1/doris/drift-events", summary="概念漂移事件", tags=["Doris"])
async def doris_drift_events(limit: int = Query(30, ge=1, le=100)):
    """从fraud_drift_event表获取ADWIN+PH+KS+PSI四算法检测到的漂移事件"""
    return doris_data_service.get_drift_events(limit)

@app.get("/api/v1/doris/feedback-stats", summary="反馈闭环统计", tags=["Doris"])
async def doris_feedback_stats(limit: int = Query(30, ge=1, le=100)):
    """从fraud_feedback_stats表获取Human-in-the-Loop反馈统计和重训练信号"""
    return doris_data_service.get_feedback_stats(limit)

@app.get("/api/v1/doris/models", summary="模型状态", tags=["Doris"])
async def doris_models():
    return doris_data_service.get_model_status()


# ============================================================
# 根路径
# ============================================================
@app.get("/", summary="API首页", tags=["系统"])
async def root():
    return {
        "service": "金融反诈智能守护系统 REST API",
        "version": "2.0.0",
        "description": "基于Flink CEP/SQL/Graph/ML/GNN五层架构的实时金融反欺诈检测系统",
        "docs": "/api/docs",
        "redoc": "/api/redoc",
        "health": "/api/v1/health",
        "endpoints": {
            "detect": "POST /api/v1/detect",
            "batch_detect": "POST /api/v1/detect/batch",
            "gnn_detect": "POST /api/v1/gnn/detect",
            "alerts": "GET /api/v1/alerts",
            "feedback": "POST /api/v1/feedback",
            "metrics": "GET /api/v1/metrics",
            "rules": "GET /api/v1/rules",
            "models": "GET /api/v1/models",
            "drift_status": "GET /api/v1/drift/status",
            "drift_history": "GET /api/v1/drift/history",
        },
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower(),
    )
