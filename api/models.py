"""
Pydantic数据模型 - 请求/响应Schema定义
对应Java端Transaction和Alert等实体类
"""
from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator


# ============================================================
# 交易请求模型
# ============================================================

class TransactionRequest(BaseModel):
    """
    单笔交易请求 - 对应Java Transaction类
    支持CEP/SQL/Graph/ML四层检测所需的所有字段
    """
    # 基础交易字段
    step: Optional[int] = Field(0, description="交易步骤序号")
    type: str = Field(..., min_length=1, max_length=50, description="交易类型: TRANSFER/CASH_OUT/PAYMENT/DEBIT等")
    amount: float = Field(..., ge=0, description="交易金额")
    nameOrig: str = Field(..., min_length=1, max_length=100, description="发起方账户ID")
    oldbalanceOrg: float = Field(0, ge=0, description="交易前发起方余额")
    newbalanceOrig: float = Field(0, ge=0, description="交易后发起方余额")
    nameDest: str = Field("UNKNOWN", max_length=100, description="接收方账户ID")
    oldbalanceDest: float = Field(0, ge=0, description="交易前接收方余额")
    newbalanceDest: float = Field(0, ge=0, description="交易后接收方余额")
    isFlaggedFraud: int = Field(0, description="是否被标记为欺诈(数据集原始标记)")

    # 扩展特征字段(合成数据)
    deviceId: Optional[str] = Field("UNKNOWN", max_length=100, description="设备ID")
    deviceType: Optional[str] = Field("UNKNOWN", max_length=50, description="设备类型: 手机/平板/PC")
    payChannel: Optional[str] = Field("UNKNOWN", max_length=50, description="支付渠道: BANK_APP/小程序/网银等")
    city: Optional[str] = Field("UNKNOWN", max_length=100, description="交易城市")
    ipSegment: Optional[str] = Field("UNKNOWN", max_length=100, description="IP段前缀")
    transactionHour: Optional[int] = Field(0, ge=0, le=23, description="交易小时数0-23")
    dailyTxCount: Optional[int] = Field(1, ge=0, description="当日交易笔数")
    deviceRiskLevel: Optional[str] = Field("LOW", max_length=20, description="设备风险等级: LOW/MEDIUM/HIGH")
    isAbroad: Optional[str] = Field("LOCAL", max_length=20, description="是否境外: LOCAL/ABROAD")
    groupId: Optional[str] = Field("NONE", max_length=100, description="欺诈团伙ID")
    fraudType: Optional[str] = Field("NORMAL", max_length=100, description="欺诈类型标签")
    isFraud: Optional[int] = Field(0, description="欺诈标签: 1=欺诈, 0=正常")

    # 可选时间戳(用于事件时间处理)
    eventTime: Optional[int] = Field(None, description="事件时间戳(毫秒)")

    class Config:
        json_schema_extra = {
            "example": {
                "type": "TRANSFER",
                "amount": 50000.0,
                "nameOrig": "C1234567890",
                "oldbalanceOrg": 100000.0,
                "newbalanceOrig": 50000.0,
                "nameDest": "C0987654321",
                "oldbalanceDest": 0.0,
                "newbalanceDest": 50000.0,
                "deviceId": "DEV_001",
                "deviceType": "手机",
                "payChannel": "BANK_APP",
                "city": "北京",
                "ipSegment": "192.168.1",
                "transactionHour": 2,
                "dailyTxCount": 5,
                "deviceRiskLevel": "HIGH",
                "isAbroad": "ABROAD",
            }
        }


class BatchDetectRequest(BaseModel):
    """批量检测请求 - 最多100笔交易"""
    transactions: List[TransactionRequest] = Field(
        ...,
        min_length=1,
        max_length=100,
        description="交易列表(最多100笔)"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "transactions": [
                    {"type": "TRANSFER", "amount": 50000.0, "nameOrig": "C1", "nameDest": "C2"},
                    {"type": "CASH_OUT", "amount": 30000.0, "nameOrig": "C3", "nameDest": "C4"},
                ]
            }
        }


# ============================================================
# 检测响应模型
# ============================================================

class FeatureContributionResponse(BaseModel):
    """特征贡献度 - 对应Java FeatureContribution类"""
    featureName: str = Field(..., max_length=200, description="特征中文名")
    featureKey: str = Field(..., max_length=100, description="特征英文键")
    featureValue: float = Field(..., description="原始特征值")
    normalizedValue: float = Field(..., description="归一化值")
    weight: float = Field(..., description="模型权重")
    contribution: float = Field(..., description="贡献度 = weight * normalizedValue")
    direction: str = Field(..., max_length=20, description="影响方向: 推高风险/降低风险")


class ExplanationResponse(BaseModel):
    """告警可解释性响应 - 对应Java AlertExplanation类"""
    topFeatures: List[FeatureContributionResponse] = Field([], description="Top特征贡献列表")
    triggeredRules: List[str] = Field([], description="命中的规则列表")
    graphPath: str = Field("", max_length=1000, description="资金流向路径")
    summary: str = Field("", max_length=1000, description="一句话自然语言总结")


class DetectResponse(BaseModel):
    """
    单笔检测结果响应
    """
    request_id: str = Field(..., description="请求唯一ID")
    is_fraud: bool = Field(..., description="是否欺诈")
    fraud_probability: float = Field(..., ge=0, le=1, description="欺诈概率(0-1)")
    risk_level: str = Field(..., description="风险等级: LOW/MEDIUM/HIGH/CRITICAL")
    fraud_type: str = Field(..., description="欺诈类型")
    confidence: float = Field(..., ge=0, le=1, description="置信度")
    source: str = Field(..., description="检测来源: CEP_RULE/ML_ENSEMBLE/GRAPH/SQL_FUSION")
    account_id: str = Field(..., description="被检测的账户ID")
    timestamp: int = Field(..., description="检测时间戳(毫秒)")
    processing_time_ms: float = Field(..., description="处理耗时(毫秒)")
    explanation: Optional[ExplanationResponse] = Field(None, description="可解释性信息")
    details: Optional[str] = Field(None, description="检测详情(JSON字符串)")


class BatchDetectResponse(BaseModel):
    """批量检测结果响应"""
    request_id: str = Field(..., description="请求唯一ID")
    total_transactions: int = Field(..., description="总交易数")
    fraud_count: int = Field(..., description="欺诈交易数")
    normal_count: int = Field(..., description="正常交易数")
    processing_time_ms: float = Field(..., description="总处理耗时(毫秒)")
    results: List[DetectResponse] = Field(..., description="各笔交易检测结果")


# ============================================================
# 告警查询模型
# ============================================================

class AlertResponse(BaseModel):
    """告警详情响应 - 对应Java Alert类"""
    alert_id: str = Field(..., description="告警ID")
    account_id: str = Field(..., description="被标记的账户ID")
    fraud_type: str = Field(..., description="欺诈类型")
    confidence: float = Field(..., ge=0, le=1, description="置信度")
    source: str = Field(..., description="告警来源(CEP_RULE/ML_ENSEMBLE/FUSION等)")
    behavior_path: str = Field("", description="行为路径")
    timestamp: int = Field(..., description="告警时间戳(毫秒)")
    timestamp_str: Optional[str] = Field(None, description="可读时间字符串")
    details: Optional[str] = Field(None, description="告警详情")
    explanation_summary: Optional[str] = Field(None, description="解释摘要")
    explanation: Optional[ExplanationResponse] = Field(None, description="完整可解释性信息")
    human_feedback: Optional[str] = Field(None, description="人工反馈(已确认/误报/未反馈)")


class AlertListResponse(BaseModel):
    """告警列表响应"""
    total: int = Field(..., description="总告警数(符合筛选条件)")
    page: int = Field(1, description="当前页码")
    page_size: int = Field(20, description="每页数量")
    alerts: List[AlertResponse] = Field(..., description="告警列表")


# ============================================================
# 反馈模型
# ============================================================

class FeedbackRequest(BaseModel):
    """人工反馈请求"""
    alert_id: str = Field(..., description="告警ID")
    feedback: str = Field(
        ...,
        description="反馈类型: CONFIRMED_FRAUD(确认欺诈) / FALSE_POSITIVE(误报) / ADJUST_CONFIDENCE(调整置信度)"
    )
    corrected_fraud_type: Optional[str] = Field(None, description="修正后的欺诈类型(可选)")
    confidence_adjustment: Optional[float] = Field(None, ge=-1, le=1, description="置信度调整值(-1~1)")
    comment: Optional[str] = Field(None, max_length=500, description="备注说明")

    @field_validator("feedback")
    @classmethod
    def validate_feedback(cls, v):
        valid = {"CONFIRMED_FRAUD", "FALSE_POSITIVE", "ADJUST_CONFIDENCE"}
        if v not in valid:
            raise ValueError(f"feedback必须是以下之一: {valid}")
        return v


class FeedbackResponse(BaseModel):
    """反馈提交响应"""
    success: bool = Field(..., description="是否提交成功")
    alert_id: str = Field(..., description="告警ID")
    message: str = Field(..., description="提示信息")
    model_update_triggered: bool = Field(False, description="是否触发模型更新")


# ============================================================
# 指标模型
# ============================================================

class LatencyMetrics(BaseModel):
    """延迟指标"""
    avg_ms: float = Field(0, description="平均延迟(毫秒)")
    p50_ms: float = Field(0, description="P50延迟(毫秒)")
    p90_ms: float = Field(0, description="P90延迟(毫秒)")
    p95_ms: float = Field(0, description="P95延迟(毫秒)")
    p99_ms: float = Field(0, description="P99延迟(毫秒)")
    max_ms: float = Field(0, description="最大延迟(毫秒)")
    min_ms: float = Field(0, description="最小延迟(毫秒)")
    sample_count: int = Field(0, description="样本数")
    distribution: Optional[Dict[str, int]] = Field(None, description="延迟分布区间")


class ThroughputMetrics(BaseModel):
    """吞吐量指标"""
    total_alerts_generated: int = Field(0, description="累计产生告警数")
    overall_avg_tps: float = Field(0, description="平均TPS")
    recent_trend_tps: float = Field(0, description="最近趋势TPS")
    uptime_seconds: int = Field(0, description="运行时长(秒)")


class LayerPerformance(BaseModel):
    """各层性能对比"""
    estimated_latency_ms: int = Field(0, description="预估延迟(毫秒)")
    estimated_throughput_tps: int = Field(0, description="预估TPS")
    bottleneck: bool = Field(False, description="是否为性能瓶颈")


class MetricsResponse(BaseModel):
    """系统性能指标响应"""
    report_type: str = Field("fraud_detection_performance_dashboard")
    generated_at: str = Field(..., description="报告生成时间")
    uptime_seconds: int = Field(0, description="系统运行时长(秒)")
    latency_metrics: LatencyMetrics = Field(default_factory=LatencyMetrics)
    throughput_metrics: ThroughputMetrics = Field(default_factory=ThroughputMetrics)
    layer_performance: Optional[Dict[str, LayerPerformance]] = Field(None, description="各层性能对比")
    competition_targets: Optional[Dict[str, Any]] = Field(None, description="竞赛指标达标情况")
    evaluation_metrics: Optional[Dict[str, Any]] = Field(None, description="模型评估指标(precision/recall/F1)")


class LatencyResponse(BaseModel):
    """详细延迟分解响应"""
    current_time: str = Field(..., description="当前时间")
    uptime_seconds: int = Field(0, description="运行时长(秒)")
    end_to_end: LatencyMetrics = Field(default_factory=LatencyMetrics, description="端到端延迟")
    by_layer: Dict[str, int] = Field(
        default_factory=lambda: {
            "CEP_RULE_ENGINE": 5,
            "SQL_CROSS_KEY_DETECTION": 15,
            "GRAPH_ANALYSIS": 20,
            "ML_ANOMALY_DETECTION": 25,
            "ALERT_FUSION_DEDUP": 5,
        },
        description="各层预估延迟(毫秒)"
    )
    api_latency: Optional[LatencyMetrics] = Field(None, description="API服务自身延迟统计")


# ============================================================
# 规则模型
# ============================================================

class RuleResponse(BaseModel):
    """检测规则响应 - 对应CEP 8种规则"""
    rule_id: str = Field(..., description="规则ID")
    rule_name: str = Field(..., description="规则名称(中文)")
    pattern_name: str = Field(..., description="Pattern方法名")
    confidence: float = Field(..., description="置信度阈值")
    time_window: str = Field(..., description="时间窗口")
    description: str = Field(..., description="规则描述")
    is_active: bool = Field(True, description="是否启用")
    match_count: int = Field(0, description="命中次数")


class RuleUpdateRequest(BaseModel):
    """规则更新请求"""
    confidence: Optional[float] = Field(None, ge=0, le=1, description="置信度阈值")
    is_active: Optional[bool] = Field(None, description="是否启用")


class RuleUpdateResponse(BaseModel):
    """规则更新响应"""
    success: bool = Field(..., description="是否更新成功")
    rule_id: str = Field(..., description="规则ID")
    old_confidence: Optional[float] = Field(None, description="旧置信度")
    new_confidence: Optional[float] = Field(None, description="新置信度")
    message: str = Field(..., description="提示信息")


# ============================================================
# 模型信息模型
# ============================================================

class ModelWeightInfo(BaseModel):
    """模型权重信息"""
    weight: float = Field(..., description="集成权重")
    status: str = Field(..., description="状态: active/inactive/loading")
    last_updated: Optional[str] = Field(None, description="最后更新时间")


class ModelInfoResponse(BaseModel):
    """模型版本和状态信息"""
    models: Dict[str, Dict[str, Any]] = Field(..., description="各模型信息")
    ensemble_weights: Dict[str, float] = Field(..., description="集成权重配置")
    current_threshold: float = Field(..., description="当前检测阈值")
    last_retrain_time: Optional[str] = Field(None, description="最后重训练时间")
    retrain_count: int = Field(0, description="重训练次数")


class RetrainRequest(BaseModel):
    """模型重训练请求"""
    reason: str = Field("manual", description="重训练原因")
    use_all_data: bool = Field(False, description="是否使用全量数据(默认仅使用近期数据)")
    feedback_only: bool = Field(False, description="是否仅使用人工反馈数据")


class RetrainResponse(BaseModel):
    """模型重训练响应"""
    success: bool = Field(..., description="是否成功")
    message: str = Field(..., description="提示信息")
    trigger_id: str = Field(..., description="触发ID")
    estimated_duration_seconds: int = Field(0, description="预估耗时(秒)")


# ============================================================
# 健康检查
# ============================================================

class HealthResponse(BaseModel):
    """健康检查响应"""
    status: str = Field(..., description="服务状态: healthy/degraded/unhealthy")
    version: str = Field("2.0.0", description="API版本")
    timestamp: str = Field(..., description="当前时间")
    uptime_seconds: float = Field(0, description="服务运行时长(秒)")
    checks: Dict[str, str] = Field(default_factory=dict, description="各项检查结果")


# ============================================================
# GNN检测模型
# ============================================================

class GnnDetectRequest(BaseModel):
    """GNN图神经网络检测请求"""
    account_id: Optional[str] = Field(None, description="账户ID")
    nameOrig: Optional[str] = Field(None, description="发起方账户ID")
    amount: Optional[float] = Field(0, ge=0, description="交易金额")
    deviceRiskLevel: Optional[str] = Field("LOW", description="设备风险等级")
    isAbroad: Optional[str] = Field("LOCAL", description="是否境外")
    type: Optional[str] = Field("TRANSFER", description="交易类型")
    nameDest: Optional[str] = Field(None, description="接收方账户ID")
