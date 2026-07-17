package com.fraud.detection.explain;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 欺诈模式描述器 — 将技术欺诈模式转换为人类可读的描述
 *
 * 覆盖系统支持的11种欺诈类型：
 * CEP 8种 + SQL 3种
 * 为每种类型提供：技术定义、人类可读解释、典型场景、处置建议
 */
public class FraudPatternDescriber implements Serializable {

    /**
     * 欺诈模式信息
     */
    public static class PatternInfo implements Serializable {
        public final String patternKey;         // 模式标识符
        public final String displayName;        // 中文展示名
        public final String technicalDefinition; // 技术定义
        public final String humanExplanation;   // 人类可读解释
        public final String typicalScenario;    // 典型场景描述
        public final String recommendedAction;  // 推荐处置动作
        public final String severity;           // 严重程度: 极高/高/中

        public PatternInfo(String patternKey, String displayName,
                           String technicalDefinition, String humanExplanation,
                           String typicalScenario, String recommendedAction,
                           String severity) {
            this.patternKey = patternKey;
            this.displayName = displayName;
            this.technicalDefinition = technicalDefinition;
            this.humanExplanation = humanExplanation;
            this.typicalScenario = typicalScenario;
            this.recommendedAction = recommendedAction;
            this.severity = severity;
        }
    }

    // 欺诈模式注册表
    private static final Map<String, PatternInfo> PATTERN_REGISTRY = new HashMap<>();

    static {
        // —— CEP Pattern 1: 小额试探大额转出 ——
        register(new PatternInfo(
                "小额试探大额转出",
                "小额试探大额转出",
                "用户在短时间窗口（2小时）内先进行1笔1000-6000元转账试探，随后发起≥30000元大额转账至境外或高风险设备",
                "该账户在短时间内先进行多笔小额转账试探系统反应，随后突然发起大额转账，符合典型的账户盗用后快速转移资金的行为模式。",
                "典型场景：黑客获取账户权限后，先小额测试账户是否可用及系统是否会拦截，确认无误后立即转走大额资金至境外账户。",
                "1. 立即冻结账户\n2. 联系持卡人确认交易真实性\n3. 追踪大额资金流向\n4. 上报反洗钱部门",
                "极高"
        ));

        // —— CEP Pattern 2: 多层链式洗钱 ——
        register(new PatternInfo(
                "多层链式洗钱",
                "多层链式洗钱",
                "大额资金在2小时内经过A→B→C三层转账链路，最终在境外或高风险设备提现",
                "资金通过多个中间账户层层流转，每层转账金额逐步递减（资金衰减），最终在境外或高风险地区完成提现，是典型的洗钱操作链路。",
                "典型场景：犯罪团伙利用多个傀儡账户进行资金分层转移，通过多次中转模糊资金来源，最终在监管薄弱的境外地区套现。",
                "1. 冻结链路中所有涉案账户\n2. 追踪完整资金路径\n3. 提取傀儡账户特征\n4. 上报公安机关",
                "极高"
        ));

        // —— CEP Pattern 3: 异地跨设备突发大额 ——
        register(new PatternInfo(
                "异地跨设备突发大额",
                "异地跨设备突发大额",
                "用户在正常城市小额交易后，3小时内切换至异常城市/设备并发起≥30000元大额交易",
                "账户在短时间内跨越不同地理位置并使用不同设备发起大额交易，这种空间跳跃与行为突变的组合是账户被盗用的强烈信号。",
                "典型场景：账户在A城市正常使用后，突然在B城市使用新设备发起大额转账，表明账户凭证可能已泄露并被异地盗用。",
                "1. 冻结账户并触发二次验证\n2. 联系持卡人确认当前位置\n3. 核实大额交易意图\n4. 标记异常设备",
                "高"
        ));

        // —— CEP Pattern 4: 分散转入集中提现 ——
        register(new PatternInfo(
                "分散转入集中提现",
                "分散转入集中提现",
                "2小时内收到2笔以上4000-30000元转账后，立即进行≥20000元大额提现",
                "账户在短时间内集中接收来自多个来源的中等金额资金，随后一次性大额提现，这种'汇集-集中'模式常用于非法资金归集。",
                "典型场景：网络诈骗赃款分散汇入中间账户（骡子账户），再由骡子账户一次性提现转移，是典型的资金归集洗钱手法。",
                "1. 暂停提现功能\n2. 追踪资金来源账户\n3. 核实提现用途\n4. 标记疑似骡子账户",
                "高"
        ));

        // —— CEP Pattern 5: 多渠道轮番转账 ——
        register(new PatternInfo(
                "多渠道轮番转账",
                "多渠道轮番转账",
                "1小时内通过3种不同支付渠道各发起≥5000元转账",
                "账户在短时间内切换多个支付渠道（银行APP、小程序、第三方平台）进行转账，试图规避单一渠道的风控限额和监控。",
                "典型场景：盗号者发现单一渠道转账限额后，迅速切换银行APP、微信小程序、支付宝等多个渠道持续转出资金。",
                "1. 触发全渠道风控拦截\n2. 联系持卡人确认操作\n3. 检查账户登录日志\n4. 临时降低转账额度",
                "高"
        ));

        // —— CEP Pattern 6: 凌晨分批掏空 ——
        register(new PatternInfo(
                "凌晨分批掏空",
                "凌晨分批掏空",
                "凌晨0-5点连续发起3笔≥5000元转账/提现，时间窗口4小时",
                "在凌晨用户深度睡眠时段分批发起多笔大额交易，利用持卡人无法及时察觉的时间窗口完成资金转移。",
                "典型场景：盗号者在凌晨2-5点期间，每隔一段时间转出一笔5000-20000元，在持卡人醒来前完成账户掏空。",
                "1. 立即触发交易拦截\n2. 电话通知持卡人\n3. 冻结账户\n4. 追回已转出资金",
                "极高"
        ));

        // —— CEP Pattern 7: 小额掩护大额跑路 ——
        register(new PatternInfo(
                "小额掩护大额跑路",
                "小额掩护大额跑路",
                "8小时内先进行2笔100-1000元正常交易，随后发起≥40000元大额转账至境外或高风险设备",
                "先用小额正常交易'热身'以降低系统警觉，随后突然发起大额转账快速跑路，是精心策划的欺诈手法。",
                "典型场景：盗号者先进行几笔小额正常消费（如充值、缴费）来模拟正常用户行为，待系统降低风控等级后，突然发起大额转账至境外。",
                "1. 紧急拦截大额转账\n2. 冻结账户\n3. 分析前期小额交易特征\n4. 上报可疑交易",
                "极高"
        ));

        // —— CEP Pattern 8: 团伙同IP批量作案 ——
        register(new PatternInfo(
                "团伙同IP批量作案",
                "团伙同IP批量作案",
                "30分钟内同一IP段下2笔以上≥15000元转账",
                "多个不同账户在相同IP段下短时间内发起大额转账，表明存在有组织的团伙作案行为。",
                "典型场景：诈骗团伙在集中窝点使用同一网络环境，批量操作多个被盗账户进行转账，是典型的团伙化作案特征。",
                "1. 批量冻结涉案账户\n2. 追踪IP物理地址\n3. 上报公安机关\n4. 提取团伙特征画像",
                "极高"
        ));

        // —— SQL Pattern: 链式洗钱(SQL) ——
        register(new PatternInfo(
                "多层链式洗钱(SQL)",
                "多层链式洗钱(SQL检测)",
                "通过跨账户SQL关联分析检测A→B→C三层转账链路，综合时间差、金额衰减率计算置信度",
                "跨账户分析发现资金沿A→B→C路径流转，转账时间间隔短且金额衰减率异常低，表明资金被有组织地分层转移。",
                "典型场景：通过跨账户关联分析发现，多个独立账户之间存在定向资金转移链路，且资金流向呈现明显的分层递减特征。",
                "1. 冻结完整链路账户\n2. 追踪资金来源\n3. 构建资金流向图谱\n4. 上报反洗钱部门",
                "极高"
        ));

        // —— SQL Pattern: 团伙同IP(SQL) ——
        register(new PatternInfo(
                "团伙同IP批量作案(SQL)",
                "团伙同IP批量作案(SQL检测)",
                "同IP段下多个账户(≥3)在短时间内累计大额转账(>60000)，基于窗口聚合分析",
                "同一IP网络环境下多个不同账户协同进行大额转账操作，累计金额异常，属于典型的团伙协同作案模式。",
                "典型场景：黑产团伙在同一机房或VPN网络下，批量操控数十个账户进行转账操作，累计金额巨大。",
                "1. 批量冻结同IP段账户\n2. 溯源IP地址\n3. 关联分析账户关系\n4. 移交公安机关",
                "极高"
        ));

        // —— SQL Pattern: 分散转入集中提现(SQL) ——
        register(new PatternInfo(
                "分散转入集中提现(SQL)",
                "分散转入集中提现(SQL检测)",
                "多账户向同一目标转账后，目标账户进行CASH_OUT提现，提现金额占转入总额≥70%",
                "跨账户分析发现，多个不同账户向同一目标集中汇入资金后，该目标账户立即进行大额提现，提现比例异常高。",
                "典型场景：多个受害者将资金汇入诈骗分子控制的中间账户，该账户在资金到账后立即提现转移，完成洗钱闭环。",
                "1. 暂停目标账户提现\n2. 追踪所有资金来源\n3. 标记骡子账户网络\n4. 协助受害人挽损",
                "极高"
        ));
    }

    /**
     * 根据欺诈类型获取模式信息
     *
     * @param fraudType 欺诈类型字符串
     * @return PatternInfo 对象，未找到时返回默认信息
     */
    public static PatternInfo describe(String fraudType) {
        if (fraudType == null || fraudType.trim().isEmpty()) {
            return getDefaultInfo();
        }

        // 精确匹配
        PatternInfo info = PATTERN_REGISTRY.get(fraudType);
        if (info != null) {
            return info;
        }

        // 模糊匹配（去掉后缀如 "(SQL)"）
        String simplified = fraudType.replaceAll("\\(.*\\)", "").trim();
        for (Map.Entry<String, PatternInfo> entry : PATTERN_REGISTRY.entrySet()) {
            if (entry.getKey().contains(simplified) || simplified.contains(entry.getKey())) {
                return entry.getValue();
            }
        }

        // 未匹配到已知模式，返回默认信息
        return getUnknownInfo(fraudType);
    }

    /**
     * 获取所有已知欺诈模式列表
     */
    public static Map<String, PatternInfo> getAllPatterns() {
        return new HashMap<>(PATTERN_REGISTRY);
    }

    /**
     * 获取已知模式数量
     */
    public static int getPatternCount() {
        return PATTERN_REGISTRY.size();
    }

    /**
     * 判断是否为已知欺诈模式
     */
    public static boolean isKnownPattern(String fraudType) {
        if (fraudType == null) return false;
        if (PATTERN_REGISTRY.containsKey(fraudType)) return true;
        String simplified = fraudType.replaceAll("\\(.*\\)", "").trim();
        for (String key : PATTERN_REGISTRY.keySet()) {
            if (key.contains(simplified) || simplified.contains(key)) return true;
        }
        return false;
    }

    private static void register(PatternInfo info) {
        PATTERN_REGISTRY.put(info.patternKey, info);
    }

    private static PatternInfo getDefaultInfo() {
        return new PatternInfo(
                "未知",
                "未知欺诈类型",
                "未识别到具体欺诈模式",
                "系统检测到异常交易行为，但暂未匹配到已知欺诈模式。",
                "交易行为存在异常特征，需要人工进一步核实。",
                "1. 人工审核交易详情\n2. 联系持卡人确认\n3. 标记待观察账户",
                "中"
        );
    }

    private static PatternInfo getUnknownInfo(String fraudType) {
        return new PatternInfo(
                fraudType,
                fraudType,
                "未收录的欺诈模式：" + fraudType,
                "系统检测到标记为「" + fraudType + "」的异常交易行为，该模式暂未纳入自动描述知识库。",
                "建议调查人员结合交易详情和账户行为进行人工研判。",
                "1. 人工审核交易详情\n2. 联系持卡人确认\n3. 考虑将该模式加入知识库",
                "中"
        );
    }
}
