package com.fraud.detection.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * 交易实体类 - 系统基础数据模型
 * 承载所有交易数据字段,包含22+个字段
 * 用于Flink流式计算中的数据传输和处理
 * 
 * 数据来源: Kafka消息 -> JSON解析 -> Transaction对象
 * 核心作用: 作为CEP/SQL/Graph/ML四层检测的基础输入
 */
public class Transaction implements Serializable {

    // ========== 基础交易字段(来自原始数据集) ==========
    
    /** 交易步骤序号(用于推断时间,step%24可得小时数) */
    public int step;
    
    /** 交易类型:TRANSFER/CASH_OUT/PAYMENT/DEBIT等 */
    public String type;
    
    /** 交易金额 */
    public double amount;
    
    /** 发起方账户ID(分组键,用于keyBy操作) */
    public String nameOrig;
    
    /** 交易前发起方余额 */
    public double oldbalanceOrg;
    
    /** 交易后发起方余额 */
    public double newbalanceOrig;
    
    /** 接收方账户ID */
    public String nameDest;
    
    /** 交易前接收方余额 */
    public double oldbalanceDest;
    
    /** 交易后接收方余额 */
    public double newbalanceDest;
    
    /** 是否被标记为欺诈(数据集原始标记,1=欺诈,0=正常) */
    public int isFlaggedFraud;

    // ========== 扩展特征字段(合成数据生成脚本10000.py生成) ==========
    
    /** 设备ID(用于跨设备检测、团伙同IP检测) */
    public String deviceId;
    
    /** 设备类型:手机/平板/PC */
    public String deviceType;
    
    /** 支付渠道:BANK_APP/小程序/网银等 */
    public String payChannel;
    
    /** 交易城市(用于异地检测) */
    public String city;
    
    /** IP段前缀(用于团伙同IP检测,如192.168.1) */
    public String ipSegment;
    
    /** 交易小时数0-23(用于凌晨检测) */
    public int transactionHour;
    
    /** 当日交易笔数(用于频率检测) */
    public int dailyTxCount;
    
    /** 设备风险等级:LOW/MEDIUM/HIGH */
    public String deviceRiskLevel;
    
    /** 是否境外:LOCAL/ABROAD */
    public String isAbroad;
    
    /** 合成数据中的欺诈团伙ID(用于关联分析) */
    public String groupId;
    
    /** 欺诈类型标签(11种模式之一,NORMAL表示正常交易) */
    public String fraudType;
    
    /** 欺诈标签:1=欺诈,0=正常(用于ML模型训练和评估) */
    public int isFraud;

    // ========== Flink运行时字段(检测过程中动态添加) ==========
    
    /** 事件时间戳(毫秒,用于Flink Watermark排序) */
    public long eventTime;
    
    /** 是否被CEP规则命中 */
    public boolean isCepMatched;
    
    /** 命中的CEP规则名称(如Pattern1_SmallTestLargeTransfer) */
    public String cepRuleName;
    
    /** 追踪账户(双向交易时关联对方账户,用于TransactionDuplicator) */
    public String trackingAccount;

    // ========== 性能指标追踪字段(用于竞赛延迟/吞吐量测量) ==========

    /** 各阶段时间戳(毫秒): t0=Kafka接入, t1=CEP完成, t2=SQL完成, t3=Graph完成, t4=ML完成, t5=Alert输出 */
    public java.util.Map<String, Long> latencyTimestamps;

    /**
     * 默认构造函数
     * 初始化所有字符串字段为UNKNOWN或默认值
     * 事件时间默认为当前系统时间
     */
    public Transaction() {
        this.eventTime = System.currentTimeMillis();
        this.isCepMatched = false;
        this.cepRuleName = "NONE";
        this.type = "UNKNOWN";
        this.nameOrig = "UNKNOWN";
        this.nameDest = "UNKNOWN";
        this.deviceId = "UNKNOWN";
        this.deviceType = "UNKNOWN";
        this.payChannel = "UNKNOWN";
        this.city = "UNKNOWN";
        this.ipSegment = "UNKNOWN";
        this.deviceRiskLevel = "LOW";
        this.isAbroad = "LOCAL";
        this.groupId = "NONE";
        this.fraudType = "NORMAL";
        this.trackingAccount = null;
        this.latencyTimestamps = new java.util.HashMap<>();
    }

    /**
     * 深拷贝当前交易对象
     * 用于TransactionDuplicator双向复制场景(转出方和转入方各需要一个副本)
     * @return 新的Transaction对象,包含当前对象的所有字段值
     */
    public Transaction copy() {
        Transaction t = new Transaction();
        t.step = this.step;
        t.type = this.type;
        t.amount = this.amount;
        t.nameOrig = this.nameOrig;
        t.oldbalanceOrg = this.oldbalanceOrg;
        t.newbalanceOrig = this.newbalanceOrig;
        t.nameDest = this.nameDest;
        t.oldbalanceDest = this.oldbalanceDest;
        t.newbalanceDest = this.newbalanceDest;
        t.isFlaggedFraud = this.isFlaggedFraud;
        t.deviceId = this.deviceId;
        t.deviceType = this.deviceType;
        t.payChannel = this.payChannel;
        t.city = this.city;
        t.ipSegment = this.ipSegment;
        t.transactionHour = this.transactionHour;
        t.dailyTxCount = this.dailyTxCount;
        t.deviceRiskLevel = this.deviceRiskLevel;
        t.isAbroad = this.isAbroad;
        t.groupId = this.groupId;
        t.fraudType = this.fraudType;
        t.isFraud = this.isFraud;
        t.eventTime = this.eventTime;
        t.isCepMatched = this.isCepMatched;
        t.cepRuleName = this.cepRuleName;
        t.trackingAccount = this.trackingAccount;
        t.latencyTimestamps = this.latencyTimestamps != null ? new java.util.HashMap<>(this.latencyTimestamps) : new java.util.HashMap<>();
        return t;
    }

    /**
     * 从Kafka JSON字符串解析交易对象
     * 支持CDC格式的嵌套data.after结构和普通扁平JSON
     * @param value Kafka消息的JSON字符串
     * @param out Flink Collector,用于输出解析后的Transaction对象
     */
    public static void fromKafkaJson(String value, Collector<Transaction> out) {
        Transaction tx;
        try {
            JSONObject root = JSON.parseObject(value);
            // 提取JSON中的data对象,支持CDC格式的after/before嵌套
            JSONObject data = extractDataObject(root);
            if (data == null || data.isEmpty()) {
                return;
            }
            tx = fromJsonObject(data);
            // 如果数据中没有eventTime,则使用CDC的timestamp或当前时间
            if (tx.eventTime <= 0) {
                tx.eventTime = root.getLongValue("timestamp") > 0 ? root.getLongValue("timestamp") : System.currentTimeMillis();
            }
        } catch (Exception e) {
            System.err.println("Kafka JSON 解析失败: " + e.getMessage() + ", 原始数据: " + abbreviate(value, 300));
            return;
        }

        // 校验交易有效性后输出
        if (tx.isValid()) {
            out.collect(tx);
        }
    }

    /**
     * 从JSON对象直接构造交易
     * 用于Kafka和API数据解析
     * @param data JSON对象(包含所有交易字段)
     * @return Transaction对象
     */
    public static Transaction fromJsonObject(JSONObject data) {
        Transaction tx = new Transaction();
        tx.step = data.getIntValue("step");
        tx.type = getString(data, "type", tx.type);
        tx.amount = data.getDoubleValue("amount");
        tx.nameOrig = getString(data, "nameOrig", tx.nameOrig);
        tx.oldbalanceOrg = data.getDoubleValue("oldbalanceOrg");
        tx.newbalanceOrig = data.getDoubleValue("newbalanceOrig");
        tx.nameDest = getString(data, "nameDest", tx.nameDest);
        tx.oldbalanceDest = data.getDoubleValue("oldbalanceDest");
        tx.newbalanceDest = data.getDoubleValue("newbalanceDest");
        tx.isFlaggedFraud = data.getIntValue("isFlaggedFraud");
        tx.deviceId = getString(data, "deviceId", tx.deviceId);
        tx.deviceType = getString(data, "deviceType", tx.deviceType);
        tx.payChannel = getString(data, "payChannel", tx.payChannel);
        tx.city = getString(data, "city", tx.city);
        tx.ipSegment = getString(data, "ipSegment", tx.ipSegment);
        tx.transactionHour = data.containsKey("transactionHour") ? data.getIntValue("transactionHour") : inferHour(tx.step);
        tx.dailyTxCount = data.getIntValue("dailyTxCount");
        tx.deviceRiskLevel = getString(data, "deviceRiskLevel", tx.deviceRiskLevel);
        tx.isAbroad = getString(data, "isAbroad", tx.isAbroad);
        tx.groupId = getString(data, "groupId", tx.groupId);
        tx.fraudType = getString(data, "fraudType", tx.fraudType);
        tx.isFraud = data.getIntValue("isFraud");
        // 优先使用数据中的eventTime(10000.py生成的时间戳)
        // 兼容三种格式: 毫秒时间戳(long)、MySQL time类型字符串(HH:MM:SS)、Debezium微秒整数
        if (data.containsKey("eventTime")) {
            long parsed = parseEventTime(data, "eventTime");
            if (parsed > 0) {
                tx.eventTime = parsed;
            }
        }
        // 初始化延迟追踪Map
        tx.latencyTimestamps = new java.util.HashMap<>();
        tx.recordLatency("t0", System.currentTimeMillis());
        // 标准化所有字符串字段
        tx.normalize();
        return tx;
    }

    /**
     * 从CSV数组构造交易对象
     * 兼容22字段和23字段(含eventTime)格式
     * @param fields CSV字符串数组
     * @return Transaction对象
     */
    public static Transaction fromCSV(String[] fields) {
        Transaction tx = new Transaction();
        try {
            if (fields.length < 22) {
                return tx;
            }
            tx.step = Integer.parseInt(fields[0]);
            tx.type = fields[1];
            tx.amount = Double.parseDouble(fields[2]);
            tx.nameOrig = fields[3];
            tx.oldbalanceOrg = Double.parseDouble(fields[4]);
            tx.newbalanceOrig = Double.parseDouble(fields[5]);
            tx.nameDest = fields[6];
            tx.oldbalanceDest = Double.parseDouble(fields[7]);
            tx.newbalanceDest = Double.parseDouble(fields[8]);
            tx.isFlaggedFraud = Integer.parseInt(fields[9]);
            tx.deviceId = fields[10];
            tx.deviceType = fields[11];
            tx.payChannel = fields[12];
            tx.city = fields[13];
            tx.ipSegment = fields[14];
            tx.transactionHour = Integer.parseInt(fields[15]);
            tx.dailyTxCount = Integer.parseInt(fields[16]);
            tx.deviceRiskLevel = fields[17];
            tx.isAbroad = fields[18];
            tx.groupId = fields[19];
            tx.fraudType = fields[20];
            tx.isFraud = Integer.parseInt(fields[21]);
            // 如果CSV有第23列eventTime,使用它作为事件时间
            if (fields.length >= 23) {
                try {
                    tx.eventTime = Long.parseLong(fields[22]);
                } catch (NumberFormatException ignored) {
                }
            }
            tx.normalize();
        } catch (Exception e) {
            System.err.println("解析 CSV 失败: " + e.getMessage());
        }
        return tx;
    }

    /**
     * 校验交易有效性
     * 必须有合法的发起方账户、交易类型和金额
     * @return true表示有效,false表示无效
     */
    public boolean isValid() {
        return nameOrig != null
                && !nameOrig.trim().isEmpty()
                && !"UNKNOWN".equals(nameOrig)
                && type != null
                && !type.trim().isEmpty()
                && amount >= 0;
    }

    /**
     * 标准化所有字符串字段
     * 将空值/null替换为默认值,去除两端空格
     */
    private void normalize() {
        type = normalizeText(type, "UNKNOWN");
        nameOrig = normalizeText(nameOrig, "UNKNOWN");
        nameDest = normalizeText(nameDest, "UNKNOWN");
        deviceId = normalizeText(deviceId, "UNKNOWN");
        deviceType = normalizeText(deviceType, "UNKNOWN");
        payChannel = normalizeText(payChannel, "UNKNOWN");
        city = normalizeText(city, "UNKNOWN");
        ipSegment = normalizeText(ipSegment, "UNKNOWN");
        deviceRiskLevel = normalizeText(deviceRiskLevel, "LOW");
        isAbroad = normalizeText(isAbroad, "LOCAL");
        groupId = normalizeText(groupId, "NONE");
        fraudType = normalizeText(fraudType, "NORMAL");
    }

    /**
     * 提取JSON中的data对象
     * 支持CDC格式的after/before嵌套结构
     * @param root 根JSON对象
     * @return data对象或after/before嵌套对象
     */
    private static JSONObject extractDataObject(JSONObject root) {
        JSONObject data = root.getJSONObject("data");
        if (data == null) {
            return root;
        }
        JSONObject after = data.getJSONObject("after");
        if (after != null) {
            return after;
        }
        JSONObject before = data.getJSONObject("before");
        if (before != null) {
            return before;
        }
        return data;
    }

    /**
     * 安全获取字符串
     * null或空时返回默认值
     * @param data JSON对象
     * @param key 字段名
     * @param defaultValue 默认值
     * @return 字段值或默认值
     */
    private static String getString(JSONObject data, String key, String defaultValue) {
        String value = data.getString(key);
        return value == null || value.trim().isEmpty() ? defaultValue : value;
    }

    /**
     * 文本标准化辅助方法
     * 去除两端空格,空值返回默认值
     * @param value 原始值
     * @param defaultValue 默认值
     * @return 标准化后的文本
     */
    private static String normalizeText(String value, String defaultValue) {
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }

    /**
     * 解析eventTime字段，兼容多种格式
     * 1. 毫秒时间戳(long): 直接使用
     * 2. MySQL time类型字符串(HH:MM:SS 或 HH:MM:SS.mmm): 转换为当天零点起的毫秒数
     * 3. Debezium微秒整数: 转换为毫秒
     * 4. ISO 8601持续时间(PT6S): 解析秒数
     */
    private static long parseEventTime(JSONObject data, String key) {
        // 1. 尝试作为long解析(毫秒时间戳)
        long longVal = data.getLongValue(key);
        if (longVal > 1000000000000L) {
            // 大于2001年的毫秒时间戳，直接使用
            return longVal;
        }
        if (longVal > 0) {
            // Debezium可能以微秒发送MySQL time值(如6000000表示6秒)
            // MySQL time最大值838:59:59 = 3029999秒 = 3029999000000微秒
            // 如果值在合理范围内(0~24小时的微秒)，转换为毫秒
            if (longVal <= 86400L * 1000000L) {
                return longVal / 1000; // 微秒转毫秒
            }
            return longVal;
        }

        // 2. 尝试作为字符串解析(HH:MM:SS 或 HH:MM:SS.mmm)
        String strVal = data.getString(key);
        if (strVal != null && !strVal.trim().isEmpty()) {
            strVal = strVal.trim();
            try {
                // 处理ISO 8601持续时间格式(Debezium可能发送): PT6S, PT1H30M5S
                if (strVal.startsWith("PT")) {
                    return parseIsoDuration(strVal);
                }
                // 处理HH:MM:SS或HH:MM:SS.mmm格式
                if (strVal.contains(":")) {
                    return parseTimeStr(strVal);
                }
                // 纯数字字符串，尝试解析为long
                long numVal = Long.parseLong(strVal);
                if (numVal > 1000000000000L) {
                    return numVal;
                }
                if (numVal > 0 && numVal <= 86400L * 1000000L) {
                    return numVal / 1000;
                }
                return numVal;
            } catch (Exception e) {
                System.err.println("eventTime解析失败: " + strVal + ", " + e.getMessage());
            }
        }
        return 0;
    }

    /**
     * 解析HH:MM:SS或HH:MM:SS.mmm格式的时间字符串
     * 返回从当天零点起的毫秒数
     */
    private static long parseTimeStr(String timeStr) {
        try {
            String[] mainParts = timeStr.split("\\.");
            String[] hms = mainParts[0].split(":");
            long hours = Long.parseLong(hms[0]);
            long minutes = hms.length > 1 ? Long.parseLong(hms[1]) : 0;
            long seconds = hms.length > 2 ? Long.parseLong(hms[2]) : 0;
            long millis = 0;
            if (mainParts.length > 1) {
                // 补齐3位毫秒
                String msStr = mainParts[1];
                if (msStr.length() == 1) msStr += "00";
                else if (msStr.length() == 2) msStr += "0";
                millis = Long.parseLong(msStr.substring(0, Math.min(3, msStr.length())));
            }
            return (hours * 3600 + minutes * 60 + seconds) * 1000 + millis;
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * 解析ISO 8601持续时间格式(Debezium格式)
     * 如: PT6S(6秒), PT1H30M5S(1小时30分5秒), PT0.123S(0.123秒)
     */
    private static long parseIsoDuration(String isoStr) {
        long totalMillis = 0;
        try {
            // 去掉PT前缀
            String content = isoStr.substring(2);
            // 提取小时
            int hIdx = content.indexOf('H');
            if (hIdx >= 0) {
                totalMillis += Long.parseLong(content.substring(0, hIdx)) * 3600000L;
                content = content.substring(hIdx + 1);
            }
            // 提取分钟
            int mIdx = content.indexOf('M');
            if (mIdx >= 0) {
                totalMillis += Long.parseLong(content.substring(0, mIdx)) * 60000L;
                content = content.substring(mIdx + 1);
            }
            // 提取秒(可能含小数)
            int sIdx = content.indexOf('S');
            if (sIdx >= 0) {
                double seconds = Double.parseDouble(content.substring(0, sIdx));
                totalMillis += (long)(seconds * 1000);
            }
        } catch (Exception e) {
            return 0;
        }
        return totalMillis;
    }

    /**
     * 根据step推断交易小时数
     * 使用step%24映射到0-23小时
     * @param step 交易步骤序号
     * @return 交易小时数(0-23)
     */
    private static int inferHour(int step) {
        return step > 0 ? Math.floorMod(step, 24) : 0;
    }

    /**
     * 截断超长字符串
     * 用于错误日志输出,避免打印过长数据
     * @param value 原始字符串
     * @param maxLength 最大长度
     * @return 截断后的字符串
     */
    private static String abbreviate(String value, int maxLength) {
        if (value == null) {
            return "null";
        }
        return value.length() <= maxLength ? value : value.substring(0, maxLength) + "...";
    }

    // ========== 性能指标追踪方法 ==========

    /**
     * 记录指定阶段的时间戳
     * @param stage 阶段名称: t0=Kafka接入, t1=CEP完成, t2=SQL完成, t3=Graph完成, t4=ML完成, t5=Alert输出
     * @param timestamp 时间戳(毫秒)
     */
    public void recordLatency(String stage, long timestamp) {
        if (this.latencyTimestamps == null) {
            this.latencyTimestamps = new java.util.HashMap<>();
        }
        this.latencyTimestamps.put(stage, timestamp);
    }

    /**
     * 获取指定阶段的时间戳
     * @param stage 阶段名称
     * @return 时间戳(毫秒), 未记录则返回0
     */
    public long getLatencyTimestamp(String stage) {
        if (this.latencyTimestamps == null) return 0;
        return this.latencyTimestamps.getOrDefault(stage, 0L);
    }

    /**
     * 获取两个阶段之间的延迟(毫秒)
     * @param fromStage 起始阶段
     * @param toStage 结束阶段
     * @return 延迟(毫秒), 任一阶段未记录则返回-1
     */
    public long getStageLatency(String fromStage, String toStage) {
        long from = getLatencyTimestamp(fromStage);
        long to = getLatencyTimestamp(toStage);
        if (from <= 0 || to <= 0) return -1;
        return to - from;
    }

    /**
     * 获取端到端总延迟(从Kafka接入到当前最新阶段)
     * @return 延迟(毫秒), 未记录t0则返回-1
     */
    public long getTotalLatency() {
        long t0 = getLatencyTimestamp("t0");
        if (t0 <= 0) return -1;
        long maxTs = t0;
        if (this.latencyTimestamps != null) {
            for (long ts : this.latencyTimestamps.values()) {
                if (ts > maxTs) maxTs = ts;
            }
        }
        return maxTs - t0;
    }

    /**
     * 交易对象的字符串表示
     * 用于日志输出和调试
     */
    @Override
    public String toString() {
        return String.format(
                "Transaction{step=%d, orig=%s, dest=%s, type=%s, amount=%.2f, oldBalance=%.2f, newBalance=%.2f, city=%s, ip=%s, hour=%d, device=%s/%s, channel=%s, risk=%s, abroad=%s, fraud=%s}",
                step, nameOrig, nameDest, type, amount, oldbalanceOrg, newbalanceOrig, city, ipSegment, transactionHour, deviceId, deviceType, payChannel, deviceRiskLevel, isAbroad, fraudType
        );
    }
}
