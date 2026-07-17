package com.fraud.detection.cep;

import com.fraud.detection.model.Alert;
import com.fraud.detection.model.Transaction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * CEP规则管理器 - 定义8种已知欺诈模式的规则
 * 使用Flink CEP库进行复杂事件模式匹配
 * 覆盖:小额试探/链式洗钱/异地大额/分散转入/多渠道/凌晨掏空/小额掩护/团伙同IP
 */
public class CEPPatternManager {

    // 小额试探后大额转出: 1000-6000 试探 -> 2小时内 >30000 转境外/高风险设备
    public static DataStream<Alert> pattern1_SmallTestLargeTransfer(
            KeyedStream<Transaction, String> keyedStream) {

        Pattern<Transaction, ?> pattern = Pattern.<Transaction>begin("small_test")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return tx.amount >= 1000 && tx.amount <= 6000
                                && "TRANSFER".equals(tx.type);
                    }
                })
                .followedBy("large_transfer")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return tx.amount >= 30000
                                && ("TRANSFER".equals(tx.type) || "CASH_OUT".equals(tx.type))
                                && ("ABROAD".equals(tx.isAbroad) || "HIGH".equals(tx.deviceRiskLevel));
                    }
                })
                .within(Time.hours(2));

        return createAlertStream(keyedStream, pattern, "小额试探大额转出", 0.85);
    }

    // 多层链式洗钱 A->B->C: 大额转入 -> 中转 -> 境外提现
    public static DataStream<Alert> pattern2_ChainMoneyLaundering(
            KeyedStream<Transaction, String> keyedStream) {

        Pattern<Transaction, ?> pattern = Pattern.<Transaction>begin("A_to_B")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return tx.amount >= 50000
                                && "TRANSFER".equals(tx.type)
                                && ("HIGH".equals(tx.deviceRiskLevel) || "BANK_APP".equals(tx.payChannel));
                    }
                })
                .followedBy("B_to_C")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return "TRANSFER".equals(tx.type)
                                && tx.amount >= 30000;
                    }
                })
                .followedBy("C_cashout")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return "CASH_OUT".equals(tx.type)
                                && ("HIGH".equals(tx.deviceRiskLevel) || "ABROAD".equals(tx.isAbroad));
                    }
                })
                .within(Time.hours(2));

        return createAlertStream(keyedStream, pattern, "多层链式洗钱", 0.90);
    }

    // 异地跨设备突发大额: 正常城市小额交易 -> 3小时内切换到异常城市大额
    public static DataStream<Alert> pattern3_CrossCityLargeAmount(
            KeyedStream<Transaction, String> keyedStream) {

        Pattern<Transaction, ?> pattern = Pattern.<Transaction>begin("normal_pay")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return tx.amount <= 5000
                                && ("PAYMENT".equals(tx.type) || "TRANSFER".equals(tx.type))
                                && "LOW".equals(tx.deviceRiskLevel);
                    }
                })
                .followedBy("abnormal_cashout")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return tx.amount >= 30000
                                && ("CASH_OUT".equals(tx.type) || "TRANSFER".equals(tx.type))
                                && ("HIGH".equals(tx.deviceRiskLevel) || "ABROAD".equals(tx.isAbroad));
                    }
                })
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        // 确保城市或设备发生变更（异地跨设备核心逻辑）
                        return "MEDIUM".equals(tx.deviceRiskLevel) || "HIGH".equals(tx.deviceRiskLevel)
                                || "ABROAD".equals(tx.isAbroad);
                    }
                })
                .within(Time.hours(3));

        return createAlertStream(keyedStream, pattern, "异地跨设备突发大额", 0.85);
    }

    // 分散转入集中提现: 2笔以上中等金额转入 -> 单笔大额提现
    public static DataStream<Alert> pattern4_ScatterInConcentrateOut(
            KeyedStream<Transaction, String> keyedStream) {

        Pattern<Transaction, ?> pattern = Pattern.<Transaction>begin("scatter_in")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return "TRANSFER".equals(tx.type)
                                && tx.amount >= 4000 && tx.amount <= 30000;
                    }
                })
                .times(2)
                .consecutive()
                .followedBy("concentrate_out")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return "CASH_OUT".equals(tx.type)
                                && tx.amount >= 20000;
                    }
                })
                .within(Time.hours(2));

        return createAlertStream(keyedStream, pattern, "分散转入集中提现", 0.80);
    }

    // 多渠道轮番转账: 3笔不同渠道的转账在1小时内
    public static DataStream<Alert> pattern5_MultiChannelTransfer(
            KeyedStream<Transaction, String> keyedStream) {

        Pattern<Transaction, ?> pattern = Pattern.<Transaction>begin("channel_bank")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return "TRANSFER".equals(tx.type)
                                && tx.amount >= 5000;
                    }
                })
                .followedBy("channel_mini")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return "TRANSFER".equals(tx.type)
                                && tx.amount >= 5000;
                    }
                })
                .followedBy("channel_third")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return "TRANSFER".equals(tx.type)
                                && tx.amount >= 5000;
                    }
                })
                .within(Time.hours(1));

        return createAlertStream(keyedStream, pattern, "多渠道轮番转账", 0.80);
    }

    // 凌晨分批掏空: 凌晨0-5点连续3笔转账
    public static DataStream<Alert> pattern6_NightBatchDrain(
            KeyedStream<Transaction, String> keyedStream) {

        Pattern<Transaction, ?> pattern = Pattern.<Transaction>begin("batch1")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return (tx.transactionHour >= 0 && tx.transactionHour <= 5)
                                && ("TRANSFER".equals(tx.type) || "CASH_OUT".equals(tx.type))
                                && tx.amount >= 5000;
                    }
                })
                .followedBy("batch2")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return (tx.transactionHour >= 0 && tx.transactionHour <= 5)
                                && ("TRANSFER".equals(tx.type) || "CASH_OUT".equals(tx.type))
                                && tx.amount >= 5000;
                    }
                })
                .followedBy("batch3")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return tx.transactionHour >= 0 && tx.transactionHour <= 5
                                && ("TRANSFER".equals(tx.type) || "CASH_OUT".equals(tx.type));
                    }
                })
                .within(Time.hours(4));

        return createAlertStream(keyedStream, pattern, "凌晨分批掏空", 0.80);
    }

    // 小额掩护大额跑路: 2笔小额正常交易 -> 大额转出境外
    public static DataStream<Alert> pattern7_SmallCoverLargeRun(
            KeyedStream<Transaction, String> keyedStream) {

        Pattern<Transaction, ?> pattern = Pattern.<Transaction>begin("small_cover")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return tx.amount >= 100 && tx.amount <= 1000
                                && ("PAYMENT".equals(tx.type) || "TRANSFER".equals(tx.type));
                    }
                })
                .times(2)
                .consecutive()
                .followedBy("runaway_transfer")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return tx.amount >= 40000
                                && ("TRANSFER".equals(tx.type) || "CASH_OUT".equals(tx.type))
                                && ("HIGH".equals(tx.deviceRiskLevel) || "ABROAD".equals(tx.isAbroad));
                    }
                })
                .within(Time.hours(8));

        return createAlertStream(keyedStream, pattern, "小额掩护大额跑路", 0.85);
    }

    // 同IP批量转账: 30分钟内同一IP段2笔以上大额转账
    public static DataStream<Alert> pattern8_SameIPGangAttack(
            KeyedStream<Transaction, String> keyedStream) {

        Pattern<Transaction, ?> pattern = Pattern.<Transaction>begin("same_ip_tx")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return tx.amount >= 15000
                                && "TRANSFER".equals(tx.type);
                    }
                })
                .times(2)
                .consecutive()
                .within(Time.minutes(30));

        return createAlertStream(keyedStream, pattern, "团伙同IP批量作案", 0.85);
    }

    // 模式9: 账户被盗急速转账 - 境外/高风险设备 + 夜间 + 连续大额转出
    public static DataStream<Alert> pattern9_AccountHijackRapidTransfer(
            KeyedStream<Transaction, String> keyedStream) {

        Pattern<Transaction, ?> pattern = Pattern.<Transaction>begin("suspicious_login")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return ("ABROAD".equals(tx.isAbroad) || "HIGH".equals(tx.deviceRiskLevel))
                                && (tx.transactionHour <= 5 || tx.transactionHour >= 22)
                                && tx.amount >= 20000;
                    }
                })
                .followedBy("rapid_out")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return tx.amount >= 30000
                                && ("TRANSFER".equals(tx.type) || "CASH_OUT".equals(tx.type));
                    }
                })
                .within(Time.hours(1));

        return createAlertStream(keyedStream, pattern, "账户被盗急速转账", 0.90);
    }

    // 模式10: 虚假交易退款套利 - 连续PAYMENT后CASH_OUT
    public static DataStream<Alert> pattern10_FakeRefundArbitrage(
            KeyedStream<Transaction, String> keyedStream) {

        Pattern<Transaction, ?> pattern = Pattern.<Transaction>begin("fake_payment")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return "PAYMENT".equals(tx.type) && tx.amount >= 10000;
                    }
                })
                .times(2)
                .consecutive()
                .followedBy("cashout")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return "CASH_OUT".equals(tx.type) && tx.amount >= 30000;
                    }
                })
                .within(Time.hours(6));

        return createAlertStream(keyedStream, pattern, "虚假交易退款套利", 0.80);
    }

    // 模式11: 养卡提额异常消费 - 连续PAYMENT + 夜间 + 递增金额
    public static DataStream<Alert> pattern11_CardFarmingAbnormalSpend(
            KeyedStream<Transaction, String> keyedStream) {

        Pattern<Transaction, ?> pattern = Pattern.<Transaction>begin("small_payment")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return "PAYMENT".equals(tx.type)
                                && tx.amount >= 5000
                                && (tx.transactionHour <= 5 || tx.transactionHour >= 22);
                    }
                })
                .followedBy("medium_payment")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return "PAYMENT".equals(tx.type)
                                && tx.amount >= 20000;
                    }
                })
                .followedBy("large_payment")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return "PAYMENT".equals(tx.type)
                                && tx.amount >= 50000;
                    }
                })
                .within(Time.hours(12));

        return createAlertStream(keyedStream, pattern, "养卡提额异常消费", 0.80);
    }

    // 合并所有CEP规则输出
    public static DataStream<Alert> buildAllCEPPatterns(
            DataStream<Transaction> transactionStream,
            KeyedStream<Transaction, String> keyedStream) {

        DataStream<Alert> pattern1 = pattern1_SmallTestLargeTransfer(keyedStream);
        DataStream<Alert> pattern2 = pattern2_ChainMoneyLaundering(keyedStream);
        DataStream<Alert> pattern3 = pattern3_CrossCityLargeAmount(keyedStream);
        DataStream<Alert> pattern4 = transactionStream == null
                ? pattern4_ScatterInConcentrateOut(keyedStream)
                : pattern4_ScatterInConcentrateOut(transactionStream.keyBy(tx -> tx.nameDest));
        DataStream<Alert> pattern5 = pattern5_MultiChannelTransfer(keyedStream);
        DataStream<Alert> pattern6 = pattern6_NightBatchDrain(keyedStream);
        DataStream<Alert> pattern7 = pattern7_SmallCoverLargeRun(keyedStream);
        DataStream<Alert> pattern8 = transactionStream == null
                ? pattern8_SameIPGangAttack(keyedStream)
                : pattern8_SameIPGangAttack(transactionStream.keyBy(tx -> tx.ipSegment));
        DataStream<Alert> pattern9 = pattern9_AccountHijackRapidTransfer(keyedStream);
        DataStream<Alert> pattern10 = pattern10_FakeRefundArbitrage(keyedStream);
        DataStream<Alert> pattern11 = pattern11_CardFarmingAbnormalSpend(keyedStream);

        return pattern1.union(pattern2, pattern3, pattern4,
                pattern5, pattern6, pattern7, pattern8, pattern9, pattern10, pattern11);
    }

/**
     * 合并所有8条CEP规则输出为一个统一的告警流
     * 使用union操作将所有Pattern的Alert DataStream合并
     * @return 统一的CEP告警流
     */
    public static DataStream<Alert> buildAllCEPPatterns(
            KeyedStream<Transaction, String> keyedStream) {
        return buildAllCEPPatterns(null, keyedStream);
    }

    // 将CEP匹配结果转换为告警
    private static DataStream<Alert> createAlertStream(
            KeyedStream<Transaction, String> keyedStream,
            Pattern<Transaction, ?> pattern,
            String fraudType,
            double confidence) {

        PatternStream<Transaction> patternStream = CEP.pattern(keyedStream, pattern);

        return patternStream.select(new PatternSelectFunction<Transaction, Alert>() {
            @Override
            public Alert select(Map<String, List<Transaction>> pattern) throws Exception {
                Transaction firstTx = null;
                for (List<Transaction> txs : pattern.values()) {
                    if (!txs.isEmpty()) {
                        firstTx = txs.get(0);
                        break;
                    }
                }

                if (firstTx == null) {
                    return new Alert("UNKNOWN", fraudType, confidence, "CEP_RULE");
                }

                Alert alert = new Alert(
                        firstTx.nameOrig,
                        fraudType,
                        confidence,
                        "CEP_RULE"
                );
                alert.withTransactionContext(firstTx);
                alert.details = buildPatternDetails(pattern);
                return alert;
            }
        });
    }

/**
     * 构建Pattern命中详情字符串
     * 列出每步匹配的交易信息,用于告警解释
     */
    private static String buildPatternDetails(Map<String, List<Transaction>> pattern) {
        StringBuilder details = new StringBuilder();
        for (Map.Entry<String, List<Transaction>> entry : pattern.entrySet()) {
            details.append(entry.getKey()).append(": ");
            for (Transaction tx : entry.getValue()) {
                details.append(String.format(
                        "[%s, %.2f, %s, %s] ",
                        tx.type, tx.amount, tx.city, tx.deviceType
                ));
            }
        }
        return details.toString();
    }
}
