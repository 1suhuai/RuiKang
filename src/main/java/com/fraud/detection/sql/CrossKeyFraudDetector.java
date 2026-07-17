package com.fraud.detection.sql;

import com.fraud.detection.model.Alert;
import com.fraud.detection.model.Transaction;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * SQL跨账户欺诈检测器
 * 使用Flink SQL进行跨账户关联分析
 * 检测:分散转入/同IP团伙/ABC链式洗钱等跨账户模式
 * 补充CEP只能检测单账户模式的不足
 * 实现 A→B→C 链式洗钱检测，替代原 CEP Pattern2 的跨账户限制
 */
public class CrossKeyFraudDetector {

    /**
     * 检测 A→B→C 三层链式转账
     * 在 1 小时窗口内，当 A 转给 B，B 再转给 C 时触发告警
     */
    public static DataStream<Alert> detectChainTransferABC(
            DataStream<Transaction> transactionStream) {

        DataStream<Transaction> transfers = transactionStream
                .filter(tx -> "TRANSFER".equals(tx.type) && tx.amount > 10000)
                .name("Filter Large Transfers");

        return transfers
                .keyBy(tx -> tx.nameDest)
                .coGroup(transfers.keyBy(tx -> tx.nameOrig))
                .where(tx -> tx.nameDest)
                .equalTo(tx -> tx.nameOrig)
                .window(org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new ChainTransferABCCoGroup());
    }

    /**
     * 使用 Interval Join 检测 A→B→C 链式转账
     * 这是更接近 SQL JOIN 语义的实现
     */
    public static DataStream<Alert> detectChainTransferABCJoin(
            DataStream<Transaction> transactionStream) {

        DataStream<Transaction> transfers = transactionStream
                .filter(tx -> "TRANSFER".equals(tx.type) && tx.amount > 5000);

        return transfers
                .keyBy(tx -> tx.nameDest)
                .intervalJoin(transfers.keyBy(tx -> tx.nameOrig))
                .between(Time.hours(-2), Time.hours(2))
                .process(new ChainTransferJoinFunction());
    }

    /**
     * 检测同 IP 团伙作案
     * 放宽条件匹配10000.py: 同IP+同城市, 多账户大额转账, 不限制设备类型
     */
    public static DataStream<Alert> detectSameIPGang(
            DataStream<Transaction> transactionStream) {

        return transactionStream
                .filter(tx -> "TRANSFER".equals(tx.type)
                        && tx.amount > 8000
                        && ("HIGH".equals(tx.deviceRiskLevel) || tx.transactionHour >= 20))
                .keyBy(tx -> tx.ipSegment)
                .window(org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows.of(Time.minutes(30)))
                .process(new SameIPGangProcess());
    }

    /**
     * 检测分散转入集中转出（Flink SQL 风格）
     * 根据 10000.py 数据生成逻辑：
     * - 每组 4 个不同账户向同一目标转账（TRANSFER，金额 5000-25000）
     * - 然后目标账户 CASH_OUT 转出（金额是总转入的 95%）
     * - 共 20 组
     * 对应原 CEP Pattern4 的跨账户增强
     */
    public static DataStream<Alert> detectScatterInConcentrateOut(
            DataStream<Transaction> transactionStream) {

        return transactionStream
                .filter(tx -> "TRANSFER".equals(tx.type)
                        && tx.amount > 3000
                        && tx.amount < 30000)
                .keyBy(tx -> tx.nameDest)
                .window(org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows.of(Time.hours(2)))
                .process(new ScatterInConcentrateOutProcess());
    }

    /**
     * 检测分散转入集中转出完整闭环（Flink SQL 风格增强版）
     * 不仅检测多个账户向同一目标转账，还验证目标账户是否有 CASH_OUT 转出
     * 根据 10000.py 数据生成逻辑：
     * - 每组 4 个不同账户向同一目标转账（TRANSFER，金额 5000-25000）
     * - 然后目标账户 CASH_OUT 转出（金额是总转入的 95%）
     * - 共 20 组
     */
    public static DataStream<Alert> detectScatterInConcentrateOutWithCashOut(
            DataStream<Transaction> transactionStream) {

        DataStream<Transaction> transfers = transactionStream
                .filter(tx -> "TRANSFER".equals(tx.type)
                        && tx.amount > 3000
                        && tx.amount < 30000);

        DataStream<Transaction> cashOuts = transactionStream
                .filter(tx -> "CASH_OUT".equals(tx.type)
                        && tx.amount > 15000);

        return transfers
                .keyBy(tx -> tx.nameDest)
                .intervalJoin(cashOuts.keyBy(tx -> tx.nameOrig))
                .between(Time.seconds(-3600), Time.hours(3))
                .process(new ScatterToCashOutJoinFunction());
    }

    /**
     * A→B→C CoGroup 函数
     */
    public static class ChainTransferABCCoGroup
            implements CoGroupFunction<Transaction, Transaction, Alert> {

        @Override
        public void coGroup(Iterable<Transaction> aToB, Iterable<Transaction> bToC,
                            Collector<Alert> out) {
            Set<String> alertedPairs = new HashSet<>();

            for (Transaction ab : aToB) {
                for (Transaction bc : bToC) {
                    if (ab.nameDest.equals(bc.nameOrig)
                            && !ab.nameOrig.equals(bc.nameDest)
                            && !ab.nameOrig.equals(ab.nameDest)
                            && !bc.nameOrig.equals(bc.nameDest)) {

                        String pairKey = ab.nameOrig + "->" + bc.nameDest;
                        if (alertedPairs.contains(pairKey)) {
                            continue;
                        }
                        alertedPairs.add(pairKey);

                        double amountDecay = ab.amount > 0
                                ? (ab.amount - bc.amount) / ab.amount
                                : 0;

                        double timeDiff = (bc.eventTime - ab.eventTime) / 1000.0;

                        double confidence = computeConfidence(ab.amount, bc.amount, timeDiff);

                        if (confidence > 0.7) {
                Alert alert = new Alert(
                        ab.nameOrig,
                        "多层链式洗钱(SQL)",
                        confidence,
                        "SQL_CHAIN_PATTERN"
                );
                alert.withTransactionContext(ab);
                alert.behaviorPath = String.format("A:%s->B:%s->C:%s",
                        ab.nameOrig, ab.nameDest, bc.nameDest);
                alert.details = String.format(
                        "amountA:%.2f,amountB:%.2f,timeDiff:%.0fs,decayRate:%.2f",
                        ab.amount, bc.amount, timeDiff, amountDecay
                );
                out.collect(alert);
            }
                    }
                }
            }
        }

        private double computeConfidence(double amountA, double amountB, double timeDiff) {
            double timeScore = timeDiff < 300 ? 0.35
                    : timeDiff < 1800 ? 0.25
                    : timeDiff < 3600 ? 0.15
                    : 0.05;
            double amountScore = amountA > 100000 ? 0.35
                    : amountA > 50000 ? 0.25
                    : amountA > 10000 ? 0.15
                    : 0.05;
            double decayRate = amountA > 0 ? Math.abs(amountA - amountB) / amountA : 0;
            double decayScore = decayRate < 0.15 ? 0.35
                    : decayRate < 0.3 ? 0.25
                    : 0.15;

            return Math.min(1.0, timeScore + amountScore + decayScore + 0.25);
        }
    }

    /**
     * Interval Join 函数 - 使用Flink状态进行分布式去重
     */
    public static class ChainTransferJoinFunction
            extends ProcessJoinFunction<Transaction, Transaction, Alert> {

        private transient ValueState<String> lastAlertedPair;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastAlertedPair = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last-alerted-pair", String.class));
        }

        @Override
        public void processElement(Transaction ab, Transaction bc, Context ctx, Collector<Alert> out) throws Exception {
            if (!ab.nameDest.equals(bc.nameOrig)
                    || ab.nameOrig.equals(bc.nameDest)
                    || ab.nameOrig.equals(ab.nameDest)
                    || bc.nameOrig.equals(bc.nameDest)) {
                return;
            }

            String pairKey = ab.nameOrig + "->" + bc.nameDest;
            String lastPair = lastAlertedPair.value();
            if (pairKey.equals(lastPair)) {
                return;
            }
            lastAlertedPair.update(pairKey);

            double amountDecay = ab.amount > 0
                    ? Math.abs(ab.amount - bc.amount) / ab.amount
                    : 0;

            double timeDiff = Math.abs(bc.eventTime - ab.eventTime) / 1000.0;

            double confidence = computeConfidence(ab.amount, bc.amount, timeDiff);

            if (confidence > 0.6) {
                Alert alert = new Alert(
                        ab.nameOrig,
                        "多层链式洗钱(SQL)",
                        confidence,
                        "SQL_CHAIN_PATTERN"
                );
                alert.withTransactionContext(ab);
                alert.behaviorPath = String.format("A:%s->B:%s->C:%s",
                        ab.nameOrig, ab.nameDest, bc.nameDest);
                alert.details = String.format(
                        "amountA:%.2f,amountB:%.2f,timeDiff:%.0fs,decayRate:%.2f",
                        ab.amount, bc.amount, timeDiff, amountDecay
                );
                out.collect(alert);
            }
        }

        private double computeConfidence(double amountA, double amountB, double timeDiff) {
            double timeScore = timeDiff < 300 ? 0.35
                    : timeDiff < 1800 ? 0.25
                    : timeDiff < 3600 ? 0.15
                    : 0.05;
            double amountScore = amountA > 100000 ? 0.35
                    : amountA > 50000 ? 0.25
                    : amountA > 10000 ? 0.15
                    : 0.05;
            double decayRate = amountA > 0 ? Math.abs(amountA - amountB) / amountA : 0;
            double decayScore = decayRate < 0.15 ? 0.35
                    : decayRate < 0.3 ? 0.25
                    : 0.15;

            return Math.min(1.0, timeScore + amountScore + decayScore + 0.25);
        }
    }

    /**
     * 同IP团伙 Process 函数
     */
    public static class SameIPGangProcess
            extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
            Transaction, Alert, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

        @Override
        public void process(String ipSegment,
                            Context context,
                            Iterable<Transaction> elements,
                            Collector<Alert> out) {
            Set<String> uniqueAccounts = new HashSet<>();
            double totalAmount = 0;
            long minTime = Long.MAX_VALUE;
            long maxTime = Long.MIN_VALUE;
            Transaction firstTx = null;

            for (Transaction tx : elements) {
                uniqueAccounts.add(tx.nameOrig);
                totalAmount += tx.amount;
                minTime = Math.min(minTime, tx.eventTime);
                maxTime = Math.max(maxTime, tx.eventTime);
                if (firstTx == null) {
                    firstTx = tx;
                }
            }

            if (uniqueAccounts.size() >= 2 && totalAmount > 30000) {
                double confidence = Math.min(1.0, 0.45
                        + (uniqueAccounts.size() - 2) * 0.08
                        + (totalAmount / 400000.0));

                if (confidence > 0.6) {
                    Alert alert = new Alert(
                            firstTx.nameOrig,
                            "团伙同IP批量作案(SQL)",
                            confidence,
                            "SQL_IP_GANG"
                    );
                    alert.withTransactionContext(firstTx);
                    alert.behaviorPath = String.format("IP:%s,Accounts:%d,TotalAmount:%.2f",
                            ipSegment, uniqueAccounts.size(), totalAmount);
                    alert.details = String.format(
                            "uniqueAccounts:%d,totalAmount:%.2f,timeWindow:%.0fs,ip:%s",
                            uniqueAccounts.size(), totalAmount,
                            (maxTime - minTime) / 1000.0, ipSegment
                    );
                    out.collect(alert);
                }
            }
        }
    }

    /**
     * 分散转入集中转出 Process 函数
     * 在窗口内统计多个不同账户向同一目标转账
     */
    public static class ScatterInConcentrateOutProcess
            extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
            Transaction, Alert, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

        @Override
        public void process(String targetAccount,
                            Context context,
                            Iterable<Transaction> elements,
                            Collector<Alert> out) {
            Map<String, List<Transaction>> senderTxMap = new HashMap<>();
            double totalAmount = 0;
            long minTime = Long.MAX_VALUE;
            long maxTime = Long.MIN_VALUE;
            Transaction lastTx = null;

            for (Transaction tx : elements) {
                senderTxMap.computeIfAbsent(tx.nameOrig, k -> new ArrayList<>()).add(tx);
                totalAmount += tx.amount;
                minTime = Math.min(minTime, tx.eventTime);
                maxTime = Math.max(maxTime, tx.eventTime);
                lastTx = tx;
            }

            int uniqueSenders = senderTxMap.size();
            if (uniqueSenders >= 2 && totalAmount > 15000) {
                double avgAmount = totalAmount / uniqueSenders;
                double maxSenderAmount = 0;
                for (List<Transaction> senderTxs : senderTxMap.values()) {
                    double senderTotal = senderTxs.stream().mapToDouble(t -> t.amount).sum();
                    maxSenderAmount = Math.max(maxSenderAmount, senderTotal);
                }

                double confidence = Math.min(1.0, 0.4
                        + (uniqueSenders - 2) * 0.1
                        + (totalAmount / 100000.0)
                        + (maxSenderAmount / avgAmount > 3.0 ? 0.1 : 0));

                if (confidence > 0.5) {
                    Alert alert = new Alert(
                            targetAccount,
                            "分散转入集中转出(SQL)",
                            confidence,
                            "SQL_SCATTER_PATTERN"
                    );
                    alert.amount = totalAmount;
                    alert.timestamp = minTime;
                    if (lastTx != null) {
                        alert.withTransactionContext(lastTx);
                    }
                    alert.behaviorPath = String.format("Target:%s,Senders:%d,TotalAmount:%.2f",
                            targetAccount, uniqueSenders, totalAmount);
                    alert.details = String.format(
                            "uniqueSenders:%d,totalAmount:%.2f,avgAmount:%.2f,maxSenderAmount:%.2f,timeWindow:%.0fs",
                            uniqueSenders, totalAmount, avgAmount, maxSenderAmount,
                            (maxTime - minTime) / 1000.0
                    );
                    out.collect(alert);
                }
            }
        }
    }

    /**
     * 分散转入集中转出闭环检测 ProcessJoinFunction - 使用Flink状态进行分布式处理
     * 检测多个账户向同一目标转账后，目标账户进行 CASH_OUT 转出
     * CASH_OUT 金额 >= 总转入金额的 70% 即视为可疑（更灵活的阈值）
     */
    public static class ScatterToCashOutJoinFunction
            extends ProcessJoinFunction<Transaction, Transaction, Alert> {

        private transient ValueState<Double> targetTotalInState;
        private transient ValueState<Long> targetMinTimeState;
        private transient ValueState<Boolean> alertedState;

        @Override
        public void open(Configuration parameters) throws Exception {
            targetTotalInState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("target-total-in", Double.class));
            targetMinTimeState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("target-min-time", Long.class));
            alertedState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("alerted-target", Boolean.class));
        }

        @Override
        public void processElement(Transaction transfer, Transaction cashOut, Context ctx, Collector<Alert> out) throws Exception {
            String targetAccount = transfer.nameDest;

            Double totalIn = targetTotalInState.value();
            if (totalIn == null) {
                totalIn = 0.0;
            }
            totalIn += transfer.amount;
            targetTotalInState.update(totalIn);

            Long minTime = targetMinTimeState.value();
            if (minTime == null || transfer.eventTime < minTime) {
                targetMinTimeState.update(transfer.eventTime);
                minTime = transfer.eventTime;
            }

            Boolean alerted = alertedState.value();
            if (alerted != null && alerted) {
                return;
            }

            double cashOutAmount = cashOut.amount;
            double cashOutRatio = totalIn > 0 ? cashOutAmount / totalIn : 0;

            if (cashOutRatio >= 0.7) {
                alertedState.update(true);

                double timeDiff = Math.abs(cashOut.eventTime - minTime) / 1000.0;

                double confidence = Math.min(1.0, 0.45
                        + (cashOutRatio - 0.7) * 0.5
                        + (totalIn > 50000 ? 0.25 : 0.15)
                        + (timeDiff < 1800 ? 0.25 : timeDiff < 3600 ? 0.15 : 0.1));

                if (confidence > 0.5) {
                    Alert alert = new Alert(
                            targetAccount,
                            "分散转入集中提现(SQL)",
                            confidence,
                            "SQL_SCATTER_CASHOUT"
                    );
                    alert.amount = cashOutAmount;
                    alert.timestamp = minTime;
                    alert.withTransactionContext(cashOut);
                    alert.behaviorPath = String.format("Target:%s,TotalIn:%.2f,CashOut:%.2f,Ratio:%.0f%%",
                            targetAccount, totalIn, cashOutAmount, cashOutRatio * 100);
                    alert.details = String.format(
                            "totalIn:%.2f,cashOut:%.2f,ratio:%.2f,timeDiff:%.0fs,confidence:%.2f",
                            totalIn, cashOutAmount, cashOutRatio, timeDiff, confidence
                    );
                    out.collect(alert);
                }
            }
        }
    }
}
