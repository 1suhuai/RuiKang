package com.fraud.detection.ml;

import com.fraud.detection.model.Transaction;
import com.fraud.detection.model.UserBehaviorSequence;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 行为序列构建器
按账户聚合交易记录,构建UserBehaviorSequence对象
使用KeyedProcessFunction维护每个账户的交易窗口
是ML检测的前置步骤
 */
public class BehaviorSequenceBuilder
        extends KeyedProcessFunction<String, Transaction, UserBehaviorSequence> {

    private static final StateTtlConfig BEHAVIOR_TTL = StateTtlConfig
            .newBuilder(Time.minutes(2))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();

    private ListState<Transaction> recentTransactions;
    private ValueState<Integer> transactionCount;
    private ValueState<Long> lastProcessTime;
    private ValueState<Long> cleanupTimer;
    private ValueState<Boolean> hasEmitted;

    private final int maxSequenceLength;
    private final long processInterval;
    private final boolean emitOncePerWindow;

    public BehaviorSequenceBuilder() {
        this(10, 60000, true);
    }

    public BehaviorSequenceBuilder(int maxSequenceLength, long processInterval) {
        this(maxSequenceLength, processInterval, true);
    }

    public BehaviorSequenceBuilder(int maxSequenceLength, long processInterval, boolean emitOncePerWindow) {
        this.maxSequenceLength = maxSequenceLength;
        this.processInterval = processInterval;
        this.emitOncePerWindow = emitOncePerWindow;
    }

    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<Transaction> txDesc =
                new ListStateDescriptor<>("recent-transactions", Transaction.class);
        txDesc.enableTimeToLive(BEHAVIOR_TTL);
        recentTransactions = getRuntimeContext().getListState(txDesc);

        ValueStateDescriptor<Integer> countDesc =
                new ValueStateDescriptor<>("tx-count", Integer.class);
        countDesc.enableTimeToLive(BEHAVIOR_TTL);
        transactionCount = getRuntimeContext().getState(countDesc);

        ValueStateDescriptor<Long> timeDesc =
                new ValueStateDescriptor<>("last-process-time", Long.class);
        timeDesc.enableTimeToLive(BEHAVIOR_TTL);
        lastProcessTime = getRuntimeContext().getState(timeDesc);

        ValueStateDescriptor<Long> cleanupTimerDesc =
                new ValueStateDescriptor<>("cleanup-timer", Long.class);
        cleanupTimerDesc.enableTimeToLive(BEHAVIOR_TTL);
        cleanupTimer = getRuntimeContext().getState(cleanupTimerDesc);

        ValueStateDescriptor<Boolean> hasEmittedDesc =
                new ValueStateDescriptor<>("has-emitted", Boolean.class);
        hasEmittedDesc.enableTimeToLive(BEHAVIOR_TTL);
        hasEmitted = getRuntimeContext().getState(hasEmittedDesc);
    }

    @Override
    public void processElement(
            Transaction tx,
            Context ctx,
            Collector<UserBehaviorSequence> out) throws Exception {

        if (!tx.isValid()) {
            return;
        }

        recentTransactions.add(tx);

        Integer count = transactionCount.value();
        if (count == null) {
            count = 0;
        }
        count++;
        transactionCount.update(count);

        long now = tx.eventTime;
        Long lastTime = lastProcessTime.value();
        boolean reachCount = count >= maxSequenceLength;
        boolean reachInterval = lastTime != null && now - lastTime >= processInterval && count >= 2;
        boolean reachSuspiciousShortSequence = count >= 2 && isSuspiciousShortSequence();

        if (reachCount || reachInterval || reachSuspiciousShortSequence) {
            buildAndEmitSequence(out);
            lastProcessTime.update(now);
            return;
        }

        Long registeredTimer = cleanupTimer.value();
        if (registeredTimer == null || registeredTimer <= now) {
            long timer = now + Math.min(processInterval, 2000L);
            ctx.timerService().registerEventTimeTimer(timer);
            cleanupTimer.update(timer);
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<UserBehaviorSequence> out) throws Exception {

        Integer count = transactionCount.value();
        if (count != null && count >= 2) {
            buildAndEmitSequence(out);
            lastProcessTime.update(timestamp);
        } else {
            clearState();
        }
    }

    private boolean isSuspiciousShortSequence() throws Exception {
        List<Transaction> txList = currentTransactions();
        if (txList.size() < 2) {
            return false;
        }

        int highRiskCount = 0;
        int abroadCount = 0;
        int nightCount = 0;
        int transferOrCashOutCount = 0;
        int paymentCount = 0;
        int cityChanges = 0;
        int deviceChanges = 0;
        int channelChanges = 0;
        double totalAmount = 0;
        double maxAmount = 0;
        double maxBalanceRatio = 0;
        double avgAmount = 0;

        Transaction previous = null;
        for (Transaction tx : txList) {
            totalAmount += tx.amount;
            maxAmount = Math.max(maxAmount, tx.amount);
            avgAmount = totalAmount / txList.size();
            if ("HIGH".equals(tx.deviceRiskLevel)) {
                highRiskCount++;
            }
            if ("ABROAD".equals(tx.isAbroad) || "YES".equals(tx.isAbroad) || "1".equals(tx.isAbroad)) {
                abroadCount++;
            }
            if (tx.transactionHour >= 22 || tx.transactionHour <= 6) {
                nightCount++;
            }
            if ("TRANSFER".equals(tx.type)) {
                transferOrCashOutCount++;
            }
            if ("CASH_OUT".equals(tx.type)) {
                transferOrCashOutCount++;
            }
            if ("PAYMENT".equals(tx.type)) {
                paymentCount++;
            }
            if (tx.oldbalanceOrg > 0) {
                maxBalanceRatio = Math.max(maxBalanceRatio, tx.amount / tx.oldbalanceOrg);
            }
            if (previous != null) {
                if (!previous.city.equals(tx.city)) {
                    cityChanges++;
                }
                if (!previous.deviceId.equals(tx.deviceId)) {
                    deviceChanges++;
                }
                if (!previous.payChannel.equals(tx.payChannel)) {
                    channelChanges++;
                }
            }
            previous = tx;
        }

        if (maxAmount >= 15000 && highRiskCount >= 1 && abroadCount >= 1) {
            return true;
        }
        if (nightCount >= 1 && highRiskCount >= 1 && transferOrCashOutCount >= 1 && maxAmount >= 10000) {
            return true;
        }
        if (nightCount >= 1 && highRiskCount >= 1 && maxAmount >= 15000) {
            return true;
        }
        if (cityChanges >= 1 && deviceChanges >= 1 && maxAmount >= 15000) {
            return true;
        }
        if (channelChanges >= 2 && transferOrCashOutCount >= 1 && totalAmount >= 20000) {
            return true;
        }
        if (maxBalanceRatio >= 0.45 && transferOrCashOutCount >= 1 && maxAmount >= 15000) {
            return true;
        }
        if (totalAmount >= 40000 && highRiskCount >= 1) {
            return true;
        }
        if (paymentCount >= 1 && transferOrCashOutCount >= 2 && totalAmount >= 25000) {
            return true;
        }
        if (txList.size() >= 3 && nightCount >= 1 && highRiskCount >= 1 && maxAmount >= 10000) {
            return true;
        }
        if (txList.size() >= 3 && totalAmount >= 50000) {
            return true;
        }
        if (avgAmount >= 15000 && highRiskCount >= 1 && maxAmount >= 20000) {
            return true;
        }
        if (abroadCount >= 1 && transferOrCashOutCount >= 1 && maxAmount >= 20000) {
            return true;
        }
        if (nightCount >= 2 && maxAmount >= 10000) {
            return true;
        }
        if (highRiskCount >= 2 && totalAmount >= 30000) {
            return true;
        }
        if (abroadCount >= 1 && nightCount >= 1 && maxAmount >= 15000) {
            return true;
        }
        if (totalAmount >= 80000) {
            return true;
        }
        if (maxAmount >= 40000 && transferOrCashOutCount >= 1) {
            return true;
        }
        if (channelChanges >= 3 && totalAmount >= 30000) {
            return true;
        }
        if (deviceChanges >= 2 && highRiskCount >= 1 && maxAmount >= 15000) {
            return true;
        }
        if (abroadCount >= 1 && maxAmount >= 25000) {
            return true;
        }
        if (nightCount >= 1 && totalAmount >= 30000 && maxAmount >= 15000) {
            return true;
        }
        if (highRiskCount >= 1 && maxBalanceRatio >= 0.7 && maxAmount >= 20000) {
            return true;
        }
        if (transferOrCashOutCount >= 3 && totalAmount >= 40000) {
            return true;
        }

        return false;
    }

    private void buildAndEmitSequence(Collector<UserBehaviorSequence> out) throws Exception {
        List<Transaction> txList = currentTransactions();

        if (txList.size() < 2) {
            clearState();
            return;
        }

        Boolean emitted = hasEmitted.value();
        if (emitOncePerWindow && Boolean.TRUE.equals(emitted)) {
            return;
        }

        txList.sort(Comparator.comparingLong(tx -> tx.eventTime));
        if (txList.size() > maxSequenceLength) {
            txList = new ArrayList<>(txList.subList(txList.size() - maxSequenceLength, txList.size()));
        }

        UserBehaviorSequence sequence = new UserBehaviorSequence();
        sequence.accountId = txList.get(0).trackingAccount != null
                ? txList.get(0).trackingAccount
                : txList.get(0).nameOrig;
        sequence.transactions = txList;
        sequence.sequenceStartTime = txList.get(0).eventTime;
        sequence.sequenceEndTime = txList.get(txList.size() - 1).eventTime;
        
        sequence.isFraud = 0;
        sequence.fraudType = "NORMAL";
        for (Transaction tx : txList) {
            if (tx.isFraud == 1 || tx.isFlaggedFraud == 1) {
                sequence.isFraud = 1;
                sequence.fraudType = tx.fraudType != null ? tx.fraudType : "UNKNOWN";
                break;
            }
        }
        
        sequence.buildBehaviorPath();
        sequence.computeFeatures();

        out.collect(sequence);
        clearState();
        hasEmitted.update(true);
    }

    private List<Transaction> currentTransactions() throws Exception {
        List<Transaction> txList = new ArrayList<>();
        for (Transaction tx : recentTransactions.get()) {
            txList.add(tx);
        }
        return txList;
    }

    private void clearState() throws Exception {
        recentTransactions.clear();
        transactionCount.clear();
        cleanupTimer.clear();
        lastProcessTime.clear();
    }
}
