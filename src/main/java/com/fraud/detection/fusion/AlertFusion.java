package com.fraud.detection.fusion;

import com.fraud.detection.model.Alert;
import com.fraud.detection.model.CEPAlertTag;
import com.fraud.detection.model.Transaction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * CEP+ML告警融合器
 * 将CEP规则命中(Tag)注入到交易流
 * 与ML检测结果融合,生成统一的告警
 * 融合策略:CEP命中则直接标记,ML补充检测未知模式
 */
public class AlertFusion {

    // TTL: CEP标签过期时间(5分钟)
    private static final StateTtlConfig CEP_TAG_TTL = StateTtlConfig
            .newBuilder(Time.minutes(5))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();

    public static class CEPTransactionTagger
            extends CoProcessFunction<Transaction, CEPAlertTag, Transaction> {

        private ValueState<Boolean> isCepMatchedState;
        private ValueState<String> matchedRuleState;
        private ValueState<Long> matchTimeState;

    @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Boolean> matchedDesc =
                    new ValueStateDescriptor<>("cep-matched", Boolean.class);
            matchedDesc.enableTimeToLive(CEP_TAG_TTL);
            isCepMatchedState = getRuntimeContext().getState(matchedDesc);

            ValueStateDescriptor<String> ruleDesc =
                    new ValueStateDescriptor<>("matched-rule", String.class);
            ruleDesc.enableTimeToLive(CEP_TAG_TTL);
            matchedRuleState = getRuntimeContext().getState(ruleDesc);

            ValueStateDescriptor<Long> timeDesc =
                    new ValueStateDescriptor<>("match-time", Long.class);
            timeDesc.enableTimeToLive(CEP_TAG_TTL);
            matchTimeState = getRuntimeContext().getState(timeDesc);
        }

        @Override
        public void processElement1(Transaction tx, Context ctx,
                                    Collector<Transaction> out) throws Exception {

            Boolean isMatched = isCepMatchedState.value();
            Long matchTime = matchTimeState.value();

            if (isMatched != null && isMatched && matchTime != null) {
                long currentTime = tx.eventTime > 0 ? tx.eventTime : System.currentTimeMillis();
                long elapsed = currentTime - matchTime;
                if (elapsed >= 0 && elapsed <= 300000) {
                    tx.isCepMatched = true;
                    tx.cepRuleName = matchedRuleState.value();
                } else if (elapsed > 300000) {
                    clearState();
                    tx.isCepMatched = false;
                    tx.cepRuleName = "NONE";
                }
            } else {
                tx.isCepMatched = false;
                tx.cepRuleName = "NONE";
            }

            out.collect(tx);
        }

        @Override
        public void processElement2(CEPAlertTag tag, Context ctx,
                                    Collector<Transaction> out) throws Exception {
            isCepMatchedState.update(true);
            matchedRuleState.update(tag.ruleName);
            matchTimeState.update(tag.matchTimestamp);
        }

        private void clearState() throws Exception {
            isCepMatchedState.clear();
            matchedRuleState.clear();
            matchTimeState.clear();
        }
    }
}
