package com.fraud.detection.fusion;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 验证结果去重算子
 * 
 * 对 MLValidator 输出的验证 JSON 去重，按 (fraud_type, account_id) 去重，
 * 避免同一账户同一欺诈类型重复写入 Doris 导致数据膨胀。
 * 
 * 修复: 不再丢弃 FRAUD 记录，改为全部放行（去重逻辑改为只记录统计）
 */
public class ValidationDeduplicator extends ProcessFunction<String, String> {

    private transient MapState<String, String> seenAccounts;
    private transient ValueState<Long> dedupCount;
    private transient ValueState<Long> passCount;

    @Override
    public void open(Configuration parameters) {
        seenAccounts = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("validation-seen", String.class, String.class));
        dedupCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("val-dedup-count", Long.class));
        passCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("val-pass-count", Long.class));
    }

    @Override
    public void processElement(String json, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
        JSONObject obj = JSONObject.parseObject(json);
        String accountId = obj.getString("account_id");
        String fraudType = obj.getString("fraud_type");
        String actualLabel = obj.getString("actual_label");

        Long dc = dedupCount.value();
        dedupCount.update(dc == null ? 1L : dc + 1);

        if ("FRAUD".equals(actualLabel)) {
            String dedupKey = accountId + "|" + fraudType;
            String existing = seenAccounts.get(dedupKey);
            Long pc = passCount.value();
            passCount.update(pc == null ? 1L : pc + 1);
            
            // 修复: 所有 FRAUD 记录都放行，不再丢弃
            if (existing == null) {
                seenAccounts.put(dedupKey, "1");
                obj.put("dedup_status", "pass_first");
            } else {
                obj.put("dedup_status", "pass_duplicate");
            }
            out.collect(obj.toJSONString());
        } else {
            obj.put("dedup_status", "normal_pass");
            out.collect(obj.toJSONString());
        }
    }
}
