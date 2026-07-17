package com.fraud.detection.util;

import com.alibaba.fastjson.JSONObject;
import com.fraud.detection.model.Transaction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 正常交易写入器
 * 将正常交易(Transaction.isFraud==0)写入Doris的fraud_normal_transaction表
 * 用于流量趋势分析和基线对比
 */
public class NormalTransactionWriter extends ProcessFunction<Transaction, String> {

    private transient long count;
    private final long emitEveryN;

    public NormalTransactionWriter() {
        this(1);
    }

    public NormalTransactionWriter(long emitEveryN) {
        this.emitEveryN = emitEveryN;
    }

    @Override
    public void open(Configuration parameters) {
        count = 0;
    }

    @Override
    public void processElement(Transaction tx, Context ctx, Collector<String> out) throws Exception {
        if (tx.isFraud != 0) return;

        count++;
        if (count % emitEveryN != 0) return;

        // 使用UUID保证tx_id全局唯一，避免Doris UNIQUE KEY覆盖导致数据丢失
        String txId = "TX_" + java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        JSONObject json = new JSONObject(true);
        json.put("tx_id", txId);
        json.put("account_id", tx.nameOrig);
        json.put("name_orig", tx.nameOrig);
        json.put("name_dest", tx.nameDest);
        json.put("type", tx.type);
        json.put("amount", tx.amount);
        json.put("oldbalance_org", tx.oldbalanceOrg);
        json.put("newbalance_orig", tx.newbalanceOrig);
        json.put("oldbalance_dest", tx.oldbalanceDest);
        json.put("newbalance_dest", tx.newbalanceDest);
        json.put("city", tx.city);
        json.put("device_id", tx.deviceId);
        json.put("device_type", tx.deviceType);
        json.put("pay_channel", tx.payChannel);
        json.put("ip_segment", tx.ipSegment);
        json.put("transaction_hour", tx.transactionHour);
        json.put("daily_tx_count", tx.dailyTxCount);
        json.put("device_risk_level", tx.deviceRiskLevel);
        json.put("is_abroad", tx.isAbroad);
        json.put("tx_time", formatTimeStr(tx.eventTime));
        json.put("dt", formatTimeStr(System.currentTimeMillis()));

        out.collect(json.toJSONString());
    }

    private static String formatTimeStr(long ts) {
        if (ts <= 0) return "00:00:00.000";
        java.util.Calendar cal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("Asia/Shanghai"));
        cal.setTimeInMillis(ts);
        int h = cal.get(java.util.Calendar.HOUR_OF_DAY);
        int m = cal.get(java.util.Calendar.MINUTE);
        int s = cal.get(java.util.Calendar.SECOND);
        int ms = cal.get(java.util.Calendar.MILLISECOND);
        return String.format("%02d:%02d:%02d.%03d", h, m, s, ms);
    }
}
