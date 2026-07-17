package com.fraud.detection.fusion;

import com.fraud.detection.model.Alert;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * 二级告警去重器 - 无状态版(零State)
 * 直接输出所有告警,由下游过滤
 */
public class FinalDeduplicator extends RichMapFunction<Alert, Alert> {

    @Override
    public Alert map(Alert alert) throws Exception {
        return alert;
    }
}
