package com.fraud.detection.fusion;

import com.fraud.detection.model.Alert;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * 一级告警去重器 - 无状态版(零State,彻底解决Checkpoint超时)
 * 直接输出所有告警,由后续FinalDeduplicator去重
 */
public class AlertDeduplicator extends RichMapFunction<Alert, Alert> {

    @Override
    public Alert map(Alert alert) throws Exception {
        // 直接输出,不做去重
        return alert;
    }
}
