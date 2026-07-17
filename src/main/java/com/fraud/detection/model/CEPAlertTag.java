package com.fraud.detection.model;

import java.io.Serializable;

/**
 * CEP规则命中标记类
 * 将CEP检测结果注入交易流,用于后续AlertFusion融合
 * 包含命中的规则名称和置信度
 */
public class CEPAlertTag implements Serializable {

    public String accountId;     // 账户ID
    public String ruleName;      // 规则名称
    public long matchTimestamp;  // 匹配时间
    public long windowEnd;       // 影响窗口结束时间

    public CEPAlertTag() {}

    public CEPAlertTag(String accountId, String ruleName, long matchTimestamp, long windowEnd) {
        this.accountId = accountId;
        this.ruleName = ruleName;
        this.matchTimestamp = matchTimestamp;
        this.windowEnd = windowEnd;
    }
}