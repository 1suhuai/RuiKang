package com.fraud.detection.ml;

import com.fraud.detection.model.Transaction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 交易双向复制器
 * 将一笔交易(A->B)复制为两笔:
 * 1. 转出方视角(A): nameOrig=A, trackingAccount=B
 * 2. 转入方视角(B): nameOrig=B, trackingAccount=A
 * 确保转账双方都能被独立检测
 */
public class TransactionDuplicator extends RichFlatMapFunction<Transaction, Transaction> {

    @Override
    public void flatMap(Transaction tx, Collector<Transaction> out) {
        tx.trackingAccount = tx.nameOrig;
        out.collect(tx);
        Transaction destView = tx.copy();
        destView.trackingAccount = tx.nameDest;
        out.collect(destView);
    }
}
