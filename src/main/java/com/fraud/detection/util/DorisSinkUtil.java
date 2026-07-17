package com.fraud.detection.util;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Doris Sink创建工具类
 * 封装Flink Doris Connector的配置
 * 用于将DataStream写入Doris表
 * 
 * Label 生成策略: {prefix}_{UUID}_{timestamp}_{random}
 * 确保每次 Sink 调用生成绝对唯一的 label，解决 Checkpoint 恢复时 label 冲突问题
 */
public class DorisSinkUtil {

    private static final String DEFAULT_FENODES = "192.168.10.10:7030";
    private static final String DEFAULT_DATABASE = "final";
    private static final String DEFAULT_USERNAME = "root";
    private static final String DEFAULT_PASSWORD = "123456";

    // 全局递增计数器，确保即使同一毫秒调用也不冲突
    private static final AtomicLong GLOBAL_COUNTER = new AtomicLong(System.currentTimeMillis());

/**
     * 创建Doris Sink,指定表名和Label前缀
     * @param fenodes Doris FE节点地址
     * @param tableIdentifier 表标识(database.table)
     * @param labelPrefix Stream Load的Label前缀(用于去重)
     * @return DorisSink实例
     */
    public static DorisSink<String> getDorisSink(String table, String labelPrefix) {
        return getDorisSink(DEFAULT_DATABASE, table, labelPrefix);
    }

/**
     * 创建Doris Sink,指定表名和Label前缀
     * @param database 数据库名
     * @param table 表名
     * @param labelPrefix Stream Load的Label前缀(用于去重)
     * @return DorisSink实例
     */
    public static DorisSink<String> getDorisSink(String database, String table, String labelPrefix) {
        Properties props = new Properties();
        props.setProperty("max_filter_ratio", "0.01");
        props.setProperty("ignore_error", "false");
        props.setProperty("strict_mode", "false");
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        props.setProperty("timezone", "Asia/Shanghai");

        // 每次调用生成唯一Label前缀: prefix_uuid_timestamp_counter
        // 确保即使在同一JVM内多次创建Sink，或Checkpoint恢复时也不会冲突
        String uniqueId = UUID.randomUUID().toString().substring(0, 8);
        long counter = GLOBAL_COUNTER.incrementAndGet();
        String uniqueLabelPrefix = labelPrefix + "_" + uniqueId + "_" + counter;

        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes(DEFAULT_FENODES)
                        .setTableIdentifier(database + "." + table)
                        .setUsername(DEFAULT_USERNAME)
                        .setPassword(DEFAULT_PASSWORD)
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder()
                        .setLabelPrefix(uniqueLabelPrefix)
                        .setDeletable(false)
                        .setBufferCount(10)
                        .setBufferSize(1024 * 1024)   // 1MB 内部缓冲区,10个buffer=10MB/subtask
                        .setCheckInterval(5000)
                        .setMaxRetries(3)
                        .setStreamLoadProp(props)
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
    }
}
