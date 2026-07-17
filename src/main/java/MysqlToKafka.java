import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.UUID;

/**
 * MySQL CDC → Kafka 数据同步
 *
 * 功能：
 * 1. 读取MySQL final.train_data → 发送到Kafka train_data Topic
 * 2. 读取MySQL final.test_data → 发送到Kafka test_data Topic
 *
 * 用途：
 * - train_data: 训练集数据，供MainJob消费训练ML模型
 * - test_data:  测试集数据，供ValidationJob消费验证模型效果
 */
public class MysqlToKafka {

    private static final String MYSQL_HOST = "192.168.10.10";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_DATABASE = "final";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "123456";

    private static final String KAFKA_SERVERS = "192.168.10.10:9092";
    private static final String TRAIN_TOPIC = "train_data";
    private static final String TEST_TOPIC = "test_data";

    private static final String TRAIN_TABLE = "final.train_data";
    private static final String TEST_TABLE = "final.test_data";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // ========== 1. 创建训练集MySQL CDC Source ==========
        MySqlSource<String> trainSource = createMySqlSource(TRAIN_TABLE, "train-cdc-source");

        DataStreamSource<String> trainMysqlStream = env.fromSource(
                trainSource, WatermarkStrategy.noWatermarks(), "MySQL_Train_Source"
        );

        SingleOutputStreamOperator<String> trainProcessed = trainMysqlStream.process(
                new DataProcessor("train")
        );

        trainProcessed.print("TRAIN_DATA: ");

        // ========== 2. 创建测试集MySQL CDC Source ==========
        MySqlSource<String> testSource = createMySqlSource(TEST_TABLE, "test-cdc-source");

        DataStreamSource<String> testMysqlStream = env.fromSource(
                testSource, WatermarkStrategy.noWatermarks(), "MySQL_Test_Source"
        );

        SingleOutputStreamOperator<String> testProcessed = testMysqlStream.process(
                new DataProcessor("test")
        );

        testProcessed.print("TEST_DATA: ");

        // ========== 3. 构建Kafka Sink ==========
        // 关键修复：为每个Sink创建独立的配置，确保client.id唯一
        KafkaSink<String> trainKafkaSink = createKafkaSink(TRAIN_TOPIC, "train-sink");
        KafkaSink<String> testKafkaSink = createKafkaSink(TEST_TOPIC, "test-sink");

        // ========== 4. 写入Kafka ==========
        trainProcessed.sinkTo(trainKafkaSink).name("Train_Kafka_Sink");
        testProcessed.sinkTo(testKafkaSink).name("Test_Kafka_Sink");

        System.out.println("========================================");
        System.out.println("MySQL CDC → Kafka 启动中...");
        System.out.println("MySQL: " + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE);
        System.out.println("Kafka: " + KAFKA_SERVERS);
        System.out.println("  训练集: " + TRAIN_TABLE + " → " + TRAIN_TOPIC);
        System.out.println("  测试集: " + TEST_TABLE + " → " + TEST_TOPIC);
        System.out.println("========================================");

        env.execute("MySQL CDC To Kafka - Train & Test Data");
    }

    /**
     * 创建MySQL CDC Source
     */
    private static MySqlSource<String> createMySqlSource(String tableList, String sourceName) {
        return MySqlSource.<String>builder()
                .hostname(MYSQL_HOST)
                .port(MYSQL_PORT)
                .databaseList(MYSQL_DATABASE)
                .tableList(tableList)
                .username(MYSQL_USER)
                .password(MYSQL_PASSWORD)
                // 修复：添加唯一的serverId，避免多个CDC源冲突
                .serverId(generateUniqueServerId())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(com.ververica.cdc.connectors.mysql.table.StartupOptions.initial())
                .build();
    }

    /**
     * 创建Kafka Sink
     * 使用AT_LEAST_ONCE + 幂等生产者，避免EXACTLY_ONCE的事务序列号冲突
     */
    private static KafkaSink<String> createKafkaSink(String topic, String sinkPrefix) {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("max.request.size", "10485760");
        kafkaProps.setProperty("acks", "all");
        kafkaProps.setProperty("retries", "10");
        kafkaProps.setProperty("retry.backoff.ms", "200");
        kafkaProps.setProperty("enable.idempotence", "true");
        kafkaProps.setProperty("max.in.flight.requests.per.connection", "5");

        // 禁用JMX注册，避免冲突
        kafkaProps.setProperty("jmx.register", "false");

        return KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // 使用AT_LEAST_ONCE避免事务性生产者的OUT_OF_ORDER_SEQUENCE_NUMBER错误
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(kafkaProps)
                .build();
    }

    /**
     * 生成唯一的serverId
     */
    private static String generateUniqueServerId() {
        // 生成5400-6400之间的随机数作为serverId
        return String.valueOf(5400 + (int)(Math.random() * 1000));
    }

    /**
     * 数据处理函数：将CDC格式转换为简单JSON
     */
    public static class DataProcessor extends ProcessFunction<String, String> {
        private final String dataType;

        public DataProcessor(String dataType) {
            this.dataType = dataType;
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            try {
                JSONObject json = JSON.parseObject(value);
                String op = json.getString("op");

                // 只处理insert和read操作（初始加载和新增数据）
                JSONObject data = new JSONObject();

                if ("c".equals(op) || "r".equals(op)) {
                    JSONObject after = json.getJSONObject("after");
                    if (after != null) {
                        data.putAll(after);
                        data.put("_data_type", dataType);
                        data.put("_op_type", "insert");
                        data.put("_timestamp", System.currentTimeMillis());
                        out.collect(data.toJSONString());
                    }
                } else if ("u".equals(op)) {
                    JSONObject after = json.getJSONObject("after");
                    if (after != null) {
                        data.putAll(after);
                        data.put("_data_type", dataType);
                        data.put("_op_type", "update");
                        data.put("_timestamp", System.currentTimeMillis());
                        out.collect(data.toJSONString());
                    }
                }
                // 忽略delete操作
            } catch (Exception e) {
                System.err.println("[" + dataType + "] JSON parse error: " + value);
                e.printStackTrace();
            }
        }
    }
}