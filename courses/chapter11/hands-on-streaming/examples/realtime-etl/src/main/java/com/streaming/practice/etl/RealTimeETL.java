package com.streaming.practice.etl;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
// import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
// import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
// import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * 实时ETL处理示例
 * 
 * 本示例演示了实时数据ETL处理管道：
 * 1. 从Kafka多个topic读取原始数据
 * 2. 数据清洗、格式转换、字段映射
 * 3. 数据质量检查和异常处理
 * 4. 多目标输出（MySQL、文件、日志等）
 * 
 * 技术要点：
 * - 多数据源接入
 * - 数据清洗和转换
 * - 侧输出流异常处理
 * - 多目标数据输出
 * - 数据质量监控
 * 
 * @author Streaming Practice Team
 * @version 1.0.0
 */
public class RealTimeETL {

    // 定义侧输出流标签用于异常数据处理
    private static final OutputTag<String> ERROR_STREAM_TAG = new OutputTag<String>("error-stream") {
    };
    private static final OutputTag<String> AUDIT_STREAM_TAG = new OutputTag<String>("audit-stream") {
    };

    /**
     * 统一数据格式
     */
    public static class UnifiedDataRecord {
        public String recordId;
        public String dataType; // user, order, product, etc.
        public Map<String, Object> fields;
        public long timestamp;
        public LocalDateTime processTime;
        public String sourceSystem;

        public UnifiedDataRecord() {
            this.fields = new HashMap<>();
        }

        public UnifiedDataRecord(String recordId, String dataType, Map<String, Object> fields,
                long timestamp, String sourceSystem) {
            this.recordId = recordId;
            this.dataType = dataType;
            this.fields = fields;
            this.timestamp = timestamp;
            this.processTime = LocalDateTime.now();
            this.sourceSystem = sourceSystem;
        }

        public String toCsv() {
            return String.format("%s,%s,%s,%d,%s",
                    recordId, dataType, fields.toString(), timestamp, sourceSystem);
        }

        @Override
        public String toString() {
            return String.format("UnifiedDataRecord{id=%s, type=%s, fields=%s, time=%s}",
                    recordId, dataType, fields, processTime);
        }
    }

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 1. 创建Kafka数据源 - 用户数据
        KafkaSource<String> userSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("user-events")
                .setGroupId("etl-user-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        // 2. 创建Kafka数据源 - 订单数据
        KafkaSource<String> orderSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("order-events")
                .setGroupId("etl-order-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        // 3. 从多个数据源读取数据
        DataStream<String> userStream = env.fromSource(
                userSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "User Source");

        DataStream<String> orderStream = env.fromSource(
                orderSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "Order Source");

        // 4. 数据合并和处理
        DataStream<UnifiedDataRecord> processedStream = userStream.union(orderStream)
                .process(new DataValidationProcessFunction())
                .map(new DataTransformationFunction())
                .filter(new DataQualityFilter());

        // 5. 主输出流 - 写入MySQL
        // 注意：由于 Flink 2.0 移除了 SinkFunction 接口，而当前 JDBC Connector 尚未完全适配，
        // 因此暂时将 MySQL 写入改为打印到控制台。待 JDBC Connector 适配后可恢复。
        /*
        processedStream.sinkTo(
                JdbcSink.sink(
                        "INSERT INTO etl_processed_data (record_id, data_type, fields_json, timestamp, source_system, process_time) " +
                                "VALUES (?, ?, ?, ?, ?, ?)",
                        (statement, record) -> {
                            statement.setString(1, record.recordId);
                            statement.setString(2, record.dataType);
                            statement.setString(3, record.fields.toString());
                            statement.setTimestamp(4, java.sql.Timestamp.valueOf(
                                    LocalDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp), ZoneId.systemDefault())));
                            statement.setString(5, record.sourceSystem);
                            statement.setTimestamp(6, java.sql.Timestamp.valueOf(record.processTime));
                        },
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://mysql:3306/streaming_db")
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername("streaming_user")
                                .withPassword("streaming123")
                                .build()
                )
        ).name("MySQL Sink");
        */
        processedStream.print("MySQL Sink (Simulated): ").name("MySQL Sink");

        // 6. 获取侧输出流 - 错误数据处理
        DataStream<String> errorStream = ((SingleOutputStreamOperator<UnifiedDataRecord>) processedStream)
                .getSideOutput(ERROR_STREAM_TAG);

        errorStream.print("Error Records: ").name("Error Output");

        // 7. 获取侧输出流 - 审计日志
        DataStream<String> auditStream = ((SingleOutputStreamOperator<UnifiedDataRecord>) processedStream)
                .getSideOutput(AUDIT_STREAM_TAG);

        auditStream.print("Audit Log: ").name("Audit Output");

        // 8. 执行作业
        env.execute("Real-time ETL Processing Pipeline");
    }

    /**
     * 数据验证处理函数
     */
    public static class DataValidationProcessFunction extends ProcessFunction<String, String> {

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            try {
                // 基础数据验证
                if (value == null || value.trim().isEmpty()) {
                    ctx.output(ERROR_STREAM_TAG, "Empty record: " + value);
                    return;
                }

                // 格式验证
                if (!isValidFormat(value)) {
                    ctx.output(ERROR_STREAM_TAG, "Invalid format: " + value);
                    return;
                }

                // 审计日志
                ctx.output(AUDIT_STREAM_TAG, "Processing record: " + value.substring(0, Math.min(100, value.length())));

                out.collect(value);

            } catch (Exception e) {
                ctx.output(ERROR_STREAM_TAG, "Processing error: " + value + ", Error: " + e.getMessage());
            }
        }

        private boolean isValidFormat(String value) {
            // 简单的格式验证逻辑
            return value.contains(",") && value.split(",").length >= 3;
        }
    }

    /**
     * 数据转换函数
     */
    public static class DataTransformationFunction extends RichMapFunction<String, UnifiedDataRecord> {

        private transient Map<String, String> fieldMappings;

        @Override
        public void open(OpenContext openContext) throws Exception {
            // 初始化字段映射配置
            fieldMappings = new HashMap<>();
            fieldMappings.put("user_id", "userId");
            fieldMappings.put("order_id", "orderId");
            fieldMappings.put("product_id", "productId");
            fieldMappings.put("create_time", "timestamp");
        }

        @Override
        public UnifiedDataRecord map(String value) throws Exception {
            String[] parts = value.split(",");

            Map<String, Object> fields = new HashMap<>();
            String dataType = "unknown";
            String recordId = "unknown";
            long timestamp = System.currentTimeMillis();
            String sourceSystem = "kafka";

            // 简化的数据解析逻辑
            if (value.contains("user")) {
                dataType = "user";
                if (parts.length >= 3) {
                    recordId = parts[0];
                    fields.put("userId", parts[0]);
                    fields.put("userName", parts[1]);
                    fields.put("email", parts[2]);
                    if (parts.length > 3) {
                        timestamp = Long.parseLong(parts[3]);
                    }
                }
            } else if (value.contains("order")) {
                dataType = "order";
                if (parts.length >= 4) {
                    recordId = parts[0];
                    fields.put("orderId", parts[0]);
                    fields.put("userId", parts[1]);
                    fields.put("amount", Double.parseDouble(parts[2]));
                    fields.put("status", parts[3]);
                    if (parts.length > 4) {
                        timestamp = Long.parseLong(parts[4]);
                    }
                }
            }

            // 字段名标准化映射
            Map<String, Object> standardizedFields = new HashMap<>();
            for (Map.Entry<String, Object> entry : fields.entrySet()) {
                String standardizedKey = fieldMappings.getOrDefault(entry.getKey(), entry.getKey());
                standardizedFields.put(standardizedKey, entry.getValue());
            }

            return new UnifiedDataRecord(recordId, dataType, standardizedFields, timestamp, sourceSystem);
        }
    }

    /**
     * 数据质量过滤器
     */
    public static class DataQualityFilter implements FilterFunction<UnifiedDataRecord> {

        @Override
        public boolean filter(UnifiedDataRecord record) throws Exception {
            // 数据质量检查规则
            if (record.recordId == null || record.recordId.trim().isEmpty()) {
                return false;
            }

            if (record.timestamp <= 0) {
                return false;
            }

            if (record.fields == null || record.fields.isEmpty()) {
                return false;
            }

            // 特定业务规则检查
            if ("user".equals(record.dataType)) {
                return record.fields.containsKey("userId") && record.fields.containsKey("userName");
            } else if ("order".equals(record.dataType)) {
                return record.fields.containsKey("orderId") && record.fields.containsKey("amount");
            }

            return true;
        }
    }
}