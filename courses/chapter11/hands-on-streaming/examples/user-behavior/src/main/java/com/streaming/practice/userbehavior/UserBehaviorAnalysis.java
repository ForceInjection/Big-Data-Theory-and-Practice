package com.streaming.practice.userbehavior;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.compactor.DecoderBasedReader;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;
import org.apache.flink.connector.file.sink.compactor.RecordWiseFileCompactor;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * 电商用户行为实时分析示例
 * 
 * 本示例演示了电商场景下的实时用户行为分析：
 * 1. 从 Kafka 实时消费用户行为事件
 * 2. 解析 CSV/文本格式的用户行为数据
 * 3. 按用户和事件类型进行滚动窗口聚合统计
 * 4. 将统计结果写入文件系统（FileSink）并打印到控制台
 * 
 * 技术要点：
 * - Kafka 源连接器使用
 * - 事件时间处理和 Watermark 生成
 * - 滚动窗口与增量聚合
 * - FileSink 数据输出与控制台打印
 * 
 * @author Streaming Practice Team
 * @version 1.0.0
 */
public class UserBehaviorAnalysis {

    /**
     * 用户行为事件数据结构
     */
    public static class UserBehaviorEvent {
        public String userId;
        public String eventType; // view, click, add_to_cart, purchase
        public String productId;
        public long timestamp;
        public LocalDateTime eventTime;

        public UserBehaviorEvent() {
        }

        public UserBehaviorEvent(String userId, String eventType, String productId, long timestamp) {
            this.userId = userId;
            this.eventType = eventType;
            this.productId = productId;
            this.timestamp = timestamp;
            this.eventTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        }

        @Override
        public String toString() {
            return "UserBehaviorEvent{" +
                    "userId='" + userId + '\'' +
                    ", eventType='" + eventType + '\'' +
                    ", productId='" + productId + '\'' +
                    ", timestamp=" + timestamp +
                    ", eventTime=" + eventTime +
                    '}';
        }
    }

    /**
     * 用户行为统计结果
     */
    public static class UserBehaviorStats {
        public String userId;
        public String eventType;
        public long count;
        public long windowStart;
        public long windowEnd;
        public LocalDateTime processTime;

        public UserBehaviorStats() {
        }

        public UserBehaviorStats(String userId, String eventType, long count, long windowStart, long windowEnd) {
            this.userId = userId;
            this.eventType = eventType;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.processTime = LocalDateTime.now();
        }

        public String toSqlValues() {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            return String.format("('%s', '%s', %d, '%s', '%s', NOW())",
                    userId, eventType, count,
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(windowStart), ZoneId.systemDefault())
                            .format(formatter),
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(windowEnd), ZoneId.systemDefault()).format(formatter));
        }

        @Override
        public String toString() {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
            String windowStartStr = LocalDateTime.ofInstant(Instant.ofEpochMilli(windowStart), ZoneId.systemDefault()).format(formatter);
            String windowEndStr = LocalDateTime.ofInstant(Instant.ofEpochMilli(windowEnd), ZoneId.systemDefault()).format(formatter);
            
            return String.format("Window[%s, %s) count=%d, processTime=%s", 
                    windowStartStr, windowEndStr, count, processTime.format(formatter));
        }
    }

    // 定义侧输出流标签
    private static final OutputTag<UserBehaviorEvent> LATE_DATA_TAG = new OutputTag<UserBehaviorEvent>("late-data") {
    };

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Set parallelism to 1 to avoid idle source issues with single partition topic

        // 开启检查点 (每 10 秒触发一次)，确保 FileSink 能及时提交文件
        env.enableCheckpointing(10000);

        // 1. 创建 Kafka 数据源
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("user-behavior-events")
                .setGroupId("user-behavior-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 2. 从 Kafka 读取数据流
        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(), // Watermark will be assigned after parsing
                "Kafka Source");

        // 3. 数据转换和处理流水线
        SingleOutputStreamOperator<UserBehaviorStats> statsStream = kafkaStream
                // 解析 JSON 数据
                .flatMap(new FlatMapFunction<String, UserBehaviorEvent>() {
                    @Override
                    public void flatMap(String value, Collector<UserBehaviorEvent> out) throws Exception {
                        try {
                            UserBehaviorEvent event = parseUserBehaviorEvent(value);
                            if (event != null) {
                                out.collect(event);
                            }
                        } catch (Exception e) {
                            // 忽略解析错误的行
                        }
                    }
                })
                .map(event -> {
                    System.out.println("Processing event: " + event + " at system time: " + System.currentTimeMillis());
                    return event;
                })
                // 分配时间戳和水位线
                // 模拟场景：乱序延迟上界 L = 2秒
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehaviorEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, timestamp) -> event.timestamp))
                // 按用户ID和事件类型分组
                .keyBy(event -> event.userId + "-" + event.eventType)
                // 5秒滚动窗口: [00, 05)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                // 允许延迟 1秒: AL = 1s
                .allowedLateness(Duration.ofSeconds(1))
                // 超迟到数据进入侧输出
                .sideOutputLateData(LATE_DATA_TAG)
                // 聚合计算
                .aggregate(new CountAggregator(), new WindowResultFunction());

        // 4. 输出结果
        // 输出到文件
        final FileSink<UserBehaviorStats> sink = FileSink
                .forRowFormat(new Path("data/output/user-behavior-result"),
                        new SimpleStringEncoder<UserBehaviorStats>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .withInactivityInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();
        statsStream.sinkTo(sink).name("File Sink");

        // 同时打印到控制台，方便在日志中查看
        statsStream.print("Console Output").name("Console Sink");

        // 处理侧输出流（迟到数据）
        DataStream<UserBehaviorEvent> lateStream = statsStream.getSideOutput(LATE_DATA_TAG);
        lateStream.print("Late Data (Side Output)").name("Late Data Sink");

        // 5. 执行作业
        env.execute("Real-time User Behavior Analysis");
    }

    /**
     * 解析用户行为事件
     */
    private static UserBehaviorEvent parseUserBehaviorEvent(String jsonLine) {
        // 简化的 JSON 解析逻辑
        // 实际生产中应该使用 Jackson 或 Gson 等 JSON 库
        try {
            String[] parts = jsonLine.split(",");
            if (parts.length >= 4) {
                String userId = parts[0].trim();
                String eventType = parts[1].trim();
                String productId = parts[2].trim();
                long timestamp = Long.parseLong(parts[3].trim());

                return new UserBehaviorEvent(userId, eventType, productId, timestamp);
            }
        } catch (Exception e) {
            System.err.println("Failed to parse user behavior event: " + jsonLine);
        }
        return null;
    }

    /**
     * 计数聚合器
     */
    public static class CountAggregator implements AggregateFunction<UserBehaviorEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehaviorEvent value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /**
     * 窗口结果处理函数
     */
    public static class WindowResultFunction
            extends ProcessWindowFunction<Long, UserBehaviorStats, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<UserBehaviorStats> out) {
            Long count = elements.iterator().next();
            String[] keyParts = key.split("-");
            String userId = keyParts[0];
            String eventType = keyParts[1];
            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();
            out.collect(new UserBehaviorStats(userId, eventType, count, windowStart, windowEnd));
        }
    }
}
