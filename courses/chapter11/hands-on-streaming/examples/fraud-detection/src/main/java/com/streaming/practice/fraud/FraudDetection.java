package com.streaming.practice.fraud;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 实时欺诈检测示例
 * 
 * 本示例演示了使用Flink CEP（复杂事件处理）进行实时欺诈检测：
 * 1. 从Kafka实时消费交易数据
 * 2. 使用CEP模式匹配检测可疑交易模式
 * 3. 实时触发欺诈告警
 * 4. 多维度欺诈规则检测
 * 
 * 技术要点：
 * - Flink CEP复杂事件处理
 * - 多模式欺诈规则定义
 * - 实时告警触发机制
 * - 状态管理和容错
 * 
 * @author Streaming Practice Team
 * @version 1.0.0
 */
public class FraudDetection {

    /**
     * 交易数据模型
     */
    public static class Transaction {
        public String transactionId;
        public String userId;
        public String cardNumber;
        public double amount;
        public String merchant;
        public String location;
        public long timestamp;
        public String status; // SUCCESS, FAILED, PENDING

        public Transaction() {}

        public Transaction(String transactionId, String userId, String cardNumber, 
                         double amount, String merchant, String location, 
                         long timestamp, String status) {
            this.transactionId = transactionId;
            this.userId = userId;
            this.cardNumber = cardNumber;
            this.amount = amount;
            this.merchant = merchant;
            this.location = location;
            this.timestamp = timestamp;
            this.status = status;
        }

        @Override
        public String toString() {
            return String.format("Transaction{id=%s, user=%s, amount=%.2f, merchant=%s}",
                    transactionId, userId, amount, merchant);
        }
    }

    /**
     * 欺诈告警事件
     */
    public static class FraudAlert {
        public String alertId;
        public String transactionId;
        public String userId;
        public String alertType; // MULTIPLE_TRANSACTIONS, HIGH_AMOUNT, SUSPICIOUS_LOCATION
        public String description;
        public double totalAmount;
        public int transactionCount;
        public long timestamp;

        public FraudAlert() {}

        public FraudAlert(String alertId, String transactionId, String userId, 
                        String alertType, String description, double totalAmount,
                        int transactionCount, long timestamp) {
            this.alertId = alertId;
            this.transactionId = transactionId;
            this.userId = userId;
            this.alertType = alertType;
            this.description = description;
            this.totalAmount = totalAmount;
            this.transactionCount = transactionCount;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("🚨 FRAUD ALERT - %s: %s (用户: %s, 金额: %.2f, 交易数: %d)",
                    alertType, description, userId, totalAmount, transactionCount);
        }
    }

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 开启检查点，确保 FileSink 能提交成品文件
        env.enableCheckpointing(10000);

        // 1. 创建Kafka数据源
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("transactions")
                .setGroupId("fraud-detection-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 2. 从Kafka读取数据流
        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "Kafka Transaction Source"
        );

        // 3. 数据转换：字符串 -> Transaction对象
        DataStream<Transaction> transactionStream = kafkaStream
                .map(new MapFunction<String, Transaction>() {
                    @Override
                    public Transaction map(String value) throws Exception {
                        return parseTransaction(value);
                    }
                })
                .filter(transaction -> transaction != null && "SUCCESS".equals(transaction.status));

        transactionStream
                .map((MapFunction<Transaction, String>) Transaction::toString)
                .name("Debug Transaction Print")
                .print();

        // 4. 定义CEP欺诈检测模式
        
        // 模式1：短时间内多次交易（可能的盗刷）
        Pattern<Transaction, ?> multipleTransactionsPattern = Pattern.<Transaction>
                begin("transactions")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction transaction) {
                        return transaction.amount > 0;
                    }
                })
                .times(3)
                .consecutive()
                .within(Duration.ofMinutes(5));

        // 模式2：大额交易检测
        Pattern<Transaction, ?> highAmountPattern = Pattern.<Transaction>
                begin("transaction")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction transaction) {
                        return transaction.amount > 5000.0; // 大额交易阈值
                    }
                });

        // 5. 应用CEP模式到数据流
        PatternStream<Transaction> multipleTransactionsStream = CEP.pattern(
                transactionStream.keyBy(transaction -> transaction.userId),
                multipleTransactionsPattern
        );

        PatternStream<Transaction> highAmountStream = CEP.pattern(
                transactionStream.keyBy(transaction -> transaction.userId),
                highAmountPattern
        );

        // 6. 处理检测到的欺诈模式
        DataStream<FraudAlert> multipleTransactionsAlerts = multipleTransactionsStream.process(
                new PatternProcessFunction<Transaction, FraudAlert>() {
                    @Override
                    public void processMatch(Map<String, List<Transaction>> match, 
                                           Context ctx, Collector<FraudAlert> out) throws Exception {
                        List<Transaction> txs = match.get("transactions");
                        if (txs == null || txs.size() < 3) {
                            return;
                        }
                        Transaction first = txs.get(0);
                        Transaction second = txs.get(1);
                        Transaction third = txs.get(2);
                        double totalAmount = first.amount + second.amount + third.amount;
                        
                        FraudAlert alert = new FraudAlert(
                                "alert-" + System.currentTimeMillis(),
                                third.transactionId,
                                third.userId,
                                "MULTIPLE_TRANSACTIONS",
                                "短时间内多次交易检测",
                                totalAmount,
                                3,
                                System.currentTimeMillis()
                        );
                        out.collect(alert);
                    }
                }
        );

        DataStream<FraudAlert> highAmountAlerts = highAmountStream.process(
                new PatternProcessFunction<Transaction, FraudAlert>() {
                    @Override
                    public void processMatch(Map<String, List<Transaction>> match, 
                                           Context ctx, Collector<FraudAlert> out) throws Exception {
                        Transaction transaction = match.get("transaction").get(0);
                        
                        FraudAlert alert = new FraudAlert(
                                "alert-" + System.currentTimeMillis(),
                                transaction.transactionId,
                                transaction.userId,
                                "HIGH_AMOUNT",
                                "大额交易检测",
                                transaction.amount,
                                1,
                                System.currentTimeMillis()
                        );
                        out.collect(alert);
                    }
                }
        );

        // 7. 合并告警流并输出
        DataStream<FraudAlert> allAlerts = multipleTransactionsAlerts.union(highAmountAlerts);

        // 控制台输出，便于调试
        allAlerts.print().name("Fraud Alert Sink");

        // 文件输出，便于脚本轮询展示
        org.apache.flink.connector.file.sink.FileSink<FraudAlert> alertSink =
                org.apache.flink.connector.file.sink.FileSink
                        .forRowFormat(new org.apache.flink.core.fs.Path("data/output/fraud-alerts"),
                                new org.apache.flink.api.common.serialization.SimpleStringEncoder<FraudAlert>("UTF-8"))
                        .withRollingPolicy(
                                org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
                                        .builder()
                                        .withRolloverInterval(java.time.Duration.ofSeconds(10))
                                        .withInactivityInterval(java.time.Duration.ofSeconds(10))
                                        .withMaxPartSize(1024 * 1024 * 1024)
                                        .build())
                        .build();

        allAlerts.sinkTo(alertSink).name("File Sink");

        // 8. 执行作业
        env.execute("Real-time Fraud Detection");
    }

    /**
     * 解析交易数据
     */
    private static Transaction parseTransaction(String line) {
        try {
            String[] parts = line.split(",");
            if (parts.length >= 8) {
                return new Transaction(
                        parts[0].trim(), // transactionId
                        parts[1].trim(), // userId
                        parts[2].trim(), // cardNumber
                        Double.parseDouble(parts[3].trim()), // amount
                        parts[4].trim(), // merchant
                        parts[5].trim(), // location
                        Long.parseLong(parts[6].trim()), // timestamp
                        parts[7].trim()  // status
                );
            }
        } catch (Exception e) {
            System.err.println("Failed to parse transaction: " + line);
        }
        return null;
    }
}
