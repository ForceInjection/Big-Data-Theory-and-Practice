package com.streaming.practice.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

/**
 * 实时词频统计示例
 * 
 * 本示例演示了 Apache Flink 最基本的流处理功能：
 * 1. 从 Socket 源实时读取文本数据
 * 2. 对文本进行分词处理
 * 3. 按单词分组并统计出现次数
 * 4. 实时输出统计结果
 * 
 * 技术要点：
 * - DataStream API 基础使用
 * - 基本转换操作：flatMap、keyBy、sum
 * - 流处理执行环境配置
 * 
 * @author Streaming Practice Team
 * @version 1.0.0
 */
public class WordCountExample {

    /**
     * 主执行方法
     * 
     * @param args 命令行参数：[hostname] [port]
     * @throws Exception 执行异常
     */
    public static void main(String[] args) throws Exception {
        // 参数处理：默认使用 localhost:9999
        final String hostname = args.length > 0 ? args[0] : "localhost";
        final int port = args.length > 1 ? Integer.parseInt(args[1]) : 9999;

        // 1. 创建流处理执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 配置执行环境
        env.setParallelism(2); // 设置并行度为2
        env.enableCheckpointing(10000); // 开启检查点 (每 10 秒触发一次)
        
        // 2. 创建数据源：从 Socket 读取文本流
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");
        
        // 3. 数据处理流水线
        DataStream<Tuple2<String, Integer>> wordCounts = text
            // 3.1 将每行文本拆分为单词
            .flatMap(new Tokenizer())
            // 3.2 按单词分组（Flink 使用 keyBy 进行分组）
            .keyBy(value -> value.f0)
            // 3.3 对每个单词的计数进行累加
            .sum(1);

        // 4. 输出结果：
        // 4.1 打印到控制台
        wordCounts.print().setParallelism(1);
        
        // 4.2 同时写入文件 (覆盖模式)
        // 注意：实际生产中通常写入 Kafka、数据库或分布式文件系统(HDFS/S3)
        // 这里为了演示方便，写入本地文件系统
        final FileSink<Tuple2<String, Integer>> sink = FileSink
            .forRowFormat(new Path("data/output/wordcount-result"), new SimpleStringEncoder<Tuple2<String, Integer>>("UTF-8"))
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(Duration.ofSeconds(10))
                    .withInactivityInterval(Duration.ofSeconds(10))
                    .withMaxPartSize(1024 * 1024 * 1024)
                    .build())
            .build();
        wordCounts.sinkTo(sink).setParallelism(1);

        // 5. 执行作业
        env.execute("Real-time WordCount Example");
    }

    /**
     * 分词器 - 将文本行拆分为单词
     * 
     * 实现 FlatMapFunction 接口，将一行文本映射为多个 (word, 1) 元组
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 将文本转换为小写并去除首尾空格
            String normalizedText = value.toLowerCase().trim();
            
            // 使用正则表达式分割单词：匹配非字母数字字符作为分隔符
            String[] words = normalizedText.split("[^a-zA-Z0-9]+");
            
            // 输出每个单词及其初始计数
            for (String word : words) {
                if (word.length() > 0) { // 忽略空字符串
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }

    /**
     * 增强版分词器 - 支持过滤停用词
     * 
     * 在实际应用中，我们通常需要过滤掉常见的停用词
     */
    public static final class AdvancedTokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        
        // 常见英文停用词列表
        private static final String[] STOP_WORDS = {
            "the", "and", "or", "but", "in", "on", "at", "to", "for", 
            "of", "with", "by", "a", "an", "is", "are", "was", "were"
        };
        
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String normalizedText = value.toLowerCase().trim();
            String[] words = normalizedText.split("[^a-zA-Z0-9]+");
            
            for (String word : words) {
                if (isValidWord(word)) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
        
        /**
         * 检查是否为有效单词
         * 
         * @param word 待检查的单词
         * @return 如果是有效单词返回 true，否则返回 false
         */
        private boolean isValidWord(String word) {
            // 过滤空字符串和单个字符
            if (word.length() <= 1) {
                return false;
            }
            
            // 过滤停用词
            for (String stopWord : STOP_WORDS) {
                if (stopWord.equals(word)) {
                    return false;
                }
            }
            
            // 过滤纯数字
            if (word.matches("^\\d+$")) {
                return false;
            }
            
            return true;
        }
    }
}