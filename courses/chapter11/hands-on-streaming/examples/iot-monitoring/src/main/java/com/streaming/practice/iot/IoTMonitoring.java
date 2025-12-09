package com.streaming.practice.iot;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * IoT设备实时监控示例
 * 
 * 本示例演示了物联网场景下的设备状态实时监控：
 * 1. 从Kafka实时消费设备传感器数据
 * 2. 检测设备异常状态（温度过高、离线等）
 * 3. 使用状态管理跟踪设备历史状态
 * 4. 实时触发告警通知
 * 
 * 技术要点：
 * - Flink状态管理（ValueState）
 * - 自定义告警规则引擎
 * - 多输出流处理
 * - 容错状态恢复
 * 
 * @author Streaming Practice Team
 * @version 1.0.0
 */
public class IoTMonitoring {

    /**
     * 设备传感器数据
     */
    public static class DeviceData {
        public String deviceId;
        public String deviceType; // temperature, humidity, pressure
        public double value;
        public long timestamp;
        public LocalDateTime eventTime;

        public DeviceData() {}

        public DeviceData(String deviceId, String deviceType, double value, long timestamp) {
            this.deviceId = deviceId;
            this.deviceType = deviceType;
            this.value = value;
            this.timestamp = timestamp;
            this.eventTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        }

        @Override
        public String toString() {
            return String.format("DeviceData{deviceId='%s', type='%s', value=%.2f, time=%s}",
                    deviceId, deviceType, value, eventTime);
        }
    }

    /**
     * 设备告警事件
     */
    public static class DeviceAlert {
        public String deviceId;
        public String alertType; // HIGH_TEMPERATURE, OFFLINE, ABNORMAL
        public String message;
        public double currentValue;
        public double threshold;
        public long timestamp;

        public DeviceAlert() {}

        public DeviceAlert(String deviceId, String alertType, String message, 
                         double currentValue, double threshold, long timestamp) {
            this.deviceId = deviceId;
            this.alertType = alertType;
            this.message = message;
            this.currentValue = currentValue;
            this.threshold = threshold;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("ALERT - %s: %s (当前值: %.2f, 阈值: %.2f)",
                    deviceId, message, currentValue, threshold);
        }
    }

    /**
     * 设备状态信息
     */
    public static class DeviceStatus {
        public String deviceId;
        public String status; // NORMAL, WARNING, CRITICAL
        public double lastValue;
        public long lastUpdateTime;
        public int alertCount;

        public DeviceStatus() {}

        public DeviceStatus(String deviceId, String status, double lastValue, 
                           long lastUpdateTime, int alertCount) {
            this.deviceId = deviceId;
            this.status = status;
            this.lastValue = lastValue;
            this.lastUpdateTime = lastUpdateTime;
            this.alertCount = alertCount;
        }
    }

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 1. 创建Kafka数据源
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("iot-device-data")
                .setGroupId("iot-monitoring-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        // 2. 从Kafka读取数据流
        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)),
                "Kafka IoT Source"
        );

        // 3. 数据转换和处理流水线
        DataStream<DeviceAlert> alertStream = kafkaStream
                .map(line -> parseDeviceData(line))
                .filter(data -> data != null)
                .keyBy(data -> data.deviceId)
                .flatMap(new DeviceMonitorFunction());

        // 4. 输出告警信息
        alertStream.print().name("Alert Sink");

        // 5. 执行作业
        env.execute("Real-time IoT Device Monitoring");
    }

    /**
     * 解析设备数据
     */
    private static DeviceData parseDeviceData(String line) {
        try {
            String[] parts = line.split(",");
            if (parts.length >= 4) {
                String deviceId = parts[0].trim();
                String deviceType = parts[1].trim();
                double value = Double.parseDouble(parts[2].trim());
                long timestamp = Long.parseLong(parts[3].trim());
                
                return new DeviceData(deviceId, deviceType, value, timestamp);
            }
        } catch (Exception e) {
            System.err.println("Failed to parse device data: " + line);
        }
        return null;
    }

    /**
     * 设备监控函数 - 使用状态管理检测异常
     */
    public static class DeviceMonitorFunction extends RichFlatMapFunction<DeviceData, DeviceAlert> {
        
        private transient ValueState<DeviceStatus> deviceStatusState;
        
        @Override
        public void open(OpenContext openContext) throws Exception {
            // 初始化设备状态
            ValueStateDescriptor<DeviceStatus> descriptor = new ValueStateDescriptor<>(
                    "deviceStatus", TypeInformation.of(DeviceStatus.class));
            deviceStatusState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(DeviceData data, Collector<DeviceAlert> out) throws Exception {
            DeviceStatus currentStatus = deviceStatusState.value();
            if (currentStatus == null) {
                currentStatus = new DeviceStatus(data.deviceId, "NORMAL", data.value, 
                                               data.timestamp, 0);
            }

            // 更新设备状态
            currentStatus.lastValue = data.value;
            currentStatus.lastUpdateTime = data.timestamp;

            // 检测温度异常
            if ("temperature".equals(data.deviceType)) {
                if (data.value > 80.0) { // 高温告警阈值
                    DeviceAlert alert = new DeviceAlert(
                            data.deviceId, 
                            "HIGH_TEMPERATURE", 
                            "设备温度过高", 
                            data.value, 
                            80.0, 
                            System.currentTimeMillis()
                    );
                    out.collect(alert);
                    currentStatus.status = "CRITICAL";
                    currentStatus.alertCount++;
                } else if (data.value > 70.0) { // 警告阈值
                    currentStatus.status = "WARNING";
                } else {
                    currentStatus.status = "NORMAL";
                }
            }

            // 检测设备离线（超过5分钟无数据）
            long timeSinceLastUpdate = System.currentTimeMillis() - currentStatus.lastUpdateTime;
            if (timeSinceLastUpdate > 5 * 60 * 1000 && currentStatus.status.equals("NORMAL")) {
                DeviceAlert alert = new DeviceAlert(
                        data.deviceId,
                        "OFFLINE",
                        "设备可能离线",
                        data.value,
                        0,
                        System.currentTimeMillis()
                );
                out.collect(alert);
                currentStatus.status = "OFFLINE";
            }

            // 保存更新后的状态
            deviceStatusState.update(currentStatus);
        }
    }
}