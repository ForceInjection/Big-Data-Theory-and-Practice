# 流式计算动手实践系列

## 项目概述

本实践系列基于《从 ETL 到流式计算入门》课程设计，提供从基础到进阶的流式计算实践体验。通过 Docker 容器化技术，您可以在单机环境下快速搭建完整的流式计算环境，并运行多个典型业务场景的示例。

---

## 核心特性

- **单机环境部署**：基于 Docker Compose 一键部署完整流式计算栈
- **多场景示例**：涵盖 5 个典型流式计算业务场景
- **开箱即用**：提供完整的运行脚本和测试数据
- **详细文档**：每个示例都有详细的技术说明和代码注释
- **生产就绪**：代码遵循最佳实践，可直接用于生产环境

---

## 技术栈

| **组件**       | **版本**                     | **用途**     |
| -------------- | ---------------------------- | ------------ |
| Apache Flink   | 运行镜像: `flink:2.1-java11` | 流处理引擎   |
| Apache Kafka   | Confluent Platform `7.4.0`   | 消息队列     |
| Zookeeper      | Confluent Platform `7.4.0`   | 协调服务     |
| MySQL          | 8.0                          | 结果存储     |
| Redis          | 7.0 (alpine)                 | 实时状态存储 |
| Docker         | 20.10+                       | 容器化环境   |
| Docker Compose | 2.0+                         | 容器编排     |

---

## 实践场景与运行示例

### 1. 实时词频统计 (WordCount)

- **场景描述**：实时统计文本流中的单词出现频率
- **技术要点**：基础 DataStream API、窗口操作、状态管理
- **业务价值**：理解流处理的基本概念和编程模式

**运行指南：**

这是一个基础入门示例，演示了从 Socket 读取文本流，进行实时分词和计数，并将结果写入文件系统。

**运行脚本：**

```bash
./scripts/run-wordcount.sh
```

**脚本说明：**

- **输入数据**：脚本会自动读取 `data/input/wordcount-input.txt` 文件内容（如果不存在会自动生成）。
- **数据发送**：脚本自动在容器内启动 `nc` (Netcat) 于 9999 端口，持续推送文本流。
- **结果输出**：作业运行结果保存在 `data/output/wordcount-result` 目录下。

**查看结果：**

脚本会自动轮询输出目录，当结果文件生成时，会在终端显示前 20 行内容。

### 2. 电商实时用户行为分析

- **场景描述**：实时分析用户浏览行为，并演示流处理中的乱序与延迟数据处理机制
- **技术要点**：
  - **Watermark 机制**：处理乱序事件
  - **Allowed Lateness**：处理允许范围内的迟到数据
  - **Side Output**：捕获并处理严重迟到的数据
  - **滚动窗口**：基于事件时间的窗口聚合
- **业务价值**：保证数据准确性的同时处理网络延迟和乱序问题

**运行指南：**

这是一个进阶示例，专门用于演示 Flink 的 Watermark 机制。它会生成一组精心设计的时间序列数据，模拟正常、乱序、迟到和严重迟到等多种场景。

**运行脚本：**

```bash
./scripts/run-user-behavior.sh
```

**脚本说明：**

- **数据生成**：自动生成包含特定时间戳的测试序列（E01, E03, EX, E02_Late, E_Final, E_SuperLate）。
- **执行流程**：先提交 Flink 作业，然后逐条发送数据并模拟真实延迟，确保 Watermark 正常推进。
- **验证报告**：脚本执行完毕后，会自动生成一份 **Watermark 机制验证报告**，清晰展示窗口触发和迟到数据丢弃的情况。

**查看结果：**

脚本运行结束后，会在终端直接打印如下格式的验证报告：

```text
📊 Watermark 机制验证报告:
--------------------------------------------------------------------------------
| 类别          | 窗口/事件时间       | 统计值 | 触发说明                      |
|---------------|---------------------|--------|-------------------------------|
| 窗口输出      | [XX:XX:00, XX:XX:05) | 2      | 首次触发 (Watermark >= End)   |
| 窗口输出      | [XX:XX:00, XX:XX:05) | 3      | 迟到更新 (Allowed Lateness)   |
| 侧输出流      | EventTime: XX:XX:02  | Drop   | 严重迟到 (Side Output)        |
--------------------------------------------------------------------------------
```

### 3. 金融交易实时风控

- **场景描述**：实时检测异常交易和欺诈行为
- **技术要点**：状态后端、精确一次语义、规则引擎集成
- **业务价值**：风险控制、资金安全保护

**运行脚本：**

```bash
# 启动环境后运行风控示例脚本，自动生成并发送交易数据到 Kafka
./scripts/run-fraud-detection.sh
```

**脚本说明：**

- **输入数据**：脚本自动生成模拟交易数据并写入 Kafka Topic `transactions`。
- **结果输出**：告警结果写入 `data/output/fraud-alerts`，包含 `part-*` 或 `.inprogress` 文件。
- **作业监控**：可在 Flink Web UI `http://localhost:8081` 查看作业状态与任务明细。
- **Topic 管理**：如 `transactions` 不存在，脚本会自动创建该 Topic。

**查看结果：**

脚本会自动轮询输出目录，展示最新告警文件的前 20 行内容。

### 4. IoT 设备实时监控

- **场景描述**：实时监控物联网设备状态和告警
- **技术要点**：时间序列处理、自定义源/接收器、告警规则
- **业务价值**：设备健康管理、预测性维护

**运行脚本：**

```bash
# 启动环境后运行 IoT 监控示例脚本，自动生成并发送设备数据到 Kafka
./scripts/run-iot-monitoring.sh
```

**脚本说明：**

- **输入数据**：脚本自动生成模拟设备数据并写入 Kafka Topic `iot-device-data`。
- **结果输出**：告警结果写入 `data/output/iot-alerts`，包含 `part-*` 或 `.inprogress` 文件。
- **作业监控**：可在 Flink Web UI `http://localhost:8081` 查看作业状态与任务明细。
- **Topic 管理**：如 `iot-device-data` 不存在，脚本会自动创建该 Topic。

**查看结果：**

脚本会自动轮询输出目录，展示最新告警文件的前 20 行内容。

### 5. 实时数据 ETL 管道

- **场景描述**：构建实时数据清洗和转换管道
- **技术要点**：数据质量检查、格式转换、多目标输出
- **业务价值**：实时数据仓库、数据湖集成

**运行脚本：**

```bash
# 启动环境后运行 ETL 示例脚本，自动生成并发送用户/订单数据到 Kafka
./scripts/run-realtime-etl.sh
```

**脚本说明：**

- **输入数据**：脚本自动生成模拟用户数据写入 `user-events`，订单数据写入 `order-events`。
- **结果输出**：清洗与转换后的统一记录写入 `data/output/realtime-etl`，包含 `part-*` 或 `.inprogress` 文件。
- **作业监控**：可在 Flink Web UI `http://localhost:8081` 查看作业状态与任务明细。
- **Topic 管理**：如 `user-events` 或 `order-events` 不存在，脚本会自动创建。

**查看结果：**

脚本会自动轮询输出目录，展示最新结果文件的前 20 行内容。

---

## 快速开始

### 环境要求

- Docker 20.10+ 和 Docker Compose 2.0+
- 4GB+ 可用内存
- Java 11+ (用于本地开发)

### 一键启动

```bash
# 进入项目目录
cd courses/chapter11/hands-on-streaming

# 启动所有服务（包含 Kafka、Flink、MySQL、Redis 等）
./scripts/start-environment.sh

# 运行基础示例
./scripts/run-wordcount.sh

# 运行用户行为分析示例
./scripts/run-user-behavior.sh
```

---

## 目录结构

```text
hands-on-streaming/
├── docker-compose.yml          # Docker 编排文件
├── scripts/                    # 运行脚本
├── config/                     # 配置文件
├── data/                       # 测试数据
├── examples/                   # 示例代码
│   ├── wordcount/              # 词频统计示例
│   ├── user-behavior/          # 用户行为分析
│   ├── fraud-detection/        # 风控检测
│   ├── iot-monitoring/         # IoT 监控
│   └── realtime-etl/           # 实时 ETL
└── README.md                   # 项目说明
```

---

## 监控和调试

### Flink Web UI

- 地址：http://localhost:8081
- 功能：作业监控、任务管理、日志查看

### Kafka UI

- 地址：http://localhost:8082
- 功能：主题管理、消息浏览、消费者监控

### 日志查看

```bash
# 查看 Flink 日志
docker-compose logs flink-jobmanager

# 查看 Kafka 日志
docker-compose logs kafka
```

---

## 常见问题

### Q: 端口冲突怎么办？

A: 修改 `docker-compose.yml` 中的端口映射配置

### Q: 内存不足怎么办？

A: 调整 Docker 内存限制或减少服务启动数量

### Q: 如何添加新的示例？

A: 参考现有示例结构，在 `examples/` 目录下创建新模块

---

## 参考文献

[1] Akidau, T., et al. "The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing." _Proceedings of the VLDB Endowment_, vol. 8, no. 12, pp. 1792-1803, 2015.

[2] Carbone, P., et al. "Apache Flink: Stream and Batch Processing in a Single Engine." _IEEE Data Eng. Bull._, vol. 38, no. 4, pp. 28-38, 2015.

[3] Kreps, J., Narkhede, N., & Rao, J. "Kafka: a Distributed Messaging System for Log Processing." _Proceedings of the NetDB_, 2011.
