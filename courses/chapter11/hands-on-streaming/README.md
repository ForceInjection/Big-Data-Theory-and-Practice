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

- **场景描述**：实时分析用户浏览、点击、购买行为
- **技术要点**：事件时间处理、滚动窗口、增量聚合
- **业务价值**：实时用户画像、个性化推荐基础

**运行指南：**

这是一个进阶示例，演示了如何从 Kafka 读取用户行为数据，计算每分钟的用户行为统计（PV/UV 等），并将结果写入文件系统。

**运行脚本：**

```bash
./scripts/run-user-behavior.sh
```

**脚本说明：**

- **输入数据**：自动生成模拟的用户行为数据（view, click, purchase 等）到 `data/input/user-behavior-input.csv`。
- **数据发送**：使用 Kafka Console Producer 将数据发送到 `user-behavior-events` Topic。
- **结果输出**：作业运行结果保存在 `data/output/user-behavior-result` 目录下。

**查看结果：**

脚本会自动轮询输出目录，显示最新的统计结果。

### 3. 金融交易实时风控

- **场景描述**：实时检测异常交易和欺诈行为
- **技术要点**：状态后端、精确一次语义、规则引擎集成
- **业务价值**：风险控制、资金安全保护

### 4. IoT 设备实时监控

- **场景描述**：实时监控物联网设备状态和告警
- **技术要点**：时间序列处理、自定义源/接收器、告警规则
- **业务价值**：设备健康管理、预测性维护

### 5. 实时数据 ETL 管道

- **场景描述**：构建实时数据清洗和转换管道
- **技术要点**：数据质量检查、格式转换、多目标输出
- **业务价值**：实时数据仓库、数据湖集成

---

## 快速开始

### 环境要求

- Docker 20.10+ 和 Docker Compose 2.0+
- 4GB+ 可用内存
- Java 11+ (用于本地开发)

### 一键启动

```bash
# 进入 hands-on-streaming 项目目录
cd hands-on-streaming

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
├── config/                    # 配置文件
├── data/                      # 测试数据
├── src/                       # 源代码
│   ├── wordcount/             # 词频统计示例
│   ├── user-behavior/         # 用户行为分析
│   ├── fraud-detection/       # 风控检测
│   ├── iot-monitoring/        # IoT 监控
│   └── realtime-etl/          # 实时 ETL
└── README.md                  # 项目说明
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

A: 参考现有示例结构，在 `src/` 目录下创建新模块

---

## 参考文献

[1] Akidau, T., et al. "The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing." _Proceedings of the VLDB Endowment_, vol. 8, no. 12, pp. 1792-1803, 2015.

[2] Carbone, P., et al. "Apache Flink: Stream and Batch Processing in a Single Engine." _IEEE Data Eng. Bull._, vol. 38, no. 4, pp. 28-38, 2015.

[3] Kreps, J., Narkhede, N., & Rao, J. "Kafka: a Distributed Messaging System for Log Processing." _Proceedings of the NetDB_, 2011.
