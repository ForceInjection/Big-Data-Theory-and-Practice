# 场景 4: Kafka Streams 处理

## 教学目的

演示 Kafka Streams 流处理能力，展示实时数据转换和处理流水线。

## 核心概念

- 流处理 (Stream Processing)
- KStream 和 KTable
- 状态存储 (State Store)
- 窗口操作 (Windowing)
- 聚合操作 (Aggregation)

## 示例说明

本示例展示如何:

1. 创建简单的流处理应用
2. 进行实时数据转换和过滤
3. 使用窗口进行时间聚合
4. 维护处理状态

## 运行步骤

### 1. 启动 ZooKeeper 和 Kafka

确保已按照主 README 的说明启动 Docker 容器化的 ZooKeeper 和 Kafka 服务。

### 2. 运行数据生成器

```bash
python3 data_generator.py
```

### 3. 启动简单流处理器

```bash
python3 simple_stream_processor.py
```

### 4. 观察处理结果

数据生成器会持续产生用户事件数据，流处理器会实时处理并输出统计结果。

## 实现说明

由于原 Kafka Streams Java/Python 实现存在环境依赖问题，本示例提供了简化版的流处理器：

- `simple_stream_processor.py`: 使用基础 Kafka 消费者/生产者实现的简单流处理逻辑
- 演示了基本的流处理概念：实时消费、状态维护、结果输出

## 处理逻辑

1. 消费 `user-events` topic 中的用户行为数据
2. 统计每个用户的购买次数和总金额
3. 将统计结果输出到 `user-purchase-stats` topic
4. 同时在控制台输出处理结果

## 预期输出

数据生成器输出:

```text
生成用户事件: user1 购买商品A 金额 100
生成用户事件: user2 浏览商品B
生成用户事件: user1 购买商品C 金额 200
```

流处理器输出:

```text
处理统计: user1 购买次数=1 总金额=100
处理统计: user1 购买次数=2 总金额=300
```
