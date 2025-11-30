# 场景 3: 精确一次语义

## 教学目的

演示 Kafka 的精确一次语义 (Exactly-Once Semantics) 实现，包括事务性生产和消费。

## 核心概念

- 精确一次语义 (EOS)
- 事务 (Transactions)
- 幂等生产者 (Idempotent Producer)
- 消费者偏移量事务提交

## 示例说明

本示例展示如何:

1. 配置幂等生产者避免重复消息
2. 使用事务保证生产消费的原子性
3. 处理消费者偏移量的事务提交
4. 演示故障恢复时的消息精确处理

## 运行步骤

### 1. 启动 ZooKeeper 和 Kafka

确保已按照主 README 的说明启动 Docker 容器化的 ZooKeeper 和 Kafka 服务。

### 2. 运行事务生产者

```bash
python3 transactional_producer.py
```

### 3. 运行事务消费者

```bash
python3 transactional_consumer.py
```

## 注意事项

⚠️ **单 broker 环境限制**:
由于使用单节点 Docker 容器环境，Kafka 事务的完整语义可能受限。
在实际多 broker 集群中，事务功能会更加完整。

### 预期行为

- 生产者会尝试初始化事务状态
- 在单 broker 环境中可能遇到超时限制
- 但仍可演示事务相关的基本概念和配置

## 多 broker 环境建议

对于完整的事务语义演示，建议使用多节点的 Kafka 集群环境。
