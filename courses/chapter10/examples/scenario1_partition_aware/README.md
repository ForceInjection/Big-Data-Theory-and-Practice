# 场景 1: 分区感知生产消费

## 教学目的

演示 Kafka 分区机制，展示消息如何根据 key 被路由到特定分区，以及消费者如何从特定分区消费。

## 核心概念

- 分区 (Partition)
- 消息键 (Message Key)
- 分区器 (Partitioner)
- 消费者分配 (Consumer Assignment)

## 示例说明

本示例展示如何:

1. 使用特定 key 发送消息到指定分区
2. 查看消息在分区中的分布
3. 消费者订阅特定分区进行消费

## 运行步骤

### 1. 启动 ZooKeeper 和 Kafka

确保已按照主 README 的说明启动 Docker 容器化的 ZooKeeper 和 Kafka 服务。

### 2. 运行生产者脚本

```bash
python3 partition_producer.py
```

### 3. 运行消费者脚本

```bash
python3 partition_consumer.py
```

### 4. 观察输出

- 生产者会发送带不同 key 的消息到不同分区
- 消费者会显示从哪个分区接收到消息
- 相同 key 的消息总是路由到同一个分区

## 预期输出

生产者输出示例:

```text
发送消息: key=user1, value=message1 -> 分区 1
发送消息: key=user2, value=message2 -> 分区 2
发送消息: key=user1, value=message3 -> 分区 1
```

消费者输出示例:

```text
分区 1: 收到消息 key=user1, value=message1
分区 2: 收到消息 key=user2, value=message2
分区 1: 收到消息 key=user1, value=message3
```
