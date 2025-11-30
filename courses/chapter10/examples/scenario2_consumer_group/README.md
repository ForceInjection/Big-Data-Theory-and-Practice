# 场景 2: 消费者组负载均衡

## 教学目的

演示 Kafka 消费者组机制，展示多个消费者如何协同工作分摊分区消费负载。

## 核心概念

- 消费者组 (Consumer Group)
- 分区再平衡 (Rebalance)
- 消费者协调器 (Consumer Coordinator)
- 负载均衡 (Load Balancing)

## 示例说明

本示例展示如何:

1. 创建多个消费者加入同一个消费者组
2. 观察分区在消费者间的自动分配
3. 演示消费者动态加入和退出时的再平衡过程

## 运行步骤

### 1. 启动 ZooKeeper 和 Kafka

确保已按照主 README 的说明启动 Docker 容器化的 ZooKeeper 和 Kafka 服务。

### 2. 运行生产者脚本

```bash
python3 group_producer.py
```

### 3. 启动多个消费者实例

打开多个终端窗口，分别运行：

终端 1:

```bash
python3 group_consumer.py --consumer-id consumer1
```

终端 2:

```bash
python3 group_consumer.py --consumer-id consumer2
```

终端 3:

```bash
python3 group_consumer.py --consumer-id consumer3
```

### 4. 观察负载均衡

- 观察每个消费者分配到的分区
- 尝试关闭一个消费者，观察再平衡过程
- 新消费者加入时，观察分区重新分配

## 预期输出

消费者输出示例:

```text
消费者 consumer1 分配到分区: [0, 1]
消费者 consumer2 分配到分区: [2, 3]
消费者 consumer3 分配到分区: [4]
```

当 consumer3 退出时:

```text
消费者 consumer1 重新分配到分区: [0, 1, 4]
消费者 consumer2 重新分配到分区: [2, 3]
```
