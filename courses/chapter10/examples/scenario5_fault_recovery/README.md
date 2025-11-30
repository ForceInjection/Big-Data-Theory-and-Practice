# 场景 5: 故障恢复演示

## 教学目的

演示 Kafka 的高可用性和故障恢复机制，包括副本同步、Leader 选举和消费者重平衡。

## 核心概念

- 副本复制 (Replication)
- ISR (In-Sync Replicas)
- Leader 选举 (Leader Election)
- 消费者重平衡 (Rebalance)
- 故障转移 (Failover)

## 示例说明

本示例展示如何:

1. 创建多副本的 Topic
2. 模拟 Broker 故障
3. 观察 Leader 自动切换
4. 演示消费者故障恢复
5. 验证数据不丢失

## 运行步骤

### 1. 启动 ZooKeeper 和 Kafka

确保已按照主 README 的说明启动 Docker 容器化的 ZooKeeper 和 Kafka 服务。

### 2. 运行故障恢复演示

```bash
python3 fault_recovery_demo.py
```

## 环境适配说明

由于使用单节点 Docker 容器环境，本示例已进行以下适配：

- **副本因子调整为 1** (原设计为 2)
- **移除了 min.insync.replicas 配置**
- **专注于演示基本的故障恢复概念**

## 演示内容

1. 创建测试 Topic (单副本)
2. 启动生产者和消费者线程
3. 模拟网络分区或服务中断
4. 观察消费者重连和继续消费
5. 验证消息处理的连续性

## 预期输出

正常运行时:

```text
生产者发送消息: Message 1 -> 分区 0
消费者收到消息: Message 1 from 分区 0
```

模拟故障后恢复:

```text
模拟网络中断...
消费者连接断开
消费者重新连接成功
继续消费: Message 2 from 分区 0
```

## 多 broker 环境建议

对于完整的高可用性演示，建议使用多节点的 Kafka 集群环境。
