# Kafka 教学示例

本目录包含 Kafka 教学实践示例，涵盖核心概念演示和典型应用场景。所有示例已通过 Docker 容器化方式验证。

## 1. 目录结构

```bash
examples/
├── README.md                 # 说明文档
├── startup_scripts/          # 启动脚本
│   ├── docker_start_zk.sh   # ZooKeeper Docker 启动脚本
│   ├── docker_start_kafka.sh # Kafka 单节点 Docker 启动脚本
│   ├── docker_start_multibroker.sh # Kafka 多节点 Docker 启动脚本
│   ├── docker_stop_all.sh   # 停止所有 Docker 服务脚本
│   ├── docker_stop_multibroker.sh # 停止多节点集群脚本
│   └── multibroker_tools.sh # 多 broker 环境工具脚本
├── scenario1_partition_aware/ # 场景1: 分区感知生产消费
├── scenario2_consumer_group/ # 场景2: 消费者组负载均衡
├── scenario3_exactly_once/   # 场景3: 精确一次语义
│   └── transactional_multibroker_test.py # 多 broker 事务测试
├── scenario4_kafka_streams/  # 场景4: Kafka Streams 处理
└── scenario5_fault_recovery/ # 场景5: 故障恢复演示
    └── fault_recovery_multibroker.py # 多 broker 故障恢复演示
```

## 2. 使用说明

### 2.1 启动 ZooKeeper 和 Kafka 服务

```bash
cd startup_scripts/
chmod +x *.sh
./docker_start_zk.sh
./docker_start_kafka.sh
```

### 2.2 运行教学示例

按顺序运行以下场景示例：

1. **分区感知生产消费** - `scenario1_partition_aware/`
2. **消费者组负载均衡** - `scenario2_consumer_group/`
3. **精确一次语义** - `scenario3_exactly_once/`
4. **Kafka Streams 处理** - `scenario4_kafka_streams/`
5. **故障恢复演示** - `scenario5_fault_recovery/`

### 2.3 停止服务

```bash
./docker_stop_all.sh
```

---

## 3. 环境要求

- Docker Desktop 或 Docker Engine
- Python 3.6+
- kafka-python 库 (`pip3 install --break-system-packages kafka-python`)

## 4. 注意事项

1. 单 broker 环境限制：由于使用单节点 Docker 容器，某些高级功能（如事务完整语义）可能受限
2. Python 依赖：需要安装 kafka-python 库，建议使用虚拟环境
3. 端口占用：确保本地 2181 (ZooKeeper) 和 9092 (Kafka) 端口未被占用

## 5. 多 Broker 集群环境（高级功能）

对于需要完整 Kafka 功能的场景（事务、高可用性、副本复制等），提供了多 broker 集群配置：

### 5.1 启动多 broker 集群

```bash
cd startup_scripts/
chmod +x *.sh
./docker_start_multibroker.sh
```

### 5.2 多 broker 集群信息

- **Broker 1**: localhost:9092
- **Broker 2**: localhost:9093
- **Broker 3**: localhost:9094
- **ZooKeeper**: localhost:2181

### 5.3 多 broker 专用工具

```bash
# 创建多副本 topic
./multibroker_tools.sh create-topic my-topic 3 3

# 列出所有 topic
./multibroker_tools.sh list-topics

# 查看 topic 详情
./multibroker_tools.sh describe-topic my-topic

# 测试事务功能
./multibroker_tools.sh test-transactions
```

### 5.4 多 broker 专用示例

1. **完整事务测试**: `scenario3_exactly_once/transactional_multibroker_test.py`
2. **真正故障恢复**: `scenario5_fault_recovery/fault_recovery_multibroker.py`

### 5.5 停止多 broker 集群

```bash
./docker_stop_multibroker.sh
```

### 5.6 多 broker 环境优势

- ✅ 完整的事务语义支持
- ✅ 真正的副本复制和高可用性
- ✅ Leader 自动选举和故障转移
- ✅ 生产级配置和最佳实践
