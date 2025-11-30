#!/bin/bash

# Docker 方式启动 Kafka
echo "使用 Docker 启动 Kafka..."

# 检查是否已安装 Docker
if ! command -v docker &> /dev/null; then
    echo "错误: 未找到 Docker，请先安装 Docker"
    exit 1
fi

# 检查 ZooKeeper 是否正在运行
if ! docker ps | grep -q kafka-zookeeper; then
    echo "警告: ZooKeeper 容器未运行"
    echo "请先运行 ./docker_start_zk.sh 启动 ZooKeeper"
    read -p "是否继续启动 Kafka? (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# 停止已存在的 Kafka 容器（如果存在）
echo "清理现有的 Kafka 容器..."
docker rm -f kafka-broker 2>/dev/null

# 获取 ZooKeeper 容器IP
ZOOKEEPER_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' kafka-zookeeper)

# 启动 Kafka 容器
echo "启动 Kafka 容器..."
docker run -d \
    --name kafka-broker \
    -p 9092:9092 \
    -e KAFKA_BROKER_ID=1 \
    -e KAFKA_ZOOKEEPER_CONNECT=${ZOOKEEPER_IP}:2181 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
    confluentinc/cp-kafka:7.4.0

# 等待 Kafka 启动
echo "等待 Kafka 启动 (15秒)..."
sleep 15

# 检查 Kafka 是否正常启动
if docker ps | grep -q kafka-broker; then
    echo "Kafka 启动成功!"
    echo "连接地址: localhost:9092"
    
    # 创建教学用的测试 topic
    echo "创建教学示例使用的 Topic..."
    docker exec kafka-broker kafka-topics --create --topic test-topic --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
    docker exec kafka-broker kafka-topics --create --topic user-behavior --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092
    docker exec kafka-broker kafka-topics --create --topic orders --partitions 2 --replication-factor 1 --bootstrap-server localhost:9092
    
    echo "Topic 创建完成:"
    docker exec kafka-broker kafka-topics --list --bootstrap-server localhost:9092
else
    echo "Kafka 启动失败，请检查日志:"
    docker logs kafka-broker
    exit 1
fi