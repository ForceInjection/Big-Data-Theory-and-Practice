#!/bin/bash

# 多 broker Kafka 集群启动脚本
# 使用 docker run 直接启动 3 个 Kafka broker 节点

echo "启动多 broker Kafka 集群 (3 个节点)..."

# 检查 Docker 是否运行
if ! docker info > /dev/null 2>&1; then
    echo "错误: Docker 没有运行，请先启动 Docker"
    exit 1
fi

# 启动 ZooKeeper (如果尚未运行)
if [ -z "$(docker ps -q -f name=zookeeper)" ]; then
    echo "启动 ZooKeeper..."
    docker run -d --name zookeeper \
        -p 2181:2181 \
        -e ZOOKEEPER_CLIENT_PORT=2181 \
        confluentinc/cp-zookeeper:7.4.0
    sleep 5
else
    echo "ZooKeeper 已经在运行"
fi

# 启动 Kafka Broker 1
echo "启动 Kafka Broker 1..."
docker run -d --name kafka-broker1 \
    -p 9092:9092 \
    -e KAFKA_BROKER_ID=1 \
    -e KAFKA_ZOOKEEPER_CONNECT=host.docker.internal:2181 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL=PLAINTEXT \
    -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2 \
    --add-host host.docker.internal:host-gateway \
    confluentinc/cp-kafka:7.4.0

# 启动 Kafka Broker 2
echo "启动 Kafka Broker 2..."
docker run -d --name kafka-broker2 \
    -p 9093:9093 \
    -e KAFKA_BROKER_ID=2 \
    -e KAFKA_ZOOKEEPER_CONNECT=host.docker.internal:2181 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL=PLAINTEXT \
    -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9093,PLAINTEXT_INTERNAL://0.0.0.0:29093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9093,PLAINTEXT_INTERNAL://kafka-broker2:29093 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT_INTERNAL \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2 \
    confluentinc/cp-kafka:7.4.0

# 启动 Kafka Broker 3
echo "启动 Kafka Broker 3..."
docker run -d --name kafka-broker3 \
    -p 9094:9094 \
    -e KAFKA_BROKER_ID=3 \
    -e KAFKA_ZOOKEEPER_CONNECT=host.docker.internal:2181 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL=PLAINTEXT \
    -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9094,PLAINTEXT_INTERNAL://0.0.0.0:29094 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9094,PLAINTEXT_INTERNAL://kafka-broker3:29094 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT_INTERNAL \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2 \
    --add-host host.docker.internal:host-gateway \
    confluentinc/cp-kafka:7.4.0

sleep 10

echo "多 broker Kafka 集群启动完成!"
echo "Broker 1: localhost:9092"
echo "Broker 2: localhost:9093"
echo "Broker 3: localhost:9094"
echo "ZooKeeper: localhost:2181"
echo ""
echo "使用 docker_stop_multibroker.sh 停止集群"