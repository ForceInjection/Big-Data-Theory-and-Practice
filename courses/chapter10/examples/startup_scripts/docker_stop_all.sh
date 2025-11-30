#!/bin/bash

# Docker 方式停止所有服务
echo "停止 Docker 容器化的 Kafka 和 ZooKeeper 服务..."

# 检查是否已安装 Docker
if ! command -v docker &> /dev/null; then
    echo "错误: 未找到 Docker"
    exit 1
fi

# 停止 Kafka 容器
if docker ps | grep -q kafka-broker; then
    echo "停止 Kafka 容器..."
    docker stop kafka-broker
    docker rm kafka-broker
    echo "Kafka 容器已停止并移除"
else
    echo "Kafka 容器未运行"
fi

# 停止 ZooKeeper 容器
if docker ps | grep -q kafka-zookeeper; then
    echo "停止 ZooKeeper 容器..."
    docker stop kafka-zookeeper
    docker rm kafka-zookeeper
    echo "ZooKeeper 容器已停止并移除"
else
    echo "ZooKeeper 容器未运行"
fi

# 清理网络（如果创建了自定义网络）
echo "清理完成"