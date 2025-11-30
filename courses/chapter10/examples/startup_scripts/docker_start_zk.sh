#!/bin/bash

# Docker 方式启动 ZooKeeper
echo "使用 Docker 启动 ZooKeeper..."

# 检查是否已安装 Docker
if ! command -v docker &> /dev/null; then
    echo "错误: 未找到 Docker，请先安装 Docker"
    exit 1
fi

# 停止已存在的 ZooKeeper 容器（如果存在）
echo "清理现有的 ZooKeeper 容器..."
docker rm -f kafka-zookeeper 2>/dev/null

# 启动 ZooKeeper 容器
echo "启动 ZooKeeper 容器..."
docker run -d \
    --name kafka-zookeeper \
    -p 2181:2181 \
    -e ZOOKEEPER_CLIENT_PORT=2181 \
    -e ZOOKEEPER_TICK_TIME=2000 \
    confluentinc/cp-zookeeper:7.4.0

# 等待 ZooKeeper 启动
echo "等待 ZooKeeper 启动 (10秒)..."
sleep 10

# 检查 ZooKeeper 是否正常启动
if docker ps | grep -q kafka-zookeeper; then
    echo "ZooKeeper 启动成功!"
    echo "连接地址: localhost:2181"
else
    echo "ZooKeeper 启动失败，请检查日志:"
    docker logs kafka-zookeeper
    exit 1
fi