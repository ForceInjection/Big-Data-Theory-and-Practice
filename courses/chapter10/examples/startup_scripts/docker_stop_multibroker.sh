#!/bin/bash

# 多 broker Kafka 集群停止脚本

echo "停止多 broker Kafka 集群..."

# 停止并删除 Kafka broker 容器
echo "停止 Kafka Broker 3..."
docker stop kafka-broker3 > /dev/null 2>&1
docker rm kafka-broker3 > /dev/null 2>&1

echo "停止 Kafka Broker 2..."
docker stop kafka-broker2 > /dev/null 2>&1
docker rm kafka-broker2 > /dev/null 2>&1

echo "停止 Kafka Broker 1..."
docker stop kafka-broker1 > /dev/null 2>&1
docker rm kafka-broker1 > /dev/null 2>&1

# 停止并删除 ZooKeeper 容器（如果由本脚本启动）
echo "停止 ZooKeeper..."
docker stop zookeeper > /dev/null 2>&1
docker rm zookeeper > /dev/null 2>&1

echo "多 broker Kafka 集群已停止并清理"