#!/bin/bash

# 多 broker 环境工具脚本
# 用于在多 broker 集群中创建和管理 topic

echo "多 broker Kafka 集群工具脚本"

# 检查参数
if [ $# -eq 0 ]; then
    echo "用法: $0 <command>"
    echo "命令:"
    echo "  create-topic <topic_name> [partitions] [replication] - 创建多副本 topic"
    echo "  list-topics - 列出所有 topic"
    echo "  describe-topic <topic_name> - 查看 topic 详情"
    echo "  test-transactions - 测试事务功能"
    exit 1
fi

COMMAND=$1

case $COMMAND in
    "create-topic")
        if [ $# -lt 2 ]; then
            echo "错误: 需要指定 topic 名称"
            exit 1
        fi
        TOPIC_NAME=$2
        PARTITIONS=${3:-3}
        REPLICATION=${4:-3}
        
        echo "创建 topic: $TOPIC_NAME, 分区数: $PARTITIONS, 副本数: $REPLICATION"
        docker exec kafka-broker1 kafka-topics --create \
            --topic $TOPIC_NAME \
            --partitions $PARTITIONS \
            --replication-factor $REPLICATION \
            --bootstrap-server localhost:9092
        ;;
        
    "list-topics")
        echo "列出所有 topic:"
        docker exec kafka-broker1 kafka-topics --list \
            --bootstrap-server localhost:9092
        ;;
        
    "describe-topic")
        if [ $# -lt 2 ]; then
            echo "错误: 需要指定 topic 名称"
            exit 1
        fi
        TOPIC_NAME=$2
        echo "查看 topic 详情: $TOPIC_NAME"
        docker exec kafka-broker1 kafka-topics --describe \
            --topic $TOPIC_NAME \
            --bootstrap-server localhost:9092
        ;;
        
    "test-transactions")
        echo "测试事务功能..."
        echo "1. 创建测试 topic"
        docker exec kafka-broker1 kafka-topics --create \
            --topic transaction-test \
            --partitions 3 \
            --replication-factor 3 \
            --bootstrap-server localhost:9092
        
        echo "2. 查看 topic 详情（确认副本分布）"
        docker exec kafka-broker1 kafka-topics --describe \
            --topic transaction-test \
            --bootstrap-server localhost:9092
        
        echo "3. 事务功能已就绪，可以使用 Python 脚本测试"
        ;;
        
    *)
        echo "未知命令: $COMMAND"
        exit 1
        ;;
esac