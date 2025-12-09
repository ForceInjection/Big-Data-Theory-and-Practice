#!/bin/bash

# 流式计算环境启动脚本
# 作者：流式计算实践项目
# 版本：1.0.0

echo "🚀 开始启动流式计算实践环境..."

# 检查 Docker 是否安装
if ! command -v docker &> /dev/null; then
    echo "❌ Docker 未安装，请先安装 Docker"
    exit 1
fi

# 检查 Docker Compose 是否安装
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose 未安装，请先安装 Docker Compose"
    exit 1
fi

# 创建必要的目录
echo "📁 创建项目目录结构..."
mkdir -p config data/input data/output logs

echo "🐳 启动 Docker 容器服务..."

# 启动所有服务
docker-compose up -d

# 等待服务启动
echo "⏳ 等待服务启动完成..."
sleep 30

# 检查服务状态
echo "🔍 检查服务状态..."
services=("zookeeper" "kafka" "flink-jobmanager" "mysql" "redis")

for service in "${services[@]}"; do
    if docker ps | grep -q "$service"; then
        echo "✅ $service 启动成功"
    else
        echo "❌ $service 启动失败"
    fi
done

# 创建 Kafka Topic
echo "📝 创建 Kafka Topic..."
docker-compose exec kafka kafka-topics --create --topic user-behavior-events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
docker-compose exec kafka kafka-topics --create --topic financial-transactions --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
docker-compose exec kafka kafka-topics --create --topic iot-device-metrics --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092

echo "🎉 环境启动完成！"
echo ""
echo "📊 服务访问地址："
echo "   - Flink Web UI: http://localhost:8081"
echo "   - Kafka UI: http://localhost:8082"
echo "   - MySQL: localhost:3306 (用户: streaming_user, 密码: streaming123)"
echo "   - Redis: localhost:6379"
echo ""
echo "🚀 接下来可以运行示例程序："
echo "   ./scripts/run-wordcount.sh"
echo "   ./scripts/run-user-behavior.sh"
echo "   ./scripts/run-fraud-detection.sh"