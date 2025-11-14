#!/bin/bash

# Spark Demo 运行脚本

echo "=== 运行 Spark Shell 快速体验 Demo ==="

# 构建项目
echo "构建项目..."
cd "$(dirname "$0")/.."
./scripts/build.sh

# 检查 Spark 集群状态
echo "检查 Spark 集群状态..."
MASTER_STATUS=$(curl -s http://localhost:8080 | grep -c "Spark Master" || echo "0")

if [ "$MASTER_STATUS" -eq "0" ]; then
    echo "Spark 集群未运行，请先运行: ./scripts/start-spark-standalone.sh"
    exit 1
fi

echo "Spark 集群运行正常"

# 运行 Demo
echo "运行 Scala Demo 程序..."
./scripts/run-scala-demo.sh

echo "=== Demo 运行完成 ==="
echo "查看 Spark Web UI: http://localhost:8080"