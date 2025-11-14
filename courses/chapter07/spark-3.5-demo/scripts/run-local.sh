#!/bin/bash

# 本地运行脚本（不依赖 Docker 集群）

echo "=== 本地运行 Spark Demo ==="

# 构建项目
echo "构建项目..."
cd "$(dirname "$0")/.."
./scripts/build.sh

# 本地运行（使用 sbt run）
echo "在本地模式下运行 Demo..."
sbt run

echo "=== 本地运行完成 ==="