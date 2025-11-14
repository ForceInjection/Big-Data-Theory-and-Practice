#!/bin/bash

echo "=== 构建 Spark 3.5 Demo 项目 ==="

# 检查 sbt 是否安装
if ! command -v sbt &> /dev/null; then
    echo "错误: sbt 未安装，请先安装 sbt"
    echo "安装方法: brew install sbt"
    exit 1
fi

# 清理并构建项目
echo "开始构建项目..."
sbt clean package

echo "=== 构建完成 ==="
echo "JAR 文件位置: target/scala-2.13/spark-3.5-demo_2.13-1.0.jar"