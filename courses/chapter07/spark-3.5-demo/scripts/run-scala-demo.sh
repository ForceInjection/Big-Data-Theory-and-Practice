#!/bin/bash

# Scala Demo 运行脚本

echo "=== 运行 Scala Demo 程序 ==="

JAR_FILE="target/scala-2.13/spark-3-5-demo_2.13-1.0.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "JAR 文件不存在，请先运行: ./scripts/build.sh"
    exit 1
fi

# 使用 Docker 运行 Spark Submit
echo "提交 Spark 作业到集群..."

docker run --rm \
  --network host \
  -v "$(pwd)/$JAR_FILE:/app/spark-demo.jar" \
  -v "$(pwd)/data:/app/data" \
  bitnami/spark:3.5.1 \
  /opt/bitnami/spark/bin/spark-submit \
  --master spark://localhost:7077 \
  --class SparkShellDemo \
  --executor-memory 1g \
  --total-executor-cores 2 \
  /app/spark-demo.jar

echo "=== Spark 作业执行完成 ==="