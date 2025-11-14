#!/bin/bash

# Spark 3.5.1 Standalone 集群启动脚本

echo "=== 启动 Spark 3.5.1 Standalone 集群 ==="

# 停止已运行的容器
echo "停止已运行的 Spark 容器..."
docker stop spark-master spark-worker 2>/dev/null
docker rm spark-master spark-worker 2>/dev/null

# 启动 Spark Master
echo "启动 Spark Master..."
docker run -d \
  --name spark-master \
  --network host \
  -e SPARK_MASTER_HOST=localhost \
  -e SPARK_MASTER_PORT=7077 \
  -e SPARK_MASTER_WEBUI_PORT=8080 \
  bitnami/spark:3.5.1 \
  /opt/bitnami/spark/bin/spark-class org.apache.spark.deploy.master.Master

# 等待 Master 启动
sleep 5

# 启动 Spark Worker
echo "启动 Spark Worker..."
docker run -d \
  --name spark-worker \
  --network host \
  -e SPARK_MODE=worker \
  -e SPARK_WORKER_CORES=2 \
  -e SPARK_WORKER_MEMORY=2g \
  -e SPARK_MASTER_URL=spark://localhost:7077 \
  bitnami/spark:3.5.1

# 等待 Worker 注册
sleep 10

echo "=== Spark 集群启动完成 ==="
echo "Master Web UI: http://localhost:8080"
echo "Master URL: spark://localhost:7077"
echo ""
echo "运行示例程序:"
echo "  ./scripts/run-demo.sh"