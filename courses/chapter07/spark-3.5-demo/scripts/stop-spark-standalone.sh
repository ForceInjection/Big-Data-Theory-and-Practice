#!/bin/bash

# Spark 集群停止脚本

echo "=== 停止 Spark Standalone 集群 ==="

docker stop spark-worker spark-master 2>/dev/null
docker rm spark-worker spark-master 2>/dev/null

echo "Spark 集群已停止"