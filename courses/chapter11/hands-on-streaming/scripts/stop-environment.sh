#!/bin/bash

# 流式计算环境停止脚本
# 作者：流式计算实践项目
# 版本：1.0.0

echo "🛑 开始停止流式计算实践环境..."

# 检查 Docker Compose 是否安装
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose 未安装"
    exit 1
fi

# 停止并移除容器
echo "🐳 停止 Docker 容器服务..."
docker-compose down

echo "🧹 清理临时文件..."
# 保留数据目录，只清理临时文件
rm -rf logs/*.log 2>/dev/null || true

# 询问是否清理数据卷
read -p "❓ 是否清理数据卷？(y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🗑️ 清理数据卷..."
    docker volume rm hands-on-streaming_mysql-data hands-on-streaming_redis-data 2>/dev/null || true
    echo "✅ 数据卷已清理"
else
    echo "📦 保留数据卷"
fi

echo "🎯 环境停止完成！"
echo ""
echo "💡 提示：数据卷已被保留，下次启动时会恢复数据"