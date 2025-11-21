#!/bin/bash

# Parquet Practice Project - Development Environment Setup Script

echo "🔧 设置 Parquet 实践练习项目开发环境..."

# 检查 Python 版本
if ! command -v python &> /dev/null; then
    echo "❌ Python 未安装，请先安装 Python 3.8+"
    exit 1
fi

PYTHON_VERSION=$(python -c 'import sys; print("{}.{}".format(sys.version_info.major, sys.version_info.minor))')
echo "🐍 检测到 Python 版本: $PYTHON_VERSION"

# 检查是否在虚拟环境中
if [ -z "$VIRTUAL_ENV" ]; then
    echo "⚠️  不在虚拟环境中，建议使用虚拟环境"
    echo "💡 可以使用: python -m venv .venv && source .venv/bin/activate"
    read -p "是否继续？(y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "👋 退出设置"
        exit 0
    fi
fi

# 安装开发依赖
echo "📦 安装开发依赖..."
pip install -e ".[dev,docs,all]"

# 安装 pre-commit hooks
echo "🔗 设置 pre-commit hooks..."
if command -v pre-commit &> /dev/null; then
    pre-commit install
else
    echo "⚠️  pre-commit 未安装，跳过 hooks 设置"
fi

# 创建环境配置文件
echo "⚙️  创建环境配置文件..."
if [ ! -f ".env" ]; then
    cp .env.example .env
    echo "✅ 已创建 .env 文件（请根据需要编辑）"
else
    echo "✅ .env 文件已存在"
fi

# 运行基础检查
echo "🔍 运行基础检查..."
python -c "import parquet_practice; print('✅ 模块导入成功')" 2>/dev/null || echo "❌ 模块导入失败"

# 显示完成信息
echo ""
echo "🎉 开发环境设置完成！"
echo ""
echo "📋 可用命令:"
echo "  make help        - 查看所有可用命令"
echo "  make test        - 运行测试"
echo "  make lint        - 代码检查"
echo "  make format      - 代码格式化"
echo "  make check       - 完整代码质量检查"
echo ""
echo "💡 提示:"
echo "  - 编辑 .env 文件配置环境变量"
echo "  - 运行 'make run-basic' 测试基础功能"
echo "  - 运行 'make benchmark' 进行性能测试"

# 设置执行权限
chmod +x "$0"