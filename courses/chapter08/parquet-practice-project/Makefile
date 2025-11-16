# Parquet 实践练习项目 Makefile
# 现代化 Python 项目管理工具集成

.PHONY: help install install-dev test test-cov lint format clean build docs run-basic run-all benchmark

# 默认目标
help:
	@echo "Parquet 实践练习项目 - 可用命令:"
	@echo ""
	@echo "🔧 环境管理:"
	@echo "  install      - 安装项目依赖 (使用 pyproject.toml)"
	@echo "  install-dev  - 安装开发依赖"
	@echo "  install-editable - 可编辑模式安装"
	@echo "  clean        - 清理构建文件和缓存"
	@echo ""
	@echo "🧪 代码质量:"
	@echo "  test         - 运行测试"
	@echo "  test-cov     - 运行测试并生成覆盖率报告"
	@echo "  lint         - 代码检查 (flake8 + mypy)"
	@echo "  format       - 代码格式化 (black + isort)"
	@echo "  check        - 完整代码质量检查"
	@echo ""
	@echo "📦 构建和分发:"
	@echo "  build        - 构建项目包"
	@echo "  docs         - 生成文档"
	@echo ""
	@echo "🚀 运行练习:"
	@echo "  run-basic    - 运行基础练习"
	@echo "  run-all      - 运行所有练习"
	@echo "  benchmark    - 运行性能基准测试"
	@echo "  interactive  - 交互式模式"
	@echo "  quick-start  - 快速开始演示"

# 使用 pyproject.toml 安装依赖
install:
	pip install -e .

# 安装开发依赖
install-dev:
	pip install -e ".[dev,docs]"

# 可编辑模式安装（推荐用于开发）
install-editable:
	pip install -e ".[dev,docs]"

# 运行测试
test:
	python -m pytest tests/ -v

# 运行测试并生成覆盖率报告
test-cov:
	python -m pytest tests/ -v --cov=src/parquet_practice --cov-report=html --cov-report=term

# 代码检查
lint:
	python -m flake8 src/ tests/ examples/ main.py
	python -m mypy src/ main.py

# 代码格式化
format:
	python -m black src/ tests/ examples/ main.py
	python -m isort src/ tests/ examples/ main.py

# 清理构建文件和缓存
clean:
	@echo "🧹 清理构建文件和缓存..."
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .mypy_cache/
	rm -rf .tox/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	rm -rf output/
	rm -rf examples/output/
	rm -rf benchmark_results/
	@echo "✅ 清理完成"

# 构建项目包
build: clean
	@echo "📦 构建项目包..."
	python -m build
	@echo "✅ 构建完成，文件位于 dist/ 目录"

# 生成文档
docs:
	@echo "📚 生成文档..."
	@if [ -d "docs" ]; then \
		cd docs && make html; \
	else \
		echo "⚠️  docs 目录不存在，跳过文档生成"; \
	fi

# 运行基础练习
run-basic:
	python main.py --exercise basic --records 5000

# 运行所有练习
run-all:
	python main.py --exercise all --records 5000

# 运行性能基准测试
benchmark:
	python examples/benchmark.py

# 交互式模式
interactive:
	python main.py --interactive

# 快速开始演示
quick-start:
	python examples/quick_start.py

# 完整代码质量检查
check: format lint test
	@echo "✅ 代码质量检查完成！"

# 发布准备
release-prep: clean format lint test build
	@echo "🚀 发布准备完成！"
	@echo "📦 构建文件位于 dist/ 目录"
	@echo "💡 使用 'twine upload dist/*' 发布到 PyPI"

# 开发环境完整设置
dev-setup: clean install-dev
	@echo "🔧 开发环境设置完成！"
	@echo "💡 运行 'make help' 查看可用命令"
	@echo "💡 运行 'make check' 进行代码质量检查"

# 快速开发循环（格式化 + 检查 + 测试）
dev-check: format lint test
	@echo "🔄 开发检查完成！"

# 安装项目到本地环境（别名）
install-local: install-editable

# 卸载项目
uninstall:
	@echo "🗑️  卸载项目..."
	pip uninstall parquet-practice -y
	@echo "✅ 卸载完成"

# 显示项目信息
info:
	@echo "📋 项目信息:"
	@echo "  名称: parquet-practice"
	@echo "  版本: 1.0.0"
	@echo "  Python: $(shell python --version)"
	@echo "  位置: $(shell pwd)"
	@echo "  虚拟环境: $(shell echo $$VIRTUAL_ENV)"

# 验证安装
verify:
	@echo "🔍 验证安装..."
	python -c "import parquet_practice; print('✅ 模块导入成功')" 2>/dev/null || echo "❌ 模块导入失败"
	python -c "import pandas, pyarrow, numpy; print('✅ 核心依赖可用')" 2>/dev/null || echo "❌ 核心依赖缺失"