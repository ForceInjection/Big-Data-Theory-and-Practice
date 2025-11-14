# Spark 3.5.x Standalone Demo

## 1. 项目概述

基于 Apache Spark 3.5.1 的 Standalone 模式演示项目，实现了 Spark Shell 快速体验的所有功能。

**功能特性**：

- ✅ RDD 创建和基本操作
- ✅ 文本处理 WordCount
- ✅ 数据缓存性能演示
- ✅ RDD 血缘关系查看
- ✅ 存储级别演示
- ✅ Standalone 集群部署
- ✅ Docker 容器化运行

---

## 2. 技术架构

### 2.1 技术栈

- **Spark Version**: 3.5.1
- **Scala Version**: 2.13.12
- **运行模式**: Standalone
- **容器**: Docker + bitnami/spark:3.5.1

### 2.2 项目结构

```text
spark-3.5-demo/
├── src/main/scala/SparkShellDemo.scala  # Scala 主程序
├── scripts/
│   ├── start-spark-standalone.sh      # 启动 Spark 集群
│   ├── stop-spark-standalone.sh       # 停止 Spark 集群
│   ├── run-demo.sh                    # 运行完整 Demo
│   ├── run-scala-demo.sh              # 运行 Scala Demo
│   ├── run-local.sh                   # 本地运行（不依赖集群）
│   └── build.sh                       # 项目构建脚本
├── data/                              # 数据目录
├── build.sbt                          # SBT 构建配置
└── README.md                          # 项目说明
```

### 2.3 Spark 集群信息

- **Master Web UI**: <http://localhost:8080>
- **Master URL**: `spark://localhost:7077`
- **Worker 配置**: 2 cores, 2GB memory
- **网络模式**: host 模式（容器直接使用主机网络栈）

---

## 3. 快速开始

### 3.1 前置要求

- Docker
- sbt (Scala Build Tool)
- Java 8+ (推荐 Java 8 或 11，Java 17 在本地模式有兼容性问题)

### 3.2 构建项目

```bash
# 构建项目（生成 JAR 文件）
./scripts/build.sh

# 生成的 JAR 文件: target/scala-2.13/spark-3-5-demo_2.13-1.0.jar
```

### 3.3 容器化运行 Demo

```bash
# 启动 Spark Standalone 集群
./scripts/start-spark-standalone.sh

# 查看集群状态
open http://localhost:8080

# 运行 Scala Demo（提交到集群）
./scripts/run-scala-demo.sh

# 或者运行完整 Demo（包含集群启动、构建、运行）
./scripts/run-demo.sh

# 停止 Spark 集群
./scripts/stop-spark-standalone.sh
```

### 3.4 本地开发测试

使用 sbt run 直接在本地 JVM 中运行，用于代码开发和调试。

```bash
# 本地模式运行（注意：Java 17 有兼容性问题）
./scripts/run-local.sh
```

---

## 4. 功能详解

源码：[SparkShellDemo.scala](src/main/scala/SparkShellDemo.scala)

### 4.1 RDD 创建和操作

- 创建数字 RDD
- 过滤偶数
- 计算平方
- 统计操作

### 4.2 文本处理 WordCount

- 读取文本文件
- 分词和词频统计
- 结果排序和展示

### 4.3 数据缓存性能演示

- 模拟耗时操作
- 缓存前后性能对比
- 展示缓存效果

### 4.4 RDD 血缘关系

- 查看 RDD 转换链
- 分析依赖关系
- 理解 Spark 执行计划

### 4.5 存储级别演示

- MEMORY_AND_DISK_2 存储级别
- OFF_HEAP 存储级别
- 不同存储级别的配置说明

---

## 5. 开发指南

### 5.1 sbt (Scala Build Tool) 介绍

sbt (Scala Build Tool) 是 Scala 语言生态系统中广泛使用的构建工具，专门为 Scala 项目设计，提供强大的依赖管理、增量编译和丰富的插件生态系统。

#### 5.1.1 主要特性

- **增量编译**: 智能检测代码变更，只重新编译受影响的部分，大幅提升编译速度
- **交互式 Shell**: 提供交互式命令行界面，支持实时执行构建命令和代码测试
- **依赖管理**: 自动解析和下载项目依赖，支持 Maven 和 Ivy 仓库
- **插件生态系统**: 丰富的插件支持，可扩展构建功能（如代码格式化、静态分析等）
- **并行执行**: 支持并行任务执行，优化构建性能

#### 5.1.2 常用命令

```bash
# 编译项目
sbt compile

# 运行测试
sbt test

# 打包项目（生成 JAR）
sbt package

# 清理编译产物
sbt clean

# 运行主程序
sbt run

# 进入交互式 shell
sbt

# 查看项目信息
sbt about

# 显示项目依赖树
sbt dependencyTree
```

#### 5.1.3 项目配置

项目配置主要在 `build.sbt` 文件中定义，包含：

- Scala 版本设置
- 项目依赖声明
- 编译器选项
- 任务和设置定义

#### 5.1.4 在 Spark 项目中的优势

1. **原生 Scala 支持**: 对 Scala 语言特性提供最佳支持
2. **Spark 集成**: 与 Spark 框架无缝集成，简化依赖管理
3. **开发效率**: 增量编译和热重载功能提升开发体验
4. **社区支持**: 丰富的 Spark 相关插件和模板

### 5.2 开发操作说明

#### 5.2.1 修改代码后重新构建

```bash
# 清理并重新构建
sbt clean assembly

# 只编译
sbt compile
```

#### 5.2.2 查看 Spark Web UI

访问 <http://localhost:8080> 可以查看：

- 集群节点状态
- 运行中的应用程序
- 已完成的任务统计
- 环境配置信息

---

## 参考文档

- [Apache Spark 官方文档](https://spark.apache.org/docs/3.5.1/)
- [Spark Programming Guide](https://spark.apache.org/docs/3.5.1/rdd-programming-guide.html)
- [Spark Configuration](https://spark.apache.org/docs/3.5.1/configuration.html)
