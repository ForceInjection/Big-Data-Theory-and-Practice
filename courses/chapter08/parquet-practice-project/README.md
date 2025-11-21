# Parquet 实践练习项目

本项目是专为 Parquet 文件格式学习设计的实践项目，通过渐进式练习帮助掌握列式存储的核心特性和最佳实践。

## 1. 项目特性

- **渐进式学习**: 从基础操作到高级特性的完整学习路径
- **性能对比**: 直观的性能测试和可视化分析
- **实用工具**: 完整的数据生成、分析和测试工具集
- **实际应用**: 贴近真实场景的练习案例

## 2. 环境要求

- Python 3.8+
- 至少 2GB 可用内存
- 500MB 可用磁盘空间

---

## 3. 快速开始

### 3.1 环境准备

```bash
# 创建虚拟环境
python3 -m venv venv

# 激活虚拟环境
source venv/bin/activate  # Linux/macOS
# 或 venv\Scripts\activate  # Windows

# 安装依赖
pip install -r requirements.txt
```

### 3.2 验证安装

```bash
python -c "import pandas, pyarrow; print('安装成功！')"
```

### 3.3 运行练习

#### 3.3.1 交互式模式（推荐）

```bash
python main.py --interactive
```

#### 3.3.2 命令行模式（按 Parquet 实践练习.md 章节顺序）

```bash
# 查看帮助
python main.py --help

# 2.1 基础练习：Parquet 文件读写（对应文档 2.1 章节）
python main.py --exercise basic --records 100000

# 2.2 进阶练习：压缩算法比较（对应文档 2.2 章节）
python main.py --exercise compression --records 50000

# 2.3 专家练习：查询优化（对应文档 2.3 章节）
python main.py --exercise query --records 100000

# 2.4 专家练习：分区存储（对应文档 2.4 章节）
python main.py --exercise partition --records 100000

# 2.5 挑战练习：嵌套数据结构（对应文档 2.5 章节）
python main.py --exercise advanced --records 1000

# 运行所有练习（完整学习路径）
python main.py --exercise all --records 50000
```

---

## 4. 学习路径与指南

本项目与 [Parquet 实践练习](../Parquet实践练习.md) 文档完全配套，提供渐进式的 Parquet 学习体验。

### 4.1 文档与项目对应关系

| 练习模块                | 对应文档章节 | 学习重点                   | 推荐记录数 |
| ----------------------- | ------------ | -------------------------- | ---------- |
| `basic.py`              | 2.1 基础练习 | Parquet 基本读写、性能对比 | 100,000    |
| `compression.py`        | 2.2 进阶练习 | 压缩算法比较、性能权衡     | 50,000     |
| `query_optimization.py` | 2.3 专家练习 | 查询优化技术、性能提升     | 100,000    |
| `partitioning.py`       | 2.4 专家练习 | 分区存储、查询优化         | 100,000    |
| `advanced.py`           | 2.5 挑战练习 | 嵌套数据结构、复杂 Schema  | 1,000      |

### 4.2 学习路线图

建议按照以下顺序完成练习，每个练习对应文档的一个章节：

1. **基础练习**（文档 2.1 章节）

   ```bash
   python main.py --exercise basic --records 100000
   ```

   - 学习目标：掌握 Parquet 文件的基本读写操作
   - 学习要点：文件大小对比、压缩效果分析、数据完整性验证

2. **压缩算法练习**（文档 2.2 章节）

   ```bash
   python main.py --exercise compression --records 50000
   ```

   - 学习目标：比较不同压缩算法的性能特点
   - 学习要点：SNAPPY、GZIP、LZ4、BROTLI 算法对比

3. **查询优化练习**（文档 2.3 章节）

   ```bash
   python main.py --exercise query --records 100000
   ```

   - 学习目标：掌握投影下推和谓词下推技术
   - 学习要点：查询性能优化、组合优化策略

4. **分区存储练习**（文档 2.4 章节）

   ```bash
   python main.py --exercise partition --records 100000
   ```

   - 学习目标：学习分区存储的实现和优势
   - 学习要点：分区裁剪、查询性能提升

5. **高级特性练习**（文档 2.5 章节）

   ```bash
   python main.py --exercise advanced --records 1000
   ```

   - 学习目标：处理嵌套数据结构和复杂 Schema
   - 学习要点：嵌套数据处理、Schema 定义

### 4.3 学习建议

1. **循序渐进**：按照文档章节顺序完成练习，从基础到高级
2. **理论结合实践**：先阅读文档的理论部分，再运行对应的练习代码
3. **结果分析**：仔细分析每个练习的输出结果，理解性能差异的原因
4. **参数调整**：尝试修改记录数量、压缩算法等参数，观察性能变化

### 4.4 预期学习成果

完成所有练习后，您将能够：

- ✅ 理解 Parquet 列式存储的原理和优势
- ✅ 掌握 Parquet 文件的基本读写操作
- ✅ 选择合适的压缩算法优化存储性能
- ✅ 应用查询优化技术提升查询性能
- ✅ 实现分区存储优化大数据查询
- ✅ 处理复杂的嵌套数据结构

### 4.5 代码实现讲解

#### 4.5.1 基础练习代码要点

```python
# 核心代码示例：数据生成和格式转换
df.to_csv('user_data.csv', index=False)  # CSV 格式
df.to_parquet('user_data.parquet', engine='pyarrow')  # Parquet 格式

# 性能对比关键指标
csv_size = os.path.getsize('user_data.csv') / (1024 * 1024)  # MB
parquet_size = os.path.getsize('user_data.parquet') / (1024 * 1024)  # MB
compression_ratio = csv_size / parquet_size  # 压缩比
```

**技术要点**：使用 `pyarrow` 引擎确保最佳性能，精确计算文件大小和压缩比。

#### 4.5.2 压缩算法练习代码要点

```python
# 不同压缩算法测试
compression_algorithms = ['snappy', 'gzip', 'lz4', 'brotli']
for algorithm in compression_algorithms:
    df.to_parquet(f'user_data_{algorithm}.parquet',
                  compression=algorithm, engine='pyarrow')

# 性能指标记录
results[algorithm] = {
    'size_mb': file_size,
    'write_time': write_time,
    'read_time': read_time
}
```

**技术要点**：统一测试环境，确保算法比较的公平性，记录完整的性能指标。

#### 4.5.3 查询优化练习代码要点

```python
# 投影下推示例：只读取需要的列
columns_to_read = ['user_id', 'username', 'city']
df_selected = pd.read_parquet('user_data.parquet', columns=columns_to_read)

# 谓词下推示例：过滤条件下推
df_filtered = pd.read_parquet('user_data.parquet',
                             filters=[('city', '=', '北京'), ('age', '>', 30)])
```

**技术要点**：利用 Parquet 的列式存储特性，减少 I/O 和数据传输量。

#### 4.5.4 分区练习代码要点

```python
# 按城市分区存储
df.to_parquet('partitioned_data', partition_cols=['city'], engine='pyarrow')

# 分区查询：只读取北京的数据
df_beijing = pd.read_parquet('partitioned_data/city=北京')

# 分区过滤查询
df_filtered = pd.read_parquet('partitioned_data',
                             filters=[('city', '=', '北京')])
```

**技术要点**：分区目录结构符合 `column=value` 格式，支持高效的分区裁剪。

#### 4.5.5 高级特性练习代码要点

```python
# 嵌套数据结构定义
import pyarrow as pa
schema = pa.schema([
    ('user_id', pa.int64()),
    ('contacts', pa.list_(pa.struct([
        ('type', pa.string()),
        ('value', pa.string())
    ])))
])

# 复杂数据写入
table = pa.Table.from_pandas(df, schema=schema)
pa.parquet.write_table(table, 'nested_data.parquet')
```

**技术要点**：使用 PyArrow Schema 明确定义复杂数据类型，确保数据一致性。

### 4.6 扩展学习

完成基础练习后，可以进一步探索：

- **集成 Apache Spark**：使用 PySpark 处理大规模 Parquet 数据
- **数据湖实践**：结合 Delta Lake 或 Apache Iceberg
- **云存储集成**：将 Parquet 文件存储到云存储服务
- **实时数据处理**：结合 Apache Kafka 实现实时数据存储

---

## 5. 项目结构

```bash
parquet-practice-project/
├── src/parquet_practice/          # 核心代码模块
│   ├── cli/                       # 命令行接口
│   │   ├── main.py               # 主程序入口
│   │   └── performance.py        # 性能测试工具
│   ├── exercises/                 # 练习模块
│   │   ├── basic.py              # 基础练习
│   │   ├── compression.py        # 压缩算法练习
│   │   ├── query_optimization.py # 查询优化练习
│   │   ├── partitioning.py       # 分区练习
│   │   └── advanced.py           # 高级特性练习
│   ├── utils.py                  # 工具函数
│   └── __init__.py              # 包初始化

├── tests/                        # 测试文件
│   ├── unit/                     # 单元测试
│   │   └── test_basic.py         # 基础功能测试
│   ├── integration/              # 集成测试
│   │   └── test_comprehensive.py  # 综合测试
│   └── __init__.py              # 测试包初始化
├── output/                      # 统一输出目录
│   ├── reports/                # 性能报告目录（存放图表文件）
│   └── *.json                  # 练习结果JSON文件（直接存放在根目录）
├── docs/                         # 文档目录
├── pyproject.toml               # 项目配置
├── pytest.ini                   # 测试配置
├── requirements.txt             # 依赖文件
└── Makefile                     # 构建工具
```

---

## 6. 输出文件

练习结果保存在 `output/` 目录：

### 6.1 JSON 结果文件

- `basic_exercise_results.json` - 基础练习结果（CSV vs Parquet 性能对比）
- `compression_results.json` - 压缩算法比较结果（SNAPPY、GZIP、LZ4、BROTLI）
- `query_optimization_results.json` - 查询优化结果（投影下推、谓词下推）
- `partitioning_results.json` - 分区存储结果（分区裁剪、性能分析）
- `advanced_results.json` - 高级特性结果（嵌套数据结构处理）

### 6.2 可视化图表

- `compression_comparison.png` - 压缩算法性能对比图表
- `reports/performance_comparison.png` - 综合性能对比图表

### 6.3 数据文件（临时生成）

练习过程中会生成临时数据文件，包括：

- CSV 格式的示例数据文件
- Parquet 格式的示例数据文件（不同压缩算法）
- 分区数据目录结构
- 嵌套数据结构文件

这些临时文件在练习完成后会自动清理，只保留最终的分析结果。

---
