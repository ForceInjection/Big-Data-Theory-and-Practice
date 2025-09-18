# Parquet 文件格式深入解析

## 1. Parquet 概述与核心优势

### 1.1 为什么我们需要 Parquet？

#### 1.1.1 传统数据存储面临的挑战

在大数据时代到来之前，我们主要使用关系型数据库来存储和处理数据。然而，随着数据量的爆炸式增长和数据类型的多样化，传统存储方式遇到了以下挑战：

**1. 存储成本高昂**：

- 传统行式存储会保存大量冗余信息
- 即使只需要几列数据，也必须读取整行
- 压缩效率低，占用大量存储空间

**2. 查询性能瓶颈**：

- 分析型查询通常只涉及部分列，但行式存储无法跳过不需要的列
- I/O 操作成为性能瓶颈
- 无法充分利用现代硬件的并行处理能力

**3. 数据类型限制**：

- 传统数据库对嵌套数据结构支持有限
- 处理 JSON、XML 等半结构化数据困难
- 缺乏对数组、映射等复杂类型的原生支持

#### 1.1.2 Parquet 的诞生背景

为了解决这些问题，业界开始探索新的数据存储格式。**Parquet** 正是在这样的背景下诞生的：

- **2013 年**：由 Twitter 和 Cloudera 联合开发
- **设计目标**：专为大数据分析场景优化的列式存储格式
- **开源理念**：成为 Apache 顶级项目，不受厂商锁定
- **生态支持**：得到 Spark、Hive、Impala 等主流大数据工具支持

**Parquet 文件格式** 应运而生，并迅速成为大规模数据存储的事实标准。

### 1.2 什么是 Parquet？

`Parquet` 是一种开源的列式存储文件格式，专为高效存储和处理大规模数据而设计。它最初由 `Apache` 软件基金会开发，现已成为大数据生态系统中的重要组成部分。`Parquet` 的设计目标是优化数据读取性能，减少存储空间占用，并支持复杂的数据类型。

### 1.3 行式存储 vs 列式存储：核心理念

为了更好地理解 Parquet 的核心思想，让我们通过一个简单的例子来对比行式存储和列式存储：

假设我们有一张用户表：

| 用户 ID | 用户名  | 年龄 | 城市 |
|--------|---------|------|------|
| 1      | Alice   | 25   | 北京 |
| 2      | Bob     | 30   | 上海 |
| 3      | Charlie | 35   | 广州 |

**行式存储（传统方式）：**

```text
行1: [1, "Alice", 25, "北京"]
行2: [2, "Bob", 30, "上海"] 
行3: [3, "Charlie", 35, "广州"]
```

**列式存储（Parquet 方式）：**

```text
用户 ID 列: [1, 2, 3]
用户名列: ["Alice", "Bob", "Charlie"]
年龄列:   [25, 30, 35]
城市列:   ["北京", "上海", "广州"]
```

### 1.4 Parquet 的核心优势

基于列式存储的设计理念，`Parquet` 具备以下核心优势：

#### 1.4.1 高效的数据压缩

`Parquet` 采用列式存储，相同类型的数据聚集在一起，这使得压缩算法能够更有效地工作。

**实际效果对比：**

- **传统 CSV 文件**：1GB 的销售数据
- **Parquet 格式**：压缩后仅需 200-300MB（压缩比 3-5:1）
- **查询速度**：比 CSV 快 5-10 倍

**压缩原理：**

```text
示例：城市列数据
原始数据: ["北京", "北京", "上海", "北京", "上海", "广州"]
压缩后:   ["北京":3次, "上海":2次, "广州":1次] + 位置索引
```

`Parquet` 支持多种压缩算法（如 `Snappy`、`GZIP` 等），并通过字典编码和运行长度编码（`RLE`）等技术进一步减少数据体积。

#### 1.4.2 快速的查询性能

列式存储可以显著提高分析型查询（`OLAP`）的性能，因为查询通常只需要访问部分列，而不是整行数据。

**性能优势体现：**

| 查询类型 | 传统行式存储 | Parquet 列式存储 | 性能提升 |
|----------|--------------|------------------|----------|
| 单列聚合 | 读取全部数据 | 只读取目标列     | 5-10x    |
| 多列筛选 | 全表扫描     | 列级别过滤       | 3-5x     |
| 范围查询 | 顺序扫描     | 利用统计信息跳过 | 10-50x   |

**查询优化原理：**

1. **列裁剪**：只读取查询需要的列，减少 I/O 操作
2. **谓词下推**：利用列统计信息跳过不符合条件的数据块
3. **并行处理**：不同列可以并行处理，充分利用多核 CPU

#### 1.4.3 支持复杂数据类型

`Parquet` 不仅支持基本的数据类型，还支持嵌套数据结构，非常适合存储半结构化数据。

**支持的数据类型：**

| 基础类型 | 复杂类型 | 实际应用 |
|----------|----------|----------|
| INT32/64 | ARRAY    | 用户标签列表 |
| FLOAT/DOUBLE | MAP | 商品属性键值对 |
| STRING | STRUCT | 嵌套的地址信息 |
| BOOLEAN | UNION | 多类型字段 |

**JSON 数据存储示例：**

```json
// 原始 JSON 数据
{
  "user_id": 123,
  "tags": ["VIP", "活跃用户"],
  "address": {
    "city": "北京",
    "district": "朝阳区"
  }
}
```

Parquet 可以直接存储这种嵌套结构，无需扁平化处理。

#### 1.4.4 跨语言和平台支持

`Parquet` 文件格式与编程语言无关，具有广泛的生态系统支持。

**生态系统支持：**

| 编程语言 | 主要库 | 大数据平台 | 云服务 |
|----------|--------|------------|--------|
| Python | pandas, pyarrow | Apache Spark | AWS S3 |
| Java | parquet-mr | Apache Hive | Google BigQuery |
| Scala | Spark DataFrame | Apache Drill | Azure Data Lake |
| R | arrow | Presto/Trino | Snowflake |

#### 1.4.5 开源与标准化

作为开源格式，`Parquet` 不受任何特定供应商的锁定，用户可以在不同的平台和工具之间自由迁移数据。

### 1.5 学习要点总结

**核心概念**：

- Parquet = 列式存储 + 高效压缩 + 跨语言支持
- 专为分析型查询（OLAP）优化
- 开源且与平台无关

**性能优势**：

- 压缩比：3-5 倍空间节省
- 查询速度：5-50 倍性能提升
- I/O 优化：只读取需要的列

**技术特性**：

- 支持复杂嵌套数据结构
- 跨平台、跨语言兼容
- 内置数据统计信息

**适用场景**：

- 大数据分析和数据仓库
- 机器学习特征存储
- 日志分析和监控系统

>**扩展阅读：**
>
> - [列式存储 vs 行式存储：它们之间的本质区别在哪里？](https://mp.weixin.qq.com/s/vIRzYLpuG0snTbprINYnRw)
> - [_**Parquet file format – everything you need to know!**_](https://data-mozart.com/parquet-file-format-everything-you-need-to-know/)

---

## 2. 快速入门

在深入了解 `Parquet` 的内部结构之前，让我们先通过一个简单的示例来体验 `Parquet` 的基本操作。这将帮助您建立直观的认识。

### 2.1 环境准备

本文以 `Python 3.11` 版本为例，演示 `Parquet` 文件的创建和读取。

```bash
# 安装必要的库
pip install pandas pyarrow
```

### 2.2 创建第一个 Parquet 文件

示例代码：

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# 创建示例数据
data = {
    'user_id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
    'age': [25, 30, 35, 28, 32],
    'city': ['Beijing', 'Shanghai', 'Guangzhou', 'Shenzhen', 'Hangzhou'],
    'salary': [50000, 60000, 70000, 55000, 65000]
}

# 创建 DataFrame
df = pd.DataFrame(data)
print("原始数据：")
print(df)

# 保存为 Parquet 文件
df.to_parquet('users.parquet', engine='pyarrow')
print("\n✅ Parquet 文件已创建：users.parquet")
```

### 2.3 读取 Parquet 文件

```python
# 读取整个文件
df_read = pd.read_parquet('users.parquet')
print("读取的数据：")
print(df_read)

# 只读取特定列（体验列式存储的优势）
df_partial = pd.read_parquet('users.parquet', columns=['name', 'salary'])
print("\n只读取姓名和薪资列：")
print(df_partial)

# 使用条件过滤
df_filtered = pd.read_parquet('users.parquet')
high_salary = df_filtered[df_filtered['salary'] > 60000]
print("\n薪资大于 60000 的用户：")
print(high_salary)
```

### 2.4 文件大小对比

```python
import os

# 比较文件大小
csv_size = os.path.getsize('users.csv') if os.path.exists('users.csv') else 0
parquet_size = os.path.getsize('users.parquet')

# 先保存 CSV 用于对比
df.to_csv('users.csv', index=False)
csv_size = os.path.getsize('users.csv')

print(f"\n📊 文件大小对比：")
print(f"CSV 文件：{csv_size} 字节")
print(f"Parquet 文件：{parquet_size} 字节")
print(f"压缩比：{csv_size/parquet_size:.2f}:1")
```

### 2.5 性能测试（可选）

```python
import time

# 创建更大的数据集进行性能测试
large_data = {
    'id': range(100000),
    'value': [i * 2 for i in range(100000)],
    'category': ['A', 'B', 'C'] * 33334  # 重复数据，便于压缩
}
large_df = pd.DataFrame(large_data)

# 测试写入性能
start_time = time.time()
large_df.to_parquet('large_data.parquet')
parquet_write_time = time.time() - start_time

start_time = time.time()
large_df.to_csv('large_data.csv', index=False)
csv_write_time = time.time() - start_time

print(f"\n⏱️ 写入性能对比（100,000 行数据）：")
print(f"Parquet 写入时间：{parquet_write_time:.3f} 秒")
print(f"CSV 写入时间：{csv_write_time:.3f} 秒")

# 测试读取性能
start_time = time.time()
pd.read_parquet('large_data.parquet')
parquet_read_time = time.time() - start_time

start_time = time.time()
pd.read_csv('large_data.csv')
csv_read_time = time.time() - start_time

print(f"Parquet 读取时间：{parquet_read_time:.3f} 秒")
print(f"CSV 读取时间：{csv_read_time:.3f} 秒")
```

### 2.6 查询优化技巧（可选）

`Parquet` 的列式存储特性支持多种查询优化技术，可以显著提升数据处理性能。

#### 2.6.1 列投影（Column Projection）

只读取需要的列，避免不必要的 I/O 操作：

```python
# 只读取特定列
df_projected = pd.read_parquet('users.parquet', columns=['name', 'salary'])
print("只读取姓名和薪资：")
print(df_projected)
```

#### 2.6.2 谓词下推（Predicate Pushdown）

在文件级别过滤数据，减少内存使用：

```python
import pyarrow.parquet as pq

# 使用 pyarrow 进行谓词下推
table = pq.read_table('users.parquet', filters=[('age', '>', 30)])
df_filtered = table.to_pandas()
print("年龄大于30的用户：")
print(df_filtered)
```

### 2.7 性能优化最佳实践

- **列投影**：只读取需要的列 `columns=['col1', 'col2']`
- **谓词下推**：在读取时过滤数据 `filters=[('age', '>', 30)]`
- **批量处理**：使用 `ParquetDataset` 处理多文件
- **引擎选择**：优先使用 `pyarrow` 引擎，性能最佳
- **行组大小**：合理设置 Row Group 大小（默认 64MB）
- **压缩算法**：选择合适的压缩算法（Snappy 平衡性能和压缩比）

### 2.8 总结

- `Parquet` 文件通常比 `CSV` 小 **50-80%**
- 读取特定列比读取整个文件快得多
- 写入时间可能稍长，但读取性能显著提升
- 对于分析型工作负载，优先选择 Parquet
- 使用 `pyarrow` 引擎获得最佳性能
- 合理设计列结构，避免过度嵌套

---

## 3. Parquet 文件结构详解

`Parquet` 文件由以下几个部分组成：

  1. **文件头（Magic Number）**
`Parquet` 文件的开头是一个 **4** 字节的 `Magic Number`，值为 ` PAR1 ` ，用于标识文件格式。
  2. **行组（Row Group）**
行组是 `Parquet` 文件的基本存储单元，包含多个列块（`Column Chunk`）。每个列块存储一列的数据。
  3. **页脚（Footer）**
页脚包含文件的元数据，如文件架构、压缩方式、行组偏移量等。页脚的长度存储在文件的最后 **4** 个字节中。

### 3.1 Parquet 文件结构图示

#### 3.1.1 Parquet 文件整体结构

```text
    +---------------------+  
    |      Magic Number   |  // 文件头，标识 Parquet 文件格式（4 字节，"PAR1"）  
    +---------------------+  
    |      Row Group 0    |  // 行组 0，包含多个列块  
    | +-----------------+ |  
    | |  Column Chunk 1 | |  // 列块 1，存储一列的数据  
    | +-----------------+ |  
    | |  Column Chunk 2 | |  // 列块 2，存储一列的数据  
    | +-----------------+ |  
    | |       ...       | |  
    +---------------------+  
    |      Row Group 1    |  // 行组 1，包含多个列块  
    | +-----------------+ |  
    | |  Column Chunk 1 | |  
    | +-----------------+ |  
    | |  Column Chunk 2 | |  
    | +-----------------+ |  
    | |       ...       | |  
    +---------------------+  
    |        ...          |  // 更多行组  
    +---------------------+  
    |        Footer       |  // 页脚，包含元数据和行组偏移量  
    +---------------------+  
    |   Footer Length     |  // 页脚长度（4 字节）  
    +---------------------+
```

#### 3.1.2 行组（Row Group）结构

```text
    
    +---------------------+  
    |      Row Group      |  
    | +-----------------+ |  
    | |  Column Chunk 1 | |  // 列块 1，存储一列的数据  
    | | +-------------+ | |  
    | | |   Page 1    | | |  // 数据页 1，存储实际数据  
    | | +-------------+ | |  
    | | |   Page 2    | | |  // 数据页 2，存储实际数据  
    | | +-------------+ | |  
    | |       ...       | |  
    | +-----------------+ |  
    | |  Column Chunk 2 | |  // 列块 2，存储一列的数据  
    | | +-------------+ | |  
    | | |   Page 1    | | |  
    | | +-------------+ | |  
    | | |   Page 2    | | |  
    | | +-------------+ | |  
    | |       ...       | |  
    | +-----------------+ |  
    |        ...          |  // 更多列块  
    +---------------------+
```

#### 3.1.3 列块（Column Chunk）结构

```text
    +---------------------+  
    |     Column Chunk    |  
    | +-----------------+ |  
    | |      Page 1     | |  // 数据页 1，存储实际数据  
    | | +-------------+ | |  
    | | |   Header    | | |  // 页头，包含页的元数据  
    | | +-------------+ | |  
    | | |    Data     | | |  // 数据部分，存储压缩后的数据  
    | | +-------------+ | |  
    | +-----------------+ |  
    | |      Page 2     | |  // 数据页 2，存储实际数据  
    | | +-------------+ | |  
    | | |   Header    | | |  
    | | +-------------+ | |  
    | | |    Data     | | |  
    | | +-------------+ | |  
    | +-----------------+ |  
    |        ...          |  // 更多数据页  
    +---------------------+
```

#### 3.1.4 页（Page）结构

```text
    +---------------------+  
    |        Page         |  
    | +-----------------+ |  
    | |      Header     | |  // 页头，包含页的元数据  
    | | +-------------+ | |  
    | | |  Page Type  | | |  // 页类型（数据页或字典页）  
    | | +-------------+ | |  
    | | |  Encoding   | | |  // 编码方式（如 PLAIN、RLE 等）  
    | | +-------------+ | |  
    | | |  Data Size  | | |  // 数据部分的大小  
    | | +-------------+ | |  
    | +-----------------+ |  
    | |      Data      | |  // 数据部分，存储压缩后的数据  
    | +-----------------+ |  
    +---------------------+
```

#### 3.1.5 页脚（Footer）结构

```text
    +---------------------+  
    |        Footer       |  
    | +-----------------+ |  
    | |  File Metadata  | |  // 文件元数据，包括文件架构、压缩方式等  
    | +-----------------+ |  
    | | Row Group Offset| |  // 行组偏移量，记录每个行组的起始位置  
    | +-----------------+ |  
    | |  Column Metadata| |  // 列元数据，记录每列的统计信息  
    | +-----------------+ |  
    | |       ...       | |  // 其他元数据  
    +---------------------+
```

#### 3.1.6 Parquet 文件示例图示

以下是一个具体的 Parquet 文件示例，包含两行数据：

```text
    +---------------------+  
    |      Magic Number   |  // "PAR1"  
    +---------------------+  
    |      Row Group 0    |  
    | +-----------------+ |  
    | |  Column Chunk 1 | |  // 用户 ID [1, 2]  
    | +-----------------+ |  
    | |  Column Chunk 2 | |  // 用户名 ["Alice", "Bob"]  
    | +-----------------+ |  
    | |  Column Chunk 3 | |  // 年龄 [25, 30]  
    | +-----------------+ |  
    | |  Column Chunk 4 | |  // 城市 ["北京", "上海"]  
    | +-----------------+ |  
    +---------------------+  
    |        Footer       |  // 文件架构、压缩方式、行组偏移量  
    +---------------------+  
    |   Footer Length     |  // 页脚长度  
    +---------------------+
```

#### 3.1.7 Parquet 文件的 16 进制表示

以下是一个简化的 Parquet 文件的 16 进制表示：

```text
    
    50 41 52 31  // Magic Number: "PAR1"  
    15 00 15 80  // Row Group 0: Column Chunk 用户 ID  
    15 80 15 80  // Row Group 0: Column Chunk 用户名  
    15 80 15 80  // Row Group 0: Column Chunk 年龄  
    15 80 15 80  // Row Group 0: Column Chunk 城市  
    15 80 15 80  // Footer: 文件架构  
    15 80 15 80  // Footer: 压缩方式  
    15 80 15 80  // Footer: 行组偏移量  
    15 80 15 80  // Footer: 其他元数据  
    15 80 15 80  // Footer: 页脚长度
```

### 3.2 总结

**文件结构层次**:

- **文件级别**：Header + Row Groups + Footer
- **行组级别**：多个 Column Chunks
- **列块级别**：多个 Pages
- **页级别**：Header + Data

**设计思想**:

- **分层存储**：便于并行处理和优化
- **元数据分离**：Footer 包含所有统计信息
- **页面化管理**：支持细粒度的压缩和编码
- `Row Group` 是并行处理的基本单位
- `Column Chunk` 实现了真正的列式存储
- `Page` 是压缩和编码的最小单位
- `Footer` 是查询优化的关键

---

## 4. 高级概念：Repetition Level 与 Definition Level

Parquet 的 **Repetition Level（重复层级）** 和 **Definition Level（定义层级）** 是处理嵌套数据结构的关键机制，尤其在列式存储中高效编码和重建复杂数据。这两个概念源自 Google 的 Dremel 论文，是 Parquet 格式处理嵌套数据的核心技术。

### 4.1 核心概念

#### 4.1.1 Repetition Level（重复层级）

**定义**：标记当前值在嵌套结构的哪个层级开始重复。

**作用机制**：

- 用于标识数组或重复字段中元素的嵌套位置
- 值范围：0 到最大嵌套深度
- 0 表示新记录的开始，大于 0 表示在某个层级的重复

**通俗理解**：当遇到一个数组或列表时，它告诉我们"当前值属于哪个层级的重复结构"。例如，一个用户有多个联系人，每个联系人有多个电话，`Repetition Level` 会标记电话属于哪个联系人。

#### 4.1.2 Definition Level（定义层级）

**定义**：标记当前值在嵌套结构中的存在深度，特别用于处理可选字段。

**作用机制**：

- 用于区分字段缺失（undefined）和字段为空值（null）
- 值范围：0 到路径中可选字段的最大深度
- 数值越大，表示路径定义得越深

**通俗理解**：如果某个字段是可选的（比如 null），`Definition Level` 会告诉我们"这个字段的父级路径存在到哪里"。例如，如果字段 `a.b.c` 存在，而路径 `a.b` 是必需的，但 `c` 是可选的，`Definition Level` 会表示 `c` 是否存在。

### 4.2 Dremel 论文示例分析

为了更好地理解 `Repetition Level` 和 `Definition Level` 的工作原理，我们使用 `Dremel` 论文中的经典示例进行详细分析。

#### 4.2.1 Schema 定义

以下是一个嵌套的 protobuf Schema 定义：

```protobuf
message Document {
  // 文档唯一标识符，可选字段（影响 Definition Level）
  optional int64 doc_id;                    
  
  // 链接信息组，重复字段（影响 Repetition Level）
  repeated group Links {                    
    optional string backward;               // 反向链接 URL，可选字段
    optional string forward;                // 正向链接 URL，可选字段
  }
  
  // 名称信息组，重复字段（多层嵌套的关键）
  repeated group Name {                     
    // 语言信息组，重复字段（嵌套重复结构）
    repeated group Language {               
      required string code;                 // 语言代码（如 "en", "zh"），必需字段
      optional string country;              // 国家代码（如 "us", "cn"），可选字段
    }
    optional string url;                    // 相关 URL，可选字段
  }
}
```

**Schema 设计特点**：

- **多层嵌套**：Document → Name → Language 形成三层嵌套结构
- **重复字段**：Links、Name、Language 都是重复字段，体现数组特性
- **可选字段**：doc_id、backward、forward、country、url 为可选，展示 null 值处理
- **必需字段**：code 为必需字段，确保数据完整性

**Schema 层级分析**：

- **层级 0**：Document（根节点）
- **层级 1**：Links、Name（重复组）
- **层级 2**：Language（嵌套重复组）
- **层级 3**：code、country（叶子节点）

#### 4.2.2 数据示例

基于上述 `Schema`，我们构造一条包含多种嵌套情况的示例数据：

```json
{
  "doc_id": 10,
  "Links": [
    { "backward": "d1", "forward": "d2" },
    { "backward": "d3", "forward": "d4" }
  ],
  "Name": [
    {
      "Language": [
        { "code": "en", "country": "us" },    // 完整的语言信息
        { "code": "zh" }                      // 缺少 country 字段
      ],
      "url": "http://example.com"
    },
    {
      "Language": [
        { "code": "fr", "country": null }     // country 显式设为 null
      ]
      // 注意：此 Name 组缺少 url 字段
    }
  ]
}
```

**数据特点分析**：

- **多层嵌套结构**：Document → Name → Language，符合 Schema 定义
- **字段缺失情况**：Name[0].Language[1] 缺少 country 字段，Name[1] 缺少 url 字段
- **显式 null 值**：Name[1].Language[0].country 显式设为 null
- **重复字段展示**：Links 有 2 个元素，Name 有 2 个元素，Language 分别有 2 个和 1 个元素

**数据一致性验证**：

| **字段路径** | **Schema 要求** | **数据实际** | **一致性** |
|-------------|---------------|-------------|-----------|
| doc_id | optional int64 | 10 | ✅ 符合 |
| Links[0].backward | optional string | "d1" | ✅ 符合 |
| Links[0].forward | optional string | "d2" | ✅ 符合 |
| Name[0].Language[0].code | required string | "en" | ✅ 符合 |
| Name[0].Language[0].country | optional string | "us" | ✅ 符合 |
| Name[0].Language[1].code | required string | "zh" | ✅ 符合 |
| Name[0].Language[1].country | optional string | 缺失 | ✅ 符合（可选） |
| Name[0].url | optional string | "<http://example.com>" | ✅ 符合 |
| Name[1].Language[0].code | required string | "fr" | ✅ 符合 |
| Name[1].Language[0].country | optional string | null | ✅ 符合 |
| Name[1].url | optional string | 缺失 | ✅ 符合（可选） |

**验证结论**：示例数据完全符合 Schema 定义，涵盖了必需字段、可选字段、字段缺失和显式 null 值等各种情况。

#### 4.2.3 字段分析：`Name.Language.code`

**字段路径分析**：

- **完整路径**：`Document.Name.Language.code`
- **路径层级**：`Document(0) → Name(1) → Language(2) → code(3)`
- **字段属性**：`code` 为必需字段（required）
- **数据取值**：`"en"`, `"zh"`, `"fr"`

##### 4.2.3.1 Repetition Level 计算

**计算规则**：标记当前值在哪个嵌套层级开始重复。

**具体计算**：

| **序号** | **值** | **位置描述** | **Repetition Level** | **计算说明** |
|---------|-------|-------------|-------------------|-------------|
| 1 | "en" | Name[0].Language[0].code | 0 | 新文档开始，无重复 |
| 2 | "zh" | Name[0].Language[1].code | 2 | 在 Language 层级重复 |
| 3 | "fr" | Name[1].Language[0].code | 1 | 在 Name 层级重复 |

**重复层级说明**：

- **RL = 0**：表示新记录的开始
- **RL = 1**：表示在 Name 层级开始新的重复
- **RL = 2**：表示在 Language 层级开始新的重复

##### 4.2.3.2 Definition Level 计算

**计算规则**：由于 `code` 是必需字段，且路径中的所有父级都存在，所有值的 Definition Level 都为最大值。

**路径深度分析**：

- `Document(0)` → `Name(1)` → `Language(2)` → `code(3)`
- 最大 Definition Level = 3

**具体计算**：

| **序号** | **值** | **Definition Level** | **计算说明** |
|---------|-------|-------------------|-------------|
| 1 | "en" | 3 | 完整路径都存在 |
| 2 | "zh" | 3 | 完整路径都存在 |
| 3 | "fr" | 3 | 完整路径都存在 |

#### 4.2.4 数据存储表格

**`Name.Language.code` 字段的列式存储**：

| **值** | **Repetition Level** | **Definition Level** | **存储说明** |
|-------|---------------------|---------------------|-------------|
| "en"  | 0                   | 3                   | 新文档开始，完整路径 |
| "zh"  | 2                   | 3                   | Language 层级重复 |
| "fr"  | 1                   | 3                   | Name 层级重复 |

**数据重建过程**：

1. **"en" (RL=0, DL=3)**：从文档根开始，构建路径 `Document → Name[0] → Language[0] → code="en"`
2. **"zh" (RL=2, DL=3)**：在 Language 层级重复，构建 `Name[0] → Language[1] → code="zh"`
3. **"fr" (RL=1, DL=3)**：在 Name 层级重复，构建 `Name[1] → Language[0] → code="fr"`

**重建算法说明**：

- 根据 Repetition Level 确定从哪个层级开始构建新的嵌套结构
- 根据 Definition Level 确定路径的完整性和字段的存在性
- 按顺序处理每个值，逐步重建完整的嵌套数据结构

#### 4.2.5 字段分析：`Name.Language.country`

**字段路径分析**：

- **完整路径**：`Document.Name.Language.country`
- **路径层级**：`Document(0) → Name(1) → Language(2) → country(3)`
- **字段属性**：`country` 为可选字段（optional）
- **数据取值**：`"us"`、缺失、显式 `null`

**关键特点**：

- `Name` 和 `Language` 是重复字段（影响 Repetition Level）
- `country` 是可选字段（影响 Definition Level 的计算）

##### 4.2.5.1 Repetition Level 计算

**具体计算**：

| **序号** | **值** | **位置描述** | **Repetition Level** | **计算说明** |
|---------|-------|-------------|-------------------|-------------|
| 1 | "us" | Name[0].Language[0].country | 0 | 新文档开始 |
| 2 | 缺失 | Name[0].Language[1].country | 2 | Language 层级重复 |
| 3 | null | Name[1].Language[0].country | 1 | Name 层级重复 |

##### 4.2.5.2 Definition Level 计算

**计算规则**：对于可选字段，Definition Level 表示路径中已定义的层级深度。

**具体计算**：

| **序号** | **值** | **Definition Level** | **计算说明** |
|---------|-------|-------------------|-------------|
| 1 | "us" | 3 | 完整路径存在，字段有值 |
| 2 | 缺失 | 2 | 路径到 Language 存在，country 未定义 |
| 3 | null | 3 | 完整路径存在，字段显式为 null |

**Definition Level 含义解释**：

- **DL = 2**：表示路径到 `Language` 层级存在，但 `country` 字段在数据中缺失
- **DL = 3**：表示完整路径存在，`country` 字段有定义（包括显式的 null 值）

#### 4.2.6 `Name.Language.country` 数据存储表格

**`Name.Language.country` 字段的列式存储**：

| **值** | **Repetition Level** | **Definition Level** | **存储说明** |
|-------|---------------------|---------------------|-------------|
| "us"  | 0                   | 3                   | 新文档开始，完整路径，字段有值 |
| 缺失   | 2                   | 2                   | Language 层级重复，字段未定义 |
| null  | 1                   | 3                   | Name 层级重复，字段显式为 null |

**数据重建过程**：

**步骤 1：处理 "us" (RL=0, DL=3)**:

```bash
算法逻辑：
1. RL=0 → 从文档根开始新的记录
2. DL=3 → 完整路径存在，字段有值
3. 构建路径：Document → Name[0] → Language[0] → country="us"
4. 结果：创建第一个完整的嵌套结构
```

**步骤 2：处理 缺失 (RL=2, DL=2)**:

```bash
算法逻辑：
1. RL=2 → 在 Language 层级重复（层级 2）
2. DL=2 → 路径到 Language 存在，但 country 字段未定义
3. 构建路径：Name[0] → Language[1]（country 字段缺失）
4. 结果：在现有 Name[0] 下添加新的 Language[1]，但不包含 country
```

**步骤 3：处理 null (RL=1, DL=3)**:

```bash
算法逻辑：
1. RL=1 → 在 Name 层级重复（层级 1）
2. DL=3 → 完整路径存在，字段显式为 null
3. 构建路径：Name[1] → Language[0] → country=null
4. 结果：创建新的 Name[1] 结构，包含显式 null 值
```

**重建算法核心原理**：

- **Repetition Level 决定重复位置**：指示在哪个层级开始重复，从而确定数据结构的嵌套关系
- **Definition Level 决定字段存在性**：区分字段缺失（DL < 最大深度）和字段为 null（DL = 最大深度）
- **组合使用实现精确重建**：通过 RL 和 DL 的组合，可以完全重建原始的嵌套数据结构

**算法优势**：

- **空间效率**：只存储实际存在的字段值
- **类型安全**：明确区分 null 值和字段缺失
- **结构完整**：保持原始数据的嵌套层次关系

### 4.3 核心概念总结

#### 4.3.1 Repetition Level

- **核心作用**：标识当前值在哪个嵌套层级开始重复
- **计算方法**：比较当前记录与前一记录的路径，找到第一个不同的重复字段层级
- **应用场景**：重建嵌套数组结构，确定数据的层次关系
- **取值范围**：0 到字段路径的最大重复层级深度

#### 4.3.2 Definition Level  

- **核心作用**：标识路径中已定义的层级深度，处理可选字段和 null 值
- **计算方法**：统计从根节点到当前字段路径中存在的层级数量
- **应用场景**：区分字段缺失、字段为 null 和字段有值的不同情况
- **取值范围**：0 到字段路径的最大可选层级深度

#### 4.3.3 完整解析流程

**编码阶段（写入）**：

```bash
1. 遍历嵌套数据结构
2. 对每个叶子字段计算 RL 和 DL
3. 按列存储：[值, RL, DL] 三元组
4. 压缩存储到 Parquet 文件
```

**解码阶段（读取）**：

```bash
1. 读取列数据：[值, RL, DL] 序列
2. 根据 RL 确定嵌套结构的重复位置
3. 根据 DL 确定字段的存在性和值状态
4. 重建完整的嵌套数据结构
```

**算法核心逻辑**：

- **RL = 0**：开始新的顶层记录
- **RL > 0**：在指定层级重复，复用上层结构
- **DL < 最大深度**：字段在某层级缺失
- **DL = 最大深度**：字段完整存在（可能为 null）

#### 4.3.4 技术价值

- **存储效率**：通过 RL 和 DL 实现嵌套数据的高效列式存储，避免冗余信息
- **查询性能**：支持对嵌套结构的快速访问和过滤操作，无需解析整个记录
- **数据完整性**：准确保存和恢复复杂嵌套数据的原始结构，包括 null 值语义
- **压缩友好**：列式存储配合 RL/DL 编码，提供更好的压缩比

通过 `Repetition Level` 和 `Definition Level` 的精妙设计，`Parquet` 成功解决了列式存储处理嵌套数据的核心难题，实现了高效存储与精确重建的完美平衡。

---

## 5. Parquet 文件的高级特性

### 5.1 编码技术

#### 5.1.1 字典编码（Dictionary Encoding）

字典编码是 `Parquet` 中最重要的压缩技术，将重复值映射为整数索引。

**工作原理**：

```text
原始数据：["北京", "上海", "北京", "广州", "上海", "北京"]
字典表：{0: "北京", 1: "上海", 2: "广州"}
编码结果：[0, 1, 0, 2, 1, 0]
压缩效果：36字节 → 24字节 (33%压缩率)
```

**适用场景**：地区、部门、状态码等重复性高的字符串数据

#### 5.1.2 运行长度编码（RLE）

RLE 将连续相同值编码为 `(值, 重复次数)` 形式。

**编码示例**：

```text
原始数据：[25, 25, 25, 30, 30, 35, 35, 35, 35]
RLE 编码：[(25, 3), (30, 2), (35, 4)]
```

**应用场景**：嵌套数据的重复级别、定义级别编码，布尔值列等

#### 5.1.3 压缩算法

| **算法** | **压缩率** | **速度** | **适用场景** |
|---------|-----------|---------|-------------|
| **Snappy** | 中等 (2-4x) | 很快 | 实时查询 |
| **GZIP** | 高 (4-10x) | 慢 | 存储优先 |
| **ZSTD** | 高 (3-8x) | 快 | 通用推荐 |

### 5.2 查询优化技术

#### 5.2.1 数据跳过（Data Skipping）

数据跳过是 `Parquet` 实现高效查询的核心机制，通过元数据中的统计信息来避免读取不相关的数据块。

**核心机制**：

- 利用行组级和页级的 `Min`/`Max` 统计信息
- 谓词下推（`Predicate Pushdown`）到存储层
- 根据过滤条件跳过不符合的数据块

**性能提升**：

- **I/O 减少**：`60%-90%` 的不相关数据被跳过
- **查询加速**：典型场景下提升 5-50 倍性能

#### 5.2.2 列裁剪（Column Pruning）

列裁剪技术只读取查询中实际需要的列，避免读取无关列数据。

**核心优势**：

- 只读取 `SELECT` 和 `WHERE` 子句中需要的列
- 跳过无关列，显著减少 `I/O`
- 典型场景下可减少 `60%-90%` 的数据读取量

### 5.3 存储优化策略

#### 5.3.1 分区策略（Partitioning）

分区存储通过将数据按列值进行物理分割来提升查询性能。

**核心策略**：

- **时间分区**：按年/月/日分区，适用于时序数据
- **业务分区**：按部门/产品分区，适用于业务数据
- **哈希分区**：按用户 ID 哈希，实现负载均衡

**最佳实践**：

- 选择查询频繁且分布均匀的列作为分区键
- 单个分区建议 100MB-1GB 大小
- 避免过细分区导致的小文件问题

#### 5.3.2 文件大小优化

**推荐大小**：

- **HDFS 存储**：128MB-1GB
- **云存储**：100MB-500MB  
- **实时查询**：50MB-200MB

### 5.4 高级特性

#### 5.4.1 索引技术

**布隆过滤器**：

- 概率性数据结构，快速判断值是否存在
- 可跳过 80%-95% 的不相关行组
- 点查询性能提升 5-20 倍

**列索引**：

- 提供页级别的统计信息
- 实现比行组级更细粒度的数据跳过
- 精度提升 10-100 倍

#### 5.4.2 性能优化

**向量化执行**：

- 利用 SIMD 指令集加速数据处理
- 批量处理模式减少函数调用开销
- 数值计算性能提升 2-8 倍

**数据类型优化**：

- 丰富的逻辑类型系统（UTF8、DECIMAL、TIMESTAMP 等）
- 原生支持嵌套数据结构（LIST、MAP、STRUCT）
- 无需序列化开销

#### 5.4.3 并行处理

**并行读取**：

- 文件级、行组级、列级多层并行
- 支持流式读取和增量处理
- 适应不同的计算资源配置

**流式读取**：

- 支持行组级、批次级、列级、页级多种流式模式
- 有效控制内存使用，避免大文件内存溢出
- 多核利用率可达 80-95%，I/O 时间减少 50-80%

#### 5.4.4 Schema 演进

**兼容性管理**：

- 支持向前和向后兼容的 `Schema` 演进
- 安全的字段添加、删除和类型变更
- 自动兼容性检查和版本控制

**最佳实践**：

- 新字段设为可选类型
- 使用语义化版本管理
- 渐进式部署 `Schema` 变更

---

## 6 总结与展望

### 6.1 技术总结

Parquet 作为现代大数据生态系统的核心存储格式，其高级特性为数据处理带来了显著优势：

**核心技术价值**：

- **存储效率**：通过列式存储、高效编码和压缩技术，实现 70-90% 的存储空间节省
- **查询性能**：支持数据跳过、列裁剪等优化技术，查询性能提升 5-10 倍
- **生态兼容**：与主流大数据框架深度集成，提供统一的数据交换格式
- **扩展性强**：支持复杂数据类型、Schema 演进和并行处理

### 6.2 生态系统集成

Parquet 已成为大数据生态系统的标准格式，在各主流框架中得到广泛支持：

**计算引擎集成**：

- **Apache Spark**：原生支持，默认存储格式，支持谓词下推和列裁剪
- **Apache Flink**：完整支持读写，适用于流批一体化场景
- **Presto/Trino**：高性能查询引擎，充分利用 Parquet 的列式特性
- **Apache Drill**：无模式查询，动态发现 Parquet 文件结构

**数据湖架构**：

- **Apache Iceberg**：表格式层，提供 ACID 事务和时间旅行功能
- **Delta Lake**：支持 ACID 事务的数据湖存储层
- **Apache Hudi**：增量数据处理和近实时分析

**云平台支持**：

- **AWS**：S3、Athena、Glue、Redshift Spectrum 全面支持
- **Google Cloud**：BigQuery、Dataflow、Cloud Storage 原生集成
- **Azure**：Synapse Analytics、Data Factory、Data Lake Storage 深度整合

### 6.3 发展趋势

**技术演进方向**：

1. **性能优化**
   - 更高效的编码算法（如 ALP、FSST）
   - 硬件加速支持（GPU、FPGA）
   - 智能索引和统计信息

2. **功能扩展**
   - 更丰富的数据类型支持
   - 增强的 Schema 演进能力
   - 更好的嵌套数据处理

3. **生态整合**
   - 与流处理系统更深度集成
   - 支持更多编程语言和工具
   - 标准化的元数据管理

**应用前景**：

- **实时分析**：结合流处理技术，支持近实时的数据分析
- **机器学习**：作为特征存储的标准格式，支持大规模 ML 工作负载
- **数据治理**：提供更好的数据血缘追踪和质量管控能力

Parquet 将继续作为大数据存储的核心技术，在云原生、实时计算和智能分析等领域发挥重要作用，为构建现代数据架构提供坚实的技术基础。

---
