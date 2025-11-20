# 大规模数据场景：Parquet 文件与 CSV 行的对应关系

## 1. 场景说明

假设我们有一个包含 **300 万行**用户数据的 CSV 文件，将其转换为 Parquet 格式后，分析文件组织结构和行对应关系。

### 1.1 数据规模

- **CSV 文件**: 300 万行用户数据
- **每行大小**: 约 200 字节（包含 15 个字段）
- **总数据量**: 约 600 MB

### 1.2 字段结构

```csv
user_id,name,age,city,department,salary,join_date,last_login,email,phone,status,level,projects,skills,performance_score
```

---

## 2. Parquet 文件组织策略

### 2.1 行组（Row Group）配置

Parquet 文件会根据配置参数将数据划分为多个行组：

**典型配置**:

- `parquet.block.size`: 128 MB（默认值）
- 目标行组大小: 100-200 MB
- 每个行组包含: **50-100 万行**（取决于数据压缩率）

### 2.2 文件生成结果

对于 300 万行数据，Parquet 通常会生成：

#### 方案 A：单个文件，单个行组

```text
300万行 CSV → 1个 Parquet 文件 → 1个行组（包含300万行）
```

- **文件大小**: 约 120-180 MB（压缩比 4:1 - 5:1，使用 Snappy 压缩）
- **行组大小**: 120-180 MB
- **适用场景**: 中等规模数据，查询需要全表扫描

#### 方案 B：单个文件，多个行组

```text
300万行 CSV → 1个 Parquet 文件 → 3个行组（各100万行）
```

- **文件大小**: 约 120-180 MB
- **行组 1**: 40-60 MB，100 万行
- **行组 2**: 40-60 MB，100 万行
- **行组 3**: 40-60 MB，100 万行
- **适用场景**: 中等规模数据，支持并行查询

#### 方案 C：多个文件（分区存储）

```text
300万行 CSV → 6个 Parquet 文件 → 每个文件1个行组（各50万行）
```

- **总文件大小**: 约 120-180 MB
- **文件 1**: 20-30 MB，50 万行（如 department=技术部）
- **文件 2**: 20-30 MB，50 万行（如 department=销售部）
- **文件 3**: 20-30 MB，50 万行（如 department=市场部）
- **文件 4**: 20-30 MB，50 万行（如 department=人事部）
- **文件 5**: 20-30 MB，50 万行（如 department=财务部）
- **文件 6**: 20-30 MB，50 万行（如 department=其他）
- **适用场景**: 大规模数据，分区查询优化

---

## 3. 实际文件结构示例

### 3.1 CSV 文件结构（行式存储）

```text
CSV 文件 (600MB)
├── 行1: 1,Alice,25,北京,技术部,15000,...
├── 行2: 2,Bob,30,上海,销售部,12000,...
├── 行3: 3,Charlie,28,广州,市场部,13000,...
├── ...
└── 行3000000: 3000000,Zoe,29,成都,人事部,11000,...
```

### 3.2 Parquet 文件结构（列式存储）

#### 3.2.1 单个文件，3 个行组示例

```text
Parquet 文件 (150MB)
├── Magic Number: "PAR1"
├── Row Group 1 (50MB)
│   ├── Column Chunk: user_id [1-1000000]
│   ├── Column Chunk: name ["Alice", "Bob", ..., 第100万个名字]
│   ├── Column Chunk: age [25, 30, 28, ..., 第100万个年龄]
│   ├── ... 其他12个列块
│   └── 元数据: 最小值、最大值、行数=1000000
├── Row Group 2 (50MB)
│   ├── Column Chunk: user_id [1000001-2000000]
│   ├── Column Chunk: name [第1000001个名字, ..., 第200万个名字]
│   ├── Column Chunk: age [第1000001个年龄, ..., 第200万个年龄]
│   ├── ... 其他12个列块
│   └── 元数据: 最小值、最大值、行数=1000000
├── Row Group 3 (50MB)
│   ├── Column Chunk: user_id [2000001-3000000]
│   ├── Column Chunk: name [第2000001个名字, ..., "Zoe"]
│   ├── Column Chunk: age [第2000001个年龄, ..., 29]
│   ├── ... 其他12个列块
│   └── 元数据: 最小值、最大值、行数=1000000
└── Footer (元数据)
    ├── FileMetaData: 总行数=3000000, 版本信息, Schema
    ├── RowGroup元数据: 偏移量、大小、行数
    ├── Column元数据: 每列的统计信息
    └── Footer Length
```

#### 3.2.2 多个文件，分区存储示例

```text
目录: /data/users/
├── department=技术部/ (30MB)
│   └── part-00000.parquet (50万行)
├── department=销售部/ (24MB)
│   └── part-00001.parquet (50万行)
├── department=市场部/ (20MB)
│   └── part-00002.parquet (50万行)
├── department=人事部/ (26MB)
│   └── part-00003.parquet (50万行)
├── department=财务部/ (22MB)
│   └── part-00004.parquet (50万行)
└── department=其他/ (28MB)
    └── part-00005.parquet (50万行)
```

---

## 4. 行对应关系详解

### 4.1 物理存储 vs 逻辑行

**CSV 逻辑行 → Parquet 物理存储**的映射关系：

```text
CSV 行号      Parquet 位置
---------    -----------
1-1000000    → Row Group 1, 各列块的前100万个值
1000001-2000000 → Row Group 2, 各列块的中间100万个值
2000001-3000000 → Row Group 3, 各列块的后100万个值
```

### 4.2 列存储的优势

#### 4.2.1 查询场景：计算平均薪资

**CSV 方式**:

```python
# 需要读取600MB数据，解析所有字段
with open('users.csv') as f:
    salaries = []
    for line in f:
        columns = line.split(',')
        salaries.append(float(columns[5]))  # 薪资在第6列
    avg_salary = sum(salaries) / len(salaries)
```

- **I/O**: 600 MB
- **CPU**: 解析 15 个字段 × 300 万行

**Parquet 方式**:

```python
# 只读取salary列的数据
import pyarrow.parquet as pq
table = pq.read_table('users.parquet', columns=['salary'])
avg_salary = table['salary'].to_pandas().mean()
```

- **I/O**: 约 12 MB（只读取 salary 列，压缩后）
- **CPU**: 只处理 salary 列数据

---

## 5. 性能对比

### 5.1 存储效率

| **指标**     | **CSV** | **Parquet** | **优势**   |
| ------------ | ------- | ----------- | ---------- |
| 文件大小     | 600 MB  | 150 MB      | 4:1 压缩比 |
| 读取 1000 行 | 600 KB  | 150 KB      | 4 倍更快   |
| 单列查询     | 600 MB  | 12 MB       | 50 倍更快  |

### 5.2 查询性能

| **查询类型** | **CSV 耗时** | **Parquet 耗时** | **加速比** |
| ------------ | ------------ | ---------------- | ---------- |
| 全表扫描     | 6.0s         | 2.4s             | 2.5x       |
| 单列聚合     | 6.0s         | 0.3s             | 20x        |
| 条件查询     | 6.0s         | 0.9s             | 6.7x       |

---

## 6. 配置建议

### 6.1 行组大小优化

```python
# Spark 配置示例
spark.conf.set("parquet.block.size", 134217728)  # 128MB
spark.conf.set("parquet.page.size", 1048576)     # 1MB
spark.conf.set("parquet.dictionary.page.size", 8388608)  # 8MB

# 控制行组行数
df.repartition(6)  # 生成6个文件，每个约50万行
```

### 6.2 分区策略

```python
# 按部门分区
df.write.partitionBy("department").parquet("/output/users")

# 按时间分区
df.write.partitionBy("year", "month").parquet("/output/users")
```

---

## 7. 总结

对于 300 万行数据的典型场景：

1. **文件数量**: 通常生成 1-6 个 Parquet 文件
2. **行组数量**: 每个文件 1-3 个行组
3. **行数分布**: 每个行组包含 50-100 万行
4. **存储节省**: 4:1 压缩比，600MB → 150MB
5. **查询优势**: 列裁剪使单列查询快 20-50 倍

Parquet 的列式存储设计通过智能的文件组织、行组划分和列裁剪技术，在大数据场景下提供了显著的性能和存储优势。
