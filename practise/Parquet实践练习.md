# Parquet 文件格式实践练习

## 1. 练习目标

通过本次实践练习，您将掌握：

1. **Parquet 文件的基本读写操作**
2. **列式存储的性能优势验证**
3. **数据压缩和编码技术的应用**
4. **查询优化技术（投影下推、谓词下推）**
5. **分区存储的实现和优势**
6. **嵌套数据结构的处理**

---

## 2. 练习题目

### 2.1 基础练习：Parquet 文件读写

**题目要求：**

1. 创建一个包含 100,000 条用户数据的数据集
2. 将数据保存为 Parquet 格式
3. 读取 Parquet 文件并验证数据完整性
4. 比较 Parquet 文件与 CSV 文件的大小差异

**数据结构：**

- 用户 ID（整数）
- 用户名（字符串）
- 年龄（整数）
- 城市（字符串，从预定义列表中随机选择）
- 注册时间（时间戳）
- 收入（浮点数）

**实现提示：**

1. **数据生成**：使用 `pandas` 和 `faker` 库生成模拟数据
2. **Parquet 写入**：使用 `pandas.DataFrame.to_parquet()` 方法
3. **数据读取**：使用 `pandas.read_parquet()` 方法
4. **文件大小比较**：使用 `os.path.getsize()` 获取文件大小

**关键代码提示：**

```python
# 数据生成示例
import pandas as pd
from faker import Faker

# Parquet 文件操作
df.to_parquet('data.parquet')
df_read = pd.read_parquet('data.parquet')
```

**评分标准：**

- 数据生成正确性（25%）
- Parquet 文件读写功能（35%）
- 数据完整性验证（25%）
- 性能对比分析（15%）

### 2.2 进阶练习：压缩算法比较

**题目要求：**

1. 使用不同的压缩算法（SNAPPY、GZIP、LZ4、BROTLI）保存同一数据集
2. 比较不同压缩算法的文件大小和读写性能
3. 绘制性能对比图表

**实现提示：**

1. **压缩算法设置**：在 `to_parquet()` 方法中使用 `compression` 参数
2. **性能测量**：使用 `time.time()` 或 `timeit` 模块测量读写时间
3. **图表绘制**：使用 `matplotlib` 或 `seaborn` 创建对比图表

**关键代码提示：**

```python
# 不同压缩算法保存
df.to_parquet('data_snappy.parquet', compression='snappy')
df.to_parquet('data_gzip.parquet', compression='gzip')

# 性能测量示例
import time
start_time = time.time()
# 执行操作
end_time = time.time()
duration = end_time - start_time
```

**评分标准：**

- 压缩算法实现正确性（30%）
- 性能测量准确性（25%）
- 数据分析和对比（25%）
- 图表展示效果（20%）

### 2.3 高级练习：查询优化

**题目要求：**

1. 实现投影下推（只读取需要的列）
2. 实现谓词下推（在读取时过滤数据）
3. 比较优化前后的查询性能

**实现提示：**

1. **投影下推**：使用 `columns` 参数只读取指定列
2. **谓词下推**：使用 `filters` 参数在读取时过滤数据
3. **性能对比**：测量全表扫描与优化查询的时间差异

**关键代码提示：**

```python
# 投影下推 - 只读取指定列
df_projected = pd.read_parquet('data.parquet', columns=['user_id', 'age'])

# 谓词下推 - 读取时过滤
import pyarrow.parquet as pq
table = pq.read_table('data.parquet', filters=[('age', '>', 25)])
df_filtered = table.to_pandas()
```

**评分标准：**

- 投影下推实现（25%）
- 谓词下推实现（25%）
- 性能测量和对比（30%）
- 优化效果分析（20%）

### 2.4 专家练习：分区存储

**题目要求：**

1. 按城市对数据进行分区存储
2. 实现分区数据的读取和查询
3. 比较分区存储与非分区存储的查询性能

**实现提示：**

1. **分区写入**：使用 `partition_cols` 参数按列分区
2. **分区读取**：读取特定分区或使用分区过滤
3. **性能对比**：测量分区查询与全表扫描的性能差异

**关键代码提示：**

```python
# 分区存储
df.to_parquet('partitioned_data', partition_cols=['city'])

# 读取特定分区
df_city = pd.read_parquet('partitioned_data/city=Beijing')

# 分区过滤查询
df_filtered = pd.read_parquet('partitioned_data', 
                             filters=[('city', '=', 'Beijing')])
```

**评分标准：**

- 分区存储实现（30%）
- 分区查询功能（25%）
- 性能对比分析（25%）
- 分区策略评估（20%）

### 2.5 挑战练习：嵌套数据结构

**题目要求：**

1. 创建包含嵌套结构的数据（用户信息包含多个联系方式）
2. 将嵌套数据保存为 Parquet 格式
3. 读取并正确解析嵌套数据

**实现提示：**

1. **嵌套数据创建**：使用字典或列表创建嵌套结构
2. **Schema 定义**：使用 PyArrow 定义复杂数据类型
3. **数据解析**：正确处理嵌套字段的读取和访问

**关键代码提示：**

```python
import pyarrow as pa

# 定义嵌套 Schema
schema = pa.schema([
    ('user_id', pa.int64()),
    ('contacts', pa.list_(pa.struct([
        ('type', pa.string()),
        ('value', pa.string())
    ])))
])

# 创建嵌套数据
nested_data = {
    'user_id': [1, 2],
    'contacts': [
        [{'type': 'email', 'value': 'user1@example.com'}],
        [{'type': 'phone', 'value': '123456789'}]
    ]
}
```

**评分标准：**

- 嵌套数据结构设计（25%）
- Parquet 存储实现（25%）
- 数据读取和解析（30%）
- 复杂查询操作（20%）

---

## 3. 练习总结和思考题

### 3.1 练习总结

通过本次实践练习，您应该已经掌握了：

1. **Parquet 文件的基本操作**：读写、压缩、查询优化
2. **性能优势**：相比 CSV 格式的存储和查询性能提升
3. **高级特性**：分区存储、嵌套数据处理
4. **实际应用**：在大数据场景中的最佳实践

### 3.2 思考题

1. **为什么 Parquet 格式在大数据场景中比 CSV 格式更受欢迎？**
2. **在什么情况下应该选择不同的压缩算法？**
3. **分区存储的优缺点是什么？如何选择合适的分区字段？**
4. **如何在实际项目中平衡查询性能和存储成本？**
5. **Parquet 格式在机器学习项目中有哪些应用场景？**

### 3.3 常见问题解答

**Q1: 为什么我的 Parquet 文件比 CSV 文件还大？**
A: 这可能是因为数据量较小或数据类型不适合列式存储。Parquet 的优势在大数据集上更明显，建议使用 10 万条以上的数据进行测试。

**Q2: 压缩算法选择有什么建议？**
A:

- **SNAPPY**：平衡压缩率和速度，适合大多数场景
- **GZIP**：高压缩率，适合存储优先的场景
- **LZ4**：极快的压缩/解压速度，适合实时处理
- **BROTLI**：最高压缩率，适合长期存储

**Q3: 分区字段应该如何选择？**
A: 选择分区字段的原则：

- 查询时经常用作过滤条件的字段
- 基数适中（不要太高也不要太低）
- 数据分布相对均匀的字段
- 避免使用高基数字段（如用户 ID）

**Q4: 遇到内存不足错误怎么办？**
A:

- 减少数据量进行测试
- 使用 `chunksize` 参数分批处理
- 优化数据类型，使用更小的数据类型
- 增加系统内存或使用更强大的机器

**Q5: PyArrow 和 Pandas 在 Parquet 操作上有什么区别？**
A:

- **PyArrow**：更底层，支持更多 Parquet 特性，性能更好
- **Pandas**：更易用，与数据分析工作流集成更好
- 建议：简单操作用 Pandas，复杂操作用 PyArrow

### 3.4 扩展练习

1. **集成 Apache Spark**：使用 PySpark 处理大规模 Parquet 数据
2. **数据湖实践**：结合 Delta Lake 或 Apache Iceberg 构建数据湖
3. **云存储集成**：将 Parquet 文件存储到 AWS S3、阿里云 OSS 等云存储服务
4. **实时数据处理**：结合 Apache Kafka 和 Parquet 实现实时数据存储

---

## 4. 参考资料

本练习基于以下技术文档和资料：

1. **Apache Parquet 官方文档**：<https://parquet.apache.org/docs/>
2. **Dremel 论文**：[Melnik 等 - Dremel Interactive Analysis of Web-Scale Datasets](../paper/Melnik%20等%20-%20Dremel%20Interactive%20Analysis%20of%20Web-Scale%20Datasets.pdf)
3. **列式存储技术综述**：相关学术论文和技术报告
4. **Pandas 和 PyArrow 官方文档**：数据处理和 `Parquet` 操作指南

---
