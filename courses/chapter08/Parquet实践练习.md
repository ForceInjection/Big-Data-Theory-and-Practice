# Parquet 文件格式实践练习

## 1. 练习目标

通过本次实践练习，您将掌握：

1. **Parquet 文件的基本读写操作**
2. **列式存储的性能优势验证**
3. **数据压缩和编码技术的应用**
4. **查询优化技术（投影下推、谓词下推）**
5. **分区存储的实现和优势**
6. **嵌套数据结构的处理**

**关键概念说明**：

- **投影下推 (Projection Pushdown)**: 只读取查询需要的列，减少 I/O 操作和数据传输量
- **谓词下推 (Predicate Pushdown)**: 在数据读取时进行过滤，避免读取不必要的数据
- **列式存储 (Columnar Storage)**: 按列而非按行存储数据，提高压缩率和查询性能
- **分区存储 (Partitioning)**: 按特定字段将数据分成多个目录，优化查询性能

---

## 2. 练习题目

### 2.1 基础练习：Parquet 文件读写

**题目要求：**

1. 创建一个包含 100,000 条用户数据的数据集
2. 将数据保存为 Parquet 格式
3. 读取 Parquet 文件并验证数据完整性
4. 比较 Parquet 文件与 CSV 文件的大小差异，记录以下指标：
   - CSV 文件大小（MB）
   - Parquet 文件大小（MB）
   - 压缩比（CSV 大小/Parquet 大小）
   - 存储空间节省百分比（(CSV 大小-Parquet 大小)/CSV 大小 ×100%）

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
5. **性能指标**：记录文件大小（MB）、读取时间（秒）、压缩比等量化指标

**完整代码示例：**

```python
# 基础练习完整实现
import pandas as pd
import numpy as np
from faker import Faker
import os
import time
from datetime import datetime, timedelta

def basic_parquet_exercise():
    """基础 Parquet 练习完整实现"""

    # 1. 数据生成
    print("生成 100,000 条用户数据...")
    fake = Faker('zh_CN')
    np.random.seed(42)

    cities = ['北京', '上海', '广州', '深圳', '杭州', '南京', '成都', '武汉', '西安', '重庆']

    data = {
        'user_id': range(1, 100001),
        'username': [f'user_{i:06d}' for i in range(1, 100001)],
        'age': np.random.randint(18, 80, 100000),
        'city': np.random.choice(cities, 100000),
        'register_time': [datetime.now() - timedelta(days=np.random.randint(0, 365))
                         for _ in range(100000)],
        'income': np.random.uniform(3000, 50000, 100000).round(2)
    }

    df = pd.DataFrame(data)
    print(f"数据生成完成，形状: {df.shape}")

    # 2. 保存为 CSV 和 Parquet
    print("\n保存文件...")

    # CSV 文件
    csv_start = time.time()
    df.to_csv('user_data.csv', index=False)
    csv_time = time.time() - csv_start

    # Parquet 文件
    parquet_start = time.time()
    df.to_parquet('user_data.parquet', engine='pyarrow')
    parquet_time = time.time() - parquet_start

    # 3. 文件大小比较
    csv_size = os.path.getsize('user_data.csv') / (1024 * 1024)  # MB
    parquet_size = os.path.getsize('user_data.parquet') / (1024 * 1024)  # MB

    compression_ratio = csv_size / parquet_size
    space_saving = ((csv_size - parquet_size) / csv_size) * 100

    print(f"\n=== 性能对比结果 ===")
    print(f"CSV 文件大小: {csv_size:.2f} MB")
    print(f"Parquet 文件大小: {parquet_size:.2f} MB")
    print(f"压缩比: {compression_ratio:.2f}")
    print(f"存储空间节省: {space_saving:.1f}%")
    print(f"CSV 写入时间: {csv_time:.3f} 秒")
    print(f"Parquet 写入时间: {parquet_time:.3f} 秒")

    # 4. 读取验证
    print("\n验证数据完整性...")

    csv_read_start = time.time()
    df_csv = pd.read_csv('user_data.csv')
    csv_read_time = time.time() - csv_read_start

    parquet_read_start = time.time()
    df_parquet = pd.read_parquet('user_data.parquet')
    parquet_read_time = time.time() - parquet_read_start

    print(f"CSV 读取时间: {csv_read_time:.3f} 秒")
    print(f"Parquet 读取时间: {parquet_read_time:.3f} 秒")

    # 数据完整性验证
    assert df.shape == df_parquet.shape, "数据形状不匹配"
    assert df.columns.tolist() == df_parquet.columns.tolist(), "列名不匹配"
    assert df['user_id'].equals(df_parquet['user_id']), "数据内容不匹配"

    print("✅ 数据完整性验证通过！")

    return {
        'csv_size_mb': csv_size,
        'parquet_size_mb': parquet_size,
        'compression_ratio': compression_ratio,
        'space_saving_percent': space_saving,
        'csv_write_time': csv_time,
        'parquet_write_time': parquet_time,
        'csv_read_time': csv_read_time,
        'parquet_read_time': parquet_read_time
    }

if __name__ == "__main__":
    results = basic_parquet_exercise()
```

**预期运行结果示例：**

```text
生成 100,000 条用户数据...
数据生成完成，形状: (100000, 6)

保存文件...

=== 性能对比结果 ===
CSV 文件大小: 8.73 MB
Parquet 文件大小: 2.15 MB
压缩比: 4.06
存储空间节省: 75.4%
CSV 写入时间: 0.452 秒
Parquet 写入时间: 0.218 秒

验证数据完整性...
CSV 读取时间: 0.125 秒
Parquet 读取时间: 0.087 秒
✅ 数据完整性验证通过！
```

**评分标准：**

- **数据生成正确性（25%）**

  - 数据类型定义准确（5%）
  - 数据范围合理（年龄、收入等）（5%）
  - 数据量达到 10 万条（5%）
  - 城市字段分布均匀（5%）
  - 时间戳格式正确（5%）

- **Parquet 文件读写功能（35%）**

  - 成功保存为 Parquet 格式（10%）
  - 正确读取 Parquet 文件（10%）
  - 数据完整性验证通过（10%）
  - 文件扩展名规范（.parquet）（5%）

- **性能对比分析（15%）**
  - CSV 和 Parquet 文件大小记录（5%）
  - 压缩比计算正确（5%）
  - 性能对比结论合理（5%）

### 2.2 进阶练习：压缩算法比较

**题目要求：**

1. 使用不同的压缩算法（SNAPPY、GZIP、LZ4、BROTLI）保存同一数据集
2. 比较不同压缩算法的文件大小和读写性能，记录以下指标：
   - 各算法压缩后的文件大小（MB）
   - 各算法的压缩率（原始大小/压缩后大小）
   - 各算法的写入时间（秒）
   - 各算法的读取时间（秒）
3. 绘制性能对比图表，包括文件大小对比和读写时间对比

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

- **压缩算法实现正确性（30%）**

  - 四种压缩算法都正确实现（SNAPPY、GZIP、LZ4、BROTLI）（10%）
  - 压缩参数设置正确（10%）
  - 文件命名体现压缩算法（5%）
  - 无压缩错误或异常（5%）

- **性能测量准确性（25%）**

  - 读写时间测量方法正确（time.time()或 timeit）（10%）
  - 多次测量取平均值（5%）
  - 时间单位正确（秒）（5%）
  - 测量环境一致（5%）

- **数据分析和对比（25%）**

  - 文件大小对比表格完整（10%）
  - 读写性能对比分析（10%）
  - 压缩率计算正确（5%）

- **图表展示效果（20%）**
  - 图表类型选择合适（柱状图/折线图）（5%）
  - 坐标轴标签清晰（5%）
  - 图例说明完整（5%）
  - 可视化效果美观（5%）

### 2.3 高级练习：查询优化

**题目要求：**

1. 实现投影下推（只读取需要的列）
2. 实现谓词下推（在读取时过滤数据）
3. 比较优化前后的查询性能，记录以下指标：
   - 全表扫描读取时间（秒）
   - 投影下推读取时间（秒）
   - 谓词下推读取时间（秒）
   - 内存使用量对比（MB）
   - 性能提升百分比

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

- **投影下推实现（25%）**

  - 正确使用 columns 参数（10%）
  - 只读取指定列（5%）
  - 内存使用减少验证（5%）
  - 读取时间对比记录（5%）

- **谓词下推实现（25%）**

  - 正确使用 filters 参数（10%）
  - 过滤条件语法正确（5%）
  - 数据过滤准确性（5%）
  - PyArrow API 使用正确（5%）

- **性能测量和对比（30%）**

  - 全表扫描时间测量（10%）
  - 优化查询时间测量（10%）
  - 性能提升百分比计算（5%）
  - 测量方法科学（多次平均）（5%）

- **优化效果分析（20%）**
  - 性能提升数据分析（10%）
  - 优化原理理解正确（5%）
  - 实际应用场景说明（5%）

### 2.4 专家练习：分区存储

**题目要求：**

1. 按城市对数据进行分区存储
2. 实现分区数据的读取和查询
3. 比较分区存储与非分区存储的查询性能，记录以下指标：
   - 非分区查询时间（秒）
   - 分区查询时间（秒）
   - 性能提升百分比
   - 分区目录数量
   - 各分区数据量分布

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

- **分区存储实现（30%）**

  - 正确使用 partition_cols 参数（10%）
  - 分区目录结构正确（5%）
  - 分区字段选择合理（5%）
  - 数据按分区正确存储（10%）

- **分区查询功能（25%）**

  - 特定分区读取功能（10%）
  - 分区过滤查询实现（10%）
  - 查询结果准确性（5%）

- **性能对比分析（25%）**

  - 分区查询时间测量（10%）
  - 全表扫描时间测量（10%）
  - 性能对比结论合理（5%）

- **分区策略评估（20%）**
  - 分区字段选择理由充分（10%）
  - 分区优缺点分析（5%）
  - 实际应用建议（5%）

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

- **嵌套数据结构设计（25%）**

  - 嵌套结构设计合理（10%）
  - 数据类型定义准确（5%）
  - 数据示例完整（5%）
  - Schema 复杂度适当（5%）

- **Parquet 存储实现（25%）**

  - 正确保存嵌套数据（10%）
  - Schema 定义正确（10%）
  - 文件格式规范（5%）

- **数据读取和解析（30%）**

  - 嵌套数据正确读取（10%）
  - 数据解析准确性（10%）
  - 嵌套字段访问正确（10%）

- **复杂查询操作（20%）**
  - 嵌套字段查询实现（10%）
  - 查询性能测量（5%）
  - 查询结果正确性（5%）

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

### 3.4 实践技巧和错误处理

**实践技巧：**

1. **数据类型优化**：

   - 使用 `category` 类型存储重复的字符串字段
   - 使用合适的整数类型（int8, int16, int32）减少存储
   - 时间戳使用 datetime64 类型

2. **性能优化**：

   - 批量处理数据，避免多次小文件操作
   - 使用合适的压缩算法平衡压缩率和速度
   - 设置合适的行组大小（通常 128MB-256MB）

3. **内存管理**：
   - 大文件使用分块读取（chunksize）
   - 及时释放不再使用的 DataFrame
   - 监控内存使用情况

**常见错误处理：**

1. **内存不足错误**：

   - 减少数据量分批处理
   - 使用 `pd.read_parquet(..., chunksize=10000)`
   - 优化数据类型减少内存占用

2. **压缩算法不支持**：

   - 确保安装了对应的压缩库
   - 检查 PyArrow 版本是否支持该算法

3. **Schema 不匹配**：

   - 写入和读取时保持 Schema 一致
   - 使用明确的 Schema 定义

4. **分区路径错误**：

   - 确保分区字段值不包含特殊字符
   - 分区目录命名符合 `column=value` 格式

5. **嵌套数据解析错误**：
   - 确保嵌套结构定义正确
   - 使用 PyArrow Schema 明确数据类型

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
