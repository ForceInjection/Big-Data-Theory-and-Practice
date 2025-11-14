# 大数据理论与实践课程实践作业二

## 1. 作业概述

本次作业是《大数据理论与实践》课程的**实践作业二**，通过三道递进式编程题目，让大家系统掌握 Hadoop 生态系统中的核心技术：**HDFS 基础操作**、**MapReduce 编程模型**，以及 **MapReduce 高级特性**和**复杂数据处理技术**。

### 1.1 实验环境信息

**开发模式**：本地开发 + 远程集群运行

- **本地开发环境**：

  - Java 版本：JDK 8（必须）
  - Hadoop 客户端：3.4.2
  - 开发工具：推荐 IntelliJ IDEA 或 Eclipse
  - 构建工具：Maven 或 Gradle

- **远程运行环境**：
  - 集群类型：共享 Hadoop 3.4.2 集群
  - 集群组件：HDFS + YARN + MapReduce
  - 访问方式：通过配置文件连接远程集群

**重要说明**：集群访问信息（包括 NameNode、DataNode、ResourceManager、NodeManager 等节点的 IP 地址和端口号）将单独提供。

### 1.2 作业目标

通过三道递进式编程题目，逐步掌握以下核心技能：

1. **基础技能**：HDFS 文件操作与标准 MapReduce 编程模型
2. **优化技能**：MapReduce 高级特性（Combiner 和 Partitioner）的应用
3. **调优技能**：MapReduce 任务调优与性能分析技术

### 1.3 评分标准

本次作业总分为 **100 分**，具体分值分配如下：

| **题目**                                   | **分值** | **主要考查内容**                                    |
| ------------------------------------------ | -------- | --------------------------------------------------- |
| **题目一：HDFS 操作与 WordCount 实现**     | 30 分    | HDFS 基础操作、MapReduce 编程模型、Driver 配置      |
| **题目二：自定义 Combiner 和 Partitioner** | 40 分    | MapReduce 高级特性、性能优化、数据分布控制          |
| **题目三：MapReduce 任务调优与性能分析**   | 30 分    | 任务调优配置、性能监控、Combiner 优化、实际应用能力 |

**评分重点**：

- **功能实现**（70%）：程序正确性、输出格式、异常处理
- **技术深度**（20%）：高级特性应用、性能优化效果
- **代码质量**（10%）：代码结构、注释完整性、编程规范

### 1.4 注意事项

1. **代码质量要求**：

   - 确保代码能够正确编译和运行
   - 遵循良好的编程规范和注释习惯
   - 在提交前进行充分的功能测试

2. **学术诚信要求**：

   - 鼓励同学间的技术讨论和思路交流
   - 严禁直接抄袭他人代码
   - 实验报告应体现个人的独立思考和分析过程

3. **技术支持渠道**：
   - 遇到技术问题可通过**微信群**寻求帮助
   - 建议先查阅相关文档和资料，培养自主解决问题的能力

---

## 2. 题目要求

本章包含三个递进式的 MapReduce 编程题目，旨在帮助学生掌握 Hadoop MapReduce 的核心概念和高级特性。每个题目都有明确的分值分配和技术要求，请按顺序完成。

### 2.1 测试数据说明

所有题目使用统一的测试数据集，数据统一存放在 HDFS 的公共数据目录 `/public/data/wordcount/` 中。我们提供了四个不同规模的测试文件，适用于不同的测试场景：

#### 2.1.1 测试文件列表

**1. 简单测试文件**：

- **HDFS 路径**：`/public/data/wordcount/simple-test.txt`
- **大小**：414 字节
- **行数**：10 行
- **用途**：适合初学者和快速功能验证
- **内容**：包含 Hadoop、MapReduce、HDFS、Spark 等大数据相关词汇

**2. 中等规模测试文件（《爱丽丝梦游仙境》）**：

- **HDFS 路径**：`/public/data/wordcount/alice-in-wonderland.txt`
- **大小**：148KB
- **行数**：3,384 行
- **来源**：古腾堡计划 (Project Gutenberg)
- **书籍 ID**：11
- **作者**：Lewis Carroll
- **用途**：中等规模数据处理测试

**3. 大规模测试文件（《傲慢与偏见》）**：

- **HDFS 路径**：`/public/data/wordcount/pride-and-prejudice.txt`
- **大小**：735KB
- **行数**：14,537 行
- **来源**：古腾堡计划 (Project Gutenberg)
- **书籍 ID**：1342
- **作者**：Jane Austen
- **用途**：大规模数据处理和性能测试

**4. 超大规模测试文件（经典文学作品合集）**：

- **HDFS 路径**：`/public/data/wordcount/all_books_merged.txt`
- **大小**：约 311MB
- **行数**：6,675,055 行
- **单词数**：56,274,736 个单词
- **内容**：614 本经典英文文学作品合集
- **来源**：古腾堡计划 (Project Gutenberg) 公共领域作品
- **作品包含**：柏拉图、莎士比亚、狄更斯、托尔斯泰、马克·吐温、简·奥斯汀等著名作家的代表作品
- **用途**：大规模 MapReduce 性能测试、调优和监控

#### 2.1.2 数据使用建议

- **题目一**：建议使用简单测试文件和中等规模文件进行基础功能验证
- **题目二**：推荐使用大规模和超大规模文件测试 Combiner 和 Partitioner 的优化效果
- **题目三**：主要使用超大规模文件进行性能监控和调优实验
- **程序调试**：建议先用简单测试文件验证程序逻辑，再用大文件测试性能

#### 2.1.3 注意事项

- 程序应支持处理单个文件或整个目录中的所有文件
- 建议在程序中添加文件存在性检查和错误处理
- 输出结果保存到个人目录 `/users/<学号>/homework1/problem[1-3]`
- 数据规模足够大，能够有效展示 MapReduce 任务调优、Combiner 优化和性能监控的效果

---

### 2.2 题目一：HDFS 基础操作与 WordCount 实现（30 分）

#### 2.2.1 任务描述

实现一个完整的 WordCount 程序，包含 **HDFS 文件操作**和 **MapReduce 词频统计**两个核心功能。此题目是后续题目的基础，要求学生熟练掌握 MapReduce 的基本编程模式。

#### 2.2.2 具体要求

1. **HDFS 操作部分**：

   - **输入检查**：程序启动时检查公共输入目录 `/public/data/wordcount` 是否存在
   - **结果展示**：程序完成后显示输出目录的文件列表和处理统计信息

2. **MapReduce 程序部分**：

   - **Mapper 实现**：

     - 实现文本分词和词频统计的 Map 逻辑
     - 处理文本编码和特殊字符
     - 统一大小写并过滤无效单词
     - 输出 (单词, 1) 键值对

   - **Reducer 实现**：

     - 实现词频聚合的 Reduce 逻辑
     - 正确累加相同单词的出现次数
     - 输出最终的词频统计结果

   - **Driver 配置**：
     - 配置 MapReduce 作业参数
     - 设置输入输出路径和格式
     - 处理输出目录的清理工作

3. **输出格式要求**:
   - **结果目录**：`/users/<学号>/homework1/problem1`
   - **主要结果文件**：`words.txt`
     - 格式：`单词\t词频`（制表符分隔），每行一个单词统计
     - 排序：按单词字典序排列
     - 示例：`apple\t15`、`banana\t8`
   - **统计信息文件**：`statistics.txt`
     - 格式：`统计项名称\t数值`（制表符分隔）
     - 排序：统计项按字典序排列
     - 必需统计项：
       - `input_files\t[数值]`：输入文件数量
       - `processing_time\t[数值]`：处理时间（毫秒）
       - `total_words\t[数值]`：总单词数
       - `unique_words\t[数值]`：不重复单词数

#### 2.2.3 测试数据

**测试建议**：

- 建议先使用简单测试文件验证程序基本功能
- 使用中等和大规模文件测试程序的稳定性和性能
- 输出结果保存到个人目录 `/users/<学号>/homework1/problem1`

#### 2.2.4 代码框架

```java
// WordCountMapper.java
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // TODO: 实现单词分割和计数逻辑
        // 1. 获取输入行并转换为小写
        // 2. 按空格分割单词
        // 3. 清理单词：去除标点符号，过滤空字符串
        // 4. 输出 (单词, 1) 键值对
    }
}

// WordCountReducer.java
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        // TODO: 实现单词计数的聚合逻辑
        // 1. 初始化计数器为 0
        // 2. 遍历 values 中的所有计数值并累加
        // 3. 将累加结果写入 result 变量
        // 4. 输出最终的 (单词, 总计数) 结果
    }
}

// WordCountDriver.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        // TODO: 创建 Configuration 和 Job 对象

        // TODO: 设置 Job 参数
        // 1. 设置 Mapper 和 Reducer 类
        // 2. 设置输出键值对类型
        // 3. 设置 JAR 文件

        // TODO: 实现 HDFS 操作
        // 1. 检查输入目录是否存在
        // 2. 删除已存在的输出目录

        // TODO: 设置输入输出路径
        // 1. 使用 FileInputFormat.addInputPath()
        // 2. 使用 FileOutputFormat.setOutputPath()

        // TODO: 提交作业并等待完成
        // 使用 job.waitForCompletion(true)

        // TODO: 显示处理结果和统计信息，并保存到个人目录
        // 1. 从 Job 中获取 Counters 信息
        // 2. 打印总单词数、不重复单词数等统计信息
        // 3. 按照 2.1.2 节中的输出格式要求保存 words.txt 和 statistics.txt 文件
    }
}
```

#### 2.2.5 评分标准（30 分）

| **评分项目**       | **分值** | **评分标准**                                         |
| ------------------ | -------- | ---------------------------------------------------- |
| **HDFS 操作**      | 6 分     | 正确检查公共数据目录、清理输出目录、显示文件统计信息 |
| **MapReduce 实现** | 18 分    | Mapper 正确解析和处理单词、Reducer 正确聚合和排序    |
| **Driver 配置**    | 4 分     | 正确配置 Job、设置输入输出路径、HDFS 操作集成        |
| **代码质量**       | 2 分     | 代码结构清晰、注释完整、命名规范                     |

### 2.3 题目二：自定义 Combiner 和 Partitioner（40 分）

#### 2.3.1 任务描述

基于题目一的 WordCount 程序，实现自定义 **Combiner** 和 **Partitioner**，深入理解 MapReduce 的性能优化机制和数据分布控制策略。

#### 2.3.2 具体要求

1. **自定义 Combiner**：

   - 实现 `WordCountCombiner` 类，在 Map 端进行本地聚合
   - 减少 Shuffle 阶段的网络传输数据量
   - 在程序中添加计数器，统计 Combiner 的输入输出记录数
   - 要求：Combiner 逻辑与 Reducer 完全相同

2. **自定义 Partitioner**：

   - 实现 `AlphabetPartitioner` 类，按单词首字母进行分区
   - **分区规则**：
     - A-F 字母开头的单词分配到分区 0
     - G-N 字母开头的单词分配到分区 1
     - O-S 字母开头的单词分配到分区 2
     - T-Z 字母开头的单词分配到分区 3
   - **字符处理规则**：
     - 单词首字母统一转换为大写后进行分区判断
     - 数字开头的单词（0-9）分配到分区 0
     - 特殊字符开头的单词（非字母非数字）分配到分区 0
     - 空字符串或 null 值分配到分区 0
   - **负载均衡验证**：
     - 统计各分区的数据分布情况
     - 验证分区策略的有效性

3. **性能对比分析**（可选）：

   - 对比启用和禁用 Combiner 的性能差异
   - 收集关键性能指标（运行时间、数据传输量等）
   - 分析 Combiner 对程序性能的影响
   - 提供性能优化建议

4. **输出格式要求**:
   - **结果目录**：`/users/<学号>/homework1/problem2`
   - **主要结果文件**：`words.txt`
     - 格式：`单词\t词频`（与题目一保持一致）
     - 排序：按单词字典序排列
     - 内容：每个单词及其在所有输入文件中的总出现次数
   - **统计信息文件**：`statistics.txt`
     - 格式：`统计项名称\t数值`（与题目一保持一致）
     - 必需统计项：
       - `combiner_input_records\t[数值]`：Combiner 输入记录数
       - `combiner_output_records\t[数值]`：Combiner 输出记录数
       - `partition_0_records\t[数值]`：分区 0 的记录数
       - `partition_1_records\t[数值]`：分区 1 的记录数
       - `partition_2_records\t[数值]`：分区 2 的记录数
       - `partition_3_records\t[数值]`：分区 3 的记录数
       - `total_words\t[数值]`：总单词数
       - `unique_words\t[数值]`：唯一单词数

#### 2.3.3 测试数据

**测试重点**：

- 使用超大规模测试文件观察 Combiner 和 Partitioner 的性能优化效果
- 比较有无 Combiner 时的网络传输数据量差异
- 测试不同 Partitioner 策略对负载均衡的影响
- 输出结果保存到个人目录 `/users/<学号>/homework1/problem2`

#### 2.3.4 代码框架

```java
// WordCountCombiner.java
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        // TODO: 实现 Combiner 逻辑
        // 1. 初始化计数器为 0
        // 2. 遍历 values 中的所有计数值并累加
        // 3. 将累加结果写入 result 变量
        // 4. 输出 (单词, 局部计数) 键值对
        // 5. 使用 context.getCounter() 统计输入和输出记录数
    }
}

// AlphabetPartitioner.java
public class AlphabetPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        // TODO: 实现按字母分区的逻辑
        // 1. 获取单词的首字母并转换为大写
        // 2. 根据分区规则：A-F → 分区0，G-N → 分区1，O-S → 分区2，T-Z → 分区3
        // 3. 处理边界情况：空值、数字和特殊字符
        // 4. 返回对应的分区编号（0、1、2 或 3）

        return 0; // 替换为实际分区逻辑
    }
}

// WordCountDriver.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        // TODO: 创建 Configuration 和 Job 对象

        // TODO: 设置基本 Job 参数
        // 1. 设置 Mapper、Combiner 和 Reducer 类
        // 2. 设置 Partitioner 类
        // 3. 设置输出键值对类型
        // 4. 设置 JAR 文件
        // 5. 设置 Reduce 任务数量为 4（对应 4 个分区）

        // TODO: 实现 HDFS 操作
        // 1. 检查输入目录是否存在
        // 2. 删除已存在的输出目录

        // TODO: 设置输入输出路径
        // 1. 使用 FileInputFormat.addInputPath()
        // 2. 使用 FileOutputFormat.setOutputPath()

        // TODO: 提交作业并等待完成
        // 使用 job.waitForCompletion(true)

        // TODO: 显示处理结果和统计信息
        // 1. 从 Job 中获取 Counters 信息
        // 2. 打印 Combiner 效果统计（输入/输出记录数）
        // 3. 打印各分区的记录分布情况
        // 4. 保存 words.txt 和 statistics.txt 文件到个人目录
    }
}
```

#### 2.3.5 评分标准（40 分）

| **评分项目**         | **分值** | **评分标准**                                                  |
| -------------------- | -------- | ------------------------------------------------------------- |
| **Combiner 实现**    | 15 分    | 正确实现 Combiner 类、逻辑与 Reducer 一致、Driver 配置        |
| **Partitioner 实现** | 15 分    | 正确实现字母分区逻辑、处理边界情况、负载均衡验证              |
| **输出格式正确性**   | 6 分     | words.txt 内容正确且按字典序排序、statistics.txt 统计信息准确 |
| **性能对比分析**     | 2 分     | 提供启用/禁用 Combiner 的对比数据和分析（可选加分项）         |
| **代码质量**         | 2 分     | 代码结构清晰、注释完整、异常处理合理                          |

**总分说明**：

- 基础分数：38 分（必做部分）
- 性能分析：2 分（可选加分项）
- 总分上限：40 分

### 2.4 题目三：MapReduce 任务调优与性能分析（30 分）

#### 2.4.1 任务描述

基于大规模英文文本数据，实现一个 MapReduce Word Count 程序，并通过调整 **Map 任务数量**、**Reduce 任务数量**和使用 **Combiner** 等技术进行性能优化。此题目考查学生对 MapReduce 任务调优和性能分析的掌握程度，使用 311MB 的大规模文本数据集来展示不同配置参数对性能的影响。

#### 2.4.2 具体要求

1. **HDFS 操作部分**：

   - **输入检查**：程序启动时检查公共输入目录 `/public/data/wordcount` 是否存在
   - **结果展示**：程序完成后显示输出目录的文件列表和性能统计信息

2. **MapReduce 程序部分**：

   - **基础功能实现**：

     - 读取大规模英文文本数据，统计单词出现频次
     - 输出格式：单词和对应的出现次数
     - 处理文本预处理（转小写、去标点符号等）

   - **任务调优实现**：

     - **Map 任务控制**：通过设置输入分片大小控制 Map 任务数量
     - **Reduce 任务控制**：通过 `job.setNumReduceTasks()` 设置 Reduce 任务数量
     - **Combiner 优化**：实现 Combiner 减少 Shuffle 阶段的数据传输量

   - **性能监控**：
     - 记录作业开始和结束时间
     - 统计 Map 和 Reduce 任务数量
     - 计算数据处理速度和吞吐量
     - 监控 Combiner 的效果（输入输出记录数对比）

3. **输出格式要求**：

   - **结果目录**：`/users/<学号>/homework1/problem3`
   - **主要结果文件**：`word-count-results.txt`
     - 格式：`单词\t出现次数`（制表符分隔）
     - 排序：按出现次数降序排列
     - 示例：`the\t123456`、`and\t98765`
   - **性能统计文件**：`performance-report.txt`
     - 格式：`统计项名称\t数值`（制表符分隔）
     - 必需统计项：
       - `total_processing_time\t[数值]`：总处理时间（毫秒）
       - `map_tasks_count\t[数值]`：Map 任务数量
       - `reduce_tasks_count\t[数值]`：Reduce 任务数量
       - `input_records\t[数值]`：输入记录数（行数）
       - `output_records\t[数值]`：输出记录数（唯一单词数）
       - `total_words\t[数值]`：总单词数
       - `combiner_enabled\t[true/false]`：是否启用 Combiner
       - `combiner_input_records\t[数值]`：Combiner 输入记录数
       - `combiner_output_records\t[数值]`：Combiner 输出记录数

#### 2.4.3 测试数据

**测试重点**：

- 使用超大规模数据集进行 MapReduce 性能监控和调优
- 测试不同 Map 和 Reduce 任务数量配置对性能的影响
- 监控和分析 MapReduce 作业的执行时间、资源使用情况
- 比较不同配置参数下的性能差异
- 输出结果保存到个人目录 `/<学号>/homework1/problem3`

#### 2.4.4 代码框架

```java
// WordCountOptimizedMapper.java
public class WordCountOptimizedMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // TODO: 实现优化的 map 方法
        // 1. 将输入行转换为小写并分割为单词
        // 2. 过滤非字母字符，只保留有效单词
        // 3. 输出键值对：(单词, 1)
        // 4. 添加计数器统计处理的单词数和行数
        // 5. 异常处理：跳过无效行并记录日志
    }
}

// WordCountOptimizedCombiner.java
public class WordCountOptimizedCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        // TODO: 实现 Combiner 方法
        // 1. 初始化计数器为 0
        // 2. 遍历相同单词的计数值，累加求和
        // 3. 输出键值对：(单词, 局部计数)
        // 4. 添加计数器统计 Combiner 的输入输出记录数
    }
}

// WordCountOptimizedReducer.java
public class WordCountOptimizedReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        // TODO: 实现 Reducer 方法
        // 1. 初始化计数器为 0
        // 2. 遍历来自 Combiner 的局部计数，累加求和
        // 3. 输出最终结果：(单词, 总计数)
        // 4. 添加计数器统计最终输出的单词数
    }
}

// WordCountOptimizedDriver.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountOptimizedDriver {
    public static void main(String[] args) throws Exception {
        // TODO: 创建 Configuration 和 Job 对象

        // TODO: 设置基本 Job 参数
        // 1. 设置 Mapper、Combiner 和 Reducer 类
        // 2. 设置输出键值对类型
        // 3. 设置 JAR 文件

        // TODO: 任务调优配置
        // 1. 设置 Map 任务数量：通过输入分片大小控制
        // 2. 设置 Reduce 任务数量：job.setNumReduceTasks(4)
        // 3. 启用 Combiner：job.setCombinerClass()

        // TODO: 实现 HDFS 操作
        // 1. 检查输入目录是否存在
        // 2. 删除已存在的输出目录

        // TODO: 设置输入输出路径
        // 1. 使用 FileInputFormat.addInputPath()
        // 2. 使用 FileOutputFormat.setOutputPath()

        // TODO: 性能监控和作业执行
        // 1. 记录开始时间
        // 2. 提交作业并等待完成：job.waitForCompletion(true)
        // 3. 记录结束时间

        // TODO: 输出性能统计信息
        // 1. 从 Job 中获取 Counters 信息
        // 2. 计算处理时间和速度
        // 3. 保存性能报告到 performance-report.txt
        // 4. 保存 words.txt 和 statistics.txt 文件到个人目录
    }
}
```

#### 2.4.5 评分标准（30 分）

| **评分项目**     | **分值** | **评分标准**                                         |
| ---------------- | -------- | ---------------------------------------------------- |
| **基础功能实现** | 12 分    | 正确实现 Word Count 统计，输出格式正确，数据处理准确 |
| **任务调优配置** | 10 分    | 正确配置 Map/Reduce 任务数量，实现 Combiner 优化     |
| **性能监控分析** | 6 分     | 实现性能统计，输出详细的性能报告和 Combiner 分析     |
| **数据处理逻辑** | 2 分     | 处理异常数据，单词清理，边界情况处理                 |

---

## 3. 开发环境配置与编译运行指南

### 3.1 开发环境配置

由于本次实践采用本地开发、远程集群运行的模式，需要在本地配置 Hadoop 开发环境。

#### 3.1.1 本地 Hadoop 环境安装

**完整 Hadoop 环境安装**：

如需完整的 Hadoop 单节点集群环境，请参考：[单节点集群安装指南](../../../env-setup/signle-node/single-node-cluster.md)

**容器开发环境**（推荐）：

```shell
docker pull registry.cn-hangzhou.aliyuncs.com/mydocker_he/hadoop-client

HOST_PATH=<YOUR_HOST_PATH>

mkdir -p ${HOST_PATH}/java_code

# 启动容器
docker run -d --name hadoop-client \
  -p 9000:9000 -p 9870:9870 -p 8032:8032 -p 8088:8088 -p 19888:19888 \
  -v ${HOST_PATH}/java_code:/home/hadoop/java_code \
  --add-host hadoop-master:<hadoop-master-ip> \
  --add-host hadoop-worker1:<hadoop-worker1-ip> \
  --add-host hadoop-worker2:<hadoop-worker2-ip> \
  --add-host hadoop-worker3:<hadoop-worker3-ip> \
  --add-host hadoop-worker4:<hadoop-worker4-ip> \
  --cap-add=NET_RAW --cap-add=NET_ADMIN \
  hadoop-client tail -f /dev/null


# 进入容器（Docker）
docker exec -it hadoop-client bash
```

- 占位符说明: 请将 <YOUR_HOST_PATH> 替换为您本地的实际路径，例如：/home/hadoop/bigdata/hw2/java_code
- 网络权限说明: --cap-add=NET_RAW --cap-add=NET_ADMIN 参数用于启用网络工具（如 ping、traceroute 等）的权限，确保容器内的网络诊断功能正常工作。
- 集群实际 ip 地址和详细说明见微信群。

**轻量级开发环境**（推荐）：

1. **安装 Java 8**：

   ```bash
   # macOS 使用 Homebrew 安装
   brew install openjdk@8

   # Ubuntu/Debian 安装
   sudo apt-get update
   sudo apt-get install openjdk-8-jdk

   # 验证安装
   java -version
   javac -version
   ```

2. **下载 Hadoop 客户端**：

   ```bash
   # 下载 Hadoop 3.4.2
   wget https://archive.apache.org/dist/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz
   tar -xzf hadoop-3.4.2.tar.gz
   sudo mv hadoop-3.4.2 /opt/hadoop
   ```

3. **配置环境变量**：

   在 `~/.bashrc` 或 `~/.zshrc` 中添加：

   ```bash
   export JAVA_HOME=/usr/local/opt/openjdk@8  # macOS
   # export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64  # Linux

   export HADOOP_HOME=/opt/hadoop
   export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
   export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
   ```

#### 3.1.2 远程集群连接配置

1. **获取集群配置文件**：从集群管理员处获取配置文件并放置到 `$HADOOP_CONF_DIR`
   1. 手动把 `config`目录下的文件复制到 `$HADOOP_CONF_DIR` 目录下， 或执行 `cp config/* $HADOOP_CONF_DIR/`。
2. **验证连接**：`hdfs dfs -ls /` 和 `yarn node -list`

#### 3.1.3 开发工具配置

**Maven 项目依赖**：

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-client</artifactId>
        <version>3.4.2</version>
    </dependency>
    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-mapreduce-client-core</artifactId>
        <version>3.4.2</version>
    </dependency>
</dependencies>
```

### 3.2 编译步骤

```bash
# 1. 创建项目目录
mkdir hadoop-assignment
cd hadoop-assignment

# 2. 创建源码目录结构
mkdir -p src/main/java/com/bigdata/assignment
mkdir -p target/classes

# 3. 编译 Java 源码
javac -cp $(hadoop classpath) -d target/classes src/main/java/com/bigdata/assignment/*.java

# 4. 打包 JAR 文件
jar cf hadoop-assignment.jar -C target/classes .
```

### 3.3 运行步骤

```bash
# 题目一：WordCount
# 1. 验证输入数据
hdfs dfs -ls /public/data/wordcount
# 数据已提前放置，无需上传

# 2. 运行程序
hadoop jar hadoop-assignment.jar com.bigdata.assignment.WordCountDriver \
    /public/data/wordcount /users/<学号>/homework1/problem1

# 3. 查看结果
hdfs dfs -cat /users/<学号>/homework1/problem1/part-r-00000

# 题目二：带 Combiner 和 Partitioner 的 WordCount
hadoop jar hadoop-assignment.jar com.bigdata.assignment.WordCountWithCombinerDriver \
    /public/data/wordcount /users/<学号>/homework1/problem2

# 题目三：MapReduce 任务调优与性能分析
# 1. 验证数据（使用公共数据目录）
hdfs dfs -ls /public/data/wordcount/

# 2. 运行程序
hadoop jar hadoop-assignment.jar com.bigdata.assignment.WordCountOptimizedDriver \
    /public/data/wordcount /users/<学号>/homework1/problem3
```

### 3.4 调试技巧

1. **查看作业日志**：

   ```bash
   yarn logs -applicationId application_xxx_xxx
   ```

2. **监控作业进度**：

   - 访问 ResourceManager Web UI：<http://namenode:8088/>
   - 查看作业详细信息和性能指标

3. **常见错误处理**：
   - 输出目录已存在：删除后重新运行
   - 类路径问题：检查 Hadoop 环境变量
   - 内存不足：调整 mapreduce.map.memory.mb 参数

---

## 4. 提交要求

### 4.1 提交内容

#### 4.1.1 项目结构与文件说明

**项目目录结构**：

```bash
学号-姓名-hadoop-assignment/
├── src/                                    # 源代码目录
│   └── main/
│       ├── java/                          # Java 源文件
│       │   └── com/bigdata/assignment/
│       │       ├── problem1/              # 题目一：HDFS 操作与 WordCount 实现
│       │       │   ├── *Mapper.java       # Mapper 类
│       │       │   ├── *Reducer.java      # Reducer 类
│       │       │   └── *Driver.java       # Driver 主程序
│       │       ├── problem2/              # 题目二：自定义 Combiner 和 Partitioner
│       │       │   ├── *Mapper.java       # Mapper 类
│       │       │   ├── *Combiner.java     # Combiner 类
│       │       │   ├── *Partitioner.java  # Partitioner 类
│       │       │   └── *Driver.java       # Driver 主程序
│       │       └── problem3/              # 题目三：MapReduce 任务调优与性能分析
│       │           ├── *Mapper.java       # Mapper 类
│       │           ├── *Reducer.java      # Reducer 类
│       │           ├── *Combiner.java     # Combiner 类（可选）
│       │           └── *Driver.java       # Driver 主程序
│       └── resources/                     # 配置文件目录
│           └── log4j.properties           # 日志配置（可选）
├── output/                                # 程序输出结果
│       ├── problem1/                     # 题目一输出
│       ├── problem2/                     # 题目二输出
│       └── problem3/                     # 题目三输出
├── scripts/                              # 运行脚本（可选）
│   ├── run-problem1.sh                   # 题目一运行脚本
│   ├── run-problem2.sh                   # 题目二运行脚本
│   └── run-problem3.sh                   # 题目三运行脚本
├── report/                               # 实验报告目录
│   ├── 实验报告.pdf                       # 实验报告文档
│   └── screenshots/                      # 截图目录
│       ├── problem1/                     # 题目一相关截图
│       ├── problem2/                     # 题目二相关截图
│       └── problem3/                     # 题目三相关截图
├── pom.xml                               # Maven 构建文件
└── README.md                             # 项目说明文档
```

**目录说明**：

1. **src/main/java/**：存放所有 Java 源代码文件

   - 按题目分目录组织，每个题目包含相应的 Mapper、Reducer、Driver 等类
   - 建议使用有意义的类名，如 `WordCountMapper`、`AlphabetPartitioner` 等

2. **output/**：存放程序运行输出结果

3. **scripts/**：存放运行脚本（可选）

   - 包含编译、运行各题目的便捷脚本

4. **report/**：存放实验报告和相关截图

   - 实验报告采用 PDF 格式
   - 截图按题目分类存放

5. **pom.xml**：Maven 项目构建文件

   - 包含 Hadoop 相关依赖配置

6. **README.md**：项目说明文档
   - 包含项目概述、编译运行说明、环境要求等

#### 4.1.2 实验报告内容要求

**报告内容要求**：

1. **算法设计思路**（对应各题目核心功能评分）：

   - **题目一**：

     - 分析 WordCount 问题的数据流处理逻辑
     - 说明 Mapper 和 Reducer 的职责分工
     - 解释键值对设计的考虑因素

   - **题目二**：

     - 分析 Combiner 的作用机制和适用场景
     - 说明 Partitioner 的分区策略和负载均衡原理
     - 对比不同实现方案的优缺点

   - **题目三**：
     - 分析 MapReduce 任务调优的策略和方法
     - 说明性能监控指标的含义和分析方法
     - 解释 Combiner 优化对性能的影响机制

2. **运行结果展示**（对应输出格式正确性评分）：

   - 每个题目的程序运行截图（包含完整的命令行输出）
   - 关键运行日志（如 MapReduce 任务进度、错误信息等）

3. **性能分析**（题目二和题目三必做，对应性能对比分析评分）：

   - **题目二**：

     - 使用 Combiner 前后的 Map 输出记录数对比
     - 不同 Partitioner 策略下各 Reducer 的数据分布情况
     - 运行时间、内存使用等关键性能指标分析

   - **题目三**：
     - 不同 Map/Reduce 任务数量配置下的性能对比
     - Combiner 优化前后的性能提升分析
     - 详细的性能监控报告和调优建议

4. **问题与解决**：

   - 实现过程中遇到的主要技术难点
   - 调试过程和解决方案
   - 对关键错误的分析和处理方法

5. **实验收获**：
   - 对 MapReduce 编程模型的深入理解
   - 分布式计算中数据分区、Combiner 优化和性能调优的认识

### 4.3 提交方式与要求

#### 4.3.1 提交方式

1. **文件格式**：压缩包形式（.zip 或 .tar.gz）
2. **文件命名规范**：`学号-姓名-hadoop-assignment.zip`
3. **文件大小限制**：不超过 50MB

#### 4.3.2 提交格式及截止时间

1. 邮件标题: "**2025 大数据理论与实践-学号-姓名-练习二**"；
2. 邮箱地址：`grissom.wang@transwarp.io`；
3. 邮件附件是 `学号-姓名-hadoop-assignment.zip`的压缩包
4. 作业提交截止时间 : 2025 年 11 月 30 日 23:59

#### 4.3.3 质量要求

1. **代码质量**：

   - 代码结构清晰，注释完整
   - 遵循 Java 编程规范
   - 无编译错误和明显的运行时错误

2. **功能完整性**：

   - 所有题目均需完成
   - 程序能够正确运行并产生预期结果
   - 包含必要的错误处理机制

3. **文档完整性**：
   - 实验报告内容完整，格式规范
   - 截图清晰，说明详细
   - README.md 包含运行说明

---

## 5. 参考资料

- [Hadoop 官方文档](https://hadoop.apache.org/docs/stable/)
- [MapReduce 教程](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
- [HDFS 用户指南](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)

---
