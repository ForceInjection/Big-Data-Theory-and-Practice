# 深入解析 Table Format：原理、架构与技术细节

Table Format（表格式）是 Lakehouse 架构的基石。本专栏旨在深入技术细节，揭示 Table Format 如何在对象存储之上构建出具备 ACID 事务、快照隔离和高性能查询能力的“表”。我们以架构最清晰的 **Apache Iceberg** 为例进行深度剖析。

## 1. 痛点：Hive 表在对象存储上的“水土不服”

在深入 Table Format 之前，我们需要理解传统 Hive 表在 S3 / OSS 等对象存储上遇到的核心技术瓶颈。

### 1.1 目录 Listing 的性能黑洞

Hive 表的数据管理基于**文件系统目录结构**。
例如，查询 `SELECT * FROM logs WHERE dt='2023-12-01'`：

1. Hive Metastore 找到分区目录 `s3://bucket/logs/dt=2023-12-01/`。
2. 引擎（Spark/Presto）必须调用 `ListObjects` 接口列举该目录下所有文件。

**技术细节**：

- S3 的 `ListObjects` 是分页的（通常每页 1000 个 key），且延迟较高。
- 如果一个分区下有 10 万个小文件，光是 Listing 就需要数秒甚至数十秒。
- **O(N) 复杂度**：查询延迟与文件数量成正比，无法随集群规模扩展。

### 1.2 原子性的缺失

对象存储通常只保证**单个文件**写入的原子性，但不保证**目录级**操作的原子性（虽然现代 S3 实现了强一致性，但 Rename 操作依然昂贵或不支持）。

- **场景**：Spark 任务正在向目录写数据，任务失败了。
- **后果**：目录下可能残留了部分“脏文件”。下游任务如果此时进行 Listing，会读到不完整的数据，导致数据不一致。

---

## 2. 核心解法：从“目录扫描”到“显式追踪”

Table Format 的核心思想非常朴素：**永远不要去 List 目录，而是用一份“清单”把所有有效文件记下来。**

> **Table Format 本质** = **Metadata Files（元数据文件）** + **Protocol（操作协议）**

当查询发生时：

1. 读取**元数据文件**，直接获得所有有效数据文件的路径列表。
2. 根据元数据中的统计信息（Min/Max），过滤掉不需要读取的文件（Data Skipping）。
3. 引擎直接按路径读取数据文件，**完全跳过目录 Listing 步骤**。

---

## 3. 技术深潜：Iceberg 的三层元数据架构

Apache Iceberg 的设计之美在于其清晰的**三层元数据树**。理解了这个结构，就理解了 Lakehouse 的一半。

### 3.1 架构图解（逻辑视图）

```mermaid
graph TD
    A[Catalog 指针] --> B[metadata.json (表级元数据)]
    B --> C[Manifest List (快照清单列表)]
    C --> D[Manifest File (数据清单文件)]
    D --> E[Parquet/ORC Data Files (实际数据)]
```

### 3.2 逐层拆解

#### 第一层：Metadata File (`v1.metadata.json`)

这是表的“大脑”，存储了表的全局信息。

- **内容**：
  - Table Schema（列定义、类型、ID）
  - Partition Spec（分区策略）
  - **Snapshots 列表**：记录了表的所有历史快照（v1, v2, v3...），以及当前指针 `current-snapshot-id`。
- **作用**：告诉引擎“哪一个快照是当前最新的”。

#### 第二层：Manifest List (`snap-123.avro`)

这是某一个快照（Snapshot）的“目录”。

- **内容**：包含了一组 **Manifest File** 的路径。
- **关键技术点**：它存储了每个 Manifest File 的**分区范围统计信息**（Partition Stats）。
- **作用**：查询优化。例如查询 `dt='2023'`，引擎检查 Manifest List，发现某个 Manifest File 只包含 `dt='2022'` 的数据，则直接丢弃该 Manifest File，不再读取。

#### 第三层：Manifest File (`123-m0.avro`)

这是最底层的“文件清单”。

- **内容**：记录了具体的**数据文件路径**（`s3://.../data.parquet`）。
- **关键技术点**：存储了每个数据文件的**列级统计信息**（Column bounds: min/max values, null counts）。
- **作用**：文件级过滤。例如查询 `id > 100`，如果某个 Parquet 文件的 `min_id = 200`，则该文件必须读取；如果 `max_id = 50`，则直接跳过。

---

## 4. 动态推演：一次 INSERT 操作的内部流程

假设我们执行 `INSERT INTO t1 VALUES (1, 'a')`，Table Format 是如何保证 ACID 的？

### Step 1: 写入数据 (Data Phase)

Spark Executor 将数据写入新的 Parquet 文件：`s3://bucket/data/file_x.parquet`。
_注意：此时该文件对用户**不可见**，因为元数据还没指向它。_

### Step 2: 写入元数据 (Metadata Phase)

1. **生成 Manifest File**：创建一个新的 `manifest_x.avro`，里面记录了 `file_x.parquet` 的路径和统计信息。
2. **生成 Manifest List**：创建一个新的 `snap-new.avro`，包含老的 Manifest Files + 新的 `manifest_x.avro`。
3. **生成 Metadata File**：读取旧的 `v1.metadata.json`，创建一个新的 `v2.metadata.json`，将 `current-snapshot-id` 指向 `snap-new`。

### Step 3: 原子提交 (Commit Phase)

这是最关键的一步。

- 客户端向 Catalog（如 Hive Metastore 或 Nessie）发送请求：“请把表 `t1` 的元数据指针从 `v1.metadata.json` 更新为 `v2.metadata.json`”。
- **CAS (Compare-And-Swap)**：如果此时有另一个人已经更新到了 v2，则本次提交失败（乐观锁重试）。
- 一旦指针更新成功，新的数据瞬间对所有用户可见。

---

## 5. 进阶机制：如何实现行级更新与删除

Lakehouse 的另一大卖点是支持 `UPDATE` 和 `DELETE` 操作。在不可变的对象存储上实现这一点，主要有两种策略：**Copy-On-Write (COW)** 和 **Merge-On-Read (MOR)**。这是 PPT 中展示技术深度的关键点。

### 5.1 Copy-On-Write (COW) - 写时复制

- **原理**：当要更新文件 A 中的一行数据时，必须重写整个文件 A，生成新的文件 A'（包含更新后的行），并标记文件 A 为删除。
- **优点**：读性能极高（因为读的时候不需要合并），适合读多写少的场景。
- **缺点**：写放大严重。修改 1 行数据可能导致重写 1GB 的文件。

### 5.2 Merge-On-Read (MOR) - 读时合并

- **原理**：
  - **Data File**：存储基础数据。
  - **Delete File**：存储“哪些行被删除了”或“哪些行被更新了”。
  - 写入时：只追加写入 Delete File 或 Delta File，速度极快。
  - 读取时：引擎需要实时合并（Merge）Data File 和 Delete File，过滤掉被删除的数据。
- **关键技术细节（Iceberg V2 Format）**：
  - **Positional Delete Files**：记录被删除行的 `(file_path, row_position)`。
  - **Equality Delete Files**：记录被删除行的主键值 `(id=100)`。
- **优点**：写入极快，适合流式高频更新。
- **缺点**：读取时有合并开销，需要定期做 Compaction（压实）转为 COW 状态。

---

## 6. 并发控制：如何防止多人同时修改冲突？

PPT 中可以展示多用户并发写入的场景，解释 Table Format 如何解决冲突。

### 6.1 乐观并发控制 (Optimistic Concurrency Control)

Iceberg 和 Delta Lake 均采用乐观锁机制：

1. **假设**：用户 A 和用户 B 同时基于快照 v1 进行修改。
2. **提交**：
   - 用户 A 先完成，将当前快照更新为 v2（基于 v1）。
   - 用户 B 后完成，尝试将当前快照更新为 v3（基于 v1）。
3. **冲突检测**：
   - Catalog 发现当前最新快照是 v2，而用户 B 是基于 v1 做的修改。
   - **检查**：用户 B 修改的数据文件，是否与用户 A 修改的文件有重叠？
   - **无重叠**：用户 B 的提交重以此基于 v2，生成 v3（Rebase），提交成功。
   - **有重叠**：抛出异常，用户 B 任务失败重试。

---

## 7. 杀手级特性原理

### 7.1 Time Travel（时间旅行）

- **原理**：因为 `metadata.json` 里保留了 `Snapshots: [v1, v2, v3]` 列表。
- **操作**：用户执行 `SELECT * FROM t1 FOR SYSTEM_VERSION AS OF v1`。
- **实现**：引擎直接读取 v1 对应的 Manifest List，忽略 v2 和 v3 的文件。这就好比按下了“撤销”键，回到了过去。

### 7.2 Schema Evolution（表结构演进）

- **痛点**：在 Hive 中，如果把列 `name` 重命名为 `user_name`，旧的 Parquet 文件里的 `name` 列就读不出来了（因为按名匹配）。
- **Iceberg 解法**：**基于 ID 绑定，而非名称绑定**。
  - 创建表时，`name` 列被分配唯一 ID = 1。
  - 重命名为 `user_name` 时，元数据中记录“ID=1 的列现在叫 user_name”。
  - 读取旧文件时，引擎找“ID=1”的数据，依然能读到内容，哪怕它在文件里叫 `name`。
  - **结论**：支持无副作用的 Add, Drop, Rename, Reorder 列操作。

### 7.3 Hidden Partitioning（隐藏分区）

- **痛点**：Hive 中通常需要手动把时间转成字符串 `dt='2023-12-01'` 才能做分区过滤。用户写 SQL 必须以此过滤，否则全表扫描。
- **Iceberg 解法**：
  - 定义分区策略为 `Partition by day(timestamp_col)`。
  - 用户查询 `WHERE timestamp_col = '2023-12-01 10:00:00'`。
  - Iceberg 自动推导出需要扫描的分区是 `2023-12-01`，用户无需关心底层是按天、按月还是按年分区。

---

- **对比表**：列出 COW 和 MOR 的优缺点（引用第 5 节内容）。
- **场景推荐**：
  - 离线 T+1 报表 -> COW（读得快）。
  - 实时 CDC 入湖 -> MOR（写得快）。
