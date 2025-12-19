# Iceberg 核心机制深度剖析：从元数据到性能优化

本文档旨在对 Iceberg 的内部机制进行工程层面的深度拆解。我们将从元数据结构出发，详细推演读写流程、并发控制、性能优化及生命周期管理等核心技术点，帮助读者掌握 Lakehouse 的“内功心法”。

## 1. 元数据层的“俄罗斯套娃”结构

Iceberg 的核心在于其**分层元数据管理**。相较于 Hive 的“扁平目录结构”，Iceberg 构建了一棵自底向上的元数据树。

### 1.1 五层结构详解

```mermaid
graph TD
    A[Catalog (指针)] -->|指向| B[Metadata File (表定义)]
    B -->|包含| C[Manifest List (快照清单)]
    C -->|引用| D[Manifest File (文件清单)]
    D -->|追踪| E[Data File (实际数据)]
```

| 层级 | 名称 | 关键职责 | 存储内容示例 |
| :--- | :--- | :--- | :--- |
| **L0** | **Data File** | 存储真实数据 | Parquet/ORC 文件，包含实际的行数据。 |
| **L1** | **Manifest File** | **文件级索引** | 记录一组 Data Files 的路径、分区值、**列级统计信息** (Min/Max/Null Count)。 |
| **L2** | **Manifest List** | **快照级索引** | 记录构成当前快照的所有 Manifest Files，以及每个 Manifest 的**分区范围统计**。 |
| **L3** | **Metadata File** | **表级定义** | 表的 Schema、分区策略、排序策略、**Snapshots 历史列表**、当前快照 ID。 |
| **L4** | **Catalog** | **入口指针** | 仅存储一个指针：`current_metadata_location = s3://.../v2.metadata.json`。 |

### 1.2 为什么设计得这么复杂？

这种分层设计是为了解决**大规模数据集下的性能问题**：
*   **Catalog 轻量化**：Catalog 只存一个指针，无论表有多大，Catalog 的压力都很小。
*   **剪枝下推**：
    *   查询 `dt='2023'` -> 检查 Manifest List -> 跳过不包含该日期的 Manifest Files。
    *   查询 `id > 100` -> 检查 Manifest File -> 跳过 `max_id < 100` 的 Data Files。
    *   **结论**：查询引擎只需读取极少量的元数据即可定位目标文件，无需扫描整个目录。

---

## 2. 读写操作的核心：Copy-On-Write (COW) 推演

在不可变的对象存储（S3/HDFS）上实现 `UPDATE`，必须依赖 Copy-On-Write 机制。

### 2.1 场景：Update 一行数据

假设执行 SQL：`UPDATE users SET status = 'active' WHERE id = 1`。

**步骤 1：定位（Read）**
*   引擎扫描当前快照，找到 `id=1` 所在的 Data File（假设为 `file_A.parquet`）。

**步骤 2：重写（Write）**
*   引擎读取 `file_A.parquet` 的所有数据。
*   在内存中修改 `id=1` 的行的 `status`。
*   将修改后的数据写入新的文件 `file_A_new.parquet`。
*   *注：此时 `file_A.parquet` 依然存在，且对其他读任务可见。*

**步骤 3：提交（Commit）**
*   生成新的 **Manifest File**：标记 `file_A.parquet` 为 **Deleted**，标记 `file_A_new.parquet` 为 **Added**。
*   生成新的 **Manifest List**。
*   生成新的 **Metadata File** (v2)，将 `current-snapshot-id` 指向新快照。
*   **原子交换 (CAS)**：通知 Catalog 将指针从 v1 切换到 v2。

### 2.2 ACID 语义保障

*   **Atomicity（原子性）**：Catalog 的指针切换是原子的。要么切换成功（新数据全可见），要么失败（新数据全不可见）。
*   **Consistency（一致性）**：新快照经过完整性校验（Schema 匹配、分区合法）后才会被提交。
*   **Isolation（隔离性）**：
    *   读任务 A 正在读 v1 快照，它持有的元数据指向 `file_A.parquet`。
    *   写任务 B 提交了 v2 快照，生成了 `file_A_new.parquet`。
    *   **互不干扰**：读任务 A 继续读它的老文件，完全感知不到 v2 的存在。

---

## 3. 性能优化的两大杀器

### 3.1 隐藏分区 (Hidden Partitioning)

**传统 Hive 的痛点**：
用户必须手动创建一个 `date_str` 列（如 "2023-12-01"）作为分区列。查询时必须写 `WHERE date_str = '...'`，如果用户写了 `WHERE timestamp = ...`，则无法利用分区剪枝，导致全表扫描。

**Iceberg 的解法**：
*   **定义**：`Partition BY days(timestamp_col)`。
*   **转换**：Iceberg 内部维护了 `timestamp -> day` 的转换函数。
*   **查询**：用户写 `WHERE timestamp_col > '2023-12-01 10:00:00'`。
*   **自动推导**：Iceberg 自动计算出需要扫描的分区是 `2023-12-01` 及之后的分区，无需用户显式指定分区列。

### 3.2 Data Skipping (数据跳过)

Iceberg 在 **Manifest File** 级别存储了每个 Data File 的列统计信息（Min/Max/Null Count）。

**案例**：
表中有 100 个文件，查询 `SELECT * FROM table WHERE id > 1000`。
*   **传统 Hive**：打开所有 100 个 Parquet 文件 footer 读取统计信息（开销巨大）。
*   **Iceberg**：
    1.  读取 Manifest File（通常只有几 MB）。
    2.  遍历 Manifest 中的 entries。
    3.  发现 `file_1` 的 `max_id = 500` -> **跳过**。
    4.  发现 `file_2` 的 `max_id = 1200` -> **保留**。
    5.  最终只打开 `file_2` 进行扫描。

---

## 4. 生命周期管理：防止存储爆炸

随着 Update/Delete 的进行，系统中会产生大量的“废弃文件”（如上文的 `file_A.parquet`）和历史快照。

### 4.1 快照过期 (Expire Snapshots)

*   **策略**：只保留最近 7 天的快照，或最近 100 个快照。
*   **操作**：`CALL catalog.system.expire_snapshots(table, retain_last => 100)`。
*   **清理**：Iceberg 会找出那些**仅被过期快照引用**的数据文件，并物理删除它们。

### 4.2 孤儿文件清理 (Remove Orphan Files)

*   **场景**：Spark 写任务写到一半挂了，留下了很多未提交的 Parquet 文件。这些文件不在任何 Metadata 中记录，被称为“孤儿文件”。
*   **清理**：`CALL catalog.system.remove_orphan_files(table)`。
*   **机制**：列举数据目录下的所有文件，对比 Metadata 中的文件列表，删除不在列表中的文件。

---

## 5. 总结：Iceberg 如何实现“像数据库一样管理文件”

1.  **显式元数据**：用文件清单代替目录扫描，解决 S3 Listing 性能问题。
2.  **快照机制**：通过不可变快照链实现 MVCC 和 ACID，支持读写分离和时间旅行。
3.  **智能索引**：利用隐藏分区和 Min/Max 统计信息，最小化 I/O 开销。
4.  **闭环治理**：提供过期清理和孤儿文件回收机制，保证系统的长期健康运行。
