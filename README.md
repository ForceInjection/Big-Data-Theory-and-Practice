# Big-Data-Theory-and-Practice

《大数据理论与实践》课程学习材料仓库

## 1. 简介

本仓库是《大数据理论与实践》课程的学习材料集合，包含课程讲义、经典论文、参考书籍和实践练习等内容。旨在为学习者提供系统性的大数据理论知识和实践技能培养。

## 2. 目录结构

### 2.1 books

存放大数据相关的参考书籍和资料

- `README.md` - 书籍目录说明
- `hadoop-definitive-guide.jpg` - Hadoop 权威指南封面图

### 2.2 courses

课程讲义和补充材料，按章节组织。

- [第一讲 - 大数据技术综述](./courses/chapter01/第01讲-大数据技术综述.pdf)
- [第二讲 - 分布式协调服务ZooKeeper](./courses/chapter02/第02讲-分布式协调服务Zookeeper.pdf)

### 2.3 paper

大数据领域的经典论文集合。

| 年份 | 技术/系统 | 论文标题 | 技术领域 |
|------|-----------|----------|----------|
| 2003 | **GFS** | `The Google File System` | [分布式文件系统](./paper/gfs-sosp2003.pdf) |
| 2004 | **MapReduce** | `MapReduce: Simplified Data Processing on Large Clusters` | [分布式计算框架](./paper/Dean%20和%20Ghemawat%20-%202008%20-%20MapReduce%20simplified%20data%20processing%20on%20large%20clu.pdf) |
| 2006 | **Bigtable** | `Bigtable: A Distributed Storage System for Structured Data` | [分布式数据库](./paper/Chang%20等%20-%202008%20-%20Bigtable%20A%20Distributed%20Storage%20System%20for%20Structu.pdf) |
| 2006 | **Chubby** | `The Chubby lock service for loosely-coupled distributed systems` | [分布式锁服务](./paper/Burrows%20-%202006%20-%20The%20Chubby%20lock%20service%20for%20loosely-coupled%20distributed%20systems.pdf) |
| 2007 | **Thrift** | `Thrift: Scalable cross-language services implementation` | [RPC 框架](./paper/Slee%20等%20-%20Thrift%20Scalable%20Cross-Language%20Services%20Implementation.pdf) |
| 2008 | **Hive** | `Hive: A warehousing solution over a map-reduce framework` | [数据仓库](./paper/Thusoo%20等%20-%202009%20-%20Hive%20a%20warehousing%20solution%20over%20a%20map-reduce%20framework.pdf) |
| 2010 | **Dremel** | `Dremel: Interactive analysis of web-scale datasets` | [交互式查询引擎](./paper/Melnik%20等%20-%20Dremel%20Interactive%20Analysis%20of%20Web-Scale%20Datasets.pdf) |
| 2010 | **Spark** | `Spark: Cluster computing with working sets` | [内存计算框架](./paper/Zaharia%20等%20-%20Spark%20Cluster%20Computing%20with%20Working%20Sets.pdf) |
| 2010 | **S4** | `S4: Distributed stream computing platform` | [流计算平台](./paper/Neumeyer%20等%20-%202010%20-%20S4%20Distributed%20Stream%20Computing%20Platform.pdf) |
| 2011 | **Megastore** | `Megastore: Providing scalable, highly available storage for interactive services` | [分布式存储](./paper/Baker%20等%20-%20Megastore%20Providing%20Scalable,%20Highly%20Available%20Storage%20for%20Interactive%20Services.pdf) |
| 2011 | **Kafka** | `Kafka: A distributed messaging system for log processing` | [消息队列系统](./paper/Kreps%20等%20-%20Kafka%20a%20Distributed%20Messaging%20System%20for%20Log%20Processing.pdf) |
| 2012 | **Spanner** | `Spanner: Google's globally distributed database` | [全球分布式数据库](./paper/Corbett%20等%20-%20Spanner%20Google’s%20Globally-Distributed%20Database.pdf) |
| 2014 | **Storm** | `Storm@Twitter` | [实时流处理](./paper/Toshniwal%20等%20-%202014%20-%20Storm@twitter.pdf) |
| 2014 | **Raft** | `In search of an understandable consensus algorithm` | [分布式一致性算法](./paper/Ongaro和Ousterhout%20-%20In%20Search%20of%20an%20Understandable%20Consensus%20Algorithm.pdf) |
| 2015 | **Dataflow** | `The dataflow model: A practical approach to balancing correctness, latency, and cost in massive-scale, unbounded, out-of-order data processing` | [流处理模型](./paper/Akidau%20等%20-%202015%20-%20The%20dataflow%20model%20a%20practical%20approach%20to%20balancing%20correctness,%20latency,%20and%20cost%20in%20massive-scal.pdf) |
| 2018 | **PolarFS** | `PolarFS: an ultra-low latency and failure resilient distributed file system for shared storage cloud database` | [云原生文件系统](./paper/Cao%20等%20-%202018%20-%20PolarFS%20an%20ultra-low%20latency%20and%20failure%20resilien.pdf) |
| 2020 | **Delta Lake** | `Delta lake: high-performance ACID table storage over cloud object stores` | [数据湖存储](./paper/Armbrust%20等%20-%202020%20-%20Delta%20lake%20high-performance%20ACID%20table%20storage%20ov.pdf) |
| 2021 | **Lakehouse** | `Lakehouse: A New Generation of Open Platforms for AI and Data Analytics` | [湖仓一体架构](./paper/Armbrust%20等%20-%202021%20-%20Lakehouse%20A%20New%20Generation%20of%20Open%20Platforms%20that.pdf) |
| 2023 | **HTAP 综述** | `HTAP 数据库关键技术综述` | [混合事务分析处理](./paper/张超，李国良，冯建华，张金涛%20和%20ZHANG%20Chao%20-%202022%20-%20HTAP数据库关键技术综述.pdf) |
| 2024 | **云原生数据库综述** | `云原生数据库综述` | [云原生数据库](./paper/云原生数据库综述.pdf) |
| 2024 | **Iceberg** | `Apache Iceberg: The Definitive Guide` | [表格式标准](./paper/apache-iceberg-TDG_ER1.pdf) |

### 2.4 practise

实践练习和项目

#### 2.4.1 Parquet 列式存储实践

- `列式存储：Parquet 文件格式解析.md` - Parquet 格式理论介绍
- `Parquet实践练习.md` - 实践练习指导
- `parquet-practice-project/` - Parquet 实践项目
  - 完整的 Python 项目结构
  - 包含示例代码、测试用例和文档
