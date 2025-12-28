# Big-Data-Theory-and-Practice

《大数据理论与实践》课程学习材料仓库

## 1. 简介

本仓库是《大数据理论与实践》课程的学习材料集合，包含课程讲义、经典论文、参考书籍、环境配置指南和实践练习等内容。旨在为学习者提供系统性的大数据理论知识和实践技能培养。

---

## 2. [参考书籍](./books/README.md)

- 《**数据密集型应用设计**》（Designing Data-Intensive Applications）
- 《**Hadoop 权威指南**》第 4 版
- 《**大数据处理框架 Apache Spark 设计与实现**》

---

## 3 课程

以下是课程章节内容：

- **第一讲**:

  - [原教材：第 01 讲-大数据技术综述](./courses/chapter01/第01讲-大数据技术综述.pdf)
  - [**1. 大数据发展的历史-从核心理念到生态演化**](./courses/chapter01/【补充】大数据发展的历史-从核心理念到生态演化.pdf)
  - [参考资料](./courses/chapter01/参考资料/)
    - [大数据发展历史](./courses/chapter01/参考资料/大数据历史.md)
    - [对比学习：Linux 文件系统](./courses/chapter01/参考资料/Linux_Filesystem_intro.md)

- **第二讲**：

  - [原教材：第 02 讲-分布式协调服务 ZooKeeper](./courses/chapter02/第02讲-分布式协调服务Zookeeper.pdf)
  - [**1. 分布式系统概述**](./courses/chapter02/参考资料/分布式系统概述.md) ([PPT](./courses/chapter02/1.%20分布式系统概述.pptx))
  - [**2. Chubby 分布式锁服务**](./courses/chapter02/参考资料/Chubby分布式锁服务.md) ([PPT](./courses/chapter02/2.%20Chubby%20分布式锁服务.pptx))
  - [**3. Paxos 算法详解：分布式共识的理论基础**](./courses/chapter02/参考资料/paxos/Paxos介绍.md) ([PDF](./courses/chapter02/3.%20Basic%20Paxos%20两阶段共识流程全解析.pdf))
  - [**4. ZooKeeper Leader 选举机制详解**](./courses/chapter02/参考资料/ZooKeeper%20Leader选举机制详解.md) ([PPT](./courses/chapter02/4.%20ZooKeeper%20Leader%20选举机制详解.pptx))
  - [参考资料](./courses/chapter02/参考资料/)

- **第三讲**：

  - [原教材：第 03 讲-分布式文件系统 HDFS（上）](./courses/chapter03/第03讲-分布式文件系统HDFS（上）.pdf)
  - [**1. Google File System (GFS) - 大规模分布式存储系统的设计与实现**](./courses/chapter03/参考资料/GFS.md) ([PPT](<./courses/chapter03/Google%20File%20System%20(GFS)%20-%20大规模分布式存储系统的设计与实现.pptx>))
  - [**2. HDFS 分布式文件系统原理与架构**](./courses/chapter03/参考资料/HDFS.md) ([PPT](./courses/chapter03/HDFS%20分布式文件系统原理与架构.pptx))
  - [参考资料](./courses/chapter03/参考资料/)
    - [HDFS 读写流程详解](./courses/chapter03/参考资料/HDFS读写流程详解.md)

- **第四讲**：

  - [原教材：第 03 讲-分布式文件系统 HDFS（下）](./courses/chapter04/第03讲-分布式文件系统HDFS（下）.pdf)
  - [**1. HDFS 关键概念复习文档**](./courses/chapter04/HDFS关键概念复习文档.md) ([PPT](./courses/chapter04/HDFS%20分布式文件系统复习.pptx))
  - [**2. HDFS 常见操作**](./courses/chapter04/HDFS常见操作.md)

- **练习一**：

  - [练习 1：HDFS 操作](./courses/chapter04/exercise_1.md)

- **第五讲**：

  - [原教材：第 04 讲-分布式计算框架 MapReduce](./courses/chapter05/第04讲-分布式计算框架MapReduce.pdf)
  - [**1. MapReduce 核心原理**](./courses/chapter05/map-reduce.md) ([PPT](./courses/chapter05/MapReduce%20分布式计算框架.pptx))
  - [学生使用指南](./courses/chapter05/student-guide.md)

- **第六讲**：

  - [原教材：第 05 讲-分布式资源管理系统 YARN](./courses/chapter06/第05讲-分布式资源管理系统YARN.pdf)
  - [**1. YARN 分布式资源管理与调度**](./courses/chapter06/yarn.md) ([PPT](./courses/chapter06/1.%20YARN%20分布式资源管理与调度.pptx))
  - [YARN 与 Kubernetes 底层隔离技术深度分析](./courses/chapter06/YARN-Kubernetes-底层隔离技术深度分析.md) ([PPT](./courses/chapter06/2.%20从Linux内核到应用层_YARN与Kubernetes资源隔离技术全栈解析.pptx))

- **练习二**：

  - [练习二：YARN 资源管理实践](./courses/chapter06/excerise_2/)

- **第七讲**：

  - [原教材：第 06 讲-分布式计算框架 Spark](./courses/chapter07/第06讲-分布式计算框架Spark.pdf)
  - [**1. Apache Spark 设计与实现**](./courses/chapter07/Apache%20Spark%20设计与实现.md) ([PPT](./courses/chapter07/1.%20Apache%20Spark%20设计与实现.pptx))
  - [Spark on Kubernetes 架构及组件介绍](./courses/chapter07/Spark%20on%20Kubernetes.md) ([PPT](./courses/chapter07/2.%20Spark%20on%20Kubernetes%20架构及组件介绍.pptx))
  - [Spark 3.5 演示项目](./courses/chapter07/spark-3.5-demo/)

- **第八讲**:

  - [原教材：第 07 讲-数据仓库开发工具 Hive](./courses/chapter08/第07讲-数据仓库开发工具Hive.pdf)
  - [**1. Apache Hive 设计与实现**](./courses/chapter08/Apache%20Hive%20设计与实现.md) ([PPT](./courses/chapter08/1.%20Apache%20Hive%20设计与实现.pptx))
  - [**2. 列式存储：Parquet 文件格式解析**](./courses/chapter08/列式存储：Parquet%20文件格式解析.md) ([PPT](./courses/chapter08/2.%20Parquet%20文件格式深入解析.pptx))
  - [Parquet 实践练习](./courses/chapter08/parquet-practice-project/Parquet实践练习.md)
    - [Parquet 实践项目](./courses/chapter08/parquet-practice-project/)

- **第九讲**:

  - [原教材：第 08 讲-分布式 NoSQL 数据库 HBase](./courses/chapter09/第08讲-分布式NoSQL数据库HBase.pdf)
  - [**1. Bigtable 论文详解**](./courses/chapter09/Bigtable论文详解.md) ([PPT](./courses/chapter09/1.%20Bigtable论文详解.pptx))
  - [**2. HBase 设计与实现**](./courses/chapter09/HBase%20设计与实现.md) ([PPT](./courses/chapter09/3.%20HBase%20设计与实现.pptx))
  - [**3. LSM-Tree 入门**](./courses/chapter09/java-lsm-tree/docs/lsm-tree-intro.md) ([PPT](./courses/chapter09/2.%20LSM-Tree%20入门.pptx))
    - [LSM-Tree 实践项目](./courses/chapter09/java-lsm-tree/))
  - [延伸：Feed 流系统架构演进综述](./courses/chapter09/feed_stream_architecture_review.md)
  - [辅助材料](./courses/chapter09/辅助材料/)
    - [从 40 亿整数到 HBase：一个 Membership Test 问题的抽象与演化](./courses/chapter09/辅助材料/membership_test.md)
    - [Bigtable 作者演讲稿 (2005)](./courses/chapter09/辅助材料/bigtable-uw-2005.pdf)
    - [Bloom Filter 简介](./courses/chapter09/辅助材料/bloom_filter_intro.md)
    - [LSM vs B-Tree](./courses/chapter09/辅助材料/lsm_vs_btree.md)

- **第十讲**：

  - [原教材：第 09 讲-分布式消息队列 Kafka](./courses/chapter10/第09讲-分布式消息队列Kafka.pdf)
  - [**1. Kafka 设计与实现**](./courses/chapter10/Kafka%20设计与实现.md) ([PPT](./courses/chapter10/1.%20Kafka设计与实现.pptx))
  - [**2. Kafka 日志收集架构设计案例**](./courses/chapter10/Kafka%20日志收集架构设计案例.md)
  - [Kafka 实践示例](./courses/chapter10/examples/README.md)

- **第十一讲**：

  - [原教材：第 10 讲-分布式数据采集工具 Flume](./courses/chapter11/第10讲-分布式数据采集工具Flume.pdf)
  - [原教材：第 11 讲-分布式流处理框架 Flink](./courses/chapter11/第11讲-分布式流处理框架Flink.pdf)
  - [**1. 从 ETL 到流式计算入门**](./courses/chapter11/从ETL到流式计算入门.md) ([PPT](./courses/chapter11/1.%20从ETL到流式计算入门.pptx))
  - [**2. Flink 设计与实现**](./courses/chapter11/Flink%20设计与实现.md) ([PPT](./courses/chapter11/2.%20Flink%20设计与实现.pptx))
  - [流式计算动手实践系列](./courses/chapter11/hands-on-streaming/README.md)
  - [辅助材料](./courses/chapter11/辅助材料/)
    - [Lambda 与 Kappa 架构](./courses/chapter11/辅助材料/Lambda_and_Kappa_Architecture.md)
    - [State 与容错机制直观解释](./courses/chapter11/辅助材料/State与容错机制直观解释.md)
    - [Watermark 直观解释与示例](./courses/chapter11/辅助材料/Watermark%20直观解释与示例.md)
    - [Window 机制与 Watermark 协同详解](./courses/chapter11/辅助材料/Window机制与Watermark协同详解.md)

- **第十二讲**：

  - [原教材：第 12 讲-数据仓库与数据集市概述](./courses/chapter12/第12讲-数据仓库与数据集市概述.pdf)
  - [原教材：第 13 讲-数据湖概述](./courses/chapter12/第13讲-数据湖概述.pdf)
  - [**1. 数仓分层架构详解**](./courses/chapter12/辅助材料/数仓分层架构详解.md) ([PPT](./courses/chapter12/1.%20数仓分层架构详解.pptx))
  - [**2. Table Format 技术详解**](./courses/chapter12/辅助材料/Table_Format_技术详解.md) ([PPT](./courses/chapter12/2.%20Table_Format技术详解.pptx))
  - [**3. Iceberg 核心机制深度剖析**](./courses/chapter12/辅助材料/Iceberg_核心机制深度剖析.md) ([PPT](./courses/chapter12/3.%20Iceberg核心机制深度剖析.pptx))
  - [**4. 从数据仓库到湖仓一体：现代数据架构的演进与原理**](./courses/chapter12/从数据仓库到数据湖.md) ([PPT](./courses/chapter12/4.%20从数据仓库到湖仓一体.pptx))
  - [辅助材料](./courses/chapter12/辅助材料/)
    - [从数据仓库到湖仓一体-教学设计与作业](./courses/chapter12/辅助材料/从数据仓库到湖仓一体-教学设计与作业.md)

- **其他**：

  - [Gossip 协议介绍](./courses/other/gossip-protocol-intro.md)
  - [Gossip 协议实验](./courses/other/gossip-lab/README.md)

- **复习**：

  - [复习 PPT](./courses/复习/大数据理论与实践Ⅰ-章节复习提纲.pptx)
  - [章节复习提纲](./courses/复习/大数据理论与实践Ⅰ-章节复习提纲.md)
  - [思考题参考答案](./courses/复习/大数据理论与实践Ⅰ-思考题参考答案.md)

---

## 4. 参考论文

大数据领域的经典论文集合。

| **年份** | **技术/系统**        | **论文标题**                                                                                                                                     | **技术领域**                                                                                                                                                                         |
| -------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 2003     | **GFS**              | `The Google File System`                                                                                                                         | [分布式文件系统](./paper/gfs-sosp2003.pdf)                                                                                                                                           |
| 2004     | **MapReduce**        | `MapReduce: Simplified Data Processing on Large Clusters`                                                                                        | [分布式计算框架](./paper/Dean%20和%20Ghemawat%20-%202008%20-%20MapReduce%20simplified%20data%20processing%20on%20large%20clu.pdf)                                                    |
| 2006     | **Bigtable**         | `Bigtable: A Distributed Storage System for Structured Data`                                                                                     | [分布式数据库](./paper/Chang%20等%20-%202008%20-%20Bigtable%20A%20Distributed%20Storage%20System%20for%20Structu.pdf)                                                                |
| 2006     | **Chubby**           | `The Chubby lock service for loosely-coupled distributed systems`                                                                                | [分布式锁服务](./paper/Burrows%20-%202006%20-%20The%20Chubby%20lock%20service%20for%20loosely-coupled%20distributed%20systems.pdf)                                                   |
| 2007     | **Thrift**           | `Thrift: Scalable cross-language services implementation`                                                                                        | [RPC 框架](./paper/Slee%20等%20-%20Thrift%20Scalable%20Cross-Language%20Services%20Implementation.pdf)                                                                               |
| 2008     | **Hive**             | `Hive: A warehousing solution over a map-reduce framework`                                                                                       | [数据仓库](./paper/Thusoo%20等%20-%202009%20-%20Hive%20a%20warehousing%20solution%20over%20a%20map-reduce%20framework.pdf)                                                           |
| 2010     | **Dremel**           | `Dremel: Interactive analysis of web-scale datasets`                                                                                             | [交互式查询引擎](./paper/Melnik%20等%20-%20Dremel%20Interactive%20Analysis%20of%20Web-Scale%20Datasets.pdf)                                                                          |
| 2010     | **Spark**            | `Spark: Cluster computing with working sets`                                                                                                     | [内存计算框架](./paper/Zaharia%20等%20-%20Spark%20Cluster%20Computing%20with%20Working%20Sets.pdf)                                                                                   |
| 2010     | **S4**               | `S4: Distributed stream computing platform`                                                                                                      | [流计算平台](./paper/Neumeyer%20等%20-%202010%20-%20S4%20Distributed%20Stream%20Computing%20Platform.pdf)                                                                            |
| 2011     | **Megastore**        | `Megastore: Providing scalable, highly available storage for interactive services`                                                               | [分布式存储](./paper/Baker%20等%20-%20Megastore%20Providing%20Scalable,%20Highly%20Available%20Storage%20for%20Interactive%20Services.pdf)                                           |
| 2011     | **Kafka**            | `Kafka: A distributed messaging system for log processing`                                                                                       | [消息队列系统](./paper/Kreps%20等%20-%20Kafka%20a%20Distributed%20Messaging%20System%20for%20Log%20Processing.pdf)                                                                   |
| 2012     | **Spanner**          | `Spanner: Google's globally distributed database`                                                                                                | [全球分布式数据库](./paper/Corbett%20等%20-%20Spanner%20Google’s%20Globally-Distributed%20Database.pdf)                                                                              |
| 2014     | **Storm**            | `Storm@Twitter`                                                                                                                                  | [实时流处理](./paper/Toshniwal%20等%20-%202014%20-%20Storm@twitter.pdf)                                                                                                              |
| 2014     | **Raft**             | `In search of an understandable consensus algorithm`                                                                                             | [分布式一致性算法](./paper/Ongaro和Ousterhout%20-%20In%20Search%20of%20an%20Understandable%20Consensus%20Algorithm.pdf)                                                              |
| 2015     | **Dataflow**         | `The dataflow model: A practical approach to balancing correctness, latency, and cost in massive-scale, unbounded, out-of-order data processing` | [流处理模型](./paper/Akidau%20等%20-%202015%20-%20The%20dataflow%20model%20a%20practical%20approach%20to%20balancing%20correctness,%20latency,%20and%20cost%20in%20massive-scal.pdf) |
| 2018     | **PolarFS**          | `PolarFS: an ultra-low latency and failure resilient distributed file system for shared storage cloud database`                                  | [云原生文件系统](./paper/Cao%20等%20-%202018%20-%20PolarFS%20an%20ultra-low%20latency%20and%20failure%20resilien.pdf)                                                                |
| 2020     | **Delta Lake**       | `Delta lake: high-performance ACID table storage over cloud object stores`                                                                       | [数据湖存储](./paper/Armbrust%20等%20-%202020%20-%20Delta%20lake%20high-performance%20ACID%20table%20storage%20ov.pdf)                                                               |
| 2021     | **Lakehouse**        | `Lakehouse: A New Generation of Open Platforms for AI and Data Analytics`                                                                        | [湖仓一体架构](./paper/Armbrust%20等%20-%202021%20-%20Lakehouse%20A%20New%20Generation%20of%20Open%20Platforms%20that.pdf)                                                           |
| 2023     | **HTAP 综述**        | `HTAP 数据库关键技术综述`                                                                                                                        | [混合事务分析处理](./paper/张超，李国良，冯建华，张金涛%20和%20ZHANG%20Chao%20-%202022%20-%20HTAP数据库关键技术综述.pdf)                                                             |
| 2024     | **云原生数据库综述** | `云原生数据库综述`                                                                                                                               | [云原生数据库](./paper/云原生数据库综述.pdf)                                                                                                                                         |
| 2024     | **Iceberg**          | `Apache Iceberg: The Definitive Guide`                                                                                                           | [表格式标准](./paper/apache-iceberg-TDG_ER1.pdf)                                                                                                                                     |

## 5 环境搭建

Hadoop 环境配置指南和部署脚本

### 5.1 单节点集群部署（开发测试环境）

- [单节点集群配置指南](./env-setup/signle-node/single-node-cluster.md) - 完整的单节点 Hadoop 集群部署教程
- [软件学院专用配置](./env-setup/signle-node/single-node-cluster_se_school.md) - 针对软件学院环境的配置指南
- [Windows 环境配置](./env-setup/signle-node/single-node-cluster_windows.md) - Windows 系统下的 Hadoop 配置

### 5.2 多节点集群部署（作业环境）

- [多节点集群配置指南](./env-setup/multi-node/multi-node-cluster.md) - 多节点集群部署
- [多用户环境配置](./env-setup/multi-node/multi-user-setup.md) - 多用户共享集群配置
- [集群部署脚本](./env-setup/multi-node/cluster-setup-scripts/) - 自动化部署脚本集合

---
