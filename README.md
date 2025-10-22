# Big-Data-Theory-and-Practice

《大数据理论与实践》课程学习材料仓库

## 1. 简介

本仓库是《大数据理论与实践》课程的学习材料集合，包含课程讲义、经典论文、参考书籍、环境配置指南和实践练习等内容。旨在为学习者提供系统性的大数据理论知识和实践技能培养。

---

## 2. 参考书籍

存放大数据相关的参考书籍和资料

- `README.md` - 书籍目录说明
- `hadoop-definitive-guide.jpg` - Hadoop 权威指南封面图

---

## 3 课程

以下是课程章节内容：

- **第一讲** - [大数据技术综述](./courses/chapter01/第01讲-大数据技术综述.pdf)

  - [补充：大数据发展的历史-从核心理念到生态演化](./courses/chapter01/【补充】大数据发展的历史-从核心理念到生态演化.pdf)

- **第二讲** - [分布式协调服务 ZooKeeper](./courses/chapter02/第02讲-分布式协调服务Zookeeper.pdf)

  - [补充：Basic Paxos 两阶段共识流程全解析](./courses/chapter02/【补充】Basic%20Paxos%20两阶段共识流程全解析.pdf)
  - [补充：Chubby - 分布式锁服务介绍](./courses/chapter02/【补充】Chubby%20-%20分布式锁服务介绍.pdf)
  - [补充：分布式系统讲义](./courses/chapter02/【补充】分布式系统讲义.pdf)

- **第三讲** - [分布式文件系统 HDFS（上）](./courses/chapter03/第03讲-分布式文件系统HDFS（上）.pdf)

  - [补充：GFS-大数据存储的奠基者](./courses/chapter03/【补充】GFS-大数据存储的奠基者.pdf)

- **第四讲** - [分布式文件系统 HDFS（下）](./courses/chapter04/第03讲-分布式文件系统HDFS（下）.pdf)

  - [HDFS 关键概念复习文档](./courses/chapter04/HDFS关键概念复习文档.md)
  - [HDFS 常见操作](./courses/chapter04/HDFS常见操作.md)
  - [练习 1](./courses/chapter04/exercise_1.md)

- **第五讲** - [MapReduce 分布式计算框架](./courses/chapter05/map-reduce.md)
  - [学生使用指南](./courses/chapter05/student-guide.md)

---

## 4. 参考论文

大数据领域的经典论文集合。

| 年份 | 技术/系统            | 论文标题                                                                                                                                         | 技术领域                                                                                                                                                                             |
| ---- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 2003 | **GFS**              | `The Google File System`                                                                                                                         | [分布式文件系统](./paper/gfs-sosp2003.pdf)                                                                                                                                           |
| 2004 | **MapReduce**        | `MapReduce: Simplified Data Processing on Large Clusters`                                                                                        | [分布式计算框架](./paper/Dean%20和%20Ghemawat%20-%202008%20-%20MapReduce%20simplified%20data%20processing%20on%20large%20clu.pdf)                                                    |
| 2006 | **Bigtable**         | `Bigtable: A Distributed Storage System for Structured Data`                                                                                     | [分布式数据库](./paper/Chang%20等%20-%202008%20-%20Bigtable%20A%20Distributed%20Storage%20System%20for%20Structu.pdf)                                                                |
| 2006 | **Chubby**           | `The Chubby lock service for loosely-coupled distributed systems`                                                                                | [分布式锁服务](./paper/Burrows%20-%202006%20-%20The%20Chubby%20lock%20service%20for%20loosely-coupled%20distributed%20systems.pdf)                                                   |
| 2007 | **Thrift**           | `Thrift: Scalable cross-language services implementation`                                                                                        | [RPC 框架](./paper/Slee%20等%20-%20Thrift%20Scalable%20Cross-Language%20Services%20Implementation.pdf)                                                                               |
| 2008 | **Hive**             | `Hive: A warehousing solution over a map-reduce framework`                                                                                       | [数据仓库](./paper/Thusoo%20等%20-%202009%20-%20Hive%20a%20warehousing%20solution%20over%20a%20map-reduce%20framework.pdf)                                                           |
| 2010 | **Dremel**           | `Dremel: Interactive analysis of web-scale datasets`                                                                                             | [交互式查询引擎](./paper/Melnik%20等%20-%20Dremel%20Interactive%20Analysis%20of%20Web-Scale%20Datasets.pdf)                                                                          |
| 2010 | **Spark**            | `Spark: Cluster computing with working sets`                                                                                                     | [内存计算框架](./paper/Zaharia%20等%20-%20Spark%20Cluster%20Computing%20with%20Working%20Sets.pdf)                                                                                   |
| 2010 | **S4**               | `S4: Distributed stream computing platform`                                                                                                      | [流计算平台](./paper/Neumeyer%20等%20-%202010%20-%20S4%20Distributed%20Stream%20Computing%20Platform.pdf)                                                                            |
| 2011 | **Megastore**        | `Megastore: Providing scalable, highly available storage for interactive services`                                                               | [分布式存储](./paper/Baker%20等%20-%20Megastore%20Providing%20Scalable,%20Highly%20Available%20Storage%20for%20Interactive%20Services.pdf)                                           |
| 2011 | **Kafka**            | `Kafka: A distributed messaging system for log processing`                                                                                       | [消息队列系统](./paper/Kreps%20等%20-%20Kafka%20a%20Distributed%20Messaging%20System%20for%20Log%20Processing.pdf)                                                                   |
| 2012 | **Spanner**          | `Spanner: Google's globally distributed database`                                                                                                | [全球分布式数据库](./paper/Corbett%20等%20-%20Spanner%20Google’s%20Globally-Distributed%20Database.pdf)                                                                              |
| 2014 | **Storm**            | `Storm@Twitter`                                                                                                                                  | [实时流处理](./paper/Toshniwal%20等%20-%202014%20-%20Storm@twitter.pdf)                                                                                                              |
| 2014 | **Raft**             | `In search of an understandable consensus algorithm`                                                                                             | [分布式一致性算法](./paper/Ongaro和Ousterhout%20-%20In%20Search%20of%20an%20Understandable%20Consensus%20Algorithm.pdf)                                                              |
| 2015 | **Dataflow**         | `The dataflow model: A practical approach to balancing correctness, latency, and cost in massive-scale, unbounded, out-of-order data processing` | [流处理模型](./paper/Akidau%20等%20-%202015%20-%20The%20dataflow%20model%20a%20practical%20approach%20to%20balancing%20correctness,%20latency,%20and%20cost%20in%20massive-scal.pdf) |
| 2018 | **PolarFS**          | `PolarFS: an ultra-low latency and failure resilient distributed file system for shared storage cloud database`                                  | [云原生文件系统](./paper/Cao%20等%20-%202018%20-%20PolarFS%20an%20ultra-low%20latency%20and%20failure%20resilien.pdf)                                                                |
| 2020 | **Delta Lake**       | `Delta lake: high-performance ACID table storage over cloud object stores`                                                                       | [数据湖存储](./paper/Armbrust%20等%20-%202020%20-%20Delta%20lake%20high-performance%20ACID%20table%20storage%20ov.pdf)                                                               |
| 2021 | **Lakehouse**        | `Lakehouse: A New Generation of Open Platforms for AI and Data Analytics`                                                                        | [湖仓一体架构](./paper/Armbrust%20等%20-%202021%20-%20Lakehouse%20A%20New%20Generation%20of%20Open%20Platforms%20that.pdf)                                                           |
| 2023 | **HTAP 综述**        | `HTAP 数据库关键技术综述`                                                                                                                        | [混合事务分析处理](./paper/张超，李国良，冯建华，张金涛%20和%20ZHANG%20Chao%20-%202022%20-%20HTAP数据库关键技术综述.pdf)                                                             |
| 2024 | **云原生数据库综述** | `云原生数据库综述`                                                                                                                               | [云原生数据库](./paper/云原生数据库综述.pdf)                                                                                                                                         |
| 2024 | **Iceberg**          | `Apache Iceberg: The Definitive Guide`                                                                                                           | [表格式标准](./paper/apache-iceberg-TDG_ER1.pdf)                                                                                                                                     |

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
