# Hadoop 多节点集群部署指南 (Ubuntu)

## 1. 概述

本文档详细介绍如何在 5 台 Ubuntu 系统上搭建 Hadoop 分布式集群。集群采用完全分布式模式（Fully Distributed Mode），其中每个 Hadoop 守护进程运行在不同的物理机器上，提供真正的分布式计算和存储能力。

**相关文档**：

- [多用户配置和管理](./multi-user-setup.md)
- [Hadoop 集群学生使用指南](./student-guide.md)

### 1.1 集群架构设计

本部署方案采用以下架构：

- **主节点（Master Node）**：1 台

  - NameNode：管理 HDFS 元数据
  - ResourceManager：管理 YARN 资源调度
  - MapReduce Job History Server：管理 MapReduce 作业历史

- **工作节点（Worker Nodes）**：4 台
  - DataNode：存储 HDFS 数据块
  - NodeManager：管理单个节点的资源和容器
  - SecondaryNameNode：部署在 Worker1 节点，辅助 NameNode 进行元数据备份

### 1.2 节点规划

| 节点角色 | 主机名         | IP 地址      | 服务组件                                    |
| -------- | -------------- | ------------ | ------------------------------------------- |
| Master   | hadoop-master  | 192.168.1.10 | NameNode, ResourceManager, JobHistoryServer |
| Worker   | hadoop-worker1 | 192.168.1.11 | DataNode, NodeManager, SecondaryNameNode    |
| Worker   | hadoop-worker2 | 192.168.1.12 | DataNode, NodeManager                       |
| Worker   | hadoop-worker3 | 192.168.1.13 | DataNode, NodeManager                       |
| Worker   | hadoop-worker4 | 192.168.1.14 | DataNode, NodeManager                       |

**注意**：请根据您的实际网络环境调整 IP 地址。

---

## 2. 系统要求

### 2.1 建议平台

- **操作系统**：GNU/Linux（推荐 Ubuntu 22.04 LTS 或更高版本）
- **架构**：x86_64

### 2.2 硬件配置要求

#### 2.2.1 主节点最低配置

- **CPU**：4 核心
- **内存**：8 GB RAM
- **存储**：100 GB 可用磁盘空间
- **网络**：千兆网络连接

#### 2.2.2 工作节点最低配置

- **CPU**：2 核心
- **内存**：4 GB RAM
- **存储**：50 GB 可用磁盘空间
- **网络**：千兆网络连接

#### 2.2.3 推荐配置

- **主节点**：

  - CPU：8 核心或更多
  - 内存：16 GB RAM 或更多
  - 存储：200 GB 可用磁盘空间（SSD 优先）

- **工作节点**：
  - CPU：4 核心或更多
  - 内存：8 GB RAM 或更多
  - 存储：100 GB 可用磁盘空间（SSD 优先）

#### 2.2.4 磁盘配置建议

- **独立数据磁盘**：每个节点推荐使用独立磁盘挂载到 `/mnt/hadoop` 目录
- **文件系统**：xfs（推荐）或 ext4
- **挂载权限**：确保 Hadoop 用户具有读写权限
- **RAID 配置**：生产环境建议使用 RAID 1 或 RAID 10 提高可靠性

### 2.3 必需软件

#### 2.3.1 Java 环境

- **Java 版本**：推荐使用 OpenJDK 8（Hadoop 3.4+ 也支持 Java 11/17，但为确保最佳兼容性建议使用 Java 8）
- **环境变量**：需要正确配置 `JAVA_HOME`

#### 2.3.2 SSH 服务

- **ssh**：用于远程连接和管理 Hadoop 守护进程
- **sshd**：SSH 守护进程必须在所有节点运行
- **pdsh**：（可选）用于更好的 SSH 资源管理

---

## 3. 集群环境准备

### 3.1 所有节点通用配置

以下步骤需要在所有 5 台机器上执行：

#### 3.1.1 更新系统包

```bash
# 更新包列表
sudo apt update

# 升级系统包
sudo apt upgrade -y
```

#### 3.1.2 安装 Java

```bash
# 安装 OpenJDK 8
sudo apt install openjdk-8-jdk -y

# 验证 Java 安装
java -version
javac -version
```

#### 3.1.3 配置 Java 环境变量

```bash
# 查找 Java 安装路径
sudo find /usr -name "java" -type f 2>/dev/null | grep bin

# 编辑环境变量文件
sudo nano /etc/environment

# 添加以下内容（根据实际路径调整）
JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"

# 重新加载环境变量
source /etc/environment

# 验证 JAVA_HOME
echo $JAVA_HOME
```

#### 3.1.4 安装 SSH 服务

```bash
# 安装 SSH 和 pdsh
sudo apt-get install ssh -y
sudo apt-get install pdsh -y

# 启动 SSH 服务
sudo systemctl start ssh
sudo systemctl enable ssh

# 验证 SSH 服务状态
sudo systemctl status ssh

# 配置 PDSH 使用 SSH
echo 'export PDSH_RCMD_TYPE=ssh' >> ~/.bashrc
source ~/.bashrc
```

#### 3.1.5 创建 Hadoop 用户

```bash
# 创建 hadoop 用户
sudo adduser hadoop
sudo usermod -aG sudo hadoop

# 设置 hadoop 用户密码（可选，建议使用密钥认证）
sudo passwd hadoop
```

### 3.2 网络配置

#### 3.2.1 配置主机名和 hosts 文件

在所有节点上执行以下操作：

```bash
# 设置主机名（每个节点设置不同的主机名）
# 主节点
sudo hostnamectl set-hostname hadoop-master

# 工作节点 1
sudo hostnamectl set-hostname hadoop-worker1

# 工作节点 2
sudo hostnamectl set-hostname hadoop-worker2

# 工作节点 3
sudo hostnamectl set-hostname hadoop-worker3

# 工作节点 4
sudo hostnamectl set-hostname hadoop-worker4
```

在所有节点的 `/etc/hosts` 文件中添加集群节点信息：

```bash
# 编辑 hosts 文件
sudo nano /etc/hosts

# 添加以下内容（根据实际 IP 地址调整）
192.168.1.10    hadoop-master
192.168.1.11    hadoop-worker1
192.168.1.12    hadoop-worker2
192.168.1.13    hadoop-worker3
192.168.1.14    hadoop-worker4
```

#### 3.2.2 测试网络连通性

```bash
# 在主节点测试到所有工作节点的连通性
ping -c 3 hadoop-worker1
ping -c 3 hadoop-worker2
ping -c 3 hadoop-worker3
ping -c 3 hadoop-worker4

# 在工作节点测试到主节点的连通性
ping -c 3 hadoop-master
```

### 3.3 SSH 无密码登录配置

#### 3.3.1 在主节点生成 SSH 密钥

```bash
# 切换到 hadoop 用户
su - hadoop

# 生成 SSH 密钥对
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa

# 将公钥添加到本地授权文件
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

# 设置正确的权限
chmod 0600 ~/.ssh/authorized_keys
chmod 700 ~/.ssh
```

#### 3.3.2 分发公钥到所有工作节点

```bash
# 从主节点分发公钥到所有工作节点
ssh-copy-id hadoop@hadoop-worker1
ssh-copy-id hadoop@hadoop-worker2
ssh-copy-id hadoop@hadoop-worker3
ssh-copy-id hadoop@hadoop-worker4

# 测试无密码 SSH 连接
ssh hadoop@hadoop-worker1 'hostname'
ssh hadoop@hadoop-worker2 'hostname'
ssh hadoop@hadoop-worker3 'hostname'
ssh hadoop@hadoop-worker4 'hostname'
```

#### 3.3.3 配置工作节点间的 SSH 连接（可选）

如果需要工作节点间相互通信，可以在每个工作节点上重复上述步骤。

### 3.4 磁盘配置

#### 3.4.1 准备数据磁盘

在所有节点上执行：

```bash
# 检查可用磁盘
lsblk
fdisk -l

# 格式化数据磁盘为 xfs（假设数据磁盘为 /dev/sdb）
sudo mkfs.xfs -f /dev/sdb

# 创建挂载点
sudo mkdir -p /mnt/hadoop

# 挂载磁盘
sudo mount -t xfs -o noatime,nodiratime,logbufs=8,logbsize=32k /dev/sdb /mnt/hadoop

# 设置权限
sudo chown -R hadoop:hadoop /mnt/hadoop
sudo chmod 755 /mnt/hadoop

# 配置开机自动挂载
echo '/dev/sdb /mnt/hadoop xfs noatime,nodiratime,logbufs=8,logbsize=32k 0 2' | sudo tee -a /etc/fstab
```

#### 3.4.2 创建 Hadoop 数据目录

在所有节点上执行：

```bash
# 切换到 hadoop 用户
su - hadoop

# 创建 Hadoop 数据目录
mkdir -p /mnt/hadoop/data/namenode
mkdir -p /mnt/hadoop/data/datanode
mkdir -p /mnt/hadoop/tmp
mkdir -p /mnt/hadoop/logs

# 设置目录权限
chmod 755 /mnt/hadoop/data/namenode
chmod 755 /mnt/hadoop/data/datanode
chmod 755 /mnt/hadoop/tmp
chmod 755 /mnt/hadoop/logs
```

---

## 4. Hadoop 安装

### 4.1 在主节点下载和安装 Hadoop

```bash
# 切换到 hadoop 用户
su - hadoop

# 进入用户主目录
cd /home/hadoop

# 下载 Hadoop 3.4.2
wget https://downloads.apache.org/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz

# 或使用阿里云镜像
# wget https://mirrors.aliyun.com/apache/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz

# 解压 Hadoop
tar -xzf hadoop-3.4.2.tar.gz

# 重命名目录
mv hadoop-3.4.2 hadoop

# 设置权限
chown -R hadoop:hadoop ~/hadoop

# 清理下载文件
rm hadoop-3.4.2.tar.gz
```

### 4.2 配置环境变量

在主节点配置环境变量：

```bash
# 编辑 .bashrc 文件
nano ~/.bashrc

# 添加以下内容到文件末尾
export HADOOP_HOME=/home/hadoop/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# 重新加载 .bashrc
source ~/.bashrc

# 验证 Hadoop 安装
hadoop version
```

### 4.3 分发 Hadoop 到工作节点

```bash
# 从主节点分发 Hadoop 到所有工作节点
scp -r ~/hadoop hadoop@hadoop-worker1:~/
scp -r ~/hadoop hadoop@hadoop-worker2:~/
scp -r ~/hadoop hadoop@hadoop-worker3:~/
scp -r ~/hadoop hadoop@hadoop-worker4:~/

# 分发环境变量配置
scp ~/.bashrc hadoop@hadoop-worker1:~/
scp ~/.bashrc hadoop@hadoop-worker2:~/
scp ~/.bashrc hadoop@hadoop-worker3:~/
scp ~/.bashrc hadoop@hadoop-worker4:~/
```

### 4.4 在工作节点加载环境变量

在每个工作节点上执行：

```bash
# 切换到 hadoop 用户
su - hadoop

# 重新加载环境变量
source ~/.bashrc

# 验证 Hadoop 安装
hadoop version
```

---

## 5. Hadoop 集群配置

### 5.1 配置 Hadoop 环境

在主节点编辑 `$HADOOP_HOME/etc/hadoop/hadoop-env.sh` 文件：

```bash
# 编辑 hadoop-env.sh
nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# 添加或修改以下行
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_LOG_DIR=/mnt/hadoop/logs
export HADOOP_PID_DIR=/mnt/hadoop/pids
```

### 5.2 配置核心组件

#### 5.2.1 配置 core-site.xml

```bash
# 编辑 core-site.xml
nano $HADOOP_HOME/etc/hadoop/core-site.xml
```

添加以下配置内容：

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://hadoop-master:9000</value>
        <description>默认文件系统 URI，指向主节点的 NameNode</description>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/mnt/hadoop/tmp</value>
        <description>Hadoop 临时目录</description>
    </property>
    <property>
        <name>io.file.buffer.size</name>
        <value>131072</value>
        <description>文件缓冲区大小，提高 I/O 性能</description>
    </property>
</configuration>
```

#### 5.2.2 配置 hdfs-site.xml

```bash
# 编辑 hdfs-site.xml
nano $HADOOP_HOME/etc/hadoop/hdfs-site.xml
```

添加以下配置内容：

```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>3</value>
        <description>数据块副本数量（建议设置为 3）</description>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/mnt/hadoop/data/namenode</value>
        <description>NameNode 数据存储目录</description>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/mnt/hadoop/data/datanode</value>
        <description>DataNode 数据存储目录</description>
    </property>
    <property>
        <name>dfs.namenode.secondary.http-address</name>
        <value>hadoop-worker1:50090</value>
        <description>SecondaryNameNode HTTP 地址</description>
    </property>
    <property>
        <name>dfs.blocksize</name>
        <value>134217728</value>
        <description>HDFS 块大小（128MB）</description>
    </property>
    <property>
        <name>dfs.namenode.handler.count</name>
        <value>20</value>
        <description>NameNode 处理线程数</description>
    </property>
    <property>
        <name>dfs.datanode.handler.count</name>
        <value>10</value>
        <description>DataNode 处理线程数</description>
    </property>
</configuration>
```

#### 5.2.3 配置 mapred-site.xml

```bash
# 编辑 mapred-site.xml
nano $HADOOP_HOME/etc/hadoop/mapred-site.xml
```

添加以下配置内容：

```xml
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
        <description>MapReduce 框架名称</description>
    </property>
    <property>
        <name>mapreduce.jobhistory.address</name>
        <value>hadoop-master:10020</value>
        <description>MapReduce JobHistory Server 地址</description>
    </property>
    <property>
        <name>mapreduce.jobhistory.webapp.address</name>
        <value>hadoop-master:19888</value>
        <description>MapReduce JobHistory Server Web UI 地址</description>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=/home/hadoop/hadoop</value>
        <description>ApplicationMaster 环境变量</description>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=/home/hadoop/hadoop</value>
        <description>Map 任务环境变量</description>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=/home/hadoop/hadoop</value>
        <description>Reduce 任务环境变量</description>
    </property>
    <property>
        <name>mapreduce.map.memory.mb</name>
        <value>1024</value>
        <description>Map 任务内存限制</description>
    </property>
    <property>
        <name>mapreduce.reduce.memory.mb</name>
        <value>2048</value>
        <description>Reduce 任务内存限制</description>
    </property>
</configuration>
```

#### 5.2.4 配置 yarn-site.xml

```bash
# 编辑 yarn-site.xml
nano $HADOOP_HOME/etc/hadoop/yarn-site.xml
```

添加以下配置内容：

```xml
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
        <description>NodeManager 辅助服务</description>
    </property>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>hadoop-master</value>
        <description>ResourceManager 主机名</description>
    </property>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>hadoop-master:8032</value>
        <description>ResourceManager 地址</description>
    </property>
    <property>
        <name>yarn.resourcemanager.scheduler.address</name>
        <value>hadoop-master:8030</value>
        <description>ResourceManager 调度器地址</description>
    </property>
    <property>
        <name>yarn.resourcemanager.resource-tracker.address</name>
        <value>hadoop-master:8031</value>
        <description>ResourceManager 资源跟踪地址</description>
    </property>
    <property>
        <name>yarn.resourcemanager.admin.address</name>
        <value>hadoop-master:8033</value>
        <description>ResourceManager 管理地址</description>
    </property>
    <property>
        <name>yarn.resourcemanager.webapp.address</name>
        <value>hadoop-master:8088</value>
        <description>ResourceManager Web UI 地址</description>
    </property>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>3072</value>
        <description>NodeManager 可用内存（根据实际情况调整）</description>
    </property>
    <property>
        <name>yarn.nodemanager.resource.cpu-vcores</name>
        <value>2</value>
        <description>NodeManager 可用 CPU 核心数（根据实际情况调整）</description>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>3072</value>
        <description>单个容器最大内存分配</description>
    </property>
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>512</value>
        <description>单个容器最小内存分配</description>
    </property>
</configuration>
```

### 5.3 配置工作节点列表

#### 5.3.1 配置 workers 文件

```bash
# 编辑 workers 文件（Hadoop 3.x 使用 workers，旧版本使用 slaves）
nano $HADOOP_HOME/etc/hadoop/workers

# 添加所有工作节点主机名
hadoop-worker1
hadoop-worker2
hadoop-worker3
hadoop-worker4
```

### 5.4 分发配置文件到所有节点

```bash
# 从主节点分发配置文件到所有工作节点
scp $HADOOP_HOME/etc/hadoop/* hadoop@hadoop-worker1:$HADOOP_HOME/etc/hadoop/
scp $HADOOP_HOME/etc/hadoop/* hadoop@hadoop-worker2:$HADOOP_HOME/etc/hadoop/
scp $HADOOP_HOME/etc/hadoop/* hadoop@hadoop-worker3:$HADOOP_HOME/etc/hadoop/
scp $HADOOP_HOME/etc/hadoop/* hadoop@hadoop-worker4:$HADOOP_HOME/etc/hadoop/
```

---

## 6. 启动 Hadoop 集群

### 6.1 启动前检查

在启动集群之前，请确保以下条件已满足：

#### 6.1.1 网络连通性检查

```bash
# 在主节点测试到所有工作节点的连通性
ping -c 3 hadoop-worker1
ping -c 3 hadoop-worker2
ping -c 3 hadoop-worker3
ping -c 3 hadoop-worker4

# 测试 SSH 无密码连接
ssh hadoop@hadoop-worker1 'hostname && exit'
ssh hadoop@hadoop-worker2 'hostname && exit'
ssh hadoop@hadoop-worker3 'hostname && exit'
ssh hadoop@hadoop-worker4 'hostname && exit'
```

#### 6.1.2 数据目录检查

```bash
# 在所有节点检查数据目录是否存在且权限正确
# 主节点
ls -la /mnt/hadoop/data/namenode
ls -la /mnt/hadoop/tmp

# 工作节点（通过 SSH 检查）
for worker in hadoop-worker1 hadoop-worker2 hadoop-worker3 hadoop-worker4; do
    echo "检查 $worker 数据目录..."
    ssh hadoop@$worker "ls -la /mnt/hadoop/data/datanode && ls -la /mnt/hadoop/tmp"
done
```

#### 6.1.3 Java 和 Hadoop 环境检查

```bash
# 在所有节点验证 Java 和 Hadoop 环境
for node in hadoop-worker1 hadoop-worker2 hadoop-worker3 hadoop-worker4; do
    echo "检查 $node 环境..."
    ssh hadoop@$node "java -version && hadoop version | head -1"
done
```

### 6.2 格式化 HDFS

**重要**：只在主节点执行，且仅在首次启动时执行：

```bash
# 在主节点格式化 NameNode
hdfs namenode -format

# 确认格式化操作，输入 'Y' 并按回车
```

### 6.3 启动 HDFS 服务

在主节点执行：

```bash
# 启动 HDFS 守护进程
start-dfs.sh

# 验证进程是否启动
jps
```

预期在主节点看到：

- NameNode

预期在 Worker1 节点看到：

- DataNode
- SecondaryNameNode

预期在其他工作节点看到：

- DataNode

### 6.4 启动 YARN 服务

在主节点执行：

```bash
# 启动 YARN 守护进程
start-yarn.sh

# 验证进程
jps
```

预期在主节点看到：

- NameNode
- ResourceManager

预期在 Worker1 节点看到：

- DataNode
- NodeManager
- SecondaryNameNode

预期在其他工作节点看到：

- DataNode
- NodeManager

### 6.5 启动 MapReduce Job History Server

在主节点执行：

```bash
# 启动 Job History Server
mapred --daemon start historyserver

# 验证进程
jps
```

预期在主节点额外看到：

- JobHistoryServer

---

## 7. 验证集群安装

### 7.1 Web 界面访问

#### 7.1.1 本地访问

如果您在本地网络部署集群，可以直接访问以下地址：

- **HDFS Web UI**：<http://hadoop-master:9870/>
- **YARN Web UI**：<http://hadoop-master:8088/>
- **MapReduce Job History Server**：<http://hadoop-master:19888/>

#### 7.1.2 远程访问（SSH 隧道）

如果集群部署在云服务器上，建议使用 SSH 隧道：

```bash
# 建立 SSH 隧道
ssh -L 9870:hadoop-master:9870 -L 8088:hadoop-master:8088 -L 19888:hadoop-master:19888 hadoop@主节点公网IP

# 然后在本地浏览器访问：
# http://localhost:9870  (HDFS Web UI)
# http://localhost:8088  (YARN Web UI)
# http://localhost:19888 (Job History Server)
```

### 7.2 集群状态检查

```bash
# 检查 HDFS 状态报告（应显示 4 个 DataNode）
hdfs dfsadmin -report

# 检查 YARN 节点状态（应显示 4 个 NodeManager）
yarn node -list

# 检查集群健康状态和拓扑结构
hdfs dfsadmin -printTopology

# 验证所有节点的服务状态
echo "=== 主节点服务状态 ==="
jps

echo "=== 工作节点服务状态 ==="
for worker in hadoop-worker1 hadoop-worker2 hadoop-worker3 hadoop-worker4; do
    echo "--- $worker ---"
    ssh hadoop@$worker "jps | grep -E '(DataNode|NodeManager)'"
done

# 检查 HDFS 数据节点详细信息
hdfs dfsadmin -report | grep -A 5 "Live datanodes"

# 验证副本分布（应该在多个节点上有副本）
hdfs fsck / -files -blocks -locations | head -20
```

### 7.3 HDFS 基本操作测试

```bash
# 创建用户目录
hdfs dfs -mkdir -p /user/hadoop

# 创建测试目录
hdfs dfs -mkdir /user/hadoop/input
hdfs dfs -mkdir /test

# 上传测试文件
hdfs dfs -put $HADOOP_HOME/etc/hadoop/*.xml /user/hadoop/input

# 创建一个较大的测试文件以验证多节点分布
dd if=/dev/zero of=large_test.txt bs=1M count=200
echo "Hello Hadoop Cluster - Multi-Node Test" > small_test.txt

# 上传文件到 HDFS
hdfs dfs -put large_test.txt /test/
hdfs dfs -put small_test.txt /test/

# 查看上传的文件
hdfs dfs -ls /user/hadoop/input
hdfs dfs -ls -h /test/

# 查看文件内容
hdfs dfs -cat /user/hadoop/input/core-site.xml
hdfs dfs -cat /test/small_test.txt

# 检查文件的副本分布
hdfs fsck /user/hadoop/input -files -blocks -locations

# 查看文件的块分布情况（验证数据分布在多个节点）
echo "=== 大文件块分布 ==="
hdfs fsck /test/large_test.txt -files -blocks -locations

echo "=== 小文件块分布 ==="
hdfs fsck /test/small_test.txt -files -blocks -locations

# 验证副本数量和分布
hdfs dfs -stat "%r %b %n" /test/large_test.txt
hdfs dfs -stat "%r %b %n" /test/small_test.txt

# 测试文件的副本分布是否符合配置（默认副本数为 3）
echo "=== 验证副本分布 ==="
hdfs fsck /test/ -files -blocks -racks
```

### 7.4 运行 MapReduce 示例

```bash
# 清理之前的输出目录（如果存在）
hdfs dfs -rm -r /user/hadoop/output 2>/dev/null || true

# 运行 WordCount 示例程序
echo "=== 开始运行 MapReduce WordCount 任务 ==="
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.2.jar wordcount /user/hadoop/input /user/hadoop/output

# 查看输出结果
echo "=== WordCount 结果 ==="
hdfs dfs -cat /user/hadoop/output/part-r-00000 | head -20

# 验证任务在多个节点上的执行情况
echo "=== 验证任务分布 ==="
hdfs dfs -ls /user/hadoop/output/

# 运行更大的测试任务以验证多节点并行处理
echo "=== 运行 Pi 估算任务（验证多节点计算） ==="
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.2.jar pi 4 1000

# 运行 TeraGen 生成测试数据（可选，用于性能测试）
echo "=== 生成测试数据 TeraGen ==="
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.2.jar teragen 1000 /user/hadoop/teragen-output

# 验证生成的数据分布
hdfs fsck /user/hadoop/teragen-output -files -blocks -locations | head -10

# 清理测试数据
hdfs dfs -rm -r /user/hadoop/teragen-output

# 将结果下载到本地
hdfs dfs -get /user/hadoop/output output
cat output/part-r-00000
```

## 8. 集群管理命令

### 8.1 启动和停止服务

```bash
# 启动所有服务（在主节点执行）
start-all.sh

# 停止所有服务（在主节点执行）
stop-all.sh

# 单独启动/停止 HDFS
start-dfs.sh
stop-dfs.sh

# 单独启动/停止 YARN
start-yarn.sh
stop-yarn.sh

# 启动/停止 Job History Server
mapred --daemon start historyserver
mapred --daemon stop historyserver
```

### 8.2 HDFS 管理命令

```bash
# 查看 HDFS 状态
hdfs dfsadmin -report

# 检查文件系统
hdfs fsck /

# 查看集群拓扑
hdfs dfsadmin -printTopology

# 进入/退出安全模式
hdfs dfsadmin -safemode enter
hdfs dfsadmin -safemode leave

# 刷新节点列表
hdfs dfsadmin -refreshNodes

# 查看 HDFS 使用情况
hdfs dfs -df -h
```

### 8.3 YARN 管理命令

```bash
# 查看节点列表
yarn node -list

# 查看应用程序列表
yarn application -list

# 查看队列信息
yarn queue -status default

# 杀死应用程序
yarn application -kill <application_id>

# 刷新节点列表
yarn rmadmin -refreshNodes
```

### 8.4 节点管理

#### 8.4.1 添加新节点

```bash
# 1. 在新节点上完成基础环境配置
# 2. 将新节点主机名添加到 workers 文件
echo "hadoop-worker5" >> $HADOOP_HOME/etc/hadoop/workers

# 3. 分发配置文件到新节点
scp $HADOOP_HOME/etc/hadoop/* hadoop@hadoop-worker5:$HADOOP_HOME/etc/hadoop/

# 4. 在新节点启动服务
ssh hadoop@hadoop-worker5 "$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode"
ssh hadoop@hadoop-worker5 "$HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager"

# 5. 刷新节点列表
hdfs dfsadmin -refreshNodes
yarn rmadmin -refreshNodes
```

#### 8.4.2 移除节点

```bash
# 1. 创建排除文件
echo "hadoop-worker4" > $HADOOP_HOME/etc/hadoop/excludes

# 2. 配置 hdfs-site.xml 和 yarn-site.xml
# 添加以下属性：
# <property>
#   <name>dfs.hosts.exclude</name>
#   <value>/home/hadoop/hadoop/etc/hadoop/excludes</value>
# </property>

# 3. 刷新节点列表
hdfs dfsadmin -refreshNodes
yarn rmadmin -refreshNodes

# 4. 等待数据迁移完成后停止节点服务
ssh hadoop@hadoop-worker4 "$HADOOP_HOME/sbin/hadoop-daemon.sh stop datanode"
ssh hadoop@hadoop-worker4 "$HADOOP_HOME/sbin/yarn-daemon.sh stop nodemanager"
```

---

## 9. 性能优化

### 9.1 JVM 参数优化

在 `hadoop-env.sh` 中添加 JVM 优化参数：

```bash
# NameNode JVM 优化
export HDFS_NAMENODE_OPTS="-Xmx4g -Xms4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# DataNode JVM 优化
export HDFS_DATANODE_OPTS="-Xmx2g -Xms2g -XX:+UseG1GC"

# ResourceManager JVM 优化
export YARN_RESOURCEMANAGER_OPTS="-Xmx4g -Xms4g -XX:+UseG1GC"

# NodeManager JVM 优化
export YARN_NODEMANAGER_OPTS="-Xmx2g -Xms2g -XX:+UseG1GC"
```

### 9.2 网络优化

```bash
# 在所有节点优化网络参数
sudo sysctl -w net.core.rmem_default=262144
sudo sysctl -w net.core.rmem_max=16777216
sudo sysctl -w net.core.wmem_default=262144
sudo sysctl -w net.core.wmem_max=16777216

# 永久生效
echo "net.core.rmem_default=262144" | sudo tee -a /etc/sysctl.conf
echo "net.core.rmem_max=16777216" | sudo tee -a /etc/sysctl.conf
echo "net.core.wmem_default=262144" | sudo tee -a /etc/sysctl.conf
echo "net.core.wmem_max=16777216" | sudo tee -a /etc/sysctl.conf
```

### 9.3 磁盘 I/O 优化

```bash
# 设置磁盘调度器为 deadline（适合 SSD）
echo deadline | sudo tee /sys/block/sdb/queue/scheduler

# 或设置为 noop（适合 SSD）
echo noop | sudo tee /sys/block/sdb/queue/scheduler

# 永久设置（添加到 /etc/rc.local）
echo 'echo deadline > /sys/block/sdb/queue/scheduler' | sudo tee -a /etc/rc.local
```

---

## 10. 故障排除

### 10.1 常见问题及解决方案

#### 10.1.1 节点无法启动

**问题**：DataNode 或 NodeManager 无法启动

**排查步骤**：

```bash
# 检查日志文件
tail -f $HADOOP_HOME/logs/hadoop-hadoop-datanode-*.log
tail -f $HADOOP_HOME/logs/yarn-hadoop-nodemanager-*.log

# 检查网络连通性
ping hadoop-master
telnet hadoop-master 9000

# 检查 SSH 连接
ssh hadoop-master

# 检查磁盘空间
df -h /mnt/hadoop

# 检查进程状态
jps
ps aux | grep hadoop
```

**常见解决方案**：

```bash
# 重新格式化 DataNode（谨慎操作，会丢失数据）
rm -rf /mnt/hadoop/data/datanode/*
hdfs --daemon start datanode

# 重启节点服务
hdfs --daemon stop datanode
hdfs --daemon start datanode

yarn --daemon stop nodemanager
yarn --daemon start nodemanager
```

#### 10.1.2 NameNode 安全模式

**问题**：NameNode 处于安全模式，无法写入数据

```bash
# 检查安全模式状态
hdfs dfsadmin -safemode get

# 查看安全模式原因
hdfs dfsadmin -report

# 手动退出安全模式（确保集群健康后）
hdfs dfsadmin -safemode leave

# 等待自动退出安全模式
hdfs dfsadmin -safemode wait
```

#### 10.1.3 磁盘空间不足

**问题**：DataNode 磁盘空间不足

```bash
# 检查磁盘使用情况
df -h /mnt/hadoop
hdfs dfsadmin -report

# 清理临时文件
rm -rf /mnt/hadoop/tmp/*

# 删除不需要的 HDFS 文件
hdfs dfs -rm -r /tmp/*
hdfs dfs -expunge

# 增加磁盘空间或添加新磁盘
```

#### 10.1.4 网络连接问题

**问题**：节点间网络连接异常

```bash
# 检查防火墙状态
sudo ufw status

# 临时关闭防火墙（测试用）
sudo ufw disable

# 检查端口占用
netstat -tulpn | grep :9000
netstat -tulpn | grep :8088

# 检查 hosts 文件配置
cat /etc/hosts

# 测试端口连通性
telnet hadoop-master 9000
telnet hadoop-master 8088
```

#### 10.1.5 内存不足

**问题**：Java 堆内存溢出

```bash
# 检查当前内存使用
free -h
ps aux | grep java

# 调整 JVM 堆内存设置
export HADOOP_HEAPSIZE=2048
export HDFS_NAMENODE_OPTS="-Xmx4g -Xms4g"
export HDFS_DATANODE_OPTS="-Xmx2g -Xms2g"

# 重启相关服务
stop-all.sh
start-all.sh
```

### 10.2 日志分析

#### 10.2.1 重要日志文件位置

```bash
# Hadoop 守护进程日志
ls $HADOOP_HOME/logs/

# NameNode 日志
tail -f $HADOOP_HOME/logs/hadoop-hadoop-namenode-*.log

# DataNode 日志
tail -f $HADOOP_HOME/logs/hadoop-hadoop-datanode-*.log

# ResourceManager 日志
tail -f $HADOOP_HOME/logs/yarn-hadoop-resourcemanager-*.log

# NodeManager 日志
tail -f $HADOOP_HOME/logs/yarn-hadoop-nodemanager-*.log
```

#### 10.2.2 日志分析技巧

```bash
# 查找错误信息
grep -i error $HADOOP_HOME/logs/*.log

# 查找警告信息
grep -i warn $HADOOP_HOME/logs/*.log

# 查找特定时间段的日志
grep "2024-01-15 10:" $HADOOP_HOME/logs/hadoop-hadoop-namenode-*.log

# 统计错误数量
grep -c "ERROR" $HADOOP_HOME/logs/*.log

# 实时监控多个日志文件
tail -f $HADOOP_HOME/logs/hadoop-hadoop-namenode-*.log $HADOOP_HOME/logs/yarn-hadoop-resourcemanager-*.log
```

### 10.3 性能监控

#### 10.3.1 系统资源监控

```bash
# CPU 使用率
top
htop

# 内存使用情况
free -h
cat /proc/meminfo

# 磁盘 I/O 监控
iostat -x 1 5
iotop

# 网络监控
iftop
netstat -i
```

#### 10.3.2 Hadoop 特定监控

```bash
# HDFS 使用情况
hdfs dfs -df -h
hdfs dfsadmin -report

# YARN 资源使用
yarn top

# MapReduce 作业监控
mapred job -list
yarn application -list

# JVM 垃圾收集监控
jstat -gc <pid>
```

> **注意**：针对多用户教学环境的专门监控工具、故障排除方法和管理脚本，请参考 [多用户配置和管理](./multi-user-setup.md) 文档中的第 3 章"多用户环境监控和故障排除"。

---

## 11. 安全配置

### 11.1 基础安全措施

#### 11.1.1 防火墙配置

**注意**：请根据实际情况调整防火墙规则，开发测试环境可以不使用防火墙。

```bash
# 配置 UFW 防火墙规则
sudo ufw enable

# 允许 SSH
sudo ufw allow 22

# 允许 Hadoop 相关端口（仅在集群内网）
sudo ufw allow from 192.168.1.0/24 to any port 9000
sudo ufw allow from 192.168.1.0/24 to any port 8088
sudo ufw allow from 192.168.1.0/24 to any port 9870
sudo ufw allow from 192.168.1.0/24 to any port 19888

# 查看防火墙状态
sudo ufw status
```

#### 11.1.2 用户权限管理

**注意**：限制 hadoop 用户权限，避免系统安全风险。

```bash
# 限制 hadoop 用户权限
sudo usermod -s /bin/bash hadoop
sudo chown -R hadoop:hadoop /mnt/hadoop
sudo chmod 750 /mnt/hadoop

# 设置文件权限
chmod 600 ~/.ssh/id_rsa
chmod 644 ~/.ssh/id_rsa.pub
chmod 600 ~/.ssh/authorized_keys
```

### 11.2 生产环境安全建议

**重要说明**：以下安全措施适用于生产环境，当前测试环境可以跳过。

#### 11.2.1 Kerberos 认证

生产环境强烈建议启用 Kerberos 认证：

```bash
# 安装 Kerberos 客户端
sudo apt-get install krb5-user -y

# 配置 Kerberos（需要 KDC 服务器）
# 详细配置请参考 Hadoop 官方安全文档
```

#### 11.2.2 SSL/TLS 加密

```bash
# 生成 SSL 证书
keytool -genkey -alias hadoop -keyalg RSA -keystore hadoop.keystore

# 配置 SSL（在 core-site.xml 中添加）
# <property>
#   <name>hadoop.ssl.require.client.cert</name>
#   <value>false</value>
# </property>
```

#### 11.2.3 访问控制列表（ACL）

```bash
# 启用 HDFS ACL
# 在 hdfs-site.xml 中添加：
# <property>
#   <name>dfs.namenode.acls.enabled</name>
#   <value>true</value>
# </property>

# 设置文件 ACL
hdfs dfs -setfacl -m user:username:rwx /path/to/file
```

### 11.3 多用户环境配置

对于需要支持多个学生同时使用集群的教学环境，建议配置多用户访问控制。多用户配置包括：

- **用户隔离策略**：创建学生用户组和个人账户
- **HDFS 目录结构设计**：为每个学生创建独立的工作目录
- **资源分配和限制**：配置 YARN 队列和 HDFS 配额
- **权限和安全配置**：设置适当的访问权限和 ACL
- **客户端配置**：为学生配置远程访问环境

详细的多用户配置步骤和脚本使用方法，请参考：[多用户配置和管理](./multi-user-setup.md)

---

## 12. 备份和恢复

### 12.1 HDFS 数据备份

```bash
# 创建 HDFS 快照
hdfs dfsadmin -allowSnapshot /user/hadoop
hdfs dfs -createSnapshot /user/hadoop snapshot_$(date +%Y%m%d)

# 列出快照
hdfs dfs -ls /user/hadoop/.snapshot

# 恢复快照
hdfs dfs -cp /user/hadoop/.snapshot/snapshot_20240115/* /user/hadoop/restore/
```

### 12.2 元数据备份

```bash
# 备份 NameNode 元数据
hdfs dfsadmin -saveNamespace

# 备份配置文件
tar -czf hadoop-config-backup-$(date +%Y%m%d).tar.gz $HADOOP_HOME/etc/hadoop/

# 备份 NameNode 数据目录
tar -czf namenode-backup-$(date +%Y%m%d).tar.gz /mnt/hadoop/data/namenode/
```

### 12.3 自动化备份脚本

使用预配置的自动化备份脚本：

```bash
# 设置自动化备份
./cluster-setup-scripts/11-setup_backup.sh
```

> **执行角色**：管理员  
> **执行位置**：集群主节点  
> **脚本说明**：该脚本提供以下备份功能：

- 自动备份 Hadoop 配置文件
- 创建 HDFS 快照
- 定期清理旧备份文件
- 支持定时任务配置

---

## 13. 附录

### 13.1 端口列表

| 服务                         | 端口  | 描述                       |
| ---------------------------- | ----- | -------------------------- |
| NameNode                     | 9000  | HDFS 文件系统访问端口      |
| NameNode Web UI              | 9870  | NameNode Web 界面          |
| SecondaryNameNode Web UI     | 9868  | SecondaryNameNode Web 界面 |
| DataNode                     | 9866  | DataNode 数据传输端口      |
| DataNode Web UI              | 9864  | DataNode Web 界面          |
| ResourceManager              | 8032  | YARN 资源管理端口          |
| ResourceManager Web UI       | 8088  | ResourceManager Web 界面   |
| NodeManager Web UI           | 8042  | NodeManager Web 界面       |
| MapReduce Job History Server | 10020 | 作业历史服务端口           |
| Job History Server Web UI    | 19888 | 作业历史 Web 界面          |

### 13.2 配置文件模板

#### 13.2.1 完整的 core-site.xml 模板

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://hadoop-master:9000</value>
        <description>默认文件系统 URI</description>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/mnt/hadoop/tmp</value>
        <description>Hadoop 临时目录</description>
    </property>
    <property>
        <name>io.file.buffer.size</name>
        <value>131072</value>
        <description>文件缓冲区大小</description>
    </property>
    <property>
        <name>hadoop.proxyuser.hadoop.hosts</name>
        <value>*</value>
        <description>代理用户主机</description>
    </property>
    <property>
        <name>hadoop.proxyuser.hadoop.groups</name>
        <value>*</value>
        <description>代理用户组</description>
    </property>
</configuration>
```

### 13.3 常用命令速查

#### 13.3.1 集群管理命令

```bash
# 启动集群
start-all.sh

# 停止集群
stop-all.sh

# 查看集群状态
hdfs dfsadmin -report
yarn node -list

# 查看运行的应用
yarn application -list

# 查看 HDFS 使用情况
hdfs dfs -df -h

# 检查文件系统
hdfs fsck /
```

#### 13.3.2 HDFS 常用命令

```bash
# 创建目录
hdfs dfs -mkdir /path/to/directory

# 上传文件
hdfs dfs -put localfile /hdfs/path

# 下载文件
hdfs dfs -get /hdfs/path localfile

# 查看文件内容
hdfs dfs -cat /hdfs/path/file

# 删除文件
hdfs dfs -rm /hdfs/path/file

# 删除目录
hdfs dfs -rm -r /hdfs/path/directory

# 查看文件详细信息
hdfs dfs -ls -h /hdfs/path

# 修改文件权限
hdfs dfs -chmod 755 /hdfs/path/file

# 修改文件所有者
hdfs dfs -chown user:group /hdfs/path/file
```

### 13.4 性能调优参数参考

#### 13.4.1 JVM 参数建议

```bash
# 根据节点内存大小调整以下参数：

# 主节点（8GB 内存）
export HDFS_NAMENODE_OPTS="-Xmx4g -Xms4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
export YARN_RESOURCEMANAGER_OPTS="-Xmx4g -Xms4g -XX:+UseG1GC"

# 工作节点（4GB 内存）
export HDFS_DATANODE_OPTS="-Xmx1g -Xms1g -XX:+UseG1GC"
export YARN_NODEMANAGER_OPTS="-Xmx1g -Xms1g -XX:+UseG1GC"
```

#### 13.4.2 YARN 资源配置建议

```bash
# 根据节点硬件配置调整：

# 4GB 内存节点
yarn.nodemanager.resource.memory-mb=3072
yarn.nodemanager.resource.cpu-vcores=2
yarn.scheduler.maximum-allocation-mb=3072
yarn.scheduler.minimum-allocation-mb=512

# 8GB 内存节点
yarn.nodemanager.resource.memory-mb=6144
yarn.nodemanager.resource.cpu-vcores=4
yarn.scheduler.maximum-allocation-mb=6144
yarn.scheduler.minimum-allocation-mb=1024
```

---

## 参考文献

[1] Apache Software Foundation. Hadoop: Cluster Setup [EB/OL]. Apache Hadoop Documentation, 2024. <https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html>

[2] Apache Software Foundation. Apache Hadoop 3.4.2 Documentation [EB/OL]. 2024. <https://hadoop.apache.org/docs/stable/>

[3] Apache Software Foundation. HDFS Architecture Guide [EB/OL]. Apache Hadoop Documentation, 2024. <https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html>

[4] Apache Software Foundation. Apache Hadoop YARN [EB/OL]. Apache Hadoop Documentation, 2024. <https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html>

---
