# Hadoop 单节点集群部署指南(Ubuntu)

## 1. 概述

本文档详细介绍如何在 Ubuntu 系统上搭建 Hadoop 单节点集群，采用 Pseudo-Distributed Operation（伪分布式）模式。在此模式下，每个 Hadoop 守护进程运行在独立的 Java 进程中，模拟分布式环境，适用于开发和测试场景。[1]

## 2. 系统要求

### 2.1 支持的平台

- **操作系统**：GNU/Linux（推荐 Ubuntu 22.04 LTS 或更高版本）
- **架构**：x86_64

### 2.2 硬件配置要求

#### 2.2.1 最低配置

- **CPU**：2 核心
- **内存**：4 GB RAM
- **存储**：20 GB 可用磁盘空间
- **网络**：稳定的网络连接

#### 2.2.2 推荐配置

- **CPU**：4 核心或更多
- **内存**：8 GB RAM 或更多
- **存储**：50 GB 可用磁盘空间（SSD 优先）
- **网络**：千兆网络连接

#### 2.2.3 开发测试环境磁盘配置

- **独立磁盘**：推荐使用独立磁盘挂载到 `/mnt/hadoop` 目录
- **磁盘容量**：至少 20 GB，推荐 50 GB 或更多
- **文件系统**：xfs
- **挂载权限**：确保 Hadoop 用户具有读写权限
- **优势**：避免 Hadoop 数据填满系统根分区，提高 I/O 性能

### 2.3 必需软件

#### 2.3.1 Java 环境

- **Java 版本**：推荐使用 OpenJDK 8 [1]
- **环境变量**：需要正确配置 `JAVA_HOME`

#### 2.3.2 SSH 服务

- **ssh**：用于远程连接和管理 Hadoop 守护进程
- **sshd**：SSH 守护进程必须运行
- **pdsh**：（可选）用于更好的 SSH 资源管理 [2]

## 3. 环境准备

### 3.1 更新系统包

```bash
# 更新包列表
sudo apt update

# 升级系统包
sudo apt upgrade -y
```

### 3.2 安装 Java

```bash
# 安装 OpenJDK 8
sudo apt install openjdk-8-jdk -y

# 验证 Java 安装
java -version
javac -version
```

### 3.3 配置 Java 环境变量

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

### 3.4 安装 SSH 服务

```bash
# 安装 SSH 和 pdsh
sudo apt-get install ssh -y
sudo apt-get install pdsh -y

# 启动 SSH 服务
sudo systemctl start ssh
sudo systemctl enable ssh

# 验证 SSH 服务状态
sudo systemctl status ssh
```

## 4. Hadoop 安装

### 4.1 下载 Hadoop

```bash
# 创建 Hadoop 用户（推荐）
sudo adduser hadoop
sudo usermod -aG sudo hadoop

# 切换到 hadoop 用户
su - hadoop

# 确保在 hadoop 用户主目录进行下载
# 注意：Hadoop 程序文件建议安装在用户主目录，数据文件将存储在独立磁盘
cd /home/hadoop

# 下载 Hadoop 3.4.2（或最新稳定版本）
# 当前位置：/home/hadoop/ 目录
wget https://downloads.apache.org/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz

# 可以使用阿里云镜像
wget https://mirrors.aliyun.com/apache/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz

# 解压 Hadoop
tar -xzf hadoop-3.4.2.tar.gz

# 重命名目录为 hadoop（最终路径：/home/hadoop/hadoop）
mv hadoop-3.4.2 hadoop

# 设置 Hadoop 目录权限
sudo chown -R hadoop:hadoop ~/hadoop

# 清理下载的压缩包（可选）
rm hadoop-3.4.2.tar.gz
```

### 4.2 配置环境变量

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

## 5. Hadoop 配置

### 5.1 配置 Hadoop 环境

编辑 `$HADOOP_HOME/etc/hadoop/hadoop-env.sh` 文件：

```bash
# 编辑 hadoop-env.sh
nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# 添加或修改以下行
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
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
        <value>hdfs://localhost:9000</value>
        <description>默认文件系统 URI</description>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/mnt/hadoop/tmp</value>
        <description>Hadoop 临时目录（使用独立磁盘避免填满根分区）</description>
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
        <value>1</value>
        <description>数据块副本数量（单节点设置为1）</description>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/mnt/hadoop/data/namenode</value>
        <description>NameNode 数据存储目录（使用独立磁盘）</description>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/mnt/hadoop/data/datanode</value>
        <description>DataNode 数据存储目录（使用独立磁盘）</description>
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
        <value>localhost</value>
        <description>ResourceManager 主机名</description>
    </property>
</configuration>
```

### 5.3 准备磁盘和创建必要目录

#### 5.3.1 挂载独立磁盘（开发测试环境）

```bash
# 如果磁盘尚未格式化为 xfs，先进行格式化
# sudo mkfs.xfs -f /dev/sdb1  # 根据实际设备名调整，-f 强制格式化

# 确保磁盘已挂载到 /mnt/hadoop（使用 xfs 优化选项）
# sudo mount -t xfs -o noatime,nodiratime,logbufs=8,logbsize=32k /dev/sdb1 /mnt/hadoop

# 验证挂载状态和文件系统类型
df -h /mnt/hadoop
mount | grep /mnt/hadoop

# 设置挂载点权限
sudo chown -R $USER:$USER /mnt/hadoop
sudo chmod 755 /mnt/hadoop
```

#### 5.3.2 创建 Hadoop 数据目录

```bash
# 创建 Hadoop 数据目录（使用独立磁盘）
mkdir -p /mnt/hadoop/data/namenode
mkdir -p /mnt/hadoop/data/datanode
mkdir -p /mnt/hadoop/tmp

# 设置目录权限
chmod 755 /mnt/hadoop/data/namenode
chmod 755 /mnt/hadoop/data/datanode
chmod 755 /mnt/hadoop/tmp
```

## 6. SSH 无密码登录配置

### 6.1 生成 SSH 密钥

```bash
# 生成 SSH 密钥对
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa

# 将公钥添加到授权文件
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

# 设置正确的权限
chmod 0600 ~/.ssh/authorized_keys
chmod 700 ~/.ssh
```

### 6.2 测试 SSH 连接

```bash
# 测试无密码 SSH 连接
ssh localhost

# 如果成功，应该能够无密码登录
# 退出 SSH 会话
exit
```

## 7. 启动 Hadoop 集群

### 7.1 格式化 HDFS

```bash
# 验证数据目录是否存在
ls -la /mnt/hadoop/data/

# 格式化 NameNode（仅在首次启动时执行）
hdfs namenode -format

# 确认格式化操作，输入 'Y' 并按回车
# 格式化成功后会在 /mnt/hadoop/data/namenode 目录下创建相关文件
```

### 7.2 启动 HDFS 服务

```bash
# 启动 HDFS 守护进程
start-dfs.sh

# 验证进程是否启动
jps
```

预期输出应包含：

- NameNode
- DataNode
- SecondaryNameNode

### 7.3 启动 YARN 服务

```bash
# 启动 YARN 守护进程
start-yarn.sh

# 再次验证进程
jps
```

预期输出应包含：

- NameNode
- DataNode
- SecondaryNameNode
- ResourceManager
- NodeManager

## 8. 验证安装

### 8.1 Web 界面访问

#### 8.1.1 HDFS Web UI

- **URL**：<http://localhost:9870/>
- **功能**：查看 HDFS 状态、文件系统信息、DataNode 状态等

#### 8.1.2 YARN Web UI

- **URL**：<http://localhost:8088/>
- **功能**：查看 YARN 集群状态、应用程序信息、资源使用情况等

### 8.2 系统状态检查

```bash
# 检查磁盘使用情况
df -h /mnt/hadoop

# 检查 Hadoop 数据目录
ls -la /mnt/hadoop/data/

# 查看 HDFS 状态报告
hdfs dfsadmin -report
```

### 8.3 HDFS 基本操作测试

```bash
# 创建用户目录
hdfs dfs -mkdir -p /user/hadoop

# 创建测试目录
hdfs dfs -mkdir /user/hadoop/input

# 上传测试文件
hdfs dfs -put $HADOOP_HOME/etc/hadoop/*.xml /user/hadoop/input

# 查看上传的文件
hdfs dfs -ls /user/hadoop/input

# 查看文件内容
hdfs dfs -cat /user/hadoop/input/core-site.xml
```

### 8.4 运行 MapReduce 示例

```bash
# 运行 grep 示例程序
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.2.jar grep /user/hadoop/input /user/hadoop/output 'dfs[a-z.]+'

# 查看输出结果
hdfs dfs -cat /user/hadoop/output/*

# 将结果下载到本地
hdfs dfs -get /user/hadoop/output output
cat output/*
```

## 9. 常用管理命令

### 9.1 启动和停止服务

```bash
# 启动所有服务
start-all.sh

# 停止所有服务
stop-all.sh

# 单独启动/停止 HDFS
start-dfs.sh
stop-dfs.sh

# 单独启动/停止 YARN
start-yarn.sh
stop-yarn.sh
```

### 9.2 HDFS 管理命令

```bash
# 查看 HDFS 状态
hdfs dfsadmin -report

# 检查文件系统
hdfs fsck /

# 进入安全模式
hdfs dfsadmin -safemode enter

# 退出安全模式
hdfs dfsadmin -safemode leave

# 查看 HDFS 使用情况
hdfs dfs -df -h
```

## 10. 故障排除

### 10.1 常见问题及解决方案

#### 10.1.1 Java 相关问题

**问题**：`JAVA_HOME` 未设置或设置错误

```bash
# 解决方案：检查并重新设置 JAVA_HOME
echo $JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
```

#### 10.1.2 SSH 连接问题

**问题**：无法无密码连接到 localhost

```bash
# 解决方案：重新配置 SSH 密钥
rm -rf ~/.ssh
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
```

#### 10.1.3 端口冲突问题

**问题**：默认端口被占用

```bash
# 检查端口占用情况
netstat -tulpn | grep :9000
netstat -tulpn | grep :9870

# 解决方案：修改配置文件中的端口号
# 在 core-site.xml 中修改 fs.defaultFS 的端口
# 在 hdfs-site.xml 中添加 dfs.namenode.http-address 配置
```

#### 10.1.4 权限问题

**问题**：Hadoop 目录权限不足

```bash
# 解决方案：重新设置目录权限
sudo chown -R $USER:$USER /mnt/hadoop
chmod -R 755 /mnt/hadoop/data
sudo chown -R hadoop:hadoop $HADOOP_HOME
```

#### 10.1.5 磁盘挂载问题

**问题**：`/mnt/hadoop` 目录不存在或磁盘未挂载

```bash
# 检查挂载状态
df -h | grep /mnt/hadoop
mount | grep /mnt/hadoop

# 检查磁盘分区
lsblk
fdisk -l

# 解决方案：重新挂载磁盘
sudo mkdir -p /mnt/hadoop
sudo mount /dev/sdb1 /mnt/hadoop  # 根据实际设备名调整
sudo chown -R $USER:$USER /mnt/hadoop

# 设置开机自动挂载（可选，使用 xfs 优化选项）
echo '/dev/sdb1 /mnt/hadoop xfs noatime,nodiratime,logbufs=8,logbsize=32k 0 2' | sudo tee -a /etc/fstab
```

**问题**：磁盘空间不足

```bash
# 检查磁盘使用情况
df -h /mnt/hadoop
du -sh /mnt/hadoop/*

# 清理不必要的文件
hdfs dfsadmin -report
hdfs dfs -rm -r /tmp/*  # 清理 HDFS 临时文件
```

### 10.2 日志文件位置

```bash
# Hadoop 日志目录
ls $HADOOP_HOME/logs/

# 查看 NameNode 日志
tail -f $HADOOP_HOME/logs/hadoop-hadoop-namenode-*.log

# 查看 DataNode 日志
tail -f $HADOOP_HOME/logs/hadoop-hadoop-datanode-*.log

# 查看 ResourceManager 日志
tail -f $HADOOP_HOME/logs/yarn-hadoop-resourcemanager-*.log
```

### 10.3 XFS 和 OpenJDK 8 特定问题

#### 10.3.1 XFS 文件系统问题

**问题**：XFS 文件系统损坏或性能问题

```bash
# 检查 XFS 文件系统状态
xfs_info /mnt/hadoop

# 检查 XFS 文件系统错误
sudo xfs_check /dev/sdb1  # 只读检查

# 修复 XFS 文件系统（需要先卸载）
sudo umount /mnt/hadoop
sudo xfs_repair /dev/sdb1
sudo mount -t xfs -o noatime,nodiratime,logbufs=8,logbsize=32k /dev/sdb1 /mnt/hadoop
```

**问题**：XFS 挂载选项未生效

```bash
# 验证当前挂载选项
mount | grep /mnt/hadoop

# 重新挂载使用优化选项
sudo umount /mnt/hadoop
sudo mount -t xfs -o noatime,nodiratime,logbufs=8,logbsize=32k /dev/sdb1 /mnt/hadoop
```

#### 10.3.2 OpenJDK 8 特定问题

**问题**：OpenJDK 8 内存溢出

```bash
# 检查当前 JVM 设置
ps aux | grep java | grep hadoop

# 调整 JVM 堆内存设置
export HADOOP_HEAPSIZE=2048
export HDFS_NAMENODE_OPTS="-Xmx2g -Xms2g -XX:+UseG1GC"
export HDFS_DATANODE_OPTS="-Xmx1g -Xms1g -XX:+UseG1GC"
```

**问题**：OpenJDK 8 垃圾收集性能问题

```bash
# 启用 G1 垃圾收集器（推荐用于 Hadoop）
export HADOOP_OPTS="$HADOOP_OPTS -XX:+UseG1GC"
export HADOOP_OPTS="$HADOOP_OPTS -XX:MaxGCPauseMillis=200"
export HADOOP_OPTS="$HADOOP_OPTS -XX:+PrintGC -XX:+PrintGCDetails"
```

#### 10.3.3 XFS 和 OpenJDK 8 兼容性优化

```bash
# 针对 XFS 的 Java I/O 优化
export HADOOP_OPTS="$HADOOP_OPTS -Djava.io.tmpdir=/mnt/hadoop/tmp"
export HADOOP_OPTS="$HADOOP_OPTS -Dsun.nio.ch.bugLevel=false"

# 检查 XFS 和 Java 的 I/O 性能
iostat -x 1 5  # 监控磁盘 I/O
jstat -gc <hadoop_pid>  # 监控 Java GC
```

## 11. 安全注意事项

### 11.1 生产环境部署建议

**重要说明**：本文档介绍的是开发/测试环境的单节点部署，以下安全措施仅供生产环境参考，不在当前配置中实施。

#### 11.1.1 Kerberos 认证（生产环境推荐）

**Kerberos 简介**：

- Kerberos 是一种网络认证协议，提供强身份认证机制
- 在 Hadoop 生产环境中，Kerberos 是事实上的安全标准
- 通过票据（Ticket）机制确保用户和服务的身份验证

**主要优势**：

- 防止未授权访问 Hadoop 集群资源
- 提供细粒度的权限控制
- 支持单点登录（SSO）
- 与企业现有认证系统集成

**注意**：Kerberos 配置复杂，需要专门的 KDC（Key Distribution Center）服务器，不适合开发测试环境。

#### 11.1.2 其他生产环境安全措施

- **网络安全**：配置防火墙规则，限制端口访问
- **数据加密**：启用传输层和存储层加密
- **访问控制**：配置 HDFS 权限和 ACL
- **审计日志**：启用操作审计和监控
- **定期更新**：及时更新 Hadoop 版本和安全补丁

### 11.2 开发测试环境安全配置

```bash
# 修改默认端口（可选，增强安全性）
# 在相应配置文件中修改以下端口：
# - NameNode Web UI: 9870 -> 自定义端口
# - ResourceManager Web UI: 8088 -> 自定义端口
# - HDFS RPC: 9000 -> 自定义端口

# 设置基本文件权限
chmod 755 $HADOOP_HOME/etc/hadoop/*.xml
chown -R $USER:$USER $HADOOP_HOME

# 限制 Web UI 访问（可选）
# 在 core-site.xml 中添加：
# <property>
#   <name>hadoop.http.filter.initializers</name>
#   <value>org.apache.hadoop.http.lib.StaticUserWebFilter</value>
# </property>
```

## 12. 总结

本文档详细介绍了在 Ubuntu 系统上部署 Hadoop 单节点集群的完整过程，包括：

1. **系统环境准备**：Java 安装、SSH 配置、用户创建
2. **Hadoop 安装配置**：下载、解压、环境变量设置
3. **核心组件配置**：core-site.xml、hdfs-site.xml、mapred-site.xml、yarn-site.xml
4. **集群启动验证**：HDFS 格式化、服务启动、功能测试
5. **故障排除指南**：常见问题解决方案和日志分析

通过本指南，您可以成功搭建一个功能完整的 Hadoop 单节点集群，为后续的大数据开发和测试工作奠定基础。

---

## 参考文献

[1] Apache Software Foundation. Hadoop: Setting up a Single Node Cluster [EB/OL]. Apache Hadoop Documentation, 2024. <https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html/>

[2] Apache Software Foundation. Apache Hadoop 3.4.2 Documentation [EB/OL]. 2024. <https://hadoop.apache.org/docs/stable/>
