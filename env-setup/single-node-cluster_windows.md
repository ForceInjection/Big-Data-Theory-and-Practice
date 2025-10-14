# Hadoop 单节点集群部署指南（Windows）

## 1 概述

本文档详细介绍如何在 **Windows 系统上的WSL环境**搭建 Hadoop 单节点集群，采用 Pseudo-Distributed Operation（伪分布式）模式。在此模式下，每个 Hadoop 守护进程运行在独立的 Java 进程中，模拟分布式环境，适用于开发和测试场景。

WSL中操作与Ubuntu基本一致，主要区别在5.3。Ubuntu环境部署可参考[Big-Data-Theory-and-Practice/env-setup/single-node-cluster.md at main · ForceInjection/Big-Data-Theory-and-Practice · GitHub](https://github.com/ForceInjection/Big-Data-Theory-and-Practice/blob/main/env-setup/single-node-cluster.md)。

## 2 系统要求

### 2.1 支持的平台

- Windows 10 版本 2004 及更高版本（内部版本 19041 及更高版本）或 Windows 11 

### 2.2 必需软件

#### 2.2.1 **Windows终端**

- [Windows Terminal - Windows官方下载 \| 微软应用商店 \| Microsoft Store](https://apps.microsoft.com/detail/9n0dx20hk701?hl=zh-CN&gl=CN)。

#### 2.2.2 **WSL**

##### WSL安装

- 在Windows Terminal中以**管理员模式**打开Powershell

````powershell

# 需要Windows 10 版本 2004 及更高版本（内部版本 19041 及更高版本）或 Windows 11 
wsl --set-default-version 2 # 在安装新的 Linux 分发版时将默认版本设置为WSL 2
wsl --list --online  # 查看可安装的发行版
wsl --install # 安装默认的ubuntu发行版

# 首次安装时设置用户名，设置密码后进入wsl环境


# 离开WSL
exit

````

````powershell
# 检查正在运行的WSL版本
wsl --list --verbose

#  NAME                      STATE           VERSION
#* Ubuntu                    Stopped         2
#  podman-machine-default    Stopped         2

# 进入WSL环境
wsl -d Ubuntu

# 指定用户进入WSL
# e.g使用hadoop用户进入ubuntu环境
wsl -d Ubuntu -u hadoop
````



##### WSL安装位置迁移（可选）

- WSL默认安装在C盘，有需要可以迁移。



```powershell
# 找到需要迁移的WSL NAME
wsl --list --verbose

#  NAME                      STATE           VERSION
#* Ubuntu                    Running         2
#  Ubuntu-24.04              Stopped         2
#  podman-machine-default    Stopped         2

# 导出备份文件
wsl --export Ubuntu-24.04  D:\env\ubuntu2404\Ubuntu2404.tar # 把Ubuntu WSL导出到D:\env\WSL目录

# 注销原本的WSL
wsl --unregister Ubuntu-24.04

#恢复备份文件
# wsl --import <NAME>  <虚拟磁盘路径> <上一步导出的tar路由>
wsl --import Ubuntu2404 D:\env\ubuntu2404 D:\env\ubuntu2404\Ubuntu2404.tar
# 查看是否导入成功
wsl -l -v
#  NAME                      STATE           VERSION
#* Ubuntu                    Stopped         2
#  podman-machine-default    Stopped         2
#  Ubuntu2404                Stopped         2

```



## 3 环境准备

在WSL环境中执行以下操作：

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

# 配置 PDSH 使用 SSH（重要：避免启动 Hadoop 服务时出错）
echo 'export PDSH_RCMD_TYPE=ssh' >> ~/.bashrc
source ~/.bashrc

```

## 4 Hadoop安装

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

## 5 Hadoop 配置

### 5.1  配置 Hadoop 环境

编辑 `$HADOOP_HOME/etc/hadoop/hadoop-env.sh` 文件：

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
</configuration>
```

**重要提示**：

- 配置文件中只能有一个 `<configuration>` 标签
- 请根据实际的 Hadoop 安装路径调整 `HADOOP_MAPRED_HOME` 的值
- 必须使用绝对路径

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

- 安装XFS工具集

```bash
%% 安装XFS 文件系统的工具集 %%
sudo apt install xfsprogs -y
```

- 查看挂载的磁盘

```bash
lsblk

# NAME  MAJ:MIN RM   SIZE RO TYPE MOUNTPOINTS
# sda     8:0    0 388.4M  1 disk
# sdb     8:16   0   186M  1 disk
# sdc     8:32   0     4G  0 disk [SWAP]
# sdd     8:48   0     1T  0 disk /mnt/wslg/distro
                                /
# 发现无可用磁盘，sda、sdb为只读设备，其他磁盘都已经被挂载
```

- 如果没有可用的独立磁盘，则在WSL 内创建loop device，并挂载到/mnt/hadoop

```PowerShell
# WSL 内创建 5GB 文件
fallocate -l 5G ~/hadoop_disk.img

# 挂载到 loop 设备
sudo losetup -fP ~/hadoop_disk.img

# 查看 loop 设备
lsblk

# 格式化 XFS
sudo mkfs.xfs /dev/loop0

# 确保磁盘已挂载到 /mnt/hadoop（使用 xfs 优化选项）
sudo mkdir -p /mnt/hadoop
sudo mount -t xfs -o noatime,nodiratime,logbufs=8,logbsize=32k /dev/loop0 /mnt/hadoop

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

## 6 SSH 无密码登录配置

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

## 7 启动 Hadoop 集群

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

## 8 验证安装

### 8.1 Web 界面访问

#### 8.1.1 本地访问

如果您在本地机器上部署 Hadoop，可以直接访问以下地址：

- **HDFS Web UI**：[http://localhost:9870/](http://localhost:9870/)
- **YARN Web UI**：[http://localhost:8088/](http://localhost:8088/)

#### 8.1.2 Web UI 功能说明

- **HDFS Web UI**：查看 HDFS 状态、文件系统信息、DataNode 状态等
- **YARN Web UI**：查看 YARN 集群状态、应用程序信息、资源使用情况等

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

**重要提醒**：如果您在配置 `mapred-site.xml` 时添加了 `HADOOP_MAPRED_HOME` 环境变量，需要重启 Hadoop 服务：

```bash
# 停止所有服务
stop-all.sh

# 启动所有服务
start-all.sh

# 验证服务状态
jps
```

然后运行 MapReduce 示例：

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

#### 10.1.1 PDSH 相关问题

**问题**：启动 HDFS 服务时出现 `pdsh` 相关错误

```bash
# 错误信息示例：
# pdsh@hostname: localhost: rcmd: socket: Permission denied
```

**解决方案**：设置 PDSH 使用 SSH

```bash
# 设置环境变量
export PDSH_RCMD_TYPE=ssh

# 永久设置（添加到 ~/.bashrc）
echo 'export PDSH_RCMD_TYPE=ssh' >> ~/.bashrc
source ~/.bashrc

# 重新启动 Hadoop 服务
stop-dfs.sh
start-dfs.sh
```

#### 10.1.2 配置文件格式问题

**问题**：配置文件格式错误导致服务启动失败

**常见错误**：

- 配置文件中有多个 `<configuration>` 标签
- XML 格式不正确

**解决方案**：

```bash
# 检查配置文件格式
xmllint --format $HADOOP_HOME/etc/hadoop/core-site.xml
xmllint --format $HADOOP_HOME/etc/hadoop/hdfs-site.xml
xmllint --format $HADOOP_HOME/etc/hadoop/mapred-site.xml
xmllint --format $HADOOP_HOME/etc/hadoop/yarn-site.xml

# 确保每个配置文件只有一个 <configuration> 标签
```

#### 10.1.3 MapReduce 环境变量问题

**问题**：运行 MapReduce 任务时出现 `HADOOP_MAPRED_HOME` 未设置的错误

**解决方案**：在 `mapred-site.xml` 中添加环境变量配置（参见 5.2.3 节），然后重启服务：

```bash
# 停止所有服务
stop-all.sh

# 启动所有服务
start-all.sh
```

#### 10.1.4 Java 相关问题

**问题**：`JAVA_HOME` 未设置或设置错误

```bash
# 解决方案：检查并重新设置 JAVA_HOME
echo $JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
```

#### 10.1.5 SSH 连接问题

**问题**：无法无密码连接到 localhost

```bash
# 解决方案：重新配置 SSH 密钥
rm -rf ~/.ssh
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
```

#### 10.1.6 端口冲突问题

**问题**：默认端口被占用

```bash
# 检查端口占用情况
netstat -tulpn | grep :9000
netstat -tulpn | grep :9870

# 解决方案：修改配置文件中的端口号
# 在 core-site.xml 中修改 fs.defaultFS 的端口
# 在 hdfs-site.xml 中添加 dfs.namenode.http-address 配置
```

#### 10.1.7 权限问题

**问题**：Hadoop 目录权限不足

```bash
# 解决方案：重新设置目录权限
sudo chown -R $USER:$USER /mnt/hadoop
chmod -R 755 /mnt/hadoop/data
sudo chown -R hadoop:hadoop $HADOOP_HOME
```

#### 10.1.8 磁盘挂载问题

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

### 10.2 XFS 和 OpenJDK 8 特定问题

#### 10.2.1 XFS 文件系统问题

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

#### 10.2.2 OpenJDK 8 特定问题

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

#### 10.2.3 XFS 和 OpenJDK 8 兼容性优化

```bash
# 针对 XFS 的 Java I/O 优化
export HADOOP_OPTS="$HADOOP_OPTS -Djava.io.tmpdir=/mnt/hadoop/tmp"
export HADOOP_OPTS="$HADOOP_OPTS -Dsun.nio.ch.bugLevel=false"

# 检查 XFS 和 Java 的 I/O 性能
iostat -x 1 5  # 监控磁盘 I/O
jstat -gc <hadoop_pid>  # 监控 Java GC
```

### 10.3 日志管理

#### 10.3.1 默认日志位置

```bash
# Hadoop 默认日志目录
ls $HADOOP_HOME/logs/

# 查看 NameNode 日志
tail -f $HADOOP_HOME/logs/hadoop-hadoop-namenode-*.log

# 查看 DataNode 日志
tail -f $HADOOP_HOME/logs/hadoop-hadoop-datanode-*.log

# 查看 ResourceManager 日志
tail -f $HADOOP_HOME/logs/yarn-hadoop-resourcemanager-*.log
```

#### 10.3.2 自定义日志目录配置

建议将 Hadoop 日志配置到独立目录，便于监控和分析：

```bash
# 创建日志目录
sudo mkdir -p /mnt/hadoop/logs
sudo chown -R hadoop:hadoop /mnt/hadoop/logs

# 在 hadoop-env.sh 中配置日志目录
export HADOOP_LOG_DIR=/mnt/hadoop/logs
```

#### 10.3.3 日志轮转配置

配置日志轮转以防止日志文件过大：

```bash
# 编辑 log4j.properties
vim $HADOOP_HOME/etc/hadoop/log4j.properties

# 添加或修改以下配置
log4j.appender.RFA=org.apache.log4j.RollingFileAppender
log4j.appender.RFA.File=${hadoop.log.dir}/${hadoop.log.file}
log4j.appender.RFA.MaxFileSize=100MB
log4j.appender.RFA.MaxBackupIndex=10
```

#### 10.3.4 日志分析工具

```bash
# 查看错误日志
grep -i error $HADOOP_LOG_DIR/*.log

# 查看警告日志
grep -i warn $HADOOP_LOG_DIR/*.log

# 实时监控日志
tail -f $HADOOP_LOG_DIR/hadoop-hadoop-namenode-*.log

# 日志统计分析
awk '/ERROR/ {print $0}' $HADOOP_LOG_DIR/*.log | wc -l
```

---

## 11. 阿里云部署特定注意事项（包括公有云）

### 11.1 网络配置

#### 11.1.1 SSH 隧道访问 Web UI

阿里云 ECS 实例默认只开放 22 端口（SSH），Hadoop Web UI 端口（9870、8088）无法直接从外网访问。推荐使用 SSH 隧道：

```bash
# 建立 SSH 隧道（在本地机器执行）
ssh -L 9870:localhost:9870 -L 8088:localhost:8088 hadoop@你的ECS公网IP

# 然后在本地浏览器访问：
# http://localhost:9870  (HDFS Web UI)
# http://localhost:8088  (YARN Web UI)
```

#### 11.1.2 安全组配置

如果需要直接访问 Web UI（不推荐），可以在阿里云控制台配置安全组：

1. 登录阿里云控制台
2. 进入 ECS 实例管理
3. 点击"安全组" → "配置规则"
4. 添加入方向规则：
   - 端口范围：9870/9870（HDFS Web UI）
   - 端口范围：8088/8088（YARN Web UI）
   - 授权对象：0.0.0.0/0（或限制为特定 IP）

**安全警告**：直接开放端口存在安全风险，建议仅在测试环境使用，生产环境应使用 SSH 隧道或 VPN。

### 11.2 存储配置

#### 11.2.1 数据盘挂载

阿里云 ECS 建议使用独立的数据盘存储 Hadoop 数据。具体的磁盘挂载步骤请参考第 5.3.1 节，注意以下阿里云特定事项：

- **设备名称**：阿里云数据盘通常为 `/dev/vdb`、`/dev/vdc` 等
- **性能优化**：建议选择 SSD 云盘或 ESSD 云盘以获得更好的 I/O 性能
- **容量规划**：根据数据量选择合适的磁盘容量，支持在线扩容

#### 11.2.2 云盘性能优化

- **推荐使用 SSD 云盘**：提供更好的 IOPS 性能
- **选择合适的云盘类型**：
  - 开发测试：高效云盘
  - 生产环境：SSD 云盘或 ESSD 云盘
- **合理规划容量**：考虑数据增长和备份需求

### 11.3 网络优化

#### 11.3.1 内网通信

如果部署多节点集群，建议：

- 使用阿里云内网 IP 进行节点间通信
- 配置专有网络（VPC）提高安全性
- 使用负载均衡器分发外部访问流量

#### 11.3.2 带宽配置

- **公网带宽**：根据数据传输需求选择合适带宽
- **内网带宽**：阿里云内网带宽通常足够，无需特殊配置

---

## 12. 安全注意事项

### 12.1 生产环境部署建议

**重要说明**：本文档介绍的是开发/测试环境的单节点部署，以下安全措施仅供生产环境参考，不在当前配置中实施。

#### 12.1.1 Kerberos 认证（生产环境推荐）

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

#### 12.1.2 其他生产环境安全措施

- **网络安全**：配置防火墙规则，限制端口访问
- **数据加密**：启用传输层和存储层加密
- **访问控制**：配置 HDFS 权限和 ACL
- **审计日志**：启用操作审计和监控
- **定期更新**：及时更新 Hadoop 版本和安全补丁

### 12.2 开发测试环境安全配置

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

## 13. 总结

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

[3] WSL安装位置迁移。[ 轻松搬迁！教你如何将WSL从C盘迁移到其他盘区，释放存储空间！ - 知乎](https://zhuanlan.zhihu.com/p/621873601)
