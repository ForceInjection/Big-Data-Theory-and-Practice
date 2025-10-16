# 多用户配置和管理

## 1 远程客户端访问方案概述

本文档采用**远程客户端访问**方案，学生在自己的开发机器上配置 Hadoop 客户端，通过网络远程访问集群，而不是直接登录到集群节点。

**选择远程访问方案的优势**：

1. **集群安全性**：

   - 避免学生直接登录集群节点，减少误操作风险
   - 防止学生修改集群系统配置或安装不当软件
   - 降低集群节点被恶意攻击的风险

2. **环境稳定性**：

   - 集群环境不会因学生的个人配置而受到影响
   - 避免多用户同时登录导致的资源竞争
   - 减少因用户环境配置错误导致的集群故障

3. **管理便利性**：

   - 管理员只需维护集群核心服务，无需管理大量用户登录环境
   - 简化用户权限管理和审计
   - 便于集群资源监控和故障排查

4. **学习体验**：
   - 模拟真实企业环境中的 Hadoop 使用场景
   - 学生学会配置和使用 Hadoop 客户端
   - 培养分布式系统的正确使用习惯

**与直接登录方案的对比**：

| **特性**       | **远程客户端访问** | **直接登录集群** |
| -------------- | ------------------ | ---------------- |
| **安全性**     | 高                 | 低               |
| **环境稳定性** | 高                 | 低               |
| **管理复杂度** | 低                 | 高               |
| **学习真实性** | 高                 | 中               |
| **配置复杂度** | 中                 | 低               |

---

## 2 多用户访问方案设计

### 2.1 用户隔离策略

为支持多个学生同时使用集群完成 MapReduce 作业，采用**远程客户端访问**的用户隔离方案，避免学生直接登录集群节点：

#### 2.1.1 集群端用户账户管理

```bash
# 仅在集群节点上创建学生用户组（用于 HDFS 权限管理）
sudo groupadd students

# 在集群上创建学生用户账户（仅用于 HDFS 和 YARN 身份识别，不提供登录权限）
# 示例：为学号 2024001 的学生创建用户
sudo useradd -M -g students -s /sbin/nologin 2024001

# 批量创建用户脚本（修改版，禁用登录）
# 使用预配置的脚本批量创建学生账户
./cluster-setup-scripts/01-create_students.sh students.txt

```

> **执行角色**：管理员  
> **执行位置**：所有集群节点（主节点和工作节点）  
> **脚本说明**：`01-create_students.sh` 脚本用于批量创建学生系统账户，使用 `-M`（不创建家目录）和 `-s /sbin/nologin`（禁用登录）参数，确保学生无法直接登录集群节点。

#### 2.1.2 远程客户端配置

学生在自己的开发机器上配置 Hadoop 客户端，通过网络远程访问集群：

```bash
# 学生在本地机器上安装 Hadoop 客户端
# 下载与集群相同版本的 Hadoop
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
tar -xzf hadoop-3.3.4.tar.gz
sudo mv hadoop-3.3.4 /opt/hadoop

# 配置环境变量
echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc
source ~/.bashrc

# 使用学生客户端配置脚本
./cluster-setup-scripts/05-setup_student_client.sh [学号]
```

> **执行角色**：学生  
> **执行位置**：学生本地开发机器  
> **脚本说明**：`05-setup_student_client.sh` 脚本会为学生配置正确的集群连接参数，包括 NameNode 地址、ResourceManager 地址等，并设置用户身份认证。

#### 2.1.3 身份认证配置

为了支持远程客户端访问，需要配置 Hadoop 的身份认证机制：

```bash
# 在集群的 core-site.xml 中配置简单认证
<property>
    <name>hadoop.security.authentication</name>
    <value>simple</value>
    <description>使用简单认证模式</description>
</property>

<property>
    <name>hadoop.security.authorization</name>
    <value>false</value>
    <description>暂时禁用授权检查（可根据需要启用）</description>
</property>

# 配置代理用户（允许管理员代表学生执行操作）
<property>
    <name>hadoop.proxyuser.hadoop.hosts</name>
    <value>*</value>
    <description>允许 hadoop 用户从任何主机代理</description>
</property>

<property>
    <name>hadoop.proxyuser.hadoop.groups</name>
    <value>students</value>
    <description>允许代理 students 组的用户</description>
</property>
```

> **执行角色**：管理员  
> **执行位置**：集群主节点  
> **说明**：配置完成后需要重启 Hadoop 服务使配置生效。

#### 2.1.4 HDFS 目录结构设计

```bash
# 创建公共数据目录（只读）
hdfs dfs -mkdir -p /public/data/wordcount
hdfs dfs -chmod 755 /public/data
hdfs dfs -chmod 755 /public/data/wordcount

# 上传测试数据到公共目录
hdfs dfs -put /path/to/simple-test.txt /public/data/wordcount/
hdfs dfs -put /path/to/alice-in-wonderland.txt /public/data/wordcount/
hdfs dfs -put /path/to/pride-and-prejudice.txt /public/data/wordcount/

# 设置公共数据为只读
hdfs dfs -chmod 644 /public/data/wordcount/*

# 创建学生个人目录结构
hdfs dfs -mkdir -p /users
hdfs dfs -chmod 755 /users

# 为每个学生创建个人目录的脚本
# 使用预配置的脚本批量创建学生 HDFS 目录
./cluster-setup-scripts/02-create_hdfs_dirs.sh students.txt
```

> **执行角色**：管理员
> **执行位置**：集群主节点
> **脚本说明**：`02-create_hdfs_dirs.sh` 脚本用于批量创建学生的 HDFS 个人目录，需要管理员权限在主节点执行。

### 2.2 资源分配和限制

#### 2.2.1 YARN 资源队列配置

使用预配置的脚本设置 YARN 队列：

```bash
# 配置 YARN 资源队列
./cluster-setup-scripts/03-setup_yarn_queues.sh
```

> **执行角色**：管理员  
> **执行位置**：集群主节点  
> **脚本说明**：`03-setup_yarn_queues.sh` 脚本会自动配置 `capacity-scheduler.xml`，创建 `default` 和 `students` 队列，分配适当的资源容量和访问权限。

#### 2.2.2 HDFS 配额管理

```bash
# 为每个学生设置 HDFS 存储配额（例如 1GB）
./cluster-setup-scripts/04-set_hdfs_quotas.sh students.txt 1
```

> **执行角色**：管理员  
> **执行位置**：集群主节点  
> **脚本说明**：`04-set_hdfs_quotas.sh` 脚本用于批量设置学生的 HDFS 存储配额，参数为学生名单文件和配额大小（GB）。

### 2.3 权限和安全配置

#### 2.3.1 HDFS 权限配置

在 `hdfs-site.xml` 中添加权限检查配置：

```xml
<property>
    <name>dfs.permissions.enabled</name>
    <value>true</value>
    <description>启用 HDFS 权限检查</description>
</property>

<property>
    <name>dfs.permissions.superusergroup</name>
    <value>hadoop</value>
    <description>超级用户组</description>
</property>

<property>
    <name>dfs.namenode.acls.enabled</name>
    <value>true</value>
    <description>启用 HDFS ACL 支持</description>
</property>
```

#### 2.3.2 用户环境配置脚本

```bash
# 学生配置自己的 Hadoop 客户端环境
./cluster-setup-scripts/05-setup_student_client.sh [学号]

# 示例：
./cluster-setup-scripts/05-setup_student_client.sh 20230001

# 或者直接运行，脚本会提示输入学号：
./cluster-setup-scripts/05-setup_student_client.sh
```

> **执行角色**：学生  
> **执行位置**：学生本地机器  
> **脚本说明**：`05-setup_student_client.sh` 脚本会为每个学生配置 Hadoop 客户端环境，包括创建配置文件、设置环境变量和创建工作目录。学生可以在自己的本地机器上执行此脚本，以便配置正确的客户端连接参数和开发环境。

---

## 3 远程访问和客户端配置

### 3.1 Hadoop 客户端配置

**1. 客户端配置文件模板**：

客户端配置文件模板已集成在 `05-setup_student_client.sh` 脚本中，包括：

- `core-site.xml`：HDFS 连接配置
- `yarn-site.xml`：YARN 资源管理器配置
- `mapred-site.xml`：MapReduce 作业配置

> **说明**：这些配置文件会自动为每个学生创建个人配置目录，并设置正确的集群连接参数和默认队列。

### 3.2 学生使用指南

#### 3.2.1 环境准备

**1. 系统要求**：

学生本地机器需要满足以下要求：

- 操作系统：Linux、macOS 或 Windows（推荐使用 Linux 或 macOS）
- Java 环境：JDK 8 或 JDK 11
- 网络连接：能够访问集群节点的相关端口
- 磁盘空间：至少 2GB 可用空间

**2. 网络配置**：

确保本地机器能够访问集群的以下端口：

```bash
# HDFS NameNode
9000    # HDFS 文件系统访问
9870    # NameNode Web UI

# YARN ResourceManager
8032    # ResourceManager 服务
8088    # ResourceManager Web UI

# MapReduce JobHistory Server
19888   # JobHistory Web UI
```

#### 3.2.2 客户端安装和配置

**1. 下载和安装 Hadoop**：

```bash
# 创建工作目录
mkdir -p ~/hadoop-client
cd ~/hadoop-client

# 下载 Hadoop（与集群版本保持一致）
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz

# 解压安装
tar -xzf hadoop-3.3.4.tar.gz
sudo mv hadoop-3.3.4 /opt/hadoop

# 设置所有者
sudo chown -R $USER:$USER /opt/hadoop
```

**2. 配置环境变量**：

```bash
# 编辑 ~/.bashrc 或 ~/.zshrc
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.bashrc  # 根据实际 Java 路径调整
echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc
echo 'export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc

# 重新加载环境变量
source ~/.bashrc

# 验证安装
hadoop version
```

**3. 获取配置文件**：

从管理员处获取客户端配置脚本，或使用以下方式配置：

```bash
# 方式1：使用配置脚本（推荐）
# 从管理员处获取 05-setup_student_client.sh 脚本
./05-setup_student_client.sh [你的学号]

# 方式2：手动配置
# 从管理员处获取配置文件模板，复制到 $HADOOP_CONF_DIR
```

#### 3.2.3 基本使用方法

**1. 验证连接**：

```bash
# 测试 HDFS 连接
hdfs dfs -ls /

# 查看个人目录
hdfs dfs -ls /users/[你的学号]

# 查看存储配额
hdfs dfsadmin -getSpaceQuota /users/[你的学号]
```

**2. 文件操作**：

```bash
# 上传文件到 HDFS
hdfs dfs -put local_file.txt /users/[你的学号]/

# 下载文件从 HDFS
hdfs dfs -get /users/[你的学号]/file.txt ./

# 查看文件内容
hdfs dfs -cat /users/[你的学号]/file.txt

# 删除文件
hdfs dfs -rm /users/[你的学号]/file.txt
```

**3. 运行 MapReduce 作业**：

```bash
# 运行 WordCount 示例
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.4.jar \
    wordcount \
    /public/data/wordcount/simple-test.txt \
    /users/[你的学号]/output/wordcount-$(date +%Y%m%d-%H%M%S)

# 查看作业结果
hdfs dfs -cat /users/[你的学号]/output/wordcount-*/part-r-00000
```

**4. 监控作业状态**：

```bash
# 查看正在运行的作业
yarn application -list

# 查看作业详情
yarn application -status [application_id]

# 查看作业日志
yarn logs -applicationId [application_id]
```

#### 3.2.4 常见问题和解决方案

**1. 连接问题**：

```bash
# 问题：无法连接到 NameNode
# 解决：检查网络连接和防火墙设置
ping [namenode_hostname]
telnet [namenode_hostname] 9000

# 问题：权限被拒绝
# 解决：检查用户身份设置
export HADOOP_USER_NAME=[你的学号]
```

**2. 配置问题**：

```bash
# 问题：找不到配置文件
# 解决：检查 HADOOP_CONF_DIR 环境变量
echo $HADOOP_CONF_DIR
ls -la $HADOOP_CONF_DIR

# 问题：Java 环境问题
# 解决：检查 JAVA_HOME 设置
echo $JAVA_HOME
java -version
```

**3. 作业执行问题**：

```bash
# 问题：作业提交失败
# 解决：检查队列配置和资源可用性
yarn queue -status students

# 问题：输出目录已存在
# 解决：删除或使用不同的输出目录
hdfs dfs -rm -r /users/[你的学号]/output/existing_dir
```

---

## 4 多用户环境监控和故障排除

### 4.1 Web 界面监控

#### 4.1.1 监控界面配置

**1. 配置远程访问**：

在 `core-site.xml` 中添加 Web UI 访问配置：

```xml
<property>
    <name>hadoop.http.staticuser.user</name>
    <value>hadoop</value>
    <description>Web UI 静态用户</description>
</property>

<property>
    <name>hadoop.http.authentication.type</name>
    <value>simple</value>
    <description>HTTP 认证类型</description>
</property>
```

**2. 学生监控面板**：

使用预配置的学生监控脚本：

```bash
# 使用学生监控脚本
./cluster-setup-scripts/06-student_monitor.sh <学号>
```

> **执行角色**：管理员或学生  
> **执行位置**：集群主节点或学生本地机器  
> **脚本说明**：该脚本提供以下监控功能：

- HDFS 个人目录状态检查
- 存储配额使用情况查看
- 当前运行作业状态
- 作业历史记录查询
- 集群资源使用情况概览

#### 4.1.2 日志查看和分析

**1. 学生作业日志查看脚本**：

使用预配置的日志查看脚本：

```bash
# 查看学生作业日志
./cluster-setup-scripts/07-view_student_logs.sh <学号> <应用ID>
```

> **执行角色**：管理员或学生
> **执行位置**：集群主节点或学生本地机器
> **脚本说明**：该脚本提供以下功能：

- 应用基本信息查询
- 应用日志获取和显示
- MapReduce 作业详细信息查看

**2. 自动化故障诊断脚本**：

使用预配置的诊断脚本：

```bash
# 诊断学生问题（服务器端）
./cluster-setup-scripts/08-diagnose_student_issues.sh <学号>

# Hadoop 客户端故障排除（客户端）
./cluster-setup-scripts/10-hadoop_client_troubleshoot.sh -s <学号>
```

> **执行角色**：管理员
> **执行位置**：集群主节点
> **脚本说明**：`08-diagnose_student_issues.sh` 脚本提供以下诊断功能：

- 用户账户存在性检查
- HDFS 个人目录状态验证
- 目录权限检查
- 存储配额使用情况
- 最近作业错误分析
- 集群状态检查

> **执行角色**：学生或管理员
> **执行位置**：学生本地机器或远程客户端
> **脚本说明**：`10-hadoop_client_troubleshoot.sh` 脚本提供以下客户端诊断功能：

- 基本环境检查（Java、Hadoop 命令）
- 网络连接测试（集群端口连通性）
- 配置文件验证（客户端配置完整性）
- 环境变量检查（HADOOP_HOME、JAVA_HOME 等）
- Hadoop 连接测试（HDFS、YARN 服务连接）
- 性能测试（简单读写测试）
- 诊断报告生成（详细报告和修复建议）
- 自动修复功能（配置问题自动修复）

### 4.2 远程支持工具

#### 4.2.1 学生自助服务脚本

使用预配置的学生自助服务脚本：

```bash
# 启动学生自助服务界面
./cluster-setup-scripts/09-student_self_service.sh
```

> **执行角色**：学生
> **执行位置**：学生本地机器
> **脚本说明**：该脚本提供以下自助服务功能：

- 个人目录状态查看
- 存储配额使用情况查询
- 正在运行作业监控
- 作业历史记录查看
- 个人临时文件清理
- 集群连接测试
- 常见问题解决方案

#### 4.2.2 管理员支持工具

使用预配置的管理员支持工具：

```bash
# 启动管理员支持工具界面
./cluster-setup-scripts/12-admin_support_tools.sh
```

> **执行角色**：管理员
> **执行位置**：集群主节点
> **脚本说明**：该工具提供以下管理功能：

- 批量创建学生账户
- 批量创建 HDFS 目录
- 设置存储配额
- 查看所有学生状态
- 诊断特定学生问题
- 查看集群资源使用情况
- 清理失败的作业
- 备份学生数据

---
