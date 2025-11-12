# 多用户配置和管理

Hadoop 多用户环境是指在同一个 Hadoop 集群上支持多个用户同时进行大数据处理任务的配置方案。在教学和企业环境中，多用户支持是必不可少的功能，它需要解决以下核心问题：

- **用户隔离**：确保不同用户的数据和作业相互独立，避免干扰
- **资源管理**：合理分配计算和存储资源，防止单个用户占用过多资源
- **安全控制**：实施身份认证和权限管理，保护数据安全
- **运维管理**：提供监控、故障排除和用户管理工具

本文档旨在为教学环境提供一套完整的 Hadoop 多用户配置和管理方案，具体目标包括：

1. **建立远程访问架构**：学生在自己的开发机器上配置 Hadoop 客户端，通过网络远程访问集群，而不是直接登录到集群节点
2. **实现用户隔离**：为每个学生创建独立的工作空间和资源配额
3. **简化部署流程**：提供自动化脚本，降低配置复杂度
4. **提供基本运维支持**：建立基本监控体系和故障排除机制，确保系统稳定运行

---

## 1 多用户访问方案设计

### 1.1 用户隔离策略

为支持多个学生同时使用集群完成 MapReduce 作业，采用**远程客户端访问**的用户隔离方案，避免学生直接登录集群节点：

#### 1.1.1 集群端用户账户管理

**管理员操作**：在集群所有节点上创建学生账户和权限设置

```bash
# 1. 创建学生用户组（用于 HDFS 权限管理）
sudo groupadd students

# 2. 创建学生用户账户（仅用于身份识别，禁用登录），可选
# 示例：为学号 2024001 的学生创建用户
sudo useradd -M -g students -s /sbin/nologin 2024001

# 3. 批量创建用户脚本（需要在所有节点执行）
./cluster-setup-scripts/01-create_students.sh students.txt
```

- **执行角色**：管理员
- **执行位置**：所有集群节点（主节点 + 工作节点）
- **安全说明**：学生账户使用 `-M`（不创建 Home 目录）和 `-s /sbin/nologin`（禁用登录）参数，确保学生无法直接登录集群节点，只能通过远程客户端访问。

#### 1.1.2 身份认证配置

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
<!-- 方案一：限制网段（推荐用于教学环境） -->
<property>
    <name>hadoop.proxyuser.hadoop.hosts</name>
    <value>hadoop-master,localhost,192.168.1.*,10.0.0.*</value>
    <description>允许 hadoop 用户从指定主机/网段代理（根据实际网络环境调整网段）</description>
</property>

<!-- 方案二：如果网络环境复杂，可临时使用通配符（需要额外安全措施） -->
<!--
<property>
    <name>hadoop.proxyuser.hadoop.hosts</name>
    <value>*</value>
    <description>允许从任何主机代理（仅限教学环境，生产环境禁用）</description>
</property>
-->

<property>
    <name>hadoop.proxyuser.hadoop.groups</name>
    <value>students</value>
    <description>允许代理 students 组的用户</description>
</property>

<!-- 额外安全配置：限制代理用户权限 -->
<property>
    <name>hadoop.proxyuser.hadoop.users</name>
    <value>*</value>
    <description>允许代理的用户（* 表示 students 组内所有用户）</description>
</property>
```

- **执行角色**：管理员
- **执行位置**：集群主节点
- **配置说明**：
  - **方案一（推荐）**：使用网段限制，根据学校网络环境调整 IP 网段
    - 常见教学网段：`192.168.1.*`、`10.0.0.*`、`172.16.0.*`
    - 可通过 `ip route` 或 `ifconfig` 命令查看网络配置
  - **方案二（备选）**：如果网络环境复杂且经常变化，可临时使用通配符 `*`
    - 仅适用于隔离的教学环境
    - 需要配合防火墙等额外安全措施
- **重要提醒**：配置完成后需要重启 Hadoop 服务使配置生效

#### 1.1.3 HDFS 目录结构设计

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

- **执行角色**：管理员
- **执行位置**：集群主节点
- **脚本说明**：`02-create_hdfs_dirs.sh` 脚本用于批量创建学生的 HDFS 个人目录，需要管理员权限在主节点执行。

### 1.2 资源分配和限制

#### 1.2.1 YARN 资源队列配置

使用预配置的脚本设置 YARN 队列：

```bash
# 配置 YARN 资源队列
./cluster-setup-scripts/03-setup_yarn_queues.sh
```

- **执行角色**：管理员
- **执行位置**：集群主节点
- **脚本说明**：`03-setup_yarn_queues.sh` 脚本会自动配置 `capacity-scheduler.xml`，创建 `default` 和 `students` 队列，分配适当的资源容量和访问权限。

#### 1.2.2 HDFS 配额管理

```bash
# 为每个学生设置 HDFS 存储配额（例如 1GB）
./cluster-setup-scripts/04-set_hdfs_quotas.sh students.txt 1
```

- **执行角色**：管理员
- **执行位置**：集群主节点
- **脚本说明**：`04-set_hdfs_quotas.sh` 脚本用于批量设置学生的 HDFS 存储配额，参数为学生名单文件和配额大小（GB）。

### 1.3 权限和安全配置

#### 1.3.1 HDFS 权限配置

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

- **执行角色**：管理员
- **执行位置**：集群主节点
- **配置说明**：
  - 权限检查确保文件和目录访问的安全性
  - ACL 支持提供更细粒度的权限控制
  - 配置完成后需要重启 HDFS 服务使配置生效

#### 1.3.2 用户权限控制机制

多用户环境的权限控制通过以下层次实现：

**1. 系统层面权限**：

- Linux 用户和组管理（`students` 组）
- 文件系统权限（755/644）

**2. HDFS 层面权限**：

- 目录所有者和组权限控制
- POSIX 风格权限（rwx）
- ACL 访问控制列表支持

**3. YARN 层面权限**：

- 队列访问控制（`students` 队列）
- 资源分配和限制
- 作业提交权限验证

**4. 配额层面控制**：

- HDFS 存储配额（默认 1GB/用户）
- 文件数量配额限制
- YARN 资源使用配额

---

## 2 远程访问和客户端配置

### 2.1 本地系统要求和网络配置

#### 2.1.1 系统要求

学生本地机器需要满足以下要求：

- **操作系统**：Linux、macOS 或 Windows（推荐使用 Linux 或 macOS）
- **Java 环境**：JDK 8
- **网络连接**：能够访问集群节点的相关端口
- **磁盘空间**：至少 2GB 可用空间

#### 2.1.2 主机名解析配置（重要）

Hadoop 集群通常使用主机名（如 `hadoop-master`、`hadoop-worker1`）进行节点间通信。学生客户端需要能够解析这些主机名到对应的 IP 地址，否则会出现连接失败的问题。

**1. Linux/macOS 系统**：

配置方法如下：

```bash
# 编辑 hosts 文件（需要管理员权限）
sudo vim /etc/hosts

# 添加集群节点的主机名映射（根据实际 IP 地址调整）
192.168.1.100   hadoop-master
192.168.1.101   hadoop-worker1
192.168.1.102   hadoop-worker2
192.168.1.103   hadoop-worker3
```

验证方法如下：

```bash
# 测试主机名解析
nslookup hadoop-master
ping -c 1 hadoop-master

# 或者使用 getent 命令
getent hosts hadoop-master
```

**2. Windows 系统**：

配置方法如下：

```bash
# 方法1：使用记事本（以管理员身份运行）
# 1. 右键点击"开始"菜单，选择"Windows PowerShell（管理员）"
# 2. 运行以下命令打开记事本编辑 hosts 文件
notepad C:\Windows\System32\drivers\etc\hosts

# 方法2：使用命令行直接添加
# 以管理员身份运行 PowerShell 或命令提示符
echo 192.168.1.100 hadoop-master >> C:\Windows\System32\drivers\etc\hosts
echo 192.168.1.101 hadoop-worker1 >> C:\Windows\System32\drivers\etc\hosts
echo 192.168.1.102 hadoop-worker2 >> C:\Windows\System32\drivers\etc\hosts
echo 192.168.1.103 hadoop-worker3 >> C:\Windows\System32\drivers\etc\hosts

# 验证修改结果
type C:\Windows\System32\drivers\etc\hosts
```

验证方法如下：

```cmd
# 测试主机名解析
nslookup hadoop-master
ping -n 1 hadoop-master

# 查看 hosts 文件内容
type C:\Windows\System32\drivers\etc\hosts | findstr hadoop
```

**注意事项**：

- Windows 系统的 hosts 文件位于 `C:\Windows\System32\drivers\etc\hosts`
- 必须以管理员权限运行编辑器或命令行
- 某些杀毒软件可能会阻止修改 hosts 文件，需要临时关闭实时保护

#### 2.1.3 网络端口配置

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

### 2.2 客户端开发环境配置

#### 2.2.1 自动配置脚本（推荐）

学生可以使用预配置的脚本快速设置客户端环境：

```bash
# 学生配置自己的 Hadoop 客户端环境
./cluster-setup-scripts/05-setup_student_client.sh [学号] [集群主节点IP/主机名]

# 示例：学号为 2024001 的学生，连接到指定集群
./cluster-setup-scripts/05-setup_student_client.sh 2024001 192.168.1.100

# 或者只提供学号，使用默认集群地址 hadoop-master
./cluster-setup-scripts/05-setup_student_client.sh 2024001

# 或者直接运行，脚本会提示输入所需参数：
./cluster-setup-scripts/05-setup_student_client.sh
```

- **执行角色**：学生
- **执行位置**：学生本地机器
- **脚本功能**：
  - 自动下载并配置 `core-site.xml`、`yarn-site.xml`、`mapred-site.xml`
  - 设置正确的集群连接地址和端口
  - 配置用户身份认证参数和默认队列
  - 创建个人工作目录和环境变量
  - 验证连接是否成功

#### 2.2.2 手动配置方法

如果无法使用自动脚本，可以手动配置客户端环境：

**1. 下载和安装 Hadoop**：

```bash
# 创建工作目录
mkdir -p ~/hadoop-client
cd ~/hadoop-client

# 下载 Hadoop（与集群版本保持一致）
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz

# 解压安装
tar -xzf hadoop-3.4.2.tar.gz
sudo mv hadoop-3.4.2 /opt/hadoop

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

**3. 手动创建配置文件**：

在 `$HADOOP_CONF_DIR` 目录下创建配置文件，从管理员处获取集群连接参数。

> **注意**：手动配置容易出错，强烈建议优先使用自动配置脚本。

### 2.3 基本使用方法

#### 2.3.1 连接验证

```bash
# 测试 HDFS 连接
hdfs dfs -ls /

# 查看个人目录
hdfs dfs -ls /users/2024001

# 查看存储配额
hdfs dfsadmin -getSpaceQuota /users/2024001
```

#### 2.3.2 文件操作

```bash
# 上传文件到 HDFS
hdfs dfs -put local_file.txt /users/2024001/

# 下载文件从 HDFS
hdfs dfs -get /users/2024001/file.txt ./

# 查看文件内容
hdfs dfs -cat /users/2024001/file.txt

# 删除文件
hdfs dfs -rm /users/2024001/file.txt
```

#### 2.3.3 MapReduce 作业执行

```bash
# 运行 WordCount 示例
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.2.jar \
    wordcount \
    /public/data/wordcount/simple-test.txt \
    /users/2024001/output/wordcount-$(date +%Y%m%d-%H%M%S)

# 查看作业结果
hdfs dfs -cat /users/2024001/output/wordcount-*/part-r-00000
```

#### 2.3.4 作业监控

```bash
# 查看正在运行的作业
yarn application -list

# 查看作业详情
yarn application -status [application_id]

# 查看作业日志
yarn logs -applicationId [application_id]
```

---

## 3 多用户环境监控和故障排除

### 3.1 Web 界面监控

#### 3.1.1 监控界面访问

**1. 集群监控界面**：

学生可以通过以下 Web 界面监控集群状态：

```bash
# HDFS NameNode Web UI
http://[namenode_hostname]:9870

# YARN ResourceManager Web UI
http://[namenode_hostname]:8088

# MapReduce JobHistory Server Web UI
http://[namenode_hostname]:19888
```

**2. 个人状态监控**：

使用预配置的学生监控脚本：

```bash
# 查看个人状态和资源使用情况
./cluster-setup-scripts/06-student_monitor.sh 2024001
```

- **执行角色**：学生
- **执行位置**：学生本地机器
- **脚本功能**：
  - HDFS 个人目录状态检查
  - 存储配额使用情况查看
  - 当前运行作业状态
  - 作业历史记录查询
  - 集群资源使用情况概览

#### 3.1.2 作业监控和日志查看

**1. 作业状态监控**：

```bash
# 查看正在运行的作业
yarn application -list -appStates RUNNING

# 查看特定作业详情
yarn application -status application_1234567890123_0001

# 查看作业进度
yarn application -list | grep 2024001
```

**2. 作业日志查看**：

```bash
# 查看作业日志（学生操作）
yarn logs -applicationId application_1234567890123_0001

# 使用日志查看脚本（管理员协助）
./cluster-setup-scripts/07-view_student_logs.sh 2024001 application_1234567890123_0001
```

- **执行角色**：学生或管理员
- **执行位置**：学生本地机器或集群主节点
- **脚本功能**：
  - 应用基本信息查询
  - 应用日志获取和显示
  - MapReduce 作业详细信息查看
  - 错误信息高亮显示

### 3.2 故障诊断和排除

#### 3.2.1 学生自助诊断

**1. 客户端连接诊断**：

```bash
# 使用客户端故障排除脚本
./cluster-setup-scripts/10-hadoop_client_troubleshoot.sh -s 2024001
```

- **执行角色**：学生
- **执行位置**：学生本地机器
- **脚本功能**：
  - 基本环境检查（Java、Hadoop 命令）
  - 网络连接测试（集群端口连通性）
  - 配置文件验证（客户端配置完整性）
  - 环境变量检查（HADOOP_HOME、JAVA_HOME 等）
  - Hadoop 连接测试（HDFS、YARN 服务连接）
  - 性能测试（简单读写测试）
  - 诊断报告生成（详细报告和修复建议）
  - 自动修复功能（配置问题自动修复）

**2. 学生自助服务**：

```bash
# 启动学生自助服务界面
./cluster-setup-scripts/09-student_self_service.sh
```

- **执行角色**：学生
- **执行位置**：学生本地机器
- **脚本功能**：
  - 个人目录状态查看
  - 存储配额使用情况查询
  - 正在运行作业监控
  - 作业历史记录查看
  - 个人临时文件清理
  - 集群连接测试
  - 常见问题解决方案

#### 3.2.2 管理员诊断工具

**1. 学生问题诊断**：

```bash
# 诊断特定学生问题
./cluster-setup-scripts/08-diagnose_student_issues.sh 2024001
```

- **执行角色**：管理员
- **执行位置**：集群主节点
- **脚本功能**：
  - 用户账户存在性检查
  - HDFS 个人目录状态验证
  - 目录权限检查
  - 存储配额使用情况
  - 最近作业错误分析
  - 集群状态检查

**2. 管理员支持工具**：

```bash
# 启动管理员支持工具界面
./cluster-setup-scripts/12-admin_support_tools.sh
```

- **执行角色**：管理员
- **执行位置**：集群主节点
- **脚本功能**：
  - 批量创建学生账户
  - 批量创建 HDFS 目录
  - 设置存储配额
  - 查看所有学生状态
  - 诊断特定学生问题
  - 查看集群资源使用情况
  - 清理失败的作业
  - 备份学生数据

### 3.3 常见问题和解决方案

#### 3.3.1 连接问题

**问题 1：无法连接到集群**：

```bash
# 症状：连接超时或拒绝连接
# 诊断步骤：
ping [namenode_hostname]
telnet [namenode_hostname] 9000
telnet [namenode_hostname] 8032

# 解决方案：
# 1. 检查网络连接
# 2. 确认防火墙设置
# 3. 验证集群服务状态
# 4. 检查主机名解析（重要！）
nslookup [namenode_hostname]
```

**问题 2：主机名解析失败**：

```bash
# 症状：java.net.UnknownHostException 或 Name or service not known
# 诊断步骤：
nslookup hadoop-master
getent hosts hadoop-master

# 解决方案：
# 1. 配置 /etc/hosts 文件
sudo echo "192.168.1.100 hadoop-master" >> /etc/hosts

# 2. 或者使用 IP 地址替代主机名
export HADOOP_CONF_DIR=~/.hadoop/conf
# 修改配置文件中的主机名为 IP 地址
```

**问题 3：权限被拒绝**：

```bash
# 症状：Permission denied 错误
# 诊断步骤：
echo $HADOOP_USER_NAME
hdfs dfs -ls /users/2024001

# 解决方案：
export HADOOP_USER_NAME=2024001
# 或在配置文件中设置用户身份
```

#### 3.3.2 配置问题

**问题 1：找不到配置文件**：

```bash
# 症状：配置文件路径错误
# 诊断步骤：
echo $HADOOP_CONF_DIR
ls -la $HADOOP_CONF_DIR

# 解决方案：
export HADOOP_CONF_DIR=~/.hadoop/conf
# 重新运行客户端配置脚本
./cluster-setup-scripts/04-setup_client_config.sh [学号]
```

**问题 2：Java 环境问题**：

```bash
# 症状：Java 相关错误
# 诊断步骤：
echo $JAVA_HOME
java -version
hadoop checknative

# 解决方案：
# 1. 检查 Java 安装
# 2. 设置正确的 JAVA_HOME
# 3. 验证 Hadoop 本地库
```

**问题 3：配置文件格式错误**：

```bash
# 症状：XML 格式错误或参数不正确
# 诊断步骤：
hadoop checknative
hadoop classpath

# 解决方案：
# 1. 重新运行客户端配置脚本
# 2. 检查配置文件语法
# 3. 验证环境变量设置
```

#### 3.3.3 作业执行问题

**问题 1：作业提交失败**：

```bash
# 症状：作业无法提交到队列
# 诊断步骤：
yarn queue -status students
yarn node -list

# 解决方案：
# 1. 检查队列资源可用性
# 2. 确认用户队列权限
# 3. 等待资源释放或联系管理员
```

**问题 2：作业运行缓慢**：

```bash
# 症状：作业长时间处于运行状态
# 诊断步骤：
yarn application -status [application_id]
yarn logs -applicationId [application_id]

# 解决方案：
# 1. 检查输入数据大小和分片
# 2. 优化 MapReduce 参数
# 3. 检查集群资源使用情况
```

**问题 3：输出目录已存在**：

```bash
# 症状：FileAlreadyExistsException
# 诊断步骤：
hdfs dfs -ls /users/2024001/output/

# 解决方案：
# 1. 删除已存在的输出目录
hdfs dfs -rm -r /users/2024001/output/existing_dir
# 2. 或使用不同的输出目录名称
```

#### 3.3.4 存储问题

**问题 1：存储配额超限**：

```bash
# 症状：无法写入文件到 HDFS
# 诊断步骤：
hdfs dfsadmin -getSpaceQuota /users/2024001
hdfs dfs -du -s -h /users/2024001

# 解决方案：
# 1. 清理不需要的文件
hdfs dfs -rm -r /users/2024001/temp/*
# 2. 联系管理员申请配额增加
```

**问题 2：文件权限问题**：

```bash
# 症状：无法访问或修改文件
# 诊断步骤：
hdfs dfs -ls -la /users/2024001/

# 解决方案：
# 1. 检查文件所有者和权限
# 2. 使用正确的用户身份操作
# 3. 联系管理员修复权限问题
```

#### 3.3.5 问题解决流程

当遇到问题时，建议按照以下流程进行诊断和解决：

**学生自助解决流程**：

```bash
# 第一步：运行自动诊断脚本
./cluster-setup-scripts/10-hadoop_client_troubleshoot.sh -s [学号]

# 第二步：查看诊断报告
cat ~/hadoop_diagnosis_report.txt

# 第三步：尝试自动修复
./cluster-setup-scripts/10-hadoop_client_troubleshoot.sh -s [学号] --auto-fix

# 第四步：如果问题仍未解决，使用自助服务工具
./cluster-setup-scripts/09-student_self_service.sh
```

**管理员支持流程**：

```bash
# 第一步：诊断学生问题
./cluster-setup-scripts/08-diagnose_student_issues.sh [学号]

# 第二步：使用管理员工具界面
./cluster-setup-scripts/12-admin_support_tools.sh

# 第三步：查看详细日志
yarn logs -applicationId [application_id]
hdfs dfsadmin -report
```

#### 3.3.6 自动化诊断工具

**学生自助诊断**：

```bash
# 运行综合诊断脚本
./cluster-setup-scripts/10-hadoop_client_troubleshoot.sh -s [学号]

# 查看诊断报告
cat ~/hadoop_diagnosis_report.txt

# 尝试自动修复
./cluster-setup-scripts/10-hadoop_client_troubleshoot.sh -s [学号] --auto-fix
```

**管理员诊断工具**：

```bash
# 诊断特定学生问题
./cluster-setup-scripts/08-diagnose_student_issues.sh [学号]

# 使用管理员支持工具
./cluster-setup-scripts/12-admin_support_tools.sh
```

---
