# Hadoop 集群学生使用指南

## 1. 快速开始

### 1.1 获取账户信息

联系课程管理员获取以下信息：

- **学号**：作为用户名使用
- **密码**：初始密码为 `hadoop123`，建议首次登录后修改
- **集群地址**：`hadoop-master` (需要配置 hosts 文件或使用 IP 地址)

### 1.2 环境准备

#### 1.2.1 配置 hosts 文件

在本地计算机上配置 hosts 文件，添加集群节点地址：

**Linux/macOS 用户：**

```bash
sudo vim /etc/hosts
```

**Windows 用户：**

```text
C:\Windows\System32\drivers\etc\hosts
```

添加以下内容（请向管理员确认实际 IP 地址）：

```text
192.168.1.100  hadoop-master
192.168.1.101  hadoop-worker1
192.168.1.102  hadoop-worker2
```

#### 1.2.2 安装 Hadoop 客户端

**方法一：使用预配置的客户端**:

下载并解压 Hadoop 客户端包：

```bash
# 下载 Hadoop 3.4.2
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz
tar -xzf hadoop-3.4.2.tar.gz
sudo mv hadoop-3.4.2 /opt/hadoop
```

**方法二：使用 Docker（推荐）**:

```bash
# 拉取 Hadoop 客户端镜像
docker pull apache/hadoop:3.4.2

# 创建客户端容器
docker run -it --name hadoop-client \
  --add-host hadoop-master:192.168.1.100 \
  --add-host hadoop-worker1:192.168.1.101 \
  --add-host hadoop-worker2:192.168.1.102 \
  apache/hadoop:3.4.2 bash
```

### 1.3 配置环境变量

在 `~/.bashrc` 或 `~/.zshrc` 中添加：

```bash
# Hadoop 环境配置
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# 设置默认队列为学生队列
export HADOOP_MAPRED_QUEUE_NAME=students
```

重新加载环境变量：

```bash
source ~/.bashrc
```

### 1.4 获取配置文件

从管理员处获取客户端配置文件，或使用以下模板：

**core-site.xml**:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://hadoop-master:9000</value>
    </property>
</configuration>
```

**yarn-site.xml**:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>hadoop-master</value>
    </property>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>hadoop-master:8032</value>
    </property>
    <property>
        <name>yarn.resourcemanager.webapp.address</name>
        <value>hadoop-master:8088</value>
    </property>
</configuration>
```

**mapred-site.xml**:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.job.queuename</name>
        <value>students</value>
    </property>
</configuration>
```

---

## 2. HDFS 基本操作

### 2.1 目录结构说明

- **个人目录**：`/users/<学号>/`
- **作业目录**：`/users/<学号>/homework1/problem1/`
- **公共数据**：`/public/data/wordcount/`

### 2.2 常用 HDFS 命令

#### 2.2.1 查看目录和文件

```bash
# 查看个人目录
hdfs dfs -ls /users/<学号>

# 查看公共数据目录
hdfs dfs -ls /public/data/wordcount

# 递归查看目录结构
hdfs dfs -ls -R /users/<学号>

# 查看文件内容
hdfs dfs -cat /public/data/wordcount/simple-test.txt

# 查看文件前几行
hdfs dfs -head /public/data/wordcount/alice-in-wonderland.txt
```

#### 2.2.2 文件上传和下载

```bash
# 上传本地文件到 HDFS
hdfs dfs -put local-file.txt /users/<学号>/

# 从 HDFS 下载文件到本地
hdfs dfs -get /users/<学号>/output/part-r-00000 ./result.txt

# 复制 HDFS 文件
hdfs dfs -cp /public/data/wordcount/simple-test.txt /users/<学号>/input/
```

#### 2.2.3 目录和文件管理

```bash
# 创建目录
hdfs dfs -mkdir -p /users/<学号>/homework1/problem1

# 删除文件
hdfs dfs -rm /users/<学号>/temp-file.txt

# 删除目录（递归）
hdfs dfs -rm -r /users/<学号>/temp-dir

# 移动文件
hdfs dfs -mv /users/<学号>/old-name.txt /users/<学号>/new-name.txt
```

#### 2.2.4 查看存储信息

```bash
# 查看目录大小
hdfs dfs -du -h /users/<学号>

# 查看存储配额
hdfs dfsadmin -getSpaceQuota /users/<学号>

# 查看文件详细信息
hdfs dfs -stat "%n %o %r %u %g %y %b" /users/<学号>/file.txt
```

---

## 3. MapReduce 作业提交

### 3.1 编译 Java 程序

#### 3.1.1 准备开发环境

```bash
# 创建工作目录
mkdir -p ~/hadoop-workspace/src
cd ~/hadoop-workspace

# 设置 CLASSPATH
export CLASSPATH=$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/yarn/*
```

#### 3.1.2 编译 MapReduce 程序

```bash
# 编译 Java 源文件
javac -d . src/WordCount*.java

# 创建 JAR 包
jar cf wordcount.jar -C . .
```

### 3.2 提交作业

#### 3.2.1 基本提交命令

```bash
# 提交 WordCount 作业
hadoop jar wordcount.jar WordCountDriver \
  /public/data/wordcount/simple-test.txt \
  /users/<学号>/homework1/problem1/output
```

#### 3.2.2 指定队列和资源

```bash
# 指定队列提交作业
hadoop jar wordcount.jar WordCountDriver \
  -Dmapreduce.job.queuename=students \
  -Dmapreduce.map.memory.mb=1024 \
  -Dmapreduce.reduce.memory.mb=1024 \
  /public/data/wordcount/alice-in-wonderland.txt \
  /users/<学号>/homework1/problem1/output
```

### 3.3 作业监控

#### 3.3.1 查看作业状态

```bash
# 查看正在运行的作业
yarn application -list -appStates RUNNING

# 查看所有作业
yarn application -list -appStates ALL

# 查看特定作业状态
yarn application -status application_1234567890123_0001
```

#### 3.3.2 查看作业日志

```bash
# 查看作业日志
yarn logs -applicationId application_1234567890123_0001

# 查看特定容器日志
yarn logs -applicationId application_1234567890123_0001 -containerId container_1234567890123_0001_01_000001
```

#### 3.3.3 Web 界面监控

访问以下 Web 界面查看集群状态：

- **YARN ResourceManager**：<http://hadoop-master:8088/>
- **HDFS NameNode**：<http://hadoop-master:9870/>
- **MapReduce Job History**：<http://hadoop-master:19888/>

---

## 4. 作业示例

### 4.1 WordCount 示例

#### 4.1.1 准备输入数据

```bash
# 创建输入目录
hdfs dfs -mkdir -p /users/<学号>/homework1/problem1/input

# 复制测试数据
hdfs dfs -cp /public/data/wordcount/simple-test.txt /users/<学号>/homework1/problem1/input/
```

#### 4.1.2 运行 WordCount

```bash
# 确保输出目录不存在
hdfs dfs -rm -r /users/<学号>/homework1/problem1/output

# 提交作业
hadoop jar wordcount.jar WordCountDriver \
  /users/<学号>/homework1/problem1/input \
  /users/<学号>/homework1/problem1/output
```

#### 4.1.3 查看结果

```bash
# 查看输出文件
hdfs dfs -ls /users/<学号>/homework1/problem1/output

# 查看结果内容
hdfs dfs -cat /users/<学号>/homework1/problem1/output/part-r-00000

# 下载结果到本地
hdfs dfs -get /users/<学号>/homework1/problem1/output ./
```

### 4.2 处理大文件

```bash
# 使用大文件进行测试
hadoop jar wordcount.jar WordCountDriver \
  -Dmapreduce.job.maps=4 \
  -Dmapreduce.job.reduces=2 \
  /public/data/wordcount/pride-and-prejudice.txt \
  /users/<学号>/homework1/problem1/output-large
```

---

## 5. 常见问题和解决方案

### 5.1 连接问题

**问题：无法连接到集群**:

解决方案：

1. 检查网络连接
2. 确认 hosts 文件配置正确
3. 验证集群服务是否正常运行

```bash
# 测试网络连接
ping hadoop-master

# 测试端口连通性
telnet hadoop-master 9000
telnet hadoop-master 8088
```

### 5.2 权限问题

**问题：Permission denied 错误**:

解决方案：

1. 确认使用正确的用户名（学号）
2. 检查目录权限
3. 确保输出目录不存在

```bash
# 检查个人目录权限
hdfs dfs -ls -d /users/<学号>

# 删除已存在的输出目录
hdfs dfs -rm -r /users/<学号>/homework1/problem1/output
```

### 5.3 存储配额问题

**问题：Quota exceeded 错误**:

解决方案：

1. 清理不需要的文件
2. 检查配额使用情况
3. 联系管理员增加配额

```bash
# 查看配额使用情况
hdfs dfsadmin -getSpaceQuota /users/<学号>

# 查看目录大小
hdfs dfs -du -h /users/<学号>

# 清理临时文件
hdfs dfs -rm -r /users/<学号>/temp
```

### 5.4 作业失败问题

**问题：MapReduce 作业失败**:

解决方案：

1. 查看作业日志
2. 检查输入输出路径
3. 验证 JAR 包和类路径

```bash
# 查看失败作业的日志
yarn logs -applicationId application_1234567890123_0001

# 检查作业配置
yarn application -status application_1234567890123_0001
```

### 5.5 编译问题

**问题：Java 编译错误**:

解决方案：

1. 检查 CLASSPATH 设置
2. 确认 Java 版本兼容性
3. 验证 Hadoop 库路径

```bash
# 检查 Java 版本
java -version

# 检查 CLASSPATH
echo $CLASSPATH

# 重新设置 CLASSPATH
export CLASSPATH=$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/yarn/*
```

### 5.6 客户端环境问题

**问题：Hadoop 客户端连接或配置问题**:

如果遇到客户端环境配置问题，可以使用自动化故障排除脚本：

```bash
# 运行客户端故障排除脚本
./cluster-setup-scripts/10-hadoop_client_troubleshoot.sh

# 指定学生 ID 进行诊断
./cluster-setup-scripts/10-hadoop_client_troubleshoot.sh -s <学号>

# 自动修复配置问题
./cluster-setup-scripts/10-hadoop_client_troubleshoot.sh --fix-config

# 重置环境变量配置
./cluster-setup-scripts/10-hadoop_client_troubleshoot.sh --reset-env

# 详细输出模式
./cluster-setup-scripts/10-hadoop_client_troubleshoot.sh -v
```

该脚本提供以下诊断功能：

1. **基本环境检查**：验证 Java 和 Hadoop 命令可用性
2. **网络连接测试**：检查与集群的网络连通性
3. **配置文件验证**：检查客户端配置文件完整性
4. **环境变量检查**：验证必要的环境变量设置
5. **Hadoop 连接测试**：测试 HDFS 和 YARN 服务连接
6. **性能测试**：执行简单的读写测试
7. **诊断报告生成**：生成详细的诊断报告和修复建议

> **提示**：该脚本会自动检测常见问题并提供修复建议，生成的诊断报告保存在用户主目录中。

---

## 6. 自助服务工具

### 6.1 使用自助服务脚本

如果管理员提供了自助服务脚本，可以使用以下命令：

```bash
# 运行自助服务脚本
./cluster-setup-scripts/09-student_self_service.sh
```

该脚本提供以下功能：

1. 查看个人目录状态
2. 查看存储配额使用情况
3. 查看正在运行的作业
4. 查看作业历史
5. 清理个人临时文件
6. 测试集群连接
7. 查看常见问题解决方案

### 6.2 监控个人状态

```bash
# 创建个人监控脚本
cat > ~/check_status.sh << 'EOF'
#!/bin/bash
STUDENT_ID=$(whoami)

echo "=== 个人状态检查 ==="
echo "用户: $STUDENT_ID"
echo

echo "1. HDFS 个人目录:"
hdfs dfs -ls /users/$STUDENT_ID

echo "2. 存储使用情况:"
hdfs dfs -du -h /users/$STUDENT_ID

echo "3. 当前作业:"
yarn application -list -appStates RUNNING | grep $STUDENT_ID || echo "无运行中的作业"

echo "4. 最近作业历史:"
yarn application -list -appStates ALL | grep $STUDENT_ID | head -5
EOF

chmod +x ~/check_status.sh
```

---

## 7. 最佳实践

### 7.1 文件管理

1. **合理组织目录结构**

   ```text
   /users/<学号>/
   ├── homework1/
   │   ├── problem1/
   │   ├── problem2/
   │   └── problem3/
   ├── input/
   ├── output/
   └── temp/
   ```

2. **及时清理临时文件**

   ```bash
   # 定期清理临时目录
   hdfs dfs -rm -r /users/<学号>/temp/*
   ```

3. **备份重要结果**

   ```bash
   # 下载重要结果到本地
   hdfs dfs -get /users/<学号>/homework1/problem1/output ./backup/
   ```

### 7.2 作业优化

1. **合理设置资源参数**

   ```bash
   # 根据数据大小调整 Map 和 Reduce 任务数
   -Dmapreduce.job.maps=4
   -Dmapreduce.job.reduces=2
   -Dmapreduce.map.memory.mb=1024
   -Dmapreduce.reduce.memory.mb=2048
   ```

2. **使用合适的输入格式**

   ```bash
   # 对于大文件，考虑使用可分割的输入格式
   -Dmapreduce.input.fileinputformat.split.maxsize=134217728
   ```

3. **监控作业进度**

   ```bash
   # 定期检查作业状态
   yarn application -list -appStates RUNNING
   ```

### 7.3 故障排除

1. **保存错误日志**

   ```bash
   # 保存失败作业的日志
   yarn logs -applicationId application_1234567890123_0001 > error.log
   ```

2. **逐步调试**

   ```bash
   # 先用小数据集测试
   hdfs dfs -head -n 100 /public/data/wordcount/alice-in-wonderland.txt > small-test.txt
   hdfs dfs -put small-test.txt /users/<学号>/input/
   ```

3. **寻求帮助**
   - 查看课程文档和 FAQ
   - 使用自助服务工具
   - 联系课程助教或管理员

---

## 附录

### A. 常用命令速查

```bash
# HDFS 操作
hdfs dfs -ls /users/<学号>                    # 列出目录
hdfs dfs -put file.txt /users/<学号>/         # 上传文件
hdfs dfs -get /users/<学号>/file.txt ./       # 下载文件
hdfs dfs -cat /users/<学号>/file.txt          # 查看文件内容
hdfs dfs -rm -r /users/<学号>/dir             # 删除目录

# YARN 操作
yarn application -list                        # 列出所有应用
yarn application -status app_id               # 查看应用状态
yarn logs -applicationId app_id               # 查看应用日志
yarn node -list                              # 列出节点状态

# MapReduce 作业
hadoop jar app.jar MainClass input output    # 提交作业
mapred job -list                             # 列出作业
mapred job -status job_id                    # 查看作业状态
```

### B. 配置文件模板

详细的配置文件模板请参考第 1.4 节的内容。

### C. 错误代码说明

| 错误代码           | 说明         | 解决方案             |
| ------------------ | ------------ | -------------------- |
| Permission denied  | 权限不足     | 检查文件/目录权限    |
| File not found     | 文件不存在   | 确认文件路径正确     |
| Quota exceeded     | 存储配额超限 | 清理文件或联系管理员 |
| Connection refused | 连接被拒绝   | 检查网络和服务状态   |
| Out of memory      | 内存不足     | 调整作业内存参数     |
