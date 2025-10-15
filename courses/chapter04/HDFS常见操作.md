# HDFS 常见操作教学指南

## 1. 教学目标与学习成果

### 1.1 教学目标

通过本课程的学习，学生应能够：

1. **掌握 HDFS 命令行操作**：熟练使用 HDFS 命令行工具进行文件系统的各种操作
2. **理解命令分类体系**：区分文件操作命令、运维管理命令和系统监控命令的用途和使用场景
3. **掌握 HDFS Java SDK 编程**：能够使用 Java API 编写 HDFS 客户端程序
4. **具备故障排除能力**：能够诊断和解决常见的 HDFS 操作问题
5. **培养最佳实践意识**：了解 HDFS 操作的最佳实践和性能优化方法

### 1.2 学习成果

完成本课程后，学生将能够：

- [ ] 独立完成 HDFS 环境的配置和连接测试
- [ ] 熟练执行各类 HDFS 文件操作命令
- [ ] 使用运维命令监控和管理 HDFS 集群
- [ ] 编写 Java 程序实现 HDFS 文件操作
- [ ] 分析和解决 HDFS 操作中的常见问题
- [ ] 应用 HDFS 最佳实践进行实际项目开发

## 2. 课程内容概览

本课程内容分为以下几个模块：

1. **环境准备与配置**
2. **HDFS 文件操作命令**
3. **HDFS 运维管理命令**
4. **HDFS Java SDK 编程**
5. **故障排除与最佳实践**

---

## 3. 环境准备与配置

### 3.1 学习目标

- 验证 HDFS 环境配置
- 掌握环境变量设置
- 学会检查服务状态

### 3.2 环境要求

#### 3.2.1 基础环境配置

确保您的环境满足以下要求：

| 组件     | 版本要求          | 说明                     |
| -------- | ----------------- | ------------------------ |
| 操作系统 | Ubuntu 20.04 LTS+ | 推荐使用 Linux 环境      |
| Java     | OpenJDK 8         | HDFS 运行的基础环境      |
| Hadoop   | 3.4.2             | 本课程使用的 Hadoop 版本 |
| 运行模式 | 伪分布式          | 单节点模拟分布式环境     |

#### 3.2.2 环境变量验证

```bash
# 验证关键环境变量（必须正确配置）
echo "JAVA_HOME: $JAVA_HOME"
echo "HADOOP_HOME: $HADOOP_HOME"
echo "HADOOP_CONF_DIR: $HADOOP_CONF_DIR"
echo "PATH: $PATH"

# 预期输出示例：
# JAVA_HOME: /usr/lib/jvm/java-8-openjdk-amd64
# HADOOP_HOME: /home/hadoop/hadoop
# HADOOP_CONF_DIR: /home/hadoop/hadoop/etc/hadoop
# PATH: /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/hadoop/hadoop/bin:/home/hadoop/hadoop/sbin
```

#### 3.2.3 服务状态检查

```bash
# 检查 Hadoop 相关进程（关键步骤）
jps

# 预期输出应包含以下进程：
# - NameNode（名称节点）
# - DataNode（数据节点）
# - SecondaryNameNode（辅助名称节点）
```

如果服务未运行，执行启动命令：

```bash
# 启动 HDFS 服务
start-dfs.sh

# 再次验证服务状态
jps
```

#### 3.2.4 连接测试

```bash
# 测试 HDFS 连接（验证配置正确性）
hdfs dfsadmin -report

# 成功连接的标志：
# - 显示集群容量信息
# - 显示 DataNode 状态为 "Live datanodes (1)"
# - 无错误信息输出
```

### 3.3 Web UI 访问

#### 3.3.1 本地环境访问

- **HDFS NameNode Web UI**：<http://localhost:9870/>
- **功能**：查看集群状态、浏览文件系统、监控存储使用情况

#### 3.3.2 远程环境访问（阿里云 ECS）

```bash
# 建立 SSH 隧道（在本地机器执行）
ssh -L 9870:localhost:9870 hadoop@你的ECS公网IP

# 然后在本地浏览器访问：http://localhost:9870/
```

---

## 4. HDFS 文件操作命令

### 4.1 学习目标

- 掌握 HDFS 基本文件操作
- 理解 HDFS 与本地文件系统的差异
- 熟练使用文件上传、下载、复制、移动等操作

### 4.2 Linux 与 HDFS 命令对比

为了帮助学生更好地理解和记忆 HDFS 命令，以下是 Linux 文件系统命令与 HDFS 命令的详细对比：

#### 4.2.1 目录操作命令对比

| **功能**         | **Linux 命令**          | **HDFS 命令**                     | **说明**                      |
| ---------------- | ----------------------- | --------------------------------- | ----------------------------- |
| **创建目录**     | `mkdir /path/to/dir`    | `hdfs dfs -mkdir /path/to/dir`    | HDFS 需要添加 `hdfs dfs` 前缀 |
| **递归创建目录** | `mkdir -p /path/to/dir` | `hdfs dfs -mkdir -p /path/to/dir` | 参数用法相同                  |
| **列出目录内容** | `ls /path`              | `hdfs dfs -ls /path`              | 输出格式略有不同              |
| **递归列出目录** | `ls -R /path`           | `hdfs dfs -ls -R /path`           | 参数用法相同                  |
| **显示文件大小** | `ls -lh /path`          | `hdfs dfs -ls -h /path`           | 人类可读格式                  |
| **删除空目录**   | `rmdir /path/to/dir`    | `hdfs dfs -rmdir /path/to/dir`    | 只能删除空目录                |
| **递归删除目录** | `rm -r /path/to/dir`    | `hdfs dfs -rm -r /path/to/dir`    | 可删除非空目录                |

#### 4.2.2 文件传输命令对比

| **功能**       | **Linux 命令**    | **HDFS 命令**                             | **说明**              |
| -------------- | ----------------- | ----------------------------------------- | --------------------- |
| **复制文件**   | `cp source dest`  | `hdfs dfs -put source dest`               | HDFS 用于本地到 HDFS  |
| **移动文件**   | `mv source dest`  | `hdfs dfs -put source dest` + `rm source` | HDFS 需要两步操作     |
| **下载文件**   | `cp remote local` | `hdfs dfs -get remote local`              | HDFS 用于 HDFS 到本地 |
| **复制到本地** | `cp file /tmp/`   | `hdfs dfs -copyToLocal file /tmp/`        | `get` 的别名          |
| **从本地复制** | `cp /tmp/file .`  | `hdfs dfs -copyFromLocal /tmp/file .`     | `put` 的别名          |

#### 4.2.3 文件查看命令对比

| **功能**         | **Linux 命令**  | **HDFS 命令**              | **说明**       |
| ---------------- | --------------- | -------------------------- | -------------- |
| **查看文件内容** | `cat filename`  | `hdfs dfs -cat filename`   | 功能完全相同   |
| **查看文件开头** | `head filename` | `hdfs dfs -head filename`  | 默认显示 1KB   |
| **查看文件结尾** | `tail filename` | `hdfs dfs -tail filename`  | 默认显示 1KB   |
| **查看文件信息** | `stat filename` | `hdfs dfs -stat filename`  | 显示文件属性   |
| **查看文件大小** | `du filename`   | `hdfs dfs -du filename`    | 显示磁盘使用量 |
| **查看目录大小** | `du -h /path`   | `hdfs dfs -du -h /path`    | 人类可读格式   |
| **汇总目录大小** | `du -sh /path`  | `hdfs dfs -du -s -h /path` | 汇总显示       |

#### 4.2.4 文件管理命令对比

| **功能**         | **Linux 命令**       | **HDFS 命令**                  | **说明**      |
| ---------------- | -------------------- | ------------------------------ | ------------- |
| **复制文件**     | `cp source dest`     | `hdfs dfs -cp source dest`     | HDFS 内部复制 |
| **移动文件**     | `mv source dest`     | `hdfs dfs -mv source dest`     | 功能相同      |
| **重命名文件**   | `mv oldname newname` | `hdfs dfs -mv oldname newname` | 功能相同      |
| **删除文件**     | `rm filename`        | `hdfs dfs -rm filename`        | 功能相同      |
| **强制删除**     | `rm -f filename`     | `hdfs dfs -rm -f filename`     | 强制删除      |
| **删除多个文件** | `rm file1 file2`     | `hdfs dfs -rm file1 file2`     | 支持多文件    |
| **通配符删除**   | `rm *.txt`           | `hdfs dfs -rm *.txt`           | 支持通配符    |

#### 4.2.5 权限管理命令对比

| **功能**             | **Linux 命令**              | **HDFS 命令**                         | **说明** |
| -------------------- | --------------------------- | ------------------------------------- | -------- |
| **修改权限（数字）** | `chmod 644 filename`        | `hdfs dfs -chmod 644 filename`        | 功能相同 |
| **修改权限（符号）** | `chmod u+x filename`        | `hdfs dfs -chmod u+x filename`        | 功能相同 |
| **递归修改权限**     | `chmod -R 755 /path`        | `hdfs dfs -chmod -R 755 /path`        | 功能相同 |
| **修改所有者**       | `chown user filename`       | `hdfs dfs -chown user filename`       | 功能相同 |
| **修改所有者和组**   | `chown user:group filename` | `hdfs dfs -chown user:group filename` | 功能相同 |
| **修改组**           | `chgrp group filename`      | `hdfs dfs -chgrp group filename`      | 功能相同 |
| **递归修改所有者**   | `chown -R user:group /path` | `hdfs dfs -chown -R user:group /path` | 功能相同 |

#### 4.2.6 学习记忆技巧

**相似性总结**：

1. **命令结构**：HDFS 命令只需在 Linux 命令前加 `hdfs dfs -` 前缀
2. **参数选项**：大部分参数选项保持一致（如 `-R`, `-h`, `-p` 等）
3. **路径格式**：HDFS 使用绝对路径，以 `/` 开头

**主要差异**：

1. **文件传输**：Linux 使用 `cp`，HDFS 区分 `put`（上传）和 `get`（下载）
2. **命令前缀**：HDFS 所有命令都需要 `hdfs dfs -` 前缀
3. **回收站机制**：HDFS 删除的文件默认进入回收站，可以恢复

**学习建议**：

1. 先熟练掌握 Linux 基本文件操作命令
2. 理解 HDFS 命令只是在 Linux 命令基础上加前缀
3. 重点记忆文件传输命令的差异（`put` vs `get`）
4. 注意 HDFS 特有的功能（如回收站、副本数等）

### 4.3 命令分类说明

HDFS 文件操作命令主要分为以下几类：

| **操作类型** | **主要命令**                                 | **功能描述**                  |
| ------------ | -------------------------------------------- | ----------------------------- |
| **目录操作** | `mkdir`, `ls`, `rmdir`                       | 创建、查看、删除目录          |
| **文件传输** | `put`, `get`, `copyFromLocal`, `copyToLocal` | 本地与 HDFS 间文件传输        |
| **文件查看** | `cat`, `head`, `tail`, `stat`                | 查看文件内容和属性            |
| **文件管理** | `cp`, `mv`, `rm`                             | HDFS 内部文件复制、移动、删除 |
| **权限管理** | `chmod`, `chown`, `chgrp`                    | 修改文件权限和所有者          |

### 4.4 目录操作命令

#### 4.4.1 创建目录

```bash
# 基本语法：hdfs dfs -mkdir [选项] <目录路径>

# 创建单个目录
hdfs dfs -mkdir /user

# 创建多级目录（-p 参数：父目录不存在时自动创建）
hdfs dfs -mkdir -p /user/hadoop/data

# 一次创建多个目录
hdfs dfs -mkdir /user/hadoop/input /user/hadoop/output /user/hadoop/temp

# 验证目录创建结果
hdfs dfs -ls /user
hdfs dfs -ls /user/hadoop
```

**命令解析**：

- `-p`：递归创建父目录，类似于 Linux 的 `mkdir -p`
- 路径必须以 `/` 开头，表示 HDFS 根目录
- 支持一次创建多个目录，提高操作效率

#### 4.4.2 查看目录内容

```bash
# 基本语法：hdfs dfs -ls [选项] <目录路径>

# 列出根目录内容
hdfs dfs -ls /

# 列出指定目录内容
hdfs dfs -ls /user

# 递归列出所有子目录内容（-R 参数）
hdfs dfs -ls -R /user

# 显示人类可读的文件大小（-h 参数）
hdfs dfs -ls -h /user/hadoop

# 显示详细信息（-l 参数，默认已包含）
hdfs dfs -ls -l /user/hadoop
```

**输出格式解析**：

```bash
# 示例输出：
# drwxr-xr-x   - hadoop supergroup          0 2024-01-01 12:00 /user/hadoop/data
# ↑权限      ↑副本数 ↑所有者 ↑组           ↑大小 ↑修改时间        ↑路径
```

#### 4.4.3 删除目录

```bash
# 删除空目录
hdfs dfs -rmdir /user/hadoop/empty_dir

# 递归删除目录及其所有内容（-r 参数）
hdfs dfs -rm -r /user/hadoop/temp

# 强制删除，跳过回收站（-skipTrash 参数）
hdfs dfs -rm -r -skipTrash /user/hadoop/temp
```

**注意事项**：

- `rmdir` 只能删除空目录
- `rm -r` 可以删除非空目录
- 删除的文件默认进入回收站，可以恢复
- `-skipTrash` 参数会永久删除文件，无法恢复

### 4.5 文件传输命令

#### 4.5.1 上传文件到 HDFS

```bash
# 准备测试数据
echo "Hello HDFS World!" > /tmp/test.txt
echo -e "Line 1\nLine 2\nLine 3" > /tmp/multiline.txt

# 基本语法：hdfs dfs -put <本地路径> <HDFS路径>

# 上传单个文件
hdfs dfs -put /tmp/test.txt /user/hadoop/

# 上传文件并重命名
hdfs dfs -put /tmp/test.txt /user/hadoop/renamed_test.txt

# 上传多个文件到指定目录
hdfs dfs -put /tmp/test.txt /tmp/multiline.txt /user/hadoop/input/

# 上传整个目录
mkdir -p /tmp/local_data
echo "File 1 content" > /tmp/local_data/file1.txt
echo "File 2 content" > /tmp/local_data/file2.txt
hdfs dfs -put /tmp/local_data /user/hadoop/

# 从标准输入上传（- 表示标准输入）
echo "Content from stdin" | hdfs dfs -put - /user/hadoop/stdin_file.txt
```

**命令对比**：

- `put` 和 `copyFromLocal` 功能相同，`put` 更常用
- 支持通配符：`hdfs dfs -put /tmp/*.txt /user/hadoop/`
- 如果目标文件已存在，会报错（除非使用 `-f` 强制覆盖）

#### 4.5.2 从 HDFS 下载文件

```bash
# 基本语法：hdfs dfs -get <HDFS路径> <本地路径>

# 下载单个文件
hdfs dfs -get /user/hadoop/test.txt /tmp/downloaded_test.txt

# 下载文件到当前目录（. 表示当前目录）
hdfs dfs -get /user/hadoop/test.txt .

# 下载多个文件
hdfs dfs -get /user/hadoop/test.txt /user/hadoop/multiline.txt /tmp/

# 下载整个目录
hdfs dfs -get /user/hadoop/local_data /tmp/downloaded_data

# 验证下载结果
ls -la /tmp/downloaded_test.txt
cat /tmp/downloaded_test.txt
```

### 4.6 文件查看命令

#### 4.6.1 查看文件内容

```bash
# 查看完整文件内容（类似 Linux cat）
hdfs dfs -cat /user/hadoop/test.txt

# 查看文件开头（默认显示前 1KB）
hdfs dfs -head /user/hadoop/multiline.txt

# 查看文件结尾（默认显示后 1KB）
hdfs dfs -tail /user/hadoop/multiline.txt

# 结合管道查看指定行数
hdfs dfs -cat /user/hadoop/large_file.txt | head -10
hdfs dfs -cat /user/hadoop/large_file.txt | tail -10
```

#### 4.6.2 查看文件属性

```bash
# 查看文件详细信息（stat 命令）
hdfs dfs -stat "%n %o %r %u %g %b %y" /user/hadoop/test.txt
# 输出格式：文件名 块大小 副本数 用户 组 字节数 修改时间

# 查看文件大小
hdfs dfs -du /user/hadoop/test.txt

# 查看目录大小（-h 参数显示人类可读格式）
hdfs dfs -du -h /user/hadoop/

# 查看目录总大小（-s 参数汇总显示）
hdfs dfs -du -s -h /user/hadoop/
```

### 4.7 文件管理命令

#### 4.7.1 HDFS 内部复制

```bash
# 基本语法：hdfs dfs -cp <源路径> <目标路径>

# 复制文件
hdfs dfs -cp /user/hadoop/test.txt /user/hadoop/backup/test_backup.txt

# 复制目录
hdfs dfs -cp /user/hadoop/input /user/hadoop/input_backup

# 复制多个文件到目录
hdfs dfs -cp /user/hadoop/test.txt /user/hadoop/multiline.txt /user/hadoop/backup/
```

#### 4.7.2 HDFS 内部移动

```bash
# 基本语法：hdfs dfs -mv <源路径> <目标路径>

# 移动文件
hdfs dfs -mv /user/hadoop/test.txt /user/hadoop/archive/

# 重命名文件（移动到同一目录下的新名称）
hdfs dfs -mv /user/hadoop/old_name.txt /user/hadoop/new_name.txt

# 移动目录
hdfs dfs -mv /user/hadoop/temp /user/hadoop/archive/temp_moved
```

#### 4.7.3 删除文件

```bash
# 删除单个文件
hdfs dfs -rm /user/hadoop/test.txt

# 删除多个文件
hdfs dfs -rm /user/hadoop/file1.txt /user/hadoop/file2.txt

# 使用通配符删除
hdfs dfs -rm /user/hadoop/*.txt
hdfs dfs -rm /user/hadoop/temp_*

# 递归删除目录
hdfs dfs -rm -r /user/hadoop/temp_directory
```

### 4.8 权限管理命令

#### 4.8.1 修改文件权限

```bash
# 基本语法：hdfs dfs -chmod <权限> <文件路径>

# 使用数字模式修改权限
hdfs dfs -chmod 644 /user/hadoop/test.txt

# 使用符号模式修改权限
hdfs dfs -chmod u+x /user/hadoop/script.sh
hdfs dfs -chmod g-w /user/hadoop/test.txt
hdfs dfs -chmod o+r /user/hadoop/test.txt

# 递归修改目录权限
hdfs dfs -chmod -R 755 /user/hadoop/data/
```

#### 4.8.2 修改所有者和组

```bash
# 修改文件所有者
hdfs dfs -chown newowner /user/hadoop/test.txt

# 修改文件所有者和组
hdfs dfs -chown newowner:newgroup /user/hadoop/test.txt

# 修改文件组
hdfs dfs -chgrp newgroup /user/hadoop/test.txt

# 递归修改目录所有者
hdfs dfs -chown -R hadoop:hadoop /user/hadoop/data/
```

### 4.9 实践练习：文件操作综合演示

```bash
# 练习 1：完整的文件操作流程
echo "=== HDFS 文件操作综合练习 ==="

# 1. 创建工作目录结构
hdfs dfs -mkdir -p /user/hadoop/practice/{input,output,backup}

# 2. 准备本地测试数据
mkdir -p /tmp/hdfs_practice
echo "This is a test file for HDFS operations" > /tmp/hdfs_practice/test1.txt
echo -e "Line 1\nLine 2\nLine 3\nLine 4\nLine 5" > /tmp/hdfs_practice/test2.txt
echo "Large file content for testing" > /tmp/hdfs_practice/large.txt

# 3. 上传文件到 HDFS
hdfs dfs -put /tmp/hdfs_practice/* /user/hadoop/practice/input/

# 4. 验证上传结果
echo "上传的文件列表："
hdfs dfs -ls -h /user/hadoop/practice/input/

# 5. 查看文件内容
echo "test1.txt 内容："
hdfs dfs -cat /user/hadoop/practice/input/test1.txt

# 6. 复制文件到备份目录
hdfs dfs -cp /user/hadoop/practice/input/* /user/hadoop/practice/backup/

# 7. 修改文件权限
hdfs dfs -chmod 644 /user/hadoop/practice/input/test1.txt

# 8. 查看文件详细信息
echo "文件详细信息："
hdfs dfs -stat "%n %o %r %u %g %b %y" /user/hadoop/practice/input/test1.txt

# 9. 下载文件到本地验证
hdfs dfs -get /user/hadoop/practice/input/test1.txt /tmp/downloaded_test1.txt
echo "下载文件内容验证："
cat /tmp/downloaded_test1.txt

# 10. 清理测试数据
hdfs dfs -rm -r /user/hadoop/practice/
rm -rf /tmp/hdfs_practice /tmp/downloaded_test1.txt

echo "=== 练习完成 ==="
```

---

## 5. HDFS 运维管理命令

### 5.1 学习目标

- 掌握 HDFS 集群状态监控
- 学会使用系统管理命令
- 理解 HDFS 的运维操作

### 5.2 命令分类说明

HDFS 运维管理命令主要分为以下几类：

| **操作类型**     | **主要命令**                                    | **功能描述**           |
| ---------------- | ----------------------------------------------- | ---------------------- |
| **集群监控**     | `dfsadmin -report`, `dfsadmin -printTopology`   | 查看集群状态和拓扑     |
| **文件系统检查** | `fsck`                                          | 检查文件系统完整性     |
| **安全模式管理** | `dfsadmin -safemode`                            | 管理 NameNode 安全模式 |
| **配额管理**     | `dfsadmin -setQuota`, `dfsadmin -setSpaceQuota` | 设置目录配额限制       |
| **快照管理**     | `dfs -createSnapshot`, `dfs -deleteSnapshot`    | 管理目录快照           |
| **块管理**       | `fsck -blocks`, `balancer`                      | 管理数据块和集群平衡   |

### 5.3 集群状态监控

#### 5.3.1 查看集群报告

```bash
# 查看详细的集群状态报告
hdfs dfsadmin -report

# 查看简要报告
hdfs dfsadmin -report -short

# 查看实时状态
hdfs dfsadmin -report -live
```

**报告内容解析**：

```bash
# 示例输出解析：
Configured Capacity: 50000000000 (46.57 GB)    # 配置的总容量
Present Capacity: 45000000000 (41.91 GB)       # 当前可用容量
DFS Remaining: 44000000000 (40.98 GB)          # 剩余空间
DFS Used: 1000000000 (953.67 MB)               # 已使用空间
DFS Used%: 2.22%                                # 使用率
Under replicated blocks: 0                      # 副本不足的块数
Blocks with corrupt replicas: 0                 # 损坏的块数
Missing blocks: 0                               # 丢失的块数
```

#### 5.3.2 查看集群拓扑

```bash
# 查看集群拓扑结构
hdfs dfsadmin -printTopology

# 示例输出：
# Rack: /default-rack
#    127.0.0.1:9866 (localhost)
```

### 5.4 文件系统检查

#### 5.4.1 基本文件系统检查

```bash
# 检查整个文件系统
hdfs fsck /

# 检查指定目录
hdfs fsck /user/hadoop

# 详细检查（显示文件和块信息）
hdfs fsck /user/hadoop -files -blocks

# 检查并显示块位置信息
hdfs fsck /user/hadoop -files -blocks -locations

# 只列出损坏的文件
hdfs fsck / -list-corruptfileblocks
```

**检查结果解析**：

```bash
# 健康状态指标：
# - Total size: 文件系统总大小
# - Total dirs: 目录总数
# - Total files: 文件总数
# - Total blocks: 块总数
# - Minimally replicated blocks: 最小副本数的块
# - Over-replicated blocks: 超额副本的块
# - Under-replicated blocks: 副本不足的块
# - Mis-replicated blocks: 错误放置的块
# - Default replication factor: 默认副本因子
# - Average block replication: 平均块副本数
# - Corrupt blocks: 损坏的块
# - Missing replicas: 丢失的副本
```

#### 5.4.2 高级检查选项

```bash
# 检查特定文件的块信息
hdfs fsck /user/hadoop/large_file.txt -files -blocks -locations

# 检查并尝试移动损坏的文件到 /lost+found
hdfs fsck / -move

# 检查并删除损坏的文件
hdfs fsck / -delete
```

### 5.5 安全模式管理

#### 5.5.1 安全模式操作

```bash
# 查看安全模式状态
hdfs dfsadmin -safemode get

# 手动进入安全模式
hdfs dfsadmin -safemode enter

# 手动退出安全模式
hdfs dfsadmin -safemode leave

# 等待安全模式自动退出
hdfs dfsadmin -safemode wait
```

**安全模式说明**：

- 安全模式是 NameNode 的一种保护状态
- 在安全模式下，文件系统只读，不能进行写操作
- NameNode 启动时会自动进入安全模式
- 当达到最小副本数要求时，会自动退出安全模式

### 5.6 配额管理

#### 5.6.1 设置目录配额

```bash
# 创建测试目录
hdfs dfs -mkdir /user/hadoop/quota_test

# 设置文件数量配额（最多 100 个文件）
hdfs dfsadmin -setQuota 100 /user/hadoop/quota_test

# 设置空间配额（最多 1GB）
hdfs dfsadmin -setSpaceQuota 1g /user/hadoop/quota_test

# 查看配额信息
hdfs dfs -count -q /user/hadoop/quota_test
```

**配额信息输出格式**：

```bash
# 示例输出：
#    QUOTA   REM_QUOTA     SPACE_QUOTA REM_SPACE_QUOTA    DIR_COUNT   FILE_COUNT       CONTENT_SIZE PATHNAME
#      100          99              1g              1g            1            0                  0 /user/hadoop/quota_test
```

#### 5.6.2 清除配额

```bash
# 清除文件数量配额
hdfs dfsadmin -clrQuota /user/hadoop/quota_test

# 清除空间配额
hdfs dfsadmin -clrSpaceQuota /user/hadoop/quota_test

# 验证配额清除
hdfs dfs -count -q /user/hadoop/quota_test
```

### 5.7 快照管理

#### 5.7.1 快照操作

```bash
# 创建测试目录和文件
hdfs dfs -mkdir /user/hadoop/snapshot_test
hdfs dfs -put /tmp/test.txt /user/hadoop/snapshot_test/

# 为目录启用快照功能
hdfs dfsadmin -allowSnapshot /user/hadoop/snapshot_test

# 创建快照
hdfs dfs -createSnapshot /user/hadoop/snapshot_test snapshot_$(date +%Y%m%d_%H%M%S)

# 列出快照
hdfs dfs -ls /user/hadoop/snapshot_test/.snapshot/

# 查看快照内容
hdfs dfs -ls /user/hadoop/snapshot_test/.snapshot/snapshot_*/
```

#### 5.7.2 快照管理操作

```bash
# 重命名快照
hdfs dfs -renameSnapshot /user/hadoop/snapshot_test snapshot_20240101_120000 backup_20240101

# 删除快照
hdfs dfs -deleteSnapshot /user/hadoop/snapshot_test backup_20240101

# 禁用快照功能
hdfs dfsadmin -disallowSnapshot /user/hadoop/snapshot_test
```

### 5.8 块管理和集群平衡

#### 5.8.1 查看块信息

```bash
# 查看文件的块分布
hdfs fsck /user/hadoop/large_file.txt -files -blocks -locations

# 查看块大小配置
hdfs getconf -confKey dfs.blocksize

# 查看副本因子配置
hdfs getconf -confKey dfs.replication
```

#### 5.8.2 集群平衡（适用于多节点环境）

```bash
# 启动集群平衡器（单节点环境不需要）
hdfs balancer

# 设置平衡阈值（默认 10%）
hdfs balancer -threshold 5

# 设置带宽限制（KB/s）
hdfs balancer -bandwidth 1000
```

### 5.9 实践练习：运维管理综合演示

```bash
# 练习 2：HDFS 运维管理综合练习
echo "=== HDFS 运维管理综合练习 ==="

# 1. 检查集群状态
echo "1. 集群状态报告："
hdfs dfsadmin -report -short

# 2. 检查文件系统健康状态
echo "2. 文件系统检查："
hdfs fsck / -files | head -10

# 3. 查看安全模式状态
echo "3. 安全模式状态："
hdfs dfsadmin -safemode get

# 4. 创建配额管理演示
echo "4. 配额管理演示："
hdfs dfs -mkdir -p /user/hadoop/quota_demo
hdfs dfsadmin -setQuota 10 /user/hadoop/quota_demo
hdfs dfsadmin -setSpaceQuota 100m /user/hadoop/quota_demo
hdfs dfs -count -q /user/hadoop/quota_demo

# 5. 快照管理演示
echo "5. 快照管理演示："
hdfs dfs -mkdir -p /user/hadoop/snapshot_demo
echo "snapshot test content" | hdfs dfs -put - /user/hadoop/snapshot_demo/test.txt
hdfs dfsadmin -allowSnapshot /user/hadoop/snapshot_demo
hdfs dfs -createSnapshot /user/hadoop/snapshot_demo demo_snapshot
hdfs dfs -ls /user/hadoop/snapshot_demo/.snapshot/

# 6. 查看系统配置
echo "6. 系统配置信息："
echo "块大小: $(hdfs getconf -confKey dfs.blocksize)"
echo "副本因子: $(hdfs getconf -confKey dfs.replication)"

# 7. 清理演示数据
echo "7. 清理演示数据："
hdfs dfs -deleteSnapshot /user/hadoop/snapshot_demo demo_snapshot
hdfs dfsadmin -disallowSnapshot /user/hadoop/snapshot_demo
hdfs dfs -rm -r /user/hadoop/snapshot_demo
hdfs dfsadmin -clrQuota /user/hadoop/quota_demo
hdfs dfsadmin -clrSpaceQuota /user/hadoop/quota_demo
hdfs dfs -rm -r /user/hadoop/quota_demo

echo "=== 运维管理练习完成 ==="
```

---

## 6. HDFS Java SDK 编程

### 6.1 学习目标

- 掌握 HDFS Java API 的基本使用
- 学会编写 HDFS 客户端程序
- 理解 HDFS 编程的最佳实践

### 6.2 开发环境准备

#### 6.2.1 Maven 依赖配置

创建 Maven 项目并添加以下依赖：

```xml
<!-- pom.xml -->
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>hdfs-client</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <hadoop.version>3.4.2</hadoop.version>
    </properties>

    <dependencies>
        <!-- Hadoop Client 依赖 -->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>${hadoop.version}</version>
        </dependency>

        <!-- 日志依赖 -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
            <version>1.7.30</version>
        </dependency>

        <!-- 测试依赖 -->
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.13.2</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

#### 6.2.2 日志配置

创建 `src/main/resources/log4j.properties`：

```properties
# log4j.properties
log4j.rootLogger=INFO, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n
```

### 6.3 HDFS 客户端基础操作

#### 6.3.1 建立 HDFS 连接

```java
package com.example.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URI;

/**
 * HDFS 客户端连接管理类
 * 功能：建立和管理 HDFS 连接
 */
public class HDFSClient {

    private FileSystem fileSystem;
    private Configuration configuration;

    // HDFS 连接配置
    private static final String HDFS_URI = "hdfs://localhost:9000";
    private static final String USER = "hadoop";

    /**
     * 初始化 HDFS 客户端连接
     * @throws IOException 连接异常
     */
    public void init() throws IOException {
        // 1. 创建配置对象
        configuration = new Configuration();

        // 2. 设置 HDFS 相关配置（可选，通常从配置文件读取）
        configuration.set("fs.defaultFS", HDFS_URI);
        configuration.set("dfs.replication", "1");

        // 3. 建立 HDFS 连接
        try {
            fileSystem = FileSystem.get(
                URI.create(HDFS_URI),
                configuration,
                USER
            );
            System.out.println("HDFS 连接建立成功: " + HDFS_URI);
        } catch (Exception e) {
            throw new IOException("HDFS 连接失败", e);
        }
    }

    /**
     * 关闭 HDFS 连接
     */
    public void close() {
        if (fileSystem != null) {
            try {
                fileSystem.close();
                System.out.println("HDFS 连接已关闭");
            } catch (IOException e) {
                System.err.println("关闭 HDFS 连接时出错: " + e.getMessage());
            }
        }
    }

    /**
     * 获取 FileSystem 对象
     * @return FileSystem 实例
     */
    public FileSystem getFileSystem() {
        return fileSystem;
    }

    /**
     * 测试连接
     */
    public static void main(String[] args) {
        HDFSClient client = new HDFSClient();
        try {
            // 建立连接
            client.init();

            // 测试连接（获取根目录状态）
            FileSystem fs = client.getFileSystem();
            System.out.println("HDFS 根目录: " + fs.getHomeDirectory());
            System.out.println("HDFS 工作目录: " + fs.getWorkingDirectory());

        } catch (IOException e) {
            System.err.println("连接测试失败: " + e.getMessage());
        } finally {
            // 关闭连接
            client.close();
        }
    }
}
```

#### 6.3.2 目录操作实现

```java
package com.example.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

/**
 * HDFS 目录操作类
 * 功能：创建、删除、列出目录
 */
public class HDFSDirectoryOperations {

    private FileSystem fileSystem;

    public HDFSDirectoryOperations(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    /**
     * 创建目录
     * @param dirPath 目录路径
     * @return 创建是否成功
     */
    public boolean createDirectory(String dirPath) {
        try {
            Path path = new Path(dirPath);

            // 检查目录是否已存在
            if (fileSystem.exists(path)) {
                System.out.println("目录已存在: " + dirPath);
                return true;
            }

            // 创建目录（自动创建父目录）
            boolean result = fileSystem.mkdirs(path);

            if (result) {
                System.out.println("目录创建成功: " + dirPath);
            } else {
                System.out.println("目录创建失败: " + dirPath);
            }

            return result;

        } catch (IOException e) {
            System.err.println("创建目录时出错: " + e.getMessage());
            return false;
        }
    }

    /**
     * 删除目录
     * @param dirPath 目录路径
     * @param recursive 是否递归删除
     * @return 删除是否成功
     */
    public boolean deleteDirectory(String dirPath, boolean recursive) {
        try {
            Path path = new Path(dirPath);

            // 检查目录是否存在
            if (!fileSystem.exists(path)) {
                System.out.println("目录不存在: " + dirPath);
                return false;
            }

            // 删除目录
            boolean result = fileSystem.delete(path, recursive);

            if (result) {
                System.out.println("目录删除成功: " + dirPath);
            } else {
                System.out.println("目录删除失败: " + dirPath);
            }

            return result;

        } catch (IOException e) {
            System.err.println("删除目录时出错: " + e.getMessage());
            return false;
        }
    }

    /**
     * 列出目录内容
     * @param dirPath 目录路径
     */
    public void listDirectory(String dirPath) {
        try {
            Path path = new Path(dirPath);

            // 检查路径是否存在
            if (!fileSystem.exists(path)) {
                System.out.println("路径不存在: " + dirPath);
                return;
            }

            // 获取目录内容
            FileStatus[] fileStatuses = fileSystem.listStatus(path);

            System.out.println("\n目录内容: " + dirPath);
            System.out.println("权限\t\t副本数\t所有者\t\t组\t\t大小\t\t修改时间\t\t\t名称");
            System.out.println("-------------------------------------------------------------------------------------");

            for (FileStatus status : fileStatuses) {
                String type = status.isDirectory() ? "d" : "-";
                String permission = type + status.getPermission().toString();
                short replication = status.getReplication();
                String owner = status.getOwner();
                String group = status.getGroup();
                long size = status.getLen();
                long modTime = status.getModificationTime();
                String name = status.getPath().getName();

                System.out.printf("%s\t%d\t%s\t\t%s\t\t%d\t\t%tF %<tT\t%s%n",
                    permission, replication, owner, group, size, modTime, name);
            }

        } catch (IOException e) {
            System.err.println("列出目录内容时出错: " + e.getMessage());
        }
    }

    /**
     * 检查路径是否存在
     * @param path 路径
     * @return 是否存在
     */
    public boolean exists(String path) {
        try {
            return fileSystem.exists(new Path(path));
        } catch (IOException e) {
            System.err.println("检查路径存在性时出错: " + e.getMessage());
            return false;
        }
    }

    /**
     * 测试目录操作
     */
    public static void main(String[] args) {
        HDFSClient client = new HDFSClient();
        try {
            // 建立连接
            client.init();

            HDFSDirectoryOperations dirOps = new HDFSDirectoryOperations(client.getFileSystem());

            // 测试目录操作
            String testDir = "/user/hadoop/java_test";

            // 1. 创建目录
            dirOps.createDirectory(testDir);
            dirOps.createDirectory(testDir + "/subdir1");
            dirOps.createDirectory(testDir + "/subdir2");

            // 2. 列出目录内容
            dirOps.listDirectory(testDir);

            // 3. 检查路径存在性
            System.out.println("目录是否存在: " + dirOps.exists(testDir));

            // 4. 删除目录
            dirOps.deleteDirectory(testDir, true);

        } catch (IOException e) {
            System.err.println("测试失败: " + e.getMessage());
        } finally {
            client.close();
        }
    }
}
```

#### 6.3.3 文件操作实现

```java
package com.example.hdfs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

/**
 * HDFS 文件操作类
 * 功能：上传、下载、读取、写入文件
 */
public class HDFSFileOperations {

    private FileSystem fileSystem;

    public HDFSFileOperations(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    /**
     * 上传本地文件到 HDFS
     * @param localPath 本地文件路径
     * @param hdfsPath HDFS 目标路径
     * @return 上传是否成功
     */
    public boolean uploadFile(String localPath, String hdfsPath) {
        try {
            Path localFilePath = new Path(localPath);
            Path hdfsFilePath = new Path(hdfsPath);

            // 检查本地文件是否存在
            File localFile = new File(localPath);
            if (!localFile.exists()) {
                System.out.println("本地文件不存在: " + localPath);
                return false;
            }

            // 如果 HDFS 文件已存在，先删除
            if (fileSystem.exists(hdfsFilePath)) {
                fileSystem.delete(hdfsFilePath, false);
                System.out.println("已删除已存在的 HDFS 文件: " + hdfsPath);
            }

            // 执行上传
            fileSystem.copyFromLocalFile(localFilePath, hdfsFilePath);
            System.out.println("文件上传成功: " + localPath + " -> " + hdfsPath);

            return true;

        } catch (IOException e) {
            System.err.println("上传文件时出错: " + e.getMessage());
            return false;
        }
    }

    /**
     * 从 HDFS 下载文件到本地
     * @param hdfsPath HDFS 文件路径
     * @param localPath 本地目标路径
     * @return 下载是否成功
     */
    public boolean downloadFile(String hdfsPath, String localPath) {
        try {
            Path hdfsFilePath = new Path(hdfsPath);
            Path localFilePath = new Path(localPath);

            // 检查 HDFS 文件是否存在
            if (!fileSystem.exists(hdfsFilePath)) {
                System.out.println("HDFS 文件不存在: " + hdfsPath);
                return false;
            }

            // 执行下载
            fileSystem.copyToLocalFile(hdfsFilePath, localFilePath);
            System.out.println("文件下载成功: " + hdfsPath + " -> " + localPath);

            return true;

        } catch (IOException e) {
            System.err.println("下载文件时出错: " + e.getMessage());
            return false;
        }
    }

    /**
     * 读取 HDFS 文件内容
     * @param hdfsPath HDFS 文件路径
     * @return 文件内容
     */
    public String readFile(String hdfsPath) {
        FSDataInputStream inputStream = null;
        try {
            Path path = new Path(hdfsPath);

            // 检查文件是否存在
            if (!fileSystem.exists(path)) {
                System.out.println("文件不存在: " + hdfsPath);
                return null;
            }

            // 打开文件输入流
            inputStream = fileSystem.open(path);

            // 读取文件内容
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            IOUtils.copyBytes(inputStream, outputStream, 4096, false);

            String content = outputStream.toString("UTF-8");
            System.out.println("文件读取成功: " + hdfsPath);

            return content;

        } catch (IOException e) {
            System.err.println("读取文件时出错: " + e.getMessage());
            return null;
        } finally {
            // 关闭输入流
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    System.err.println("关闭输入流时出错: " + e.getMessage());
                }
            }
        }
    }

    /**
     * 写入内容到 HDFS 文件
     * @param hdfsPath HDFS 文件路径
     * @param content 要写入的内容
     * @return 写入是否成功
     */
    public boolean writeFile(String hdfsPath, String content) {
        FSDataOutputStream outputStream = null;
        try {
            Path path = new Path(hdfsPath);

            // 如果文件已存在，先删除
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, false);
            }

            // 创建文件输出流
            outputStream = fileSystem.create(path);

            // 写入内容
            outputStream.writeBytes(content);
            outputStream.flush();

            System.out.println("文件写入成功: " + hdfsPath);
            return true;

        } catch (IOException e) {
            System.err.println("写入文件时出错: " + e.getMessage());
            return false;
        } finally {
            // 关闭输出流
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    System.err.println("关闭输出流时出错: " + e.getMessage());
                }
            }
        }
    }

    /**
     * 复制 HDFS 文件
     * @param srcPath 源文件路径
     * @param dstPath 目标文件路径
     * @return 复制是否成功
     */
    public boolean copyFile(String srcPath, String dstPath) {
        try {
            Path src = new Path(srcPath);
            Path dst = new Path(dstPath);

            // 检查源文件是否存在
            if (!fileSystem.exists(src)) {
                System.out.println("源文件不存在: " + srcPath);
                return false;
            }

            // 执行复制
            boolean result = org.apache.hadoop.fs.util.FileUtil.copy(
                fileSystem, src, fileSystem, dst, false, fileSystem.getConf()
            );

            if (result) {
                System.out.println("文件复制成功: " + srcPath + " -> " + dstPath);
            } else {
                System.out.println("文件复制失败");
            }

            return result;

        } catch (IOException e) {
            System.err.println("复制文件时出错: " + e.getMessage());
            return false;
        }
    }

    /**
     * 删除 HDFS 文件
     * @param hdfsPath HDFS 文件路径
     * @return 删除是否成功
     */
    public boolean deleteFile(String hdfsPath) {
        try {
            Path path = new Path(hdfsPath);

            // 检查文件是否存在
            if (!fileSystem.exists(path)) {
                System.out.println("文件不存在: " + hdfsPath);
                return false;
            }

            // 删除文件
            boolean result = fileSystem.delete(path, false);

            if (result) {
                System.out.println("文件删除成功: " + hdfsPath);
            } else {
                System.out.println("文件删除失败: " + hdfsPath);
            }

            return result;

        } catch (IOException e) {
            System.err.println("删除文件时出错: " + e.getMessage());
            return false;
        }
    }

    /**
     * 测试文件操作
     */
    public static void main(String[] args) {
        HDFSClient client = new HDFSClient();
        try {
            // 建立连接
            client.init();

            HDFSFileOperations fileOps = new HDFSFileOperations(client.getFileSystem());

            // 准备测试数据
            String testContent = "这是一个 HDFS Java API 测试文件\n" +
                               "包含中文和英文内容\n" +
                               "测试时间: " + new java.util.Date();

            String hdfsTestFile = "/user/hadoop/java_test.txt";
            String localTestFile = "/tmp/java_test_local.txt";

            // 1. 写入文件到 HDFS
            fileOps.writeFile(hdfsTestFile, testContent);

            // 2. 读取 HDFS 文件内容
            String readContent = fileOps.readFile(hdfsTestFile);
            System.out.println("读取的文件内容:\n" + readContent);

            // 3. 下载文件到本地
            fileOps.downloadFile(hdfsTestFile, localTestFile);

            // 4. 复制文件
            fileOps.copyFile(hdfsTestFile, hdfsTestFile + ".backup");

            // 5. 清理测试文件
            fileOps.deleteFile(hdfsTestFile);
            fileOps.deleteFile(hdfsTestFile + ".backup");

            // 删除本地测试文件
            new File(localTestFile).delete();

        } catch (IOException e) {
            System.err.println("测试失败: " + e.getMessage());
        } finally {
            client.close();
        }
    }
}
```

### 6.4 完整的 HDFS 客户端示例

```java
package com.example.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;
import java.util.Scanner;

/**
 * HDFS 客户端主程序
 * 功能：提供交互式的 HDFS 操作界面
 */
public class HDFSClientMain {

    private HDFSClient client;
    private HDFSDirectoryOperations dirOps;
    private HDFSFileOperations fileOps;
    private Scanner scanner;

    public HDFSClientMain() {
        client = new HDFSClient();
        scanner = new Scanner(System.in);
    }

    /**
     * 初始化客户端
     */
    public void init() throws IOException {
        client.init();
        FileSystem fs = client.getFileSystem();
        dirOps = new HDFSDirectoryOperations(fs);
        fileOps = new HDFSFileOperations(fs);
        System.out.println("HDFS 客户端初始化完成");
    }

    /**
     * 显示菜单
     */
    public void showMenu() {
        System.out.println("\n=== HDFS 客户端操作菜单 ===");
        System.out.println("1. 列出目录内容");
        System.out.println("2. 创建目录");
        System.out.println("3. 删除目录");
        System.out.println("4. 上传文件");
        System.out.println("5. 下载文件");
        System.out.println("6. 读取文件内容");
        System.out.println("7. 写入文件内容");
        System.out.println("8. 复制文件");
        System.out.println("9. 删除文件");
        System.out.println("0. 退出");
        System.out.print("请选择操作 (0-9): ");
    }

    /**
     * 处理用户输入
     */
    public void handleUserInput() {
        while (true) {
            showMenu();

            try {
                int choice = scanner.nextInt();
                scanner.nextLine(); // 消费换行符

                switch (choice) {
                    case 1:
                        listDirectory();
                        break;
                    case 2:
                        createDirectory();
                        break;
                    case 3:
                        deleteDirectory();
                        break;
                    case 4:
                        uploadFile();
                        break;
                    case 5:
                        downloadFile();
                        break;
                    case 6:
                        readFile();
                        break;
                    case 7:
                        writeFile();
                        break;
                    case 8:
                        copyFile();
                        break;
                    case 9:
                        deleteFile();
                        break;
                    case 0:
                        System.out.println("退出程序");
                        return;
                    default:
                        System.out.println("无效选择，请重新输入");
                }
            } catch (Exception e) {
                System.err.println("操作出错: " + e.getMessage());
                scanner.nextLine(); // 清除错误输入
            }
        }
    }

    private void listDirectory() {
        System.out.print("请输入目录路径: ");
        String path = scanner.nextLine();
        dirOps.listDirectory(path);
    }

    private void createDirectory() {
        System.out.print("请输入要创建的目录路径: ");
        String path = scanner.nextLine();
        dirOps.createDirectory(path);
    }

    private void deleteDirectory() {
        System.out.print("请输入要删除的目录路径: ");
        String path = scanner.nextLine();
        System.out.print("是否递归删除? (y/n): ");
        boolean recursive = scanner.nextLine().toLowerCase().startsWith("y");
        dirOps.deleteDirectory(path, recursive);
    }

    private void uploadFile() {
        System.out.print("请输入本地文件路径: ");
        String localPath = scanner.nextLine();
        System.out.print("请输入 HDFS 目标路径: ");
        String hdfsPath = scanner.nextLine();
        fileOps.uploadFile(localPath, hdfsPath);
    }

    private void downloadFile() {
        System.out.print("请输入 HDFS 文件路径: ");
        String hdfsPath = scanner.nextLine();
        System.out.print("请输入本地目标路径: ");
        String localPath = scanner.nextLine();
        fileOps.downloadFile(hdfsPath, localPath);
    }

    private void readFile() {
        System.out.print("请输入 HDFS 文件路径: ");
        String path = scanner.nextLine();
        String content = fileOps.readFile(path);
        if (content != null) {
            System.out.println("文件内容:");
            System.out.println("----------------------------------------");
            System.out.println(content);
            System.out.println("----------------------------------------");
        }
    }

    private void writeFile() {
        System.out.print("请输入 HDFS 文件路径: ");
        String path = scanner.nextLine();
        System.out.println("请输入文件内容 (输入 'END' 结束):");

        StringBuilder content = new StringBuilder();
        String line;
        while (!(line = scanner.nextLine()).equals("END")) {
            content.append(line).append("\n");
        }

        fileOps.writeFile(path, content.toString());
    }

    private void copyFile() {
        System.out.print("请输入源文件路径: ");
        String srcPath = scanner.nextLine();
        System.out.print("请输入目标文件路径: ");
        String dstPath = scanner.nextLine();
        fileOps.copyFile(srcPath, dstPath);
    }

    private void deleteFile() {
        System.out.print("请输入要删除的文件路径: ");
        String path = scanner.nextLine();
        fileOps.deleteFile(path);
    }

    /**
     * 关闭客户端
     */
    public void close() {
        if (scanner != null) {
            scanner.close();
        }
        if (client != null) {
            client.close();
        }
    }

    /**
     * 主程序入口
     */
    public static void main(String[] args) {
        HDFSClientMain clientMain = new HDFSClientMain();

        try {
            // 初始化客户端
            clientMain.init();

            // 处理用户交互
            clientMain.handleUserInput();

        } catch (IOException e) {
            System.err.println("客户端初始化失败: " + e.getMessage());
        } finally {
            // 关闭客户端
            clientMain.close();
        }
    }
}
```

### 6.5 实践练习：Java SDK 编程

#### 练习 1：批量文件处理程序

```java
package com.example.hdfs.practice;

import com.example.hdfs.HDFSClient;
import com.example.hdfs.HDFSFileOperations;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * 练习 1：批量文件处理程序
 * 功能：批量上传本地文件到 HDFS，并进行统计分析
 */
public class BatchFileProcessor {

    private HDFSClient client;
    private HDFSFileOperations fileOps;

    public BatchFileProcessor() {
        client = new HDFSClient();
    }

    /**
     * 初始化处理器
     */
    public void init() throws IOException {
        client.init();
        fileOps = new HDFSFileOperations(client.getFileSystem());
    }

    /**
     * 批量处理本地文件
     * @param localDir 本地目录路径
     * @param hdfsDir HDFS 目标目录
     */
    public void batchProcess(String localDir, String hdfsDir) {
        File dir = new File(localDir);
        if (!dir.exists() || !dir.isDirectory()) {
            System.out.println("本地目录不存在或不是目录: " + localDir);
            return;
        }

        File[] files = dir.listFiles();
        if (files == null) {
            System.out.println("目录为空: " + localDir);
            return;
        }

        int successCount = 0;
        int failCount = 0;
        long totalSize = 0;

        System.out.println("开始批量处理文件...");

        for (File file : files) {
            if (file.isFile()) {
                String localPath = file.getAbsolutePath();
                String hdfsPath = hdfsDir + "/" + file.getName();

                if (fileOps.uploadFile(localPath, hdfsPath)) {
                    successCount++;
                    totalSize += file.length();
                } else {
                    failCount++;
                }
            }
        }

        // 输出统计信息
        System.out.println("\n=== 批量处理统计 ===");
        System.out.println("成功上传: " + successCount + " 个文件");
        System.out.println("失败: " + failCount + " 个文件");
        System.out.println("总大小: " + formatFileSize(totalSize));
    }

    /**
     * 格式化文件大小
     */
    private String formatFileSize(long size) {
        if (size < 1024) return size + " B";
        if (size < 1024 * 1024) return String.format("%.2f KB", size / 1024.0);
        if (size < 1024 * 1024 * 1024) return String.format("%.2f MB", size / (1024.0 * 1024));
        return String.format("%.2f GB", size / (1024.0 * 1024 * 1024));
    }

    /**
     * 关闭处理器
     */
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    /**
     * 测试批量处理
     */
    public static void main(String[] args) {
        BatchFileProcessor processor = new BatchFileProcessor();

        try {
            // 初始化
            processor.init();

            // 创建测试文件
            String testDir = "/tmp/batch_test";
            File dir = new File(testDir);
            dir.mkdirs();

            // 生成测试文件
            for (int i = 1; i <= 5; i++) {
                FileWriter writer = new FileWriter(testDir + "/test" + i + ".txt");
                writer.write("这是测试文件 " + i + "\n内容行 1\n内容行 2\n");
                writer.close();
            }

            // 批量处理
            processor.batchProcess(testDir, "/user/hadoop/batch_upload");

            // 清理测试文件
            for (File file : dir.listFiles()) {
                file.delete();
            }
            dir.delete();

        } catch (IOException e) {
            System.err.println("批量处理失败: " + e.getMessage());
        } finally {
            processor.close();
        }
    }
}
```

---

## 7. 故障排除与最佳实践

### 7.1 学习目标

- 掌握常见 HDFS 问题的诊断和解决方法
- 了解 HDFS 性能优化策略
- 学习 HDFS 运维最佳实践

### 7.2 常见问题及解决方案

#### 7.2.1 连接问题

**问题现象**：无法连接到 HDFS

```bash
# 典型错误信息：
# Call From hostname to localhost:9000 failed on connection exception
# java.net.ConnectException: Connection refused
```

**诊断步骤**：

```bash
# 1. 检查 HDFS 服务状态
jps | grep -E "(NameNode|DataNode)"

# 2. 检查端口监听状态
netstat -tulpn | grep 9000
netstat -tulpn | grep 9870

# 3. 检查配置文件
cat $HADOOP_CONF_DIR/core-site.xml | grep -A 2 -B 2 "fs.defaultFS"

# 4. 查看 NameNode 日志
tail -f $HADOOP_HOME/logs/hadoop-*-namenode-*.log
```

**解决方案**：

```bash
# 1. 重启 HDFS 服务
stop-dfs.sh
start-dfs.sh

# 2. 检查防火墙设置
sudo ufw status
sudo ufw allow 9000
sudo ufw allow 9870

# 3. 验证主机名解析
ping localhost
nslookup localhost
```

#### 7.2.2 权限问题

**问题现象**：权限被拒绝

```bash
# 典型错误信息：
# Permission denied: user=username, access=WRITE, inode="/":hadoop:supergroup:drwxr-xr-x
```

**解决方案**：

```bash
# 1. 创建用户目录
hdfs dfs -mkdir -p /user/$USER

# 2. 修改目录所有者
sudo -u hadoop hdfs dfs -chown $USER:$USER /user/$USER

# 3. 设置适当权限
sudo -u hadoop hdfs dfs -chmod 755 /user/$USER

# 4. 验证权限设置
hdfs dfs -ls -d /user/$USER
```

### 7.3 性能优化策略

#### 7.3.1 文件大小优化

```bash
# 问题：大量小文件会影响 NameNode 性能
# 解决方案：合并小文件

# 1. 使用 Hadoop Archive 合并小文件
hadoop archive -archiveName small_files.har -p /user/hadoop/small_files /user/hadoop/archives

# 2. 检查文件大小分布
hdfs dfs -count -h /user/hadoop/*

# 3. 设置合适的块大小
hdfs dfs -D dfs.blocksize=268435456 -put large_file.txt /user/hadoop/
```

### 7.4 实践练习：故障排除综合演示

```bash
# 练习：HDFS 故障排除综合演示
echo "=== HDFS 故障排除综合练习 ==="

# 1. 模拟连接问题
echo "1. 模拟和解决连接问题"
# 停止 DataNode 服务
hadoop-daemon.sh stop datanode

# 尝试操作，观察错误
hdfs dfs -ls / 2>&1 | head -5

# 重启服务
hadoop-daemon.sh start datanode
sleep 10

# 验证恢复
hdfs dfs -ls /

# 2. 模拟权限问题
echo "2. 模拟和解决权限问题"
# 创建受限目录
sudo -u hadoop hdfs dfs -mkdir /restricted
sudo -u hadoop hdfs dfs -chmod 700 /restricted

# 尝试访问（会失败）
hdfs dfs -ls /restricted 2>&1 | head -3

# 修复权限
sudo -u hadoop hdfs dfs -chmod 755 /restricted

# 验证修复
hdfs dfs -ls /restricted

echo "=== 故障排除练习完成 ==="
```

---

## 8. 课程总结与评估

### 8.1 知识点回顾

通过本课程的学习，我们掌握了以下核心内容：

#### 8.1.1 命令行操作技能

- **文件操作命令**：`put`, `get`, `cat`, `ls`, `mkdir`, `rm`, `cp`, `mv`
- **运维管理命令**：`dfsadmin`, `fsck`, `safemode`, `quota`, `snapshot`
- **权限管理命令**：`chmod`, `chown`, `chgrp`

#### 8.1.2 Java SDK 编程能力

- HDFS 客户端连接管理
- 文件和目录操作的编程实现
- 异常处理和资源管理
- 批量处理和监控程序开发

#### 8.1.3 运维管理技能

- 集群状态监控和健康检查
- 故障诊断和问题解决
- 性能优化和最佳实践

### 8.2 课后习题

本章的实践能力评估已独立为课后习题，包含基础操作和编程开发两个部分的综合练习。

**习题文件**：[HDFS 课后习题](./exercise_1.md)

**习题内容概览**：

- **第一部分：基础操作评估（50 分）**
  - 目录和文件管理操作
  - 批量操作和通配符使用
  - 结合 NameNode Web UI 确认操作结果
- **第二部分：编程能力评估（50 分）**
  - Java SDK 编程实现
  - HDFS 文件管理器开发
  - 通过 Web 界面验证程序执行效果

**完成建议**：

1. 按顺序完成两个部分的习题
2. 每个部分都有明确的评分标准
3. 建议完成时间：3-4 小时
4. 注重实践操作和代码质量
5. 充分利用 NameNode Web UI（<http://localhost:9870/>）验证操作结果

---

## 9. 参考资料与扩展阅读

### 9.1 官方文档

1. **Apache Hadoop 官方文档**

   - [Hadoop Documentation](https://hadoop.apache.org/docs/current/)
   - [HDFS Commands Guide](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HDFSCommands.html)
   - [HDFS Architecture](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)

2. **API 参考文档**
   - [Hadoop Java API](https://hadoop.apache.org/docs/current/api/)
   - [FileSystem API](https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html)

### 9.2 技术书籍

1. **《Hadoop 权威指南》**（第四版）

   - 作者：Tom White
   - 出版社：O'Reilly Media
   - 内容：全面介绍 Hadoop 生态系统

2. **《大数据技术原理与应用》**
   - 作者：林子雨
   - 出版社：人民邮电出版社
   - 内容：大数据技术基础理论和实践

### 9.3 在线资源

1. **学习平台**

   - [Cloudera University](https://university.cloudera.com/)
   - [Hortonworks Tutorials](https://hortonworks.com/tutorials/)
   - [Apache Hadoop Wiki](https://wiki.apache.org/hadoop/)

2. **社区论坛**
   - [Stack Overflow - Hadoop](https://stackoverflow.com/questions/tagged/hadoop)
   - [Apache Hadoop Mailing Lists](https://hadoop.apache.org/mailing_lists.html)

---
