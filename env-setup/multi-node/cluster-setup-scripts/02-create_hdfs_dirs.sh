#!/bin/bash
# 批量创建学生 HDFS 目录结构脚本
# 用法: ./02-create_hdfs_dirs.sh [学生名单文件]
# 默认使用 students.txt 文件

STUDENT_LIST="${1:-students.txt}"

# 检查学生名单文件是否存在
if [ ! -f "$STUDENT_LIST" ]; then
    echo "错误: 学生名单文件 $STUDENT_LIST 不存在"
    echo "请先运行 01-create_students.sh 创建学生账户"
    exit 1
fi

# 检查 HDFS 是否可用
if ! hdfs dfs -ls / >/dev/null 2>&1; then
    echo "错误: 无法连接到 HDFS，请确保 Hadoop 集群正在运行"
    exit 1
fi

echo "=== 开始创建 HDFS 目录结构 ==="
echo "使用学生名单文件: $STUDENT_LIST"
echo

# 创建公共数据目录（如果不存在）
echo "创建公共数据目录..."
hdfs dfs -mkdir -p /public/data/wordcount
hdfs dfs -chmod 755 /public/data
hdfs dfs -chmod 755 /public/data/wordcount

# 创建用户根目录（如果不存在）
hdfs dfs -mkdir -p /users
hdfs dfs -chmod 755 /users

# 统计信息
total_students=0
created_dirs=0
existing_dirs=0

# 为每个学生创建个人目录结构
while read -r student_id; do
    # 跳过空行和注释行
    if [ -z "$student_id" ] || [[ "$student_id" =~ ^#.* ]]; then
        continue
    fi

    total_students=$((total_students + 1))
    
    # 检查用户是否存在
    if ! id "$student_id" &>/dev/null; then
        echo "警告: 用户 $student_id 不存在，跳过目录创建"
        continue
    fi

    echo "为学生 $student_id 创建 HDFS 目录结构..."

    # 检查个人根目录是否已存在
    if hdfs dfs -test -d "/users/$student_id"; then
        echo "目录 /users/$student_id 已存在，跳过创建"
        existing_dirs=$((existing_dirs + 1))
        continue
    fi

    # 创建个人根目录
    hdfs dfs -mkdir -p "/users/$student_id"
    
    # 创建作业目录结构
    hdfs dfs -mkdir -p "/users/$student_id/homework1"
    hdfs dfs -mkdir -p "/users/$student_id/homework1/problem1"
    hdfs dfs -mkdir -p "/users/$student_id/homework1/problem1/input"
    hdfs dfs -mkdir -p "/users/$student_id/homework1/problem1/output"
    hdfs dfs -mkdir -p "/users/$student_id/homework1/problem2"
    hdfs dfs -mkdir -p "/users/$student_id/homework1/problem2/input"
    hdfs dfs -mkdir -p "/users/$student_id/homework1/problem2/output"
    hdfs dfs -mkdir -p "/users/$student_id/homework1/problem3"
    hdfs dfs -mkdir -p "/users/$student_id/homework1/problem3/input"
    hdfs dfs -mkdir -p "/users/$student_id/homework1/problem3/output"
    
    # 创建临时目录
    hdfs dfs -mkdir -p "/users/$student_id/temp"
    
    # 设置目录所有者和权限
    hdfs dfs -chown -R "$student_id:students" "/users/$student_id"
    hdfs dfs -chmod -R 755 "/users/$student_id"
    
    # 设置输出目录权限（确保可写）
    hdfs dfs -chmod 755 "/users/$student_id/homework1/problem1/output"
    hdfs dfs -chmod 755 "/users/$student_id/homework1/problem2/output"
    hdfs dfs -chmod 755 "/users/$student_id/homework1/problem3/output"

    echo "学生 $student_id 的 HDFS 目录结构创建完成"
    created_dirs=$((created_dirs + 1))
    echo
done < "$STUDENT_LIST"

echo "=== HDFS 目录结构创建完成 ==="
echo "总学生数: $total_students"
echo "新创建目录: $created_dirs"
echo "已存在目录: $existing_dirs"
echo
echo "目录结构示例:"
echo "/users/<学号>/"
echo "├── homework1/"
echo "│   ├── problem1/"
echo "│   │   ├── input/"
echo "│   │   └── output/"
echo "│   ├── problem2/"
echo "│   │   ├── input/"
echo "│   │   └── output/"
echo "│   └── problem3/"
echo "│       ├── input/"
echo "│       └── output/"
echo "└── temp/"
echo
echo "下一步: 运行 04-set_hdfs_quotas.sh 设置存储配额"