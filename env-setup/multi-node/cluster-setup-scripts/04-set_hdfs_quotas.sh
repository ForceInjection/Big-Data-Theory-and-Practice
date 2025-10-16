#!/bin/bash
# 设置学生 HDFS 存储配额脚本
# 用法: ./04-set_hdfs_quotas.sh [学生名单文件] [配额大小GB]
# 默认使用 students.txt 文件，配额 1GB

STUDENT_LIST="${1:-students.txt}"
QUOTA_GB="${2:-1}"
QUOTA_BYTES=$((QUOTA_GB * 1024 * 1024 * 1024))

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

echo "=== 开始设置 HDFS 存储配额 ==="
echo "使用学生名单文件: $STUDENT_LIST"
echo "配额大小: ${QUOTA_GB}GB (${QUOTA_BYTES} bytes)"
echo

# 统计信息
total_students=0
set_quotas=0
failed_quotas=0

# 为每个学生设置存储配额
while read -r student_id; do
    # 跳过空行和注释行
    if [ -z "$student_id" ] || [[ "$student_id" =~ ^#.* ]]; then
        continue
    fi

    total_students=$((total_students + 1))
    
    # 检查用户目录是否存在
    if ! hdfs dfs -test -d "/users/$student_id"; then
        echo "警告: 目录 /users/$student_id 不存在，跳过配额设置"
        failed_quotas=$((failed_quotas + 1))
        continue
    fi

    echo "为学生 $student_id 设置存储配额 ${QUOTA_GB}GB..."

    # 设置空间配额
    if hdfs dfsadmin -setSpaceQuota "${QUOTA_BYTES}" "/users/$student_id"; then
        echo "学生 $student_id 的存储配额设置成功"
        set_quotas=$((set_quotas + 1))
    else
        echo "错误: 学生 $student_id 的存储配额设置失败"
        failed_quotas=$((failed_quotas + 1))
    fi
    
    echo
done < "$STUDENT_LIST"

echo "=== HDFS 存储配额设置完成 ==="
echo "总学生数: $total_students"
echo "成功设置: $set_quotas"
echo "设置失败: $failed_quotas"
echo
echo "查看配额使用情况:"
echo "hdfs dfs -count -q /users/<学号>"
echo
echo "下一步: 运行 03-setup_yarn_queues.sh 配置 YARN 队列"