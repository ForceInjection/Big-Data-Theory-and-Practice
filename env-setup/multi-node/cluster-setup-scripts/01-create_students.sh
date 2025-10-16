#!/bin/bash
# 批量创建学生系统账户脚本
# 用法: ./01-create_students.sh [学生名单文件]
# 默认使用 students.txt 文件

STUDENT_LIST="${1:-students.txt}"

# 检查学生名单文件是否存在
if [ ! -f "$STUDENT_LIST" ]; then
    echo "错误: 学生名单文件 $STUDENT_LIST 不存在"
    echo "请创建该文件，每行包含一个学号"
    echo "示例内容:"
    echo "2024001"
    echo "2024002"
    echo "2024003"
    exit 1
fi

# 检查是否以 root 权限运行
if [ "$EUID" -ne 0 ]; then
    echo "错误: 此脚本需要 root 权限运行"
    echo "请使用: sudo $0"
    exit 1
fi

echo "=== 开始批量创建学生账户 ==="
echo "使用学生名单文件: $STUDENT_LIST"
echo

# 创建 students 组（如果不存在）
if ! getent group students > /dev/null 2>&1; then
    echo "创建 students 组..."
    groupadd students
    echo "students 组创建完成"
else
    echo "students 组已存在"
fi

# 统计信息
total_students=0
created_students=0
existing_students=0

# 读取学生名单并创建账户
while read -r student_id; do
    # 跳过空行和注释行
    if [ -z "$student_id" ] || [[ "$student_id" =~ ^#.* ]]; then
        continue
    fi

    total_students=$((total_students + 1))
    
    # 检查用户是否已存在
    if id "$student_id" &>/dev/null; then
        echo "用户 $student_id 已存在，跳过创建"
        existing_students=$((existing_students + 1))
        continue
    fi

    echo "创建用户: $student_id (仅用于 HDFS 和 YARN 身份识别，禁用登录)"
    
    # 创建用户账户（不创建家目录，禁用登录）
    useradd -M -g students -s /sbin/nologin "$student_id"
    
    if [ $? -eq 0 ]; then
        echo "用户 $student_id 创建完成（已禁用登录权限）"
        created_students=$((created_students + 1))
    else
        echo "错误: 用户 $student_id 创建失败"
    fi
    
    echo
done < "$STUDENT_LIST"

echo "=== 学生账户创建完成 ==="
echo "总学生数: $total_students"
echo "新创建: $created_students"
echo "已存在: $existing_students"
echo
echo "注意: 所有学生账户已禁用登录权限，仅用于 HDFS 和 YARN 身份识别"
echo "学生需要在本地机器配置 Hadoop 客户端进行远程访问"
echo
echo "下一步:"
echo "1. 运行 02-create_hdfs_dirs.sh 创建 HDFS 目录"
echo "2. 学生在本地机器运行 05-setup_student_client.sh 配置客户端"