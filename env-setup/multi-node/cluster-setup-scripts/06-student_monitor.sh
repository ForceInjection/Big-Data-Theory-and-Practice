#!/bin/bash
# 学生状态监控脚本
# 用法: ./06-student_monitor.sh [学号]
# 如果不指定学号，则监控所有学生

STUDENT_ID="$1"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查 HDFS 连接
check_hdfs_connection() {
    if ! hdfs dfs -ls / >/dev/null 2>&1; then
        print_error "无法连接到 HDFS，请检查 Hadoop 集群状态"
        return 1
    fi
    return 0
}

# 监控单个学生
monitor_student() {
    local student_id="$1"
    
    echo "========================================"
    print_info "监控学生: $student_id"
    echo "========================================"
    
    # 检查系统账户
    if id "$student_id" &>/dev/null; then
        print_success "系统账户存在"
        
        # 获取用户信息
        user_info=$(id "$student_id")
        echo "  用户信息: $user_info"
        
        # 检查家目录
        home_dir="/home/$student_id"
        if [ -d "$home_dir" ]; then
            print_success "家目录存在: $home_dir"
            
            # 检查工作目录
            workspace="$home_dir/hadoop-workspace"
            if [ -d "$workspace" ]; then
                print_success "工作目录存在: $workspace"
            else
                print_warning "工作目录不存在: $workspace"
            fi
        else
            print_warning "家目录不存在: $home_dir"
        fi
    else
        print_error "系统账户不存在"
        return 1
    fi
    
    # 检查 HDFS 目录
    hdfs_dir="/users/$student_id"
    if hdfs dfs -test -d "$hdfs_dir" 2>/dev/null; then
        print_success "HDFS 个人目录存在: $hdfs_dir"
        
        # 检查目录权限
        permissions=$(hdfs dfs -ls /users | grep "$student_id" | awk '{print $1, $3, $4}')
        echo "  目录权限: $permissions"
        
        # 检查存储配额
        quota_info=$(hdfs dfs -count -q "$hdfs_dir" 2>/dev/null)
        if [ $? -eq 0 ]; then
            echo "  存储配额信息:"
            echo "    $quota_info"
            
            # 解析配额信息
            space_quota=$(echo "$quota_info" | awk '{print $2}')
            space_used=$(echo "$quota_info" | awk '{print $3}')
            
            if [ "$space_quota" != "none" ] && [ "$space_quota" -gt 0 ]; then
                usage_percent=$((space_used * 100 / space_quota))
                echo "    存储使用率: ${usage_percent}%"
                
                if [ "$usage_percent" -gt 80 ]; then
                    print_warning "存储使用率较高: ${usage_percent}%"
                elif [ "$usage_percent" -gt 95 ]; then
                    print_error "存储空间即将耗尽: ${usage_percent}%"
                fi
            fi
        else
            print_warning "无法获取存储配额信息"
        fi
        
        # 检查作业目录结构
        echo "  作业目录结构:"
        hdfs dfs -ls "$hdfs_dir" 2>/dev/null | while read line; do
            echo "    $line"
        done
        
    else
        print_error "HDFS 个人目录不存在: $hdfs_dir"
    fi
    
    # 检查正在运行的作业
    echo
    print_info "检查正在运行的作业..."
    running_jobs=$(yarn application -list -appStates RUNNING 2>/dev/null | grep "$student_id" || true)
    if [ -n "$running_jobs" ]; then
        print_success "发现正在运行的作业:"
        echo "$running_jobs"
    else
        echo "  无正在运行的作业"
    fi
    
    # 检查最近的作业历史
    echo
    print_info "检查最近的作业历史..."
    recent_jobs=$(yarn application -list -appStates FINISHED,FAILED,KILLED 2>/dev/null | grep "$student_id" | head -5 || true)
    if [ -n "$recent_jobs" ]; then
        echo "  最近 5 个作业:"
        echo "$recent_jobs"
    else
        echo "  无作业历史记录"
    fi
    
    echo
}

# 监控所有学生
monitor_all_students() {
    print_info "监控所有学生状态"
    echo
    
    # 获取所有 students 组用户
    students=$(getent group students | cut -d: -f4 | tr ',' ' ')
    
    if [ -z "$students" ]; then
        print_warning "未找到 students 组用户"
        return 1
    fi
    
    total_students=0
    active_students=0
    
    for student in $students; do
        total_students=$((total_students + 1))
        
        # 简化监控信息
        echo "----------------------------------------"
        echo "学生: $student"
        
        # 检查 HDFS 目录
        if hdfs dfs -test -d "/users/$student" 2>/dev/null; then
            echo "  HDFS 目录: ✓"
            
            # 检查存储使用情况
            quota_info=$(hdfs dfs -count -q "/users/$student" 2>/dev/null)
            if [ $? -eq 0 ]; then
                space_used=$(echo "$quota_info" | awk '{print $3}')
                echo "  存储使用: $(numfmt --to=iec $space_used)"
            fi
            
            active_students=$((active_students + 1))
        else
            echo "  HDFS 目录: ✗"
        fi
        
        # 检查正在运行的作业
        running_count=$(yarn application -list -appStates RUNNING 2>/dev/null | grep -c "$student" || echo "0")
        echo "  运行作业: $running_count"
        
    done
    
    echo "========================================"
    echo "监控摘要:"
    echo "  总学生数: $total_students"
    echo "  活跃学生: $active_students"
    echo "  集群状态: $(yarn node -list 2>/dev/null | grep -c RUNNING || echo "未知")"
    echo "========================================"
}

# 主函数
main() {
    echo "=== Hadoop 学生监控工具 ==="
    echo "时间: $(date)"
    echo
    
    # 检查 HDFS 连接
    if ! check_hdfs_connection; then
        exit 1
    fi
    
    if [ -n "$STUDENT_ID" ]; then
        # 监控指定学生
        monitor_student "$STUDENT_ID"
    else
        # 监控所有学生
        monitor_all_students
    fi
    
    echo
    print_info "监控完成"
}

# 执行主函数
main