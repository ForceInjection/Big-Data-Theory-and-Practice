#!/bin/bash
# 学生问题诊断脚本
# 用法: ./08-diagnose_student_issues.sh <学号>

STUDENT_ID="$1"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[⚠]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

print_check() {
    echo -e "${PURPLE}[检查]${NC} $1"
}

# 显示使用说明
show_usage() {
    echo "用法: $0 <学号>"
    echo
    echo "功能: 全面诊断学生在使用 Hadoop 集群时可能遇到的问题"
    echo
    echo "检查项目:"
    echo "  - 系统账户状态"
    echo "  - HDFS 目录和权限"
    echo "  - 存储配额"
    echo "  - 网络连接"
    echo "  - 作业执行环境"
    echo "  - 常见配置问题"
    echo
    echo "示例: $0 2024001"
}

# 检查参数
if [ -z "$STUDENT_ID" ]; then
    print_error "缺少学号参数"
    show_usage
    exit 1
fi

# 诊断结果统计
total_checks=0
passed_checks=0
warning_checks=0
failed_checks=0

# 记录检查结果
record_result() {
    local result="$1"  # success, warning, error
    total_checks=$((total_checks + 1))
    
    case "$result" in
        "success")
            passed_checks=$((passed_checks + 1))
            ;;
        "warning")
            warning_checks=$((warning_checks + 1))
            ;;
        "error")
            failed_checks=$((failed_checks + 1))
            ;;
    esac
}

# 检查系统账户
check_system_account() {
    print_check "检查系统账户状态"
    
    if id "$STUDENT_ID" &>/dev/null; then
        print_success "系统账户存在"
        record_result "success"
        
        # 检查用户组
        user_groups=$(groups "$STUDENT_ID" 2>/dev/null)
        if echo "$user_groups" | grep -q "students"; then
            print_success "用户属于 students 组"
            record_result "success"
        else
            print_error "用户不属于 students 组"
            echo "  解决方案: sudo usermod -a -G students $STUDENT_ID"
            record_result "error"
        fi
        
        # 检查家目录
        home_dir="/home/$STUDENT_ID"
        if [ -d "$home_dir" ]; then
            print_success "家目录存在: $home_dir"
            record_result "success"
            
            # 检查家目录权限
            home_perms=$(stat -c "%a" "$home_dir" 2>/dev/null)
            if [ "$home_perms" = "755" ] || [ "$home_perms" = "750" ]; then
                print_success "家目录权限正常: $home_perms"
                record_result "success"
            else
                print_warning "家目录权限可能有问题: $home_perms"
                echo "  建议权限: 755"
                record_result "warning"
            fi
        else
            print_error "家目录不存在: $home_dir"
            echo "  解决方案: sudo mkhomedir_helper $STUDENT_ID"
            record_result "error"
        fi
        
    else
        print_error "系统账户不存在"
        echo "  解决方案: 运行 01-create_students.sh 创建账户"
        record_result "error"
        return 1
    fi
    
    return 0
}

# 检查 HDFS 目录和权限
check_hdfs_directory() {
    print_check "检查 HDFS 目录和权限"
    
    # 检查 HDFS 连接
    if ! hdfs dfs -ls / >/dev/null 2>&1; then
        print_error "无法连接到 HDFS"
        echo "  可能原因: Hadoop 集群未启动或网络问题"
        record_result "error"
        return 1
    fi
    
    print_success "HDFS 连接正常"
    record_result "success"
    
    # 检查个人根目录
    hdfs_dir="/users/$STUDENT_ID"
    if hdfs dfs -test -d "$hdfs_dir" 2>/dev/null; then
        print_success "HDFS 个人目录存在: $hdfs_dir"
        record_result "success"
        
        # 检查目录所有者
        dir_info=$(hdfs dfs -ls /users | grep "$STUDENT_ID" 2>/dev/null)
        if echo "$dir_info" | grep -q "$STUDENT_ID.*$STUDENT_ID"; then
            print_success "目录所有者正确"
            record_result "success"
        else
            print_error "目录所有者不正确"
            echo "  当前信息: $dir_info"
            echo "  解决方案: hdfs dfs -chown -R $STUDENT_ID:students $hdfs_dir"
            record_result "error"
        fi
        
        # 检查目录权限
        dir_perms=$(echo "$dir_info" | awk '{print $1}')
        if [[ "$dir_perms" =~ ^drwxr.* ]]; then
            print_success "目录权限正常: $dir_perms"
            record_result "success"
        else
            print_warning "目录权限可能有问题: $dir_perms"
            echo "  建议权限: drwxr-xr-x"
            record_result "warning"
        fi
        
        # 检查作业目录结构
        required_dirs=("homework1" "homework1/problem1" "homework1/problem1/input" "homework1/problem1/output")
        for subdir in "${required_dirs[@]}"; do
            if hdfs dfs -test -d "$hdfs_dir/$subdir" 2>/dev/null; then
                print_success "作业目录存在: $subdir"
                record_result "success"
            else
                print_warning "作业目录缺失: $subdir"
                echo "  解决方案: hdfs dfs -mkdir -p $hdfs_dir/$subdir"
                record_result "warning"
            fi
        done
        
    else
        print_error "HDFS 个人目录不存在: $hdfs_dir"
        echo "  解决方案: 运行 02-create_hdfs_dirs.sh 创建目录"
        record_result "error"
    fi
}

# 检查存储配额
check_storage_quota() {
    print_check "检查存储配额"
    
    hdfs_dir="/users/$STUDENT_ID"
    if ! hdfs dfs -test -d "$hdfs_dir" 2>/dev/null; then
        print_error "HDFS 目录不存在，跳过配额检查"
        record_result "error"
        return 1
    fi
    
    quota_info=$(hdfs dfs -count -q "$hdfs_dir" 2>/dev/null)
    if [ $? -ne 0 ]; then
        print_error "无法获取配额信息"
        record_result "error"
        return 1
    fi
    
    # 解析配额信息
    space_quota=$(echo "$quota_info" | awk '{print $2}')
    space_used=$(echo "$quota_info" | awk '{print $3}')
    
    if [ "$space_quota" = "none" ] || [ "$space_quota" -eq 0 ]; then
        print_warning "未设置存储配额"
        echo "  建议: 设置适当的存储配额以防止滥用"
        record_result "warning"
    else
        print_success "已设置存储配额: $(numfmt --to=iec $space_quota)"
        record_result "success"
        
        # 检查使用率
        if [ "$space_used" -gt 0 ]; then
            usage_percent=$((space_used * 100 / space_quota))
            echo "  当前使用: $(numfmt --to=iec $space_used) (${usage_percent}%)"
            
            if [ "$usage_percent" -gt 90 ]; then
                print_error "存储空间即将耗尽: ${usage_percent}%"
                record_result "error"
            elif [ "$usage_percent" -gt 75 ]; then
                print_warning "存储使用率较高: ${usage_percent}%"
                record_result "warning"
            else
                print_success "存储使用率正常: ${usage_percent}%"
                record_result "success"
            fi
        else
            print_success "当前使用: 0 字节"
            record_result "success"
        fi
    fi
}

# 检查网络连接
check_network_connectivity() {
    print_check "检查网络连接"
    
    # 检查 NameNode 连接
    namenode_host="hadoop-master"
    namenode_port="9000"
    
    if timeout 5 bash -c "</dev/tcp/$namenode_host/$namenode_port" 2>/dev/null; then
        print_success "NameNode 连接正常 ($namenode_host:$namenode_port)"
        record_result "success"
    else
        print_error "无法连接到 NameNode ($namenode_host:$namenode_port)"
        echo "  可能原因: 网络问题或 NameNode 未启动"
        record_result "error"
    fi
    
    # 检查 ResourceManager 连接
    rm_host="hadoop-master"
    rm_port="8032"
    
    if timeout 5 bash -c "</dev/tcp/$rm_host/$rm_port" 2>/dev/null; then
        print_success "ResourceManager 连接正常 ($rm_host:$rm_port)"
        record_result "success"
    else
        print_error "无法连接到 ResourceManager ($rm_host:$rm_port)"
        echo "  可能原因: 网络问题或 ResourceManager 未启动"
        record_result "error"
    fi
    
    # 检查 Web UI 连接
    web_ui_port="8088"
    if timeout 5 bash -c "</dev/tcp/$rm_host/$web_ui_port" 2>/dev/null; then
        print_success "Web UI 连接正常 ($rm_host:$web_ui_port)"
        record_result "success"
    else
        print_warning "无法连接到 Web UI ($rm_host:$web_ui_port)"
        echo "  影响: 无法通过浏览器查看作业状态"
        record_result "warning"
    fi
}

# 检查作业执行环境
check_job_environment() {
    print_check "检查作业执行环境"
    
    # 检查 YARN 队列
    if yarn queue -list >/dev/null 2>&1; then
        print_success "YARN 服务正常"
        record_result "success"
        
        # 检查 students 队列
        if yarn queue -list 2>/dev/null | grep -q "students"; then
            print_success "students 队列存在"
            record_result "success"
        else
            print_error "students 队列不存在"
            echo "  解决方案: 运行 03-setup_yarn_queues.sh 配置队列"
            record_result "error"
        fi
    else
        print_error "YARN 服务异常"
        record_result "error"
    fi
    
    # 检查最近的作业
    recent_jobs=$(yarn application -list -appStates ALL 2>/dev/null | grep "$STUDENT_ID" | head -3 || true)
    if [ -n "$recent_jobs" ]; then
        print_success "发现历史作业记录"
        record_result "success"
        
        # 检查失败作业
        failed_jobs=$(yarn application -list -appStates FAILED 2>/dev/null | grep "$STUDENT_ID" | head -1 || true)
        if [ -n "$failed_jobs" ]; then
            print_warning "发现失败的作业"
            echo "  最近失败作业: $(echo "$failed_jobs" | awk '{print $1}')"
            echo "  建议: 使用 07-view_student_logs.sh 查看详细错误信息"
            record_result "warning"
        fi
    else
        print_info "未发现作业记录（可能是新用户）"
        record_result "success"
    fi
}

# 检查客户端配置
check_client_configuration() {
    print_check "检查客户端配置"
    
    home_dir="/home/$STUDENT_ID"
    hadoop_conf_dir="$home_dir/.hadoop/conf"
    
    if [ -d "$hadoop_conf_dir" ]; then
        print_success "Hadoop 配置目录存在: $hadoop_conf_dir"
        record_result "success"
        
        # 检查配置文件
        config_files=("core-site.xml" "yarn-site.xml" "mapred-site.xml")
        for config_file in "${config_files[@]}"; do
            if [ -f "$hadoop_conf_dir/$config_file" ]; then
                print_success "配置文件存在: $config_file"
                record_result "success"
            else
                print_error "配置文件缺失: $config_file"
                echo "  解决方案: 运行 05-setup_student_client.sh 重新配置"
                record_result "error"
            fi
        done
        
    else
        print_error "Hadoop 配置目录不存在: $hadoop_conf_dir"
        echo "  解决方案: 运行 05-setup_student_client.sh 配置客户端"
        record_result "error"
    fi
    
    # 检查环境变量配置
    bashrc_file="$home_dir/.bashrc"
    if [ -f "$bashrc_file" ]; then
        if grep -q "HADOOP_HOME" "$bashrc_file"; then
            print_success "环境变量已配置"
            record_result "success"
        else
            print_warning "环境变量可能未配置"
            echo "  建议: 检查 ~/.bashrc 中的 Hadoop 环境变量"
            record_result "warning"
        fi
    else
        print_warning ".bashrc 文件不存在"
        record_result "warning"
    fi
}

# 检查常见问题
check_common_issues() {
    print_check "检查常见问题"
    
    # 检查临时目录权限
    temp_dir="/tmp/hadoop-$STUDENT_ID"
    if [ -d "$temp_dir" ]; then
        temp_owner=$(stat -c "%U" "$temp_dir" 2>/dev/null)
        if [ "$temp_owner" = "$STUDENT_ID" ]; then
            print_success "临时目录权限正常"
            record_result "success"
        else
            print_warning "临时目录所有者不正确: $temp_owner"
            echo "  解决方案: sudo chown -R $STUDENT_ID:students $temp_dir"
            record_result "warning"
        fi
    else
        print_info "临时目录不存在（正常情况）"
        record_result "success"
    fi
    
    # 检查 Java 环境
    if command -v java >/dev/null 2>&1; then
        java_version=$(java -version 2>&1 | head -n 1)
        print_success "Java 环境正常: $java_version"
        record_result "success"
    else
        print_error "Java 环境未安装"
        echo "  解决方案: 安装 OpenJDK 8"
        record_result "error"
    fi
    
    # 检查磁盘空间
    home_usage=$(df -h "$home_dir" 2>/dev/null | tail -1 | awk '{print $5}' | sed 's/%//')
    if [ -n "$home_usage" ] && [ "$home_usage" -lt 90 ]; then
        print_success "本地磁盘空间充足 (${home_usage}% 已用)"
        record_result "success"
    elif [ -n "$home_usage" ]; then
        print_warning "本地磁盘空间不足 (${home_usage}% 已用)"
        echo "  建议: 清理不必要的文件"
        record_result "warning"
    else
        print_warning "无法检查磁盘空间"
        record_result "warning"
    fi
}

# 生成诊断报告
generate_report() {
    echo
    echo "========================================"
    echo "           诊断报告摘要"
    echo "========================================"
    echo "学生: $STUDENT_ID"
    echo "时间: $(date)"
    echo
    echo "检查统计:"
    echo "  总检查项: $total_checks"
    echo "  通过: $passed_checks"
    echo "  警告: $warning_checks"
    echo "  失败: $failed_checks"
    echo
    
    if [ "$failed_checks" -eq 0 ] && [ "$warning_checks" -eq 0 ]; then
        print_success "所有检查项都通过，环境配置正常！"
    elif [ "$failed_checks" -eq 0 ]; then
        print_warning "环境基本正常，但有 $warning_checks 个警告项需要注意"
    else
        print_error "发现 $failed_checks 个严重问题，需要立即解决"
        echo
        echo "建议操作顺序:"
        echo "1. 解决系统账户和权限问题"
        echo "2. 修复 HDFS 目录和配额问题"
        echo "3. 检查网络连接"
        echo "4. 重新配置客户端环境"
    fi
    
    echo
    echo "如需进一步帮助，请联系管理员或查看学生使用指南"
    echo "========================================"
}

# 主函数
main() {
    echo "=== Hadoop 学生问题诊断工具 ==="
    echo "开始诊断学生: $STUDENT_ID"
    echo "时间: $(date)"
    echo
    
    # 执行各项检查
    check_system_account
    echo
    
    check_hdfs_directory
    echo
    
    check_storage_quota
    echo
    
    check_network_connectivity
    echo
    
    check_job_environment
    echo
    
    check_client_configuration
    echo
    
    check_common_issues
    echo
    
    # 生成诊断报告
    generate_report
}

# 执行主函数
main