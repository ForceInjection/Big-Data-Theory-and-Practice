#!/bin/bash
# 管理员支持工具脚本
# 提供管理员常用的集群管理和学生支持功能

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${BLUE}[信息]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[成功]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[警告]${NC} $1"
}

print_error() {
    echo -e "${RED}[错误]${NC} $1"
}

print_title() {
    echo -e "${CYAN}=== $1 ===${NC}"
}

# 检查管理员权限
check_admin_privileges() {
    if [ "$EUID" -ne 0 ] && ! groups | grep -q "sudo\|wheel\|admin"; then
        print_warning "建议使用管理员权限运行此脚本以获得完整功能"
        echo -n "是否继续？[y/N]: "
        read continue_choice
        if [ "$continue_choice" != "y" ] && [ "$continue_choice" != "Y" ]; then
            exit 1
        fi
    fi
}

# 显示主菜单
show_main_menu() {
    clear
    echo -e "${CYAN}╔══════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║        Hadoop 管理员支持工具          ║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════╝${NC}"
    echo
    echo -e "${BLUE}管理员: ${YELLOW}$(whoami)${NC}"
    echo -e "${BLUE}时间: ${YELLOW}$(date)${NC}"
    echo
    echo "请选择功能："
    echo
    echo "  ${GREEN}1.${NC} 用户管理"
    echo "  ${GREEN}2.${NC} 集群状态监控"
    echo "  ${GREEN}3.${NC} 学生支持"
    echo "  ${GREEN}4.${NC} 资源管理"
    echo "  ${GREEN}5.${NC} 数据备份与恢复"
    echo "  ${GREEN}6.${NC} 系统维护"
    echo "  ${GREEN}7.${NC} 日志分析"
    echo "  ${GREEN}8.${NC} 配置管理"
    echo "  ${GREEN}0.${NC} 退出"
    echo
    echo -n "请输入选项 [0-8]: "
}

# 用户管理菜单
user_management_menu() {
    while true; do
        clear
        print_title "用户管理"
        echo
        echo "  ${GREEN}1.${NC} 批量创建学生账户"
        echo "  ${GREEN}2.${NC} 查看所有学生状态"
        echo "  ${GREEN}3.${NC} 重置学生密码"
        echo "  ${GREEN}4.${NC} 删除学生账户"
        echo "  ${GREEN}5.${NC} 设置存储配额"
        echo "  ${GREEN}6.${NC} 查看配额使用情况"
        echo "  ${GREEN}0.${NC} 返回主菜单"
        echo
        echo -n "请输入选项 [0-6]: "
        
        read choice
        case $choice in
            1) batch_create_students ;;
            2) show_all_students_status ;;
            3) reset_student_password ;;
            4) delete_student_account ;;
            5) set_storage_quota ;;
            6) show_quota_usage ;;
            0) break ;;
            *) print_error "无效选项，请重新选择" ;;
        esac
    done
}

# 批量创建学生账户
batch_create_students() {
    print_title "批量创建学生账户"
    
    echo -n "请输入学生名单文件路径（默认: students.txt）: "
    read student_file
    if [ -z "$student_file" ]; then
        student_file="students.txt"
    fi
    
    if [ ! -f "$student_file" ]; then
        print_error "学生名单文件不存在: $student_file"
        echo "请创建包含学号的文件，每行一个学号"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    script_dir="$(dirname "$0")"
    
    print_info "正在创建系统账户..."
    if [ -f "$script_dir/01-create_students.sh" ]; then
    bash "$script_dir/01-create_students.sh" "$student_file"
else
    print_error "01-create_students.sh 脚本不存在"
    fi
    
    echo
    print_info "正在创建 HDFS 目录..."
    if [ -f "$script_dir/02-create_hdfs_dirs.sh" ]; then
    bash "$script_dir/02-create_hdfs_dirs.sh" "$student_file"
else
    print_error "02-create_hdfs_dirs.sh 脚本不存在"
    fi
    
    echo
    print_info "正在设置存储配额..."
    echo -n "请输入默认配额大小（GB，默认5）: "
    read quota_size
    if [ -z "$quota_size" ]; then
        quota_size=5
    fi
    
    if [ -f "$script_dir/04-set_hdfs_quotas.sh" ]; then
    bash "$script_dir/04-set_hdfs_quotas.sh" "$student_file" "$quota_size"
else
    print_error "04-set_hdfs_quotas.sh 脚本不存在"
    fi
    
    echo
    print_info "正在配置客户端环境..."
    if [ -f "$script_dir/05-setup_student_client.sh" ]; then
    bash "$script_dir/05-setup_student_client.sh" "$student_file"
else
    print_error "05-setup_student_client.sh 脚本不存在"
    fi
    
    print_success "批量创建完成"
    echo
    echo "按任意键继续..."
    read -n 1
}

# 查看所有学生状态
show_all_students_status() {
    print_title "所有学生状态"
    
    echo "系统学生账户："
    students=$(getent group students | cut -d: -f4 | tr ',' '\n' | sort)
    
    if [ -z "$students" ]; then
        print_info "未找到学生账户"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    total_students=0
    active_students=0
    
    printf "%-12s %-8s %-15s %-15s %-10s\n" "学号" "状态" "HDFS目录" "存储使用" "最近登录"
    echo "--------------------------------------------------------------------"
    
    for student in $students; do
        if [ -z "$student" ]; then
            continue
        fi
        
        total_students=$((total_students + 1))
        
        # 检查账户状态
        if id "$student" &>/dev/null; then
            account_status="正常"
            active_students=$((active_students + 1))
        else
            account_status="异常"
        fi
        
        # 检查 HDFS 目录
        hdfs_dir="/users/$student"
        if hdfs dfs -test -d "$hdfs_dir" 2>/dev/null; then
            hdfs_status="存在"
            
            # 获取存储使用情况
            quota_info=$(hdfs dfs -count -q "$hdfs_dir" 2>/dev/null)
            if [ $? -eq 0 ]; then
                space_used=$(echo "$quota_info" | awk '{print $3}')
                if [ "$space_used" -gt 0 ]; then
                    storage_usage="$(numfmt --to=iec $space_used)"
                else
                    storage_usage="0B"
                fi
            else
                storage_usage="未知"
            fi
        else
            hdfs_status="缺失"
            storage_usage="N/A"
        fi
        
        # 获取最近登录时间
        last_login=$(last -n 1 "$student" 2>/dev/null | head -1 | awk '{print $4" "$5}' || echo "从未")
        if [ "$last_login" = "从未" ] || [ -z "$last_login" ]; then
            last_login="从未"
        fi
        
        printf "%-12s %-8s %-15s %-15s %-10s\n" "$student" "$account_status" "$hdfs_status" "$storage_usage" "$last_login"
    done
    
    echo "--------------------------------------------------------------------"
    echo "统计信息："
    echo "  总学生数: $total_students"
    echo "  正常账户: $active_students"
    echo "  异常账户: $((total_students - active_students))"
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 重置学生密码
reset_student_password() {
    print_title "重置学生密码"
    
    echo -n "请输入学生学号: "
    read student_id
    
    if [ -z "$student_id" ]; then
        print_error "学号不能为空"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    if ! id "$student_id" &>/dev/null; then
        print_error "学生账户不存在: $student_id"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    echo -n "请输入新密码（默认: hadoop123）: "
    read -s new_password
    echo
    
    if [ -z "$new_password" ]; then
        new_password="hadoop123"
    fi
    
    if echo "$student_id:$new_password" | sudo chpasswd 2>/dev/null; then
        print_success "密码重置成功"
        print_info "新密码: $new_password"
    else
        print_error "密码重置失败"
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 删除学生账户
delete_student_account() {
    print_title "删除学生账户"
    
    echo -n "请输入要删除的学生学号: "
    read student_id
    
    if [ -z "$student_id" ]; then
        print_error "学号不能为空"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    if ! id "$student_id" &>/dev/null; then
        print_error "学生账户不存在: $student_id"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    echo
    print_warning "确认要删除学生账户 $student_id 吗？"
    print_warning "这将删除系统账户和所有 HDFS 数据！"
    echo -n "输入 'DELETE' 确认删除: "
    read confirmation
    
    if [ "$confirmation" != "DELETE" ]; then
        print_info "取消删除操作"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    # 删除 HDFS 数据
    hdfs_dir="/users/$student_id"
    if hdfs dfs -test -d "$hdfs_dir" 2>/dev/null; then
        print_info "正在删除 HDFS 数据..."
        hdfs dfs -rm -r "$hdfs_dir" 2>/dev/null || print_warning "HDFS 数据删除失败"
    fi
    
    # 删除系统账户
    print_info "正在删除系统账户..."
    if sudo userdel -r "$student_id" 2>/dev/null; then
        print_success "学生账户删除成功"
    else
        print_error "系统账户删除失败"
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 设置存储配额
set_storage_quota() {
    print_title "设置存储配额"
    
    echo -n "请输入学生学号（或 'all' 表示所有学生）: "
    read student_input
    
    if [ -z "$student_input" ]; then
        print_error "输入不能为空"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    echo -n "请输入配额大小（GB）: "
    read quota_gb
    
    if [ -z "$quota_gb" ] || ! [[ "$quota_gb" =~ ^[0-9]+$ ]]; then
        print_error "配额大小必须是正整数"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    quota_bytes=$((quota_gb * 1024 * 1024 * 1024))
    
    if [ "$student_input" = "all" ]; then
        print_info "正在为所有学生设置配额..."
        students=$(getent group students | cut -d: -f4 | tr ',' '\n')
        
        for student in $students; do
            if [ -n "$student" ] && hdfs dfs -test -d "/users/$student" 2>/dev/null; then
                if hdfs dfsadmin -setSpaceQuota "$quota_bytes" "/users/$student" 2>/dev/null; then
                    print_success "已为 $student 设置配额: ${quota_gb}GB"
                else
                    print_error "为 $student 设置配额失败"
                fi
            fi
        done
    else
        if ! id "$student_input" &>/dev/null; then
            print_error "学生账户不存在: $student_input"
            echo "按任意键继续..."
            read -n 1
            return
        fi
        
        hdfs_dir="/users/$student_input"
        if ! hdfs dfs -test -d "$hdfs_dir" 2>/dev/null; then
            print_error "HDFS 目录不存在: $hdfs_dir"
            echo "按任意键继续..."
            read -n 1
            return
        fi
        
        if hdfs dfsadmin -setSpaceQuota "$quota_bytes" "$hdfs_dir" 2>/dev/null; then
            print_success "配额设置成功: ${quota_gb}GB"
        else
            print_error "配额设置失败"
        fi
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 查看配额使用情况
show_quota_usage() {
    print_title "配额使用情况"
    
    students=$(getent group students | cut -d: -f4 | tr ',' '\n' | sort)
    
    if [ -z "$students" ]; then
        print_info "未找到学生账户"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    printf "%-12s %-12s %-12s %-8s %-10s\n" "学号" "配额" "已使用" "使用率" "状态"
    echo "------------------------------------------------------------"
    
    total_quota=0
    total_used=0
    
    for student in $students; do
        if [ -z "$student" ]; then
            continue
        fi
        
        hdfs_dir="/users/$student"
        if hdfs dfs -test -d "$hdfs_dir" 2>/dev/null; then
            quota_info=$(hdfs dfs -count -q "$hdfs_dir" 2>/dev/null)
            if [ $? -eq 0 ]; then
                space_quota=$(echo "$quota_info" | awk '{print $2}')
                space_used=$(echo "$quota_info" | awk '{print $3}')
                
                if [ "$space_quota" = "none" ] || [ "$space_quota" -eq 0 ]; then
                    quota_display="无限制"
                    usage_percent="N/A"
                    status="正常"
                else
                    quota_display="$(numfmt --to=iec $space_quota)"
                    usage_percent=$((space_used * 100 / space_quota))
                    
                    if [ "$usage_percent" -gt 90 ]; then
                        status="警告"
                    elif [ "$usage_percent" -gt 75 ]; then
                        status="注意"
                    else
                        status="正常"
                    fi
                    
                    total_quota=$((total_quota + space_quota))
                fi
                
                used_display="$(numfmt --to=iec $space_used)"
                total_used=$((total_used + space_used))
                
                printf "%-12s %-12s %-12s %-8s %-10s\n" "$student" "$quota_display" "$used_display" "${usage_percent}%" "$status"
            else
                printf "%-12s %-12s %-12s %-8s %-10s\n" "$student" "错误" "错误" "N/A" "异常"
            fi
        else
            printf "%-12s %-12s %-12s %-8s %-10s\n" "$student" "无目录" "N/A" "N/A" "异常"
        fi
    done
    
    echo "------------------------------------------------------------"
    echo "总计："
    echo "  总配额: $(numfmt --to=iec $total_quota)"
    echo "  总使用: $(numfmt --to=iec $total_used)"
    if [ "$total_quota" -gt 0 ]; then
        total_usage_percent=$((total_used * 100 / total_quota))
        echo "  总使用率: ${total_usage_percent}%"
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 集群状态监控
cluster_monitoring_menu() {
    while true; do
        clear
        print_title "集群状态监控"
        echo
        echo "  ${GREEN}1.${NC} HDFS 状态"
        echo "  ${GREEN}2.${NC} YARN 状态"
        echo "  ${GREEN}3.${NC} 节点状态"
        echo "  ${GREEN}4.${NC} 作业统计"
        echo "  ${GREEN}5.${NC} 资源使用情况"
        echo "  ${GREEN}6.${NC} 性能指标"
        echo "  ${GREEN}0.${NC} 返回主菜单"
        echo
        echo -n "请输入选项 [0-6]: "
        
        read choice
        case $choice in
            1) show_hdfs_status ;;
            2) show_yarn_status ;;
            3) show_node_status ;;
            4) show_job_statistics ;;
            5) show_resource_usage ;;
            6) show_performance_metrics ;;
            0) break ;;
            *) print_error "无效选项，请重新选择" ;;
        esac
    done
}

# 显示 HDFS 状态
show_hdfs_status() {
    print_title "HDFS 状态"
    
    print_info "HDFS 概览："
    hdfs dfsadmin -report 2>/dev/null | head -20
    
    echo
    print_info "HDFS 安全模式状态："
    hdfs dfsadmin -safemode get 2>/dev/null
    
    echo
    print_info "损坏的块："
    hdfs fsck / -list-corruptfileblocks 2>/dev/null | head -10
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 显示 YARN 状态
show_yarn_status() {
    print_title "YARN 状态"
    
    print_info "YARN 节点状态："
    yarn node -list 2>/dev/null
    
    echo
    print_info "YARN 队列状态："
    yarn queue -list 2>/dev/null
    
    echo
    print_info "YARN 应用统计："
    yarn application -list -appStates ALL 2>/dev/null | head -10
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 显示节点状态
show_node_status() {
    print_title "节点状态"
    
    print_info "DataNode 状态："
    hdfs dfsadmin -printTopology 2>/dev/null
    
    echo
    print_info "NodeManager 状态："
    yarn node -list -all 2>/dev/null
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 显示作业统计
show_job_statistics() {
    print_title "作业统计"
    
    print_info "当前运行的作业："
    running_jobs=$(yarn application -list -appStates RUNNING 2>/dev/null | wc -l)
    echo "  运行中: $((running_jobs - 1))"
    
    print_info "今日完成的作业："
    today=$(date +%Y-%m-%d)
    completed_today=$(yarn application -list -appStates FINISHED 2>/dev/null | grep "$today" | wc -l)
    echo "  完成: $completed_today"
    
    print_info "今日失败的作业："
    failed_today=$(yarn application -list -appStates FAILED 2>/dev/null | grep "$today" | wc -l)
    echo "  失败: $failed_today"
    
    echo
    print_info "按用户统计（最近10个作业）："
    yarn application -list -appStates ALL 2>/dev/null | head -11 | tail -10 | awk '{print $2}' | sort | uniq -c | sort -nr
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 显示资源使用情况
show_resource_usage() {
    print_title "资源使用情况"
    
    print_info "内存使用情况："
    yarn top 2>/dev/null | head -20
    
    echo
    print_info "磁盘使用情况："
    df -h | grep -E "(Filesystem|/dev/)"
    
    echo
    print_info "HDFS 存储使用："
    hdfs dfsadmin -report 2>/dev/null | grep -E "(Configured Capacity|DFS Used|DFS Remaining)"
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 显示性能指标
show_performance_metrics() {
    print_title "性能指标"
    
    print_info "系统负载："
    uptime
    
    echo
    print_info "内存使用："
    free -h
    
    echo
    print_info "CPU 使用："
    top -bn1 | head -5
    
    echo
    print_info "网络连接："
    netstat -an | grep -E ":9000|:8032|:8088" | head -5
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 学生支持菜单
student_support_menu() {
    while true; do
        clear
        print_title "学生支持"
        echo
        echo "  ${GREEN}1.${NC} 诊断学生问题"
        echo "  ${GREEN}2.${NC} 查看学生日志"
        echo "  ${GREEN}3.${NC} 重启学生作业"
        echo "  ${GREEN}4.${NC} 清理学生数据"
        echo "  ${GREEN}5.${NC} 学生环境重置"
        echo "  ${GREEN}0.${NC} 返回主菜单"
        echo
        echo -n "请输入选项 [0-5]: "
        
        read choice
        case $choice in
            1) diagnose_student_problem ;;
            2) view_student_logs ;;
            3) restart_student_job ;;
            4) cleanup_student_data ;;
            5) reset_student_environment ;;
            0) break ;;
            *) print_error "无效选项，请重新选择" ;;
        esac
    done
}

# 诊断学生问题
diagnose_student_problem() {
    print_title "诊断学生问题"
    
    echo -n "请输入学生学号: "
    read student_id
    
    if [ -z "$student_id" ]; then
        print_error "学号不能为空"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    script_dir="$(dirname "$0")"
    diagnosis_script="$script_dir/08-diagnose_student_issues.sh"
    
    if [ -f "$diagnosis_script" ]; then
        bash "$diagnosis_script" "$student_id"
    else
        print_error "诊断脚本不存在: $diagnosis_script"
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 查看学生日志
view_student_logs() {
    print_title "查看学生日志"
    
    echo -n "请输入学生学号: "
    read student_id
    
    if [ -z "$student_id" ]; then
        print_error "学号不能为空"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    script_dir="$(dirname "$0")"
    log_script="$script_dir/07-view_student_logs.sh"
    
    if [ -f "$log_script" ]; then
        bash "$log_script" "$student_id"
    else
        print_error "日志查看脚本不存在: $log_script"
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 重启学生作业
restart_student_job() {
    print_title "重启学生作业"
    
    echo -n "请输入学生学号: "
    read student_id
    
    if [ -z "$student_id" ]; then
        print_error "学号不能为空"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    echo -n "请输入作业 ID (application_xxx): "
    read app_id
    
    if [ -z "$app_id" ]; then
        print_error "作业 ID 不能为空"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    # 检查作业是否属于该学生
    job_info=$(yarn application -status "$app_id" 2>/dev/null | grep "User:" || true)
    if ! echo "$job_info" | grep -q "$student_id"; then
        print_error "作业不属于该学生或作业不存在"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    print_warning "确认要重启作业 $app_id 吗？"
    echo -n "输入 'yes' 确认: "
    read confirmation
    
    if [ "$confirmation" = "yes" ]; then
        if yarn application -kill "$app_id" 2>/dev/null; then
            print_success "作业已终止，请学生重新提交"
        else
            print_error "作业终止失败"
        fi
    else
        print_info "取消重启操作"
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 清理学生数据
cleanup_student_data() {
    print_title "清理学生数据"
    
    echo -n "请输入学生学号: "
    read student_id
    
    if [ -z "$student_id" ]; then
        print_error "学号不能为空"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    if ! id "$student_id" &>/dev/null; then
        print_error "学生账户不存在: $student_id"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    echo "清理选项："
    echo "  1. 清理临时文件"
    echo "  2. 清理输出目录"
    echo "  3. 清理所有数据（保留目录结构）"
    echo -n "请选择 [1-3]: "
    read cleanup_option
    
    hdfs_dir="/users/$student_id"
    
    case $cleanup_option in
        1)
            print_info "正在清理临时文件..."
            hdfs dfs -rm -r "$hdfs_dir/tmp/*" 2>/dev/null || print_info "临时目录为空"
            ;;
        2)
            print_info "正在清理输出目录..."
            hdfs dfs -rm -r "$hdfs_dir/*/output" 2>/dev/null || print_info "输出目录为空"
            ;;
        3)
            print_warning "确认要清理所有数据吗？"
            echo -n "输入 'CLEAN' 确认: "
            read confirmation
            
            if [ "$confirmation" = "CLEAN" ]; then
                print_info "正在清理所有数据..."
                hdfs dfs -rm -r "$hdfs_dir/*" 2>/dev/null || print_info "目录已为空"
                
                # 重新创建基本目录结构
                hdfs dfs -mkdir -p "$hdfs_dir/homework1/problem1/input"
                hdfs dfs -mkdir -p "$hdfs_dir/homework1/problem1/output"
                hdfs dfs -mkdir -p "$hdfs_dir/tmp"
                hdfs dfs -chown -R "$student_id:students" "$hdfs_dir"
                
                print_success "数据清理完成，目录结构已重建"
            else
                print_info "取消清理操作"
            fi
            ;;
        *)
            print_error "无效选项"
            ;;
    esac
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 重置学生环境
reset_student_environment() {
    print_title "重置学生环境"
    
    echo -n "请输入学生学号: "
    read student_id
    
    if [ -z "$student_id" ]; then
        print_error "学号不能为空"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    if ! id "$student_id" &>/dev/null; then
        print_error "学生账户不存在: $student_id"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    print_warning "确认要重置学生环境吗？这将："
    echo "  - 重新配置客户端环境"
    echo "  - 重建 HDFS 目录结构"
    echo "  - 重置权限设置"
    echo -n "输入 'RESET' 确认: "
    read confirmation
    
    if [ "$confirmation" != "RESET" ]; then
        print_info "取消重置操作"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    script_dir="$(dirname "$0")"
    
    # 创建临时学生文件
    temp_file="/tmp/reset_student_$$.txt"
    echo "$student_id" > "$temp_file"
    
    print_info "正在重置客户端配置..."
    if [ -f "$script_dir/05-setup_student_client.sh" ]; then
        bash "$script_dir/05-setup_student_client.sh" "$temp_file"
    fi
    
    print_info "正在重建 HDFS 目录..."
    if [ -f "$script_dir/02-create_hdfs_dirs.sh" ]; then
        bash "$script_dir/02-create_hdfs_dirs.sh" "$temp_file"
    fi
    
    # 清理临时文件
    rm -f "$temp_file"
    
    print_success "学生环境重置完成"
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 主函数
main() {
    check_admin_privileges
    
    while true; do
        show_main_menu
        read choice
        
        case $choice in
            1) user_management_menu ;;
            2) cluster_monitoring_menu ;;
            3) student_support_menu ;;
            4) print_info "资源管理功能开发中..." ; sleep 2 ;;
            5) print_info "数据备份与恢复功能开发中..." ; sleep 2 ;;
            6) print_info "系统维护功能开发中..." ; sleep 2 ;;
            7) print_info "日志分析功能开发中..." ; sleep 2 ;;
            8) print_info "配置管理功能开发中..." ; sleep 2 ;;
            0) 
                echo
                print_info "感谢使用 Hadoop 管理员支持工具！"
                exit 0
                ;;
            *)
                print_error "无效选项，请重新选择"
                sleep 1
                ;;
        esac
    done
}

# 运行主程序
main