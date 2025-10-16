#!/bin/bash
# 学生自助服务脚本
# 提供学生常用的自助服务功能

# 获取当前用户
CURRENT_USER=$(whoami)

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

# 显示主菜单
show_main_menu() {
    clear
    echo -e "${CYAN}╔══════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║        Hadoop 学生自助服务系统        ║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════╝${NC}"
    echo
    echo -e "${BLUE}当前用户: ${YELLOW}$CURRENT_USER${NC}"
    echo -e "${BLUE}登录时间: ${YELLOW}$(date)${NC}"
    echo
    echo "请选择服务："
    echo
    echo "  ${GREEN}1.${NC} 查看个人信息"
    echo "  ${GREEN}2.${NC} HDFS 文件操作"
    echo "  ${GREEN}3.${NC} 作业管理"
    echo "  ${GREEN}4.${NC} 系统状态查看"
    echo "  ${GREEN}5.${NC} 问题诊断"
    echo "  ${GREEN}6.${NC} 常见问题解答"
    echo "  ${GREEN}7.${NC} 使用帮助"
    echo "  ${GREEN}0.${NC} 退出"
    echo
    echo -n "请输入选项 [0-7]: "
}

# 查看个人信息
show_personal_info() {
    print_title "个人信息"
    
    echo "基本信息："
    echo "  学号: $CURRENT_USER"
    echo "  用户组: $(groups $CURRENT_USER)"
    echo "  家目录: $HOME"
    echo
    
    # 检查 HDFS 个人目录
    hdfs_dir="/users/$CURRENT_USER"
    if hdfs dfs -test -d "$hdfs_dir" 2>/dev/null; then
        print_success "HDFS 个人目录: $hdfs_dir"
        
        # 显示存储配额信息
        quota_info=$(hdfs dfs -count -q "$hdfs_dir" 2>/dev/null)
        if [ $? -eq 0 ]; then
            space_quota=$(echo "$quota_info" | awk '{print $2}')
            space_used=$(echo "$quota_info" | awk '{print $3}')
            
            if [ "$space_quota" != "none" ] && [ "$space_quota" -gt 0 ]; then
                usage_percent=$((space_used * 100 / space_quota))
                echo "  存储配额: $(numfmt --to=iec $space_quota)"
                echo "  已使用: $(numfmt --to=iec $space_used) (${usage_percent}%)"
                
                if [ "$usage_percent" -gt 90 ]; then
                    print_warning "存储空间即将耗尽！"
                elif [ "$usage_percent" -gt 75 ]; then
                    print_warning "存储使用率较高，请注意清理"
                fi
            else
                echo "  存储配额: 未设置"
            fi
        fi
    else
        print_error "HDFS 个人目录不存在"
    fi
    
    echo
    echo "按任意键返回主菜单..."
    read -n 1
}

# HDFS 文件操作菜单
hdfs_operations_menu() {
    while true; do
        clear
        print_title "HDFS 文件操作"
        echo
        echo "  ${GREEN}1.${NC} 查看个人目录"
        echo "  ${GREEN}2.${NC} 查看目录内容"
        echo "  ${GREEN}3.${NC} 上传文件"
        echo "  ${GREEN}4.${NC} 下载文件"
        echo "  ${GREEN}5.${NC} 删除文件/目录"
        echo "  ${GREEN}6.${NC} 创建目录"
        echo "  ${GREEN}7.${NC} 查看文件内容"
        echo "  ${GREEN}8.${NC} 清理临时文件"
        echo "  ${GREEN}0.${NC} 返回主菜单"
        echo
        echo -n "请输入选项 [0-8]: "
        
        read choice
        case $choice in
            1) list_personal_directory ;;
            2) list_directory_content ;;
            3) upload_file ;;
            4) download_file ;;
            5) delete_file ;;
            6) create_directory ;;
            7) view_file_content ;;
            8) cleanup_temp_files ;;
            0) break ;;
            *) print_error "无效选项，请重新选择" ;;
        esac
    done
}

# 查看个人目录
list_personal_directory() {
    print_title "个人目录结构"
    
    hdfs_dir="/users/$CURRENT_USER"
    if hdfs dfs -test -d "$hdfs_dir" 2>/dev/null; then
        echo "目录: $hdfs_dir"
        hdfs dfs -ls -h "$hdfs_dir" 2>/dev/null || print_error "无法访问个人目录"
    else
        print_error "个人目录不存在: $hdfs_dir"
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 查看目录内容
list_directory_content() {
    echo
    echo -n "请输入要查看的目录路径（相对于个人目录）: "
    read dir_path
    
    if [ -z "$dir_path" ]; then
        dir_path="."
    fi
    
    full_path="/users/$CURRENT_USER/$dir_path"
    
    print_title "目录内容: $full_path"
    
    if hdfs dfs -test -d "$full_path" 2>/dev/null; then
        hdfs dfs -ls -h "$full_path" 2>/dev/null || print_error "无法访问目录"
    else
        print_error "目录不存在: $full_path"
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 上传文件
upload_file() {
    echo
    echo -n "请输入本地文件路径: "
    read local_file
    
    if [ ! -f "$local_file" ]; then
        print_error "本地文件不存在: $local_file"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    echo -n "请输入 HDFS 目标路径（相对于个人目录）: "
    read hdfs_path
    
    if [ -z "$hdfs_path" ]; then
        hdfs_path="."
    fi
    
    full_hdfs_path="/users/$CURRENT_USER/$hdfs_path"
    
    print_info "正在上传文件..."
    if hdfs dfs -put "$local_file" "$full_hdfs_path" 2>/dev/null; then
        print_success "文件上传成功"
    else
        print_error "文件上传失败"
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 下载文件
download_file() {
    echo
    echo -n "请输入 HDFS 文件路径（相对于个人目录）: "
    read hdfs_file
    
    full_hdfs_path="/users/$CURRENT_USER/$hdfs_file"
    
    if ! hdfs dfs -test -f "$full_hdfs_path" 2>/dev/null; then
        print_error "HDFS 文件不存在: $full_hdfs_path"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    echo -n "请输入本地保存路径（默认为当前目录）: "
    read local_path
    
    if [ -z "$local_path" ]; then
        local_path="."
    fi
    
    print_info "正在下载文件..."
    if hdfs dfs -get "$full_hdfs_path" "$local_path" 2>/dev/null; then
        print_success "文件下载成功"
    else
        print_error "文件下载失败"
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 删除文件
delete_file() {
    echo
    echo -n "请输入要删除的文件/目录路径（相对于个人目录）: "
    read file_path
    
    full_path="/users/$CURRENT_USER/$file_path"
    
    if ! hdfs dfs -test -e "$full_path" 2>/dev/null; then
        print_error "文件/目录不存在: $full_path"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    echo
    print_warning "确认要删除 $full_path 吗？"
    echo -n "输入 'yes' 确认删除: "
    read confirmation
    
    if [ "$confirmation" = "yes" ]; then
        if hdfs dfs -rm -r "$full_path" 2>/dev/null; then
            print_success "删除成功"
        else
            print_error "删除失败"
        fi
    else
        print_info "取消删除操作"
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 创建目录
create_directory() {
    echo
    echo -n "请输入要创建的目录路径（相对于个人目录）: "
    read dir_path
    
    full_path="/users/$CURRENT_USER/$dir_path"
    
    if hdfs dfs -test -d "$full_path" 2>/dev/null; then
        print_warning "目录已存在: $full_path"
    else
        if hdfs dfs -mkdir -p "$full_path" 2>/dev/null; then
            print_success "目录创建成功: $full_path"
        else
            print_error "目录创建失败"
        fi
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 查看文件内容
view_file_content() {
    echo
    echo -n "请输入文件路径（相对于个人目录）: "
    read file_path
    
    full_path="/users/$CURRENT_USER/$file_path"
    
    if hdfs dfs -test -f "$full_path" 2>/dev/null; then
        print_title "文件内容: $full_path"
        hdfs dfs -cat "$full_path" 2>/dev/null | head -50
        echo
        print_info "（仅显示前50行）"
    else
        print_error "文件不存在: $full_path"
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 清理临时文件
cleanup_temp_files() {
    print_title "清理临时文件"
    
    temp_dir="/users/$CURRENT_USER/tmp"
    
    if hdfs dfs -test -d "$temp_dir" 2>/dev/null; then
        echo "发现临时目录: $temp_dir"
        
        # 显示临时文件
        temp_files=$(hdfs dfs -ls "$temp_dir" 2>/dev/null | wc -l)
        if [ "$temp_files" -gt 1 ]; then
            echo "临时文件数量: $((temp_files - 1))"
            echo
            print_warning "确认要清理所有临时文件吗？"
            echo -n "输入 'yes' 确认清理: "
            read confirmation
            
            if [ "$confirmation" = "yes" ]; then
                if hdfs dfs -rm -r "$temp_dir/*" 2>/dev/null; then
                    print_success "临时文件清理完成"
                else
                    print_error "清理失败"
                fi
            else
                print_info "取消清理操作"
            fi
        else
            print_info "临时目录为空，无需清理"
        fi
    else
        print_info "临时目录不存在"
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 作业管理菜单
job_management_menu() {
    while true; do
        clear
        print_title "作业管理"
        echo
        echo "  ${GREEN}1.${NC} 查看运行中的作业"
        echo "  ${GREEN}2.${NC} 查看作业历史"
        echo "  ${GREEN}3.${NC} 查看作业详情"
        echo "  ${GREEN}4.${NC} 终止作业"
        echo "  ${GREEN}5.${NC} 提交示例作业"
        echo "  ${GREEN}0.${NC} 返回主菜单"
        echo
        echo -n "请输入选项 [0-5]: "
        
        read choice
        case $choice in
            1) show_running_jobs ;;
            2) show_job_history ;;
            3) show_job_details ;;
            4) kill_job ;;
            5) submit_example_job ;;
            0) break ;;
            *) print_error "无效选项，请重新选择" ;;
        esac
    done
}

# 查看运行中的作业
show_running_jobs() {
    print_title "运行中的作业"
    
    running_jobs=$(yarn application -list -appStates RUNNING 2>/dev/null | grep "$CURRENT_USER" || true)
    
    if [ -n "$running_jobs" ]; then
        echo "$running_jobs"
    else
        print_info "当前没有运行中的作业"
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 查看作业历史
show_job_history() {
    print_title "作业历史（最近10个）"
    
    job_history=$(yarn application -list -appStates ALL 2>/dev/null | grep "$CURRENT_USER" | head -10 || true)
    
    if [ -n "$job_history" ]; then
        echo "$job_history"
    else
        print_info "没有作业历史记录"
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 查看作业详情
show_job_details() {
    echo
    echo -n "请输入作业 ID (application_xxx): "
    read app_id
    
    if [ -z "$app_id" ]; then
        print_error "作业 ID 不能为空"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    print_title "作业详情: $app_id"
    
    yarn application -status "$app_id" 2>/dev/null || print_error "无法获取作业详情"
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 终止作业
kill_job() {
    echo
    echo -n "请输入要终止的作业 ID (application_xxx): "
    read app_id
    
    if [ -z "$app_id" ]; then
        print_error "作业 ID 不能为空"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    # 检查作业是否属于当前用户
    job_info=$(yarn application -status "$app_id" 2>/dev/null | grep "User:" || true)
    if ! echo "$job_info" | grep -q "$CURRENT_USER"; then
        print_error "无法终止其他用户的作业"
        echo "按任意键继续..."
        read -n 1
        return
    fi
    
    echo
    print_warning "确认要终止作业 $app_id 吗？"
    echo -n "输入 'yes' 确认终止: "
    read confirmation
    
    if [ "$confirmation" = "yes" ]; then
        if yarn application -kill "$app_id" 2>/dev/null; then
            print_success "作业终止成功"
        else
            print_error "作业终止失败"
        fi
    else
        print_info "取消终止操作"
    fi
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 提交示例作业
submit_example_job() {
    print_title "提交示例作业"
    
    echo "可用的示例作业："
    echo "  1. WordCount（词频统计）"
    echo "  2. Pi 计算"
    echo "  3. TeraSort 排序测试"
    echo
    echo -n "请选择示例作业 [1-3]: "
    read example_choice
    
    case $example_choice in
        1) submit_wordcount_job ;;
        2) submit_pi_job ;;
        3) submit_terasort_job ;;
        *) print_error "无效选项" ;;
    esac
    
    echo
    echo "按任意键继续..."
    read -n 1
}

# 提交 WordCount 作业
submit_wordcount_job() {
    print_info "准备提交 WordCount 作业..."
    
    # 检查输入文件
    input_dir="/users/$CURRENT_USER/homework1/problem1/input"
    output_dir="/users/$CURRENT_USER/homework1/problem1/output"
    
    if ! hdfs dfs -test -d "$input_dir" 2>/dev/null; then
        print_error "输入目录不存在: $input_dir"
        return
    fi
    
    # 检查输入文件是否存在
    input_files=$(hdfs dfs -ls "$input_dir" 2>/dev/null | grep -v "^d" | wc -l)
    if [ "$input_files" -eq 0 ]; then
        print_error "输入目录为空，请先上传测试文件"
        return
    fi
    
    # 删除输出目录（如果存在）
    hdfs dfs -rm -r "$output_dir" 2>/dev/null || true
    
    # 提交作业
    print_info "正在提交 WordCount 作业..."
    if hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount "$input_dir" "$output_dir"; then
        print_success "WordCount 作业提交成功"
        print_info "输出目录: $output_dir"
    else
        print_error "WordCount 作业提交失败"
    fi
}

# 提交 Pi 计算作业
submit_pi_job() {
    print_info "准备提交 Pi 计算作业..."
    
    echo -n "请输入 Map 任务数量（默认10）: "
    read map_tasks
    if [ -z "$map_tasks" ]; then
        map_tasks=10
    fi
    
    echo -n "请输入每个 Map 的采样数（默认1000）: "
    read samples
    if [ -z "$samples" ]; then
        samples=1000
    fi
    
    print_info "正在提交 Pi 计算作业（Map任务: $map_tasks, 采样数: $samples）..."
    if hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar pi "$map_tasks" "$samples"; then
        print_success "Pi 计算作业提交成功"
    else
        print_error "Pi 计算作业提交失败"
    fi
}

# 提交 TeraSort 作业
submit_terasort_job() {
    print_info "准备提交 TeraSort 作业..."
    
    echo -n "请输入数据大小（行数，默认1000）: "
    read num_rows
    if [ -z "$num_rows" ]; then
        num_rows=1000
    fi
    
    input_dir="/users/$CURRENT_USER/terasort/input"
    output_dir="/users/$CURRENT_USER/terasort/output"
    
    # 清理旧数据
    hdfs dfs -rm -r "/users/$CURRENT_USER/terasort" 2>/dev/null || true
    hdfs dfs -mkdir -p "$input_dir"
    
    print_info "正在生成测试数据..."
    if hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar teragen "$num_rows" "$input_dir"; then
        print_success "测试数据生成成功"
        
        print_info "正在执行排序..."
        if hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar terasort "$input_dir" "$output_dir"; then
            print_success "TeraSort 作业完成"
            print_info "输出目录: $output_dir"
        else
            print_error "TeraSort 作业失败"
        fi
    else
        print_error "测试数据生成失败"
    fi
}

# 系统状态查看
show_system_status() {
    print_title "系统状态"
    
    echo "集群状态："
    
    # 检查 HDFS 状态
    if hdfs dfsadmin -report 2>/dev/null | head -10; then
        print_success "HDFS 服务正常"
    else
        print_error "HDFS 服务异常"
    fi
    
    echo
    echo "YARN 队列状态："
    yarn queue -list 2>/dev/null | grep -E "(Queue Name|Queue State|Capacity)" || print_error "无法获取队列信息"
    
    echo
    echo "个人资源使用情况："
    running_apps=$(yarn application -list -appStates RUNNING 2>/dev/null | grep "$CURRENT_USER" | wc -l)
    echo "  运行中的作业: $running_apps"
    
    echo
    echo "按任意键返回主菜单..."
    read -n 1
}

# 问题诊断
run_diagnosis() {
    print_title "问题诊断"
    
    print_info "正在运行诊断脚本..."
    
    # 检查诊断脚本是否存在
    script_dir="$(dirname "$0")"
    diagnosis_script="$script_dir/08-diagnose_student_issues.sh"
    
    if [ -f "$diagnosis_script" ]; then
        bash "$diagnosis_script" "$CURRENT_USER"
    else
        print_error "诊断脚本不存在: $diagnosis_script"
        print_info "请联系管理员"
    fi
    
    echo
    echo "按任意键返回主菜单..."
    read -n 1
}

# 常见问题解答
show_faq() {
    clear
    print_title "常见问题解答"
    
    cat << 'EOF'

Q1: 如何上传文件到 HDFS？
A1: 使用命令：hdfs dfs -put <本地文件> /users/<学号>/<目标路径>
    或者使用本脚本的 "HDFS 文件操作" 功能

Q2: 如何查看作业运行状态？
A2: 使用命令：yarn application -list
    或者访问 Web UI：http://hadoop-master:8088

Q3: 作业失败了怎么办？
A3: 1. 使用 "问题诊断" 功能检查环境
    2. 查看作业日志：yarn logs -applicationId <app_id>
    3. 检查输入数据和输出目录

Q4: 存储空间不足怎么办？
A4: 1. 清理不需要的文件：hdfs dfs -rm -r <路径>
    2. 清理临时文件：使用本脚本的清理功能
    3. 联系管理员增加配额

Q5: 无法连接到集群怎么办？
A5: 1. 检查网络连接
    2. 确认集群服务正在运行
    3. 检查客户端配置文件

Q6: 如何提交 MapReduce 作业？
A6: hadoop jar <jar文件> <主类> <输入路径> <输出路径>
    注意：输出路径必须不存在

Q7: 如何查看 HDFS 文件内容？
A7: hdfs dfs -cat <文件路径>
    或者：hdfs dfs -text <文件路径>（支持压缩文件）

Q8: 作业运行很慢怎么办？
A8: 1. 检查输入数据大小和分片
    2. 调整 Map 和 Reduce 任务数
    3. 检查集群资源使用情况

Q9: 权限被拒绝怎么办？
A9: 1. 确认操作的是自己的目录
    2. 检查文件权限：hdfs dfs -ls -l
    3. 联系管理员检查权限配置

Q10: 如何备份重要数据？
A10: 1. 下载到本地：hdfs dfs -get <hdfs路径> <本地路径>
     2. 复制到其他 HDFS 目录：hdfs dfs -cp <源路径> <目标路径>

EOF
    
    echo
    echo "按任意键返回主菜单..."
    read -n 1
}

# 使用帮助
show_help() {
    clear
    print_title "使用帮助"
    
    cat << 'EOF'

=== Hadoop 学生自助服务系统使用指南 ===

1. 系统功能概述
   - 个人信息查看：查看账户状态、存储配额等
   - HDFS 文件操作：上传、下载、删除文件等
   - 作业管理：提交、监控、终止 MapReduce 作业
   - 系统状态：查看集群和个人资源状态
   - 问题诊断：自动检测和诊断常见问题
   - 常见问题解答：查看解决方案

2. 重要目录说明
   - 个人 HDFS 目录：/users/<学号>
   - 作业输入目录：/users/<学号>/homework1/problem1/input
   - 作业输出目录：/users/<学号>/homework1/problem1/output
   - 临时文件目录：/users/<学号>/tmp

3. 常用命令
   - 查看 HDFS 文件：hdfs dfs -ls <路径>
   - 上传文件：hdfs dfs -put <本地文件> <HDFS路径>
   - 下载文件：hdfs dfs -get <HDFS路径> <本地路径>
   - 提交作业：hadoop jar <jar文件> <主类> <参数>
   - 查看作业：yarn application -list

4. Web 界面
   - YARN ResourceManager：http://hadoop-master:8088
   - HDFS NameNode：http://hadoop-master:9870
   - MapReduce JobHistory：http://hadoop-master:19888

5. 注意事项
   - 请勿删除其他用户的文件
   - 注意存储配额限制
   - 作业输出目录必须不存在
   - 定期清理临时文件

6. 获取帮助
   - 使用本系统的问题诊断功能
   - 查看常见问题解答
   - 联系管理员或助教

EOF
    
    echo
    echo "按任意键返回主菜单..."
    read -n 1
}

# 主循环
main() {
    # 检查是否在 Hadoop 环境中
    if [ -z "$HADOOP_HOME" ]; then
        print_error "Hadoop 环境未配置，请先配置 HADOOP_HOME"
        exit 1
    fi
    
    while true; do
        show_main_menu
        read choice
        
        case $choice in
            1) show_personal_info ;;
            2) hdfs_operations_menu ;;
            3) job_management_menu ;;
            4) show_system_status ;;
            5) run_diagnosis ;;
            6) show_faq ;;
            7) show_help ;;
            0) 
                echo
                print_info "感谢使用 Hadoop 学生自助服务系统！"
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