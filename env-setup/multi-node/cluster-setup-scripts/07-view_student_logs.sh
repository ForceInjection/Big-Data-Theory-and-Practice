#!/bin/bash
# 查看学生作业日志脚本
# 用法: ./07-view_student_logs.sh <学号> [应用ID]

STUDENT_ID="$1"
APP_ID="$2"

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

# 显示使用说明
show_usage() {
    echo "用法: $0 <学号> [应用ID]"
    echo
    echo "参数说明:"
    echo "  学号     - 必需，要查看日志的学生学号"
    echo "  应用ID   - 可选，具体的应用ID（如 application_xxx）"
    echo
    echo "示例:"
    echo "  $0 2024001                    # 查看学生 2024001 的所有应用"
    echo "  $0 2024001 application_xxx    # 查看学生 2024001 的特定应用日志"
    echo
    echo "功能:"
    echo "  - 列出学生的所有应用"
    echo "  - 查看应用详细信息"
    echo "  - 查看应用日志"
    echo "  - 查看容器日志"
}

# 检查参数
if [ -z "$STUDENT_ID" ]; then
    print_error "缺少学号参数"
    show_usage
    exit 1
fi

# 检查用户是否存在
if ! id "$STUDENT_ID" &>/dev/null; then
    print_error "用户 $STUDENT_ID 不存在"
    exit 1
fi

# 检查 YARN 连接
check_yarn_connection() {
    if ! yarn application -list >/dev/null 2>&1; then
        print_error "无法连接到 YARN，请检查 Hadoop 集群状态"
        return 1
    fi
    return 0
}

# 列出学生的所有应用
list_student_applications() {
    local student_id="$1"
    
    print_info "查找学生 $student_id 的应用..."
    
    # 获取所有应用并过滤
    all_apps=$(yarn application -list -appStates ALL 2>/dev/null)
    student_apps=$(echo "$all_apps" | grep "$student_id" || true)
    
    if [ -z "$student_apps" ]; then
        print_warning "未找到学生 $student_id 的应用"
        return 1
    fi
    
    echo "学生 $student_id 的应用列表:"
    echo "----------------------------------------"
    echo "$student_apps"
    echo "----------------------------------------"
    
    # 提取应用ID列表
    app_ids=$(echo "$student_apps" | awk '{print $1}' | grep "^application_")
    echo
    print_info "应用ID列表:"
    for app_id in $app_ids; do
        echo "  $app_id"
    done
    
    return 0
}

# 查看应用详细信息
show_application_info() {
    local app_id="$1"
    
    print_info "查看应用 $app_id 的详细信息..."
    
    # 获取应用状态
    app_status=$(yarn application -status "$app_id" 2>/dev/null)
    if [ $? -ne 0 ]; then
        print_error "无法获取应用 $app_id 的状态信息"
        return 1
    fi
    
    echo "应用状态信息:"
    echo "----------------------------------------"
    echo "$app_status"
    echo "----------------------------------------"
    
    # 获取应用尝试信息
    print_info "获取应用尝试信息..."
    app_attempts=$(yarn applicationattempt -list "$app_id" 2>/dev/null)
    if [ $? -eq 0 ] && [ -n "$app_attempts" ]; then
        echo "应用尝试信息:"
        echo "----------------------------------------"
        echo "$app_attempts"
        echo "----------------------------------------"
    fi
    
    return 0
}

# 查看应用日志
show_application_logs() {
    local app_id="$1"
    
    print_info "查看应用 $app_id 的日志..."
    
    # 获取应用日志
    app_logs=$(yarn logs -applicationId "$app_id" 2>/dev/null)
    if [ $? -ne 0 ]; then
        print_warning "无法获取应用 $app_id 的日志，可能应用还在运行或日志已清理"
        
        # 尝试获取运行中应用的实时日志
        print_info "尝试获取实时日志信息..."
        
        # 获取容器列表
        containers=$(yarn container -list "$app_id" 2>/dev/null || true)
        if [ -n "$containers" ]; then
            echo "容器列表:"
            echo "$containers"
        else
            print_warning "无法获取容器信息"
        fi
        
        return 1
    fi
    
    echo "应用日志:"
    echo "========================================"
    echo "$app_logs"
    echo "========================================"
    
    return 0
}

# 查看最近的错误日志
show_recent_errors() {
    local student_id="$1"
    
    print_info "查找学生 $student_id 最近的错误..."
    
    # 获取失败的应用
    failed_apps=$(yarn application -list -appStates FAILED 2>/dev/null | grep "$student_id" | head -3 || true)
    
    if [ -z "$failed_apps" ]; then
        print_success "未发现最近的失败应用"
        return 0
    fi
    
    echo "最近失败的应用:"
    echo "----------------------------------------"
    echo "$failed_apps"
    echo "----------------------------------------"
    
    # 获取失败应用的详细信息
    failed_app_ids=$(echo "$failed_apps" | awk '{print $1}')
    for app_id in $failed_app_ids; do
        echo
        print_info "分析失败应用: $app_id"
        
        # 获取应用状态
        app_status=$(yarn application -status "$app_id" 2>/dev/null)
        if [ $? -eq 0 ]; then
            # 提取失败原因
            final_status=$(echo "$app_status" | grep "Final-State" || true)
            diagnostics=$(echo "$app_status" | grep -A 5 "Diagnostics" || true)
            
            if [ -n "$final_status" ]; then
                echo "  最终状态: $final_status"
            fi
            
            if [ -n "$diagnostics" ]; then
                echo "  诊断信息:"
                echo "$diagnostics" | sed 's/^/    /'
            fi
        fi
    done
    
    return 0
}

# 交互式日志查看
interactive_log_viewer() {
    local student_id="$1"
    
    while true; do
        echo
        echo "=== 学生 $student_id 日志查看器 ==="
        echo "1. 列出所有应用"
        echo "2. 查看应用详细信息"
        echo "3. 查看应用日志"
        echo "4. 查看最近错误"
        echo "5. 退出"
        echo
        read -p "请选择操作 (1-5): " choice
        
        case $choice in
            1)
                list_student_applications "$student_id"
                ;;
            2)
                read -p "请输入应用ID: " app_id
                if [ -n "$app_id" ]; then
                    show_application_info "$app_id"
                else
                    print_error "应用ID不能为空"
                fi
                ;;
            3)
                read -p "请输入应用ID: " app_id
                if [ -n "$app_id" ]; then
                    show_application_logs "$app_id"
                else
                    print_error "应用ID不能为空"
                fi
                ;;
            4)
                show_recent_errors "$student_id"
                ;;
            5)
                print_info "退出日志查看器"
                break
                ;;
            *)
                print_error "无效选择，请输入 1-5"
                ;;
        esac
        
        echo
        read -p "按回车键继续..."
    done
}

# 主函数
main() {
    echo "=== Hadoop 学生日志查看工具 ==="
    echo "时间: $(date)"
    echo "学生: $STUDENT_ID"
    echo
    
    # 检查 YARN 连接
    if ! check_yarn_connection; then
        exit 1
    fi
    
    if [ -n "$APP_ID" ]; then
        # 查看指定应用
        print_info "查看指定应用: $APP_ID"
        show_application_info "$APP_ID"
        echo
        show_application_logs "$APP_ID"
    else
        # 交互式查看
        interactive_log_viewer "$STUDENT_ID"
    fi
    
    echo
    print_info "日志查看完成"
}

# 执行主函数
main