#!/bin/bash

# Hadoop 远程客户端故障排除脚本
# 用于诊断和解决常见的 Hadoop 客户端连接问题

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 显示帮助信息
show_help() {
    cat << EOF
Hadoop 远程客户端故障排除脚本

用法: $0 [选项] [集群地址]

选项:
    -h, --help          显示此帮助信息
    -s, --student-id    指定学生 ID (默认从环境变量获取)
    -v, --verbose       详细输出模式
    --fix-config        自动修复配置问题
    --reset-env         重置环境变量配置

参数:
    集群地址            Hadoop 集群主节点地址 (默认: hadoop-master)

示例:
    $0                                    # 使用默认设置进行诊断
    $0 192.168.1.100                     # 指定集群地址
    $0 -s student001 hadoop-cluster      # 指定学生 ID 和集群地址
    $0 --fix-config                      # 自动修复配置问题
    $0 --reset-env                       # 重置环境变量

EOF
}

# 解析命令行参数
CLUSTER_MASTER="hadoop-master"
STUDENT_ID="${HADOOP_USER_NAME:-${USER}}"
VERBOSE=false
FIX_CONFIG=false
RESET_ENV=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -s|--student-id)
            STUDENT_ID="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        --fix-config)
            FIX_CONFIG=true
            shift
            ;;
        --reset-env)
            RESET_ENV=true
            shift
            ;;
        -*)
            log_error "未知选项: $1"
            show_help
            exit 1
            ;;
        *)
            CLUSTER_MASTER="$1"
            shift
            ;;
    esac
done

# 验证学生 ID 格式
if [[ ! "$STUDENT_ID" =~ ^[a-zA-Z0-9_-]+$ ]]; then
    log_error "无效的学生 ID 格式: $STUDENT_ID"
    log_info "学生 ID 应该只包含字母、数字、下划线和连字符"
    exit 1
fi

echo "=== Hadoop 远程客户端故障排除 ==="
echo "学生 ID: $STUDENT_ID"
echo "集群地址: $CLUSTER_MASTER"
echo "时间: $(date)"
echo

# 1. 检查基本环境
check_basic_environment() {
    log_info "1. 检查基本环境..."
    
    # 检查 Java 环境
    if command -v java >/dev/null 2>&1; then
        java_version=$(java -version 2>&1 | head -n 1)
        log_success "Java 已安装: $java_version"
    else
        log_error "Java 未安装或未在 PATH 中"
        log_info "请安装 Java 8 或更高版本"
        return 1
    fi
    
    # 检查 Hadoop 命令
    if command -v hadoop >/dev/null 2>&1; then
        hadoop_version=$(hadoop version 2>/dev/null | head -n 1 || echo "无法获取版本")
        log_success "Hadoop 命令可用: $hadoop_version"
    else
        log_error "Hadoop 命令不可用"
        log_info "请检查 HADOOP_HOME 和 PATH 环境变量"
        return 1
    fi
    
    # 检查 HDFS 命令
    if command -v hdfs >/dev/null 2>&1; then
        log_success "HDFS 命令可用"
    else
        log_error "HDFS 命令不可用"
        return 1
    fi
    
    # 检查 YARN 命令
    if command -v yarn >/dev/null 2>&1; then
        log_success "YARN 命令可用"
    else
        log_warning "YARN 命令不可用"
    fi
    
    return 0
}

# 2. 检查网络连接
check_network_connectivity() {
    log_info "2. 检查网络连接..."
    
    # 检查主机名解析
    if nslookup "$CLUSTER_MASTER" >/dev/null 2>&1 || getent hosts "$CLUSTER_MASTER" >/dev/null 2>&1; then
        log_success "主机名解析成功: $CLUSTER_MASTER"
    else
        log_error "无法解析主机名: $CLUSTER_MASTER"
        log_info "请检查 DNS 配置或在 /etc/hosts 中添加主机映射"
        return 1
    fi
    
    # 检查关键端口连接
    local ports=(9000 8088 9870 8020 8032 19888)
    local port_names=("HDFS NameNode" "YARN ResourceManager Web" "HDFS NameNode Web" "HDFS NameNode RPC" "YARN ResourceManager" "MapReduce JobHistory")
    
    for i in "${!ports[@]}"; do
        local port="${ports[$i]}"
        local name="${port_names[$i]}"
        
        if timeout 5 bash -c "echo >/dev/tcp/$CLUSTER_MASTER/$port" 2>/dev/null; then
            log_success "端口 $port ($name) 连接成功"
        else
            log_warning "端口 $port ($name) 连接失败"
            if [[ "$VERBOSE" == "true" ]]; then
                log_info "  可能原因: 防火墙阻止、服务未启动或网络不通"
            fi
        fi
    done
    
    return 0
}

# 3. 检查配置文件
check_configuration_files() {
    log_info "3. 检查配置文件..."
    
    local hadoop_conf_dir="${HADOOP_CONF_DIR:-$HOME/.hadoop/conf}"
    
    if [[ ! -d "$hadoop_conf_dir" ]]; then
        log_error "Hadoop 配置目录不存在: $hadoop_conf_dir"
        if [[ "$FIX_CONFIG" == "true" ]]; then
            log_info "创建配置目录..."
            mkdir -p "$hadoop_conf_dir"
            log_success "配置目录已创建"
        else
            log_info "运行 05-setup_student_client.sh 来创建配置"
        fi
        return 1
    fi
    
    # 检查核心配置文件
    local config_files=("core-site.xml" "yarn-site.xml" "mapred-site.xml")
    local missing_files=()
    
    for config_file in "${config_files[@]}"; do
        local file_path="$hadoop_conf_dir/$config_file"
        if [[ -f "$file_path" ]]; then
            log_success "配置文件存在: $config_file"
            
            # 检查文件内容
            if [[ "$VERBOSE" == "true" ]]; then
                if grep -q "$CLUSTER_MASTER" "$file_path" 2>/dev/null; then
                    log_success "  配置文件包含正确的集群地址"
                else
                    log_warning "  配置文件可能包含错误的集群地址"
                fi
            fi
        else
            log_error "配置文件缺失: $config_file"
            missing_files+=("$config_file")
        fi
    done
    
    if [[ ${#missing_files[@]} -gt 0 ]] && [[ "$FIX_CONFIG" == "true" ]]; then
        log_info "尝试修复缺失的配置文件..."
        # 这里可以调用 05-setup_student_client.sh 或创建基本配置
log_info "请运行: 05-setup_student_client.sh $STUDENT_ID $CLUSTER_MASTER"
    fi
    
    return 0
}

# 4. 检查环境变量
check_environment_variables() {
    log_info "4. 检查环境变量..."
    
    local required_vars=("HADOOP_HOME" "HADOOP_CONF_DIR" "JAVA_HOME")
    local missing_vars=()
    
    for var in "${required_vars[@]}"; do
        if [[ -n "${!var}" ]]; then
            log_success "$var = ${!var}"
            
            # 检查路径是否存在
            if [[ "$var" == "HADOOP_HOME" || "$var" == "JAVA_HOME" ]] && [[ ! -d "${!var}" ]]; then
                log_warning "  路径不存在: ${!var}"
            fi
        else
            log_error "$var 未设置"
            missing_vars+=("$var")
        fi
    done
    
    # 检查 HADOOP_USER_NAME
    if [[ -n "$HADOOP_USER_NAME" ]]; then
        log_success "HADOOP_USER_NAME = $HADOOP_USER_NAME"
    else
        log_warning "HADOOP_USER_NAME 未设置，将使用当前用户: $USER"
    fi
    
    # 检查 PATH
    if echo "$PATH" | grep -q "hadoop"; then
        log_success "PATH 包含 Hadoop 路径"
    else
        log_warning "PATH 可能不包含 Hadoop 路径"
    fi
    
    if [[ ${#missing_vars[@]} -gt 0 ]] && [[ "$RESET_ENV" == "true" ]]; then
        log_info "重置环境变量配置..."
        # 移除旧的配置
        if [[ -f "$HOME/.bashrc" ]]; then
            sed -i.bak '/# Hadoop 远程客户端环境配置/,/^$/d' "$HOME/.bashrc"
            log_success "已清理旧的环境变量配置"
            log_info "请重新运行 05-setup_student_client.sh 来配置环境变量"
        fi
    fi
    
    return 0
}

# 5. 测试 Hadoop 连接
test_hadoop_connectivity() {
    log_info "5. 测试 Hadoop 连接..."
    
    # 设置用户身份
    export HADOOP_USER_NAME="$STUDENT_ID"
    
    # 测试 HDFS 连接
    log_info "测试 HDFS 连接..."
    if hdfs dfs -ls / >/dev/null 2>&1; then
        log_success "HDFS 连接成功"
    else
        log_error "HDFS 连接失败"
        if [[ "$VERBOSE" == "true" ]]; then
            log_info "尝试详细诊断..."
            hdfs dfs -ls / 2>&1 | head -10
        fi
        return 1
    fi
    
    # 测试个人目录访问
    log_info "测试个人目录访问..."
    if hdfs dfs -ls "/users/$STUDENT_ID" >/dev/null 2>&1; then
        log_success "个人目录访问成功"
    else
        log_error "个人目录访问失败"
        log_info "可能原因: 账户未创建或权限不足"
        
        # 尝试创建个人目录
        if [[ "$FIX_CONFIG" == "true" ]]; then
            log_info "尝试创建个人目录..."
            if hdfs dfs -mkdir -p "/users/$STUDENT_ID" 2>/dev/null; then
                log_success "个人目录创建成功"
            else
                log_error "个人目录创建失败，请联系管理员"
            fi
        fi
    fi
    
    # 测试 YARN 连接
    log_info "测试 YARN 连接..."
    if yarn application -list >/dev/null 2>&1; then
        log_success "YARN 连接成功"
    else
        log_warning "YARN 连接失败"
        if [[ "$VERBOSE" == "true" ]]; then
            yarn application -list 2>&1 | head -5
        fi
    fi
    
    return 0
}

# 6. 性能测试
performance_test() {
    log_info "6. 执行性能测试..."
    
    # 简单的 HDFS 读写测试
    local test_file="/users/$STUDENT_ID/test-$(date +%Y%m%d-%H%M%S).txt"
    local test_content="Hadoop client test - $(date)"
    
    log_info "测试 HDFS 写入..."
    if echo "$test_content" | hdfs dfs -put - "$test_file" 2>/dev/null; then
        log_success "HDFS 写入测试成功"
        
        log_info "测试 HDFS 读取..."
        if hdfs dfs -cat "$test_file" >/dev/null 2>&1; then
            log_success "HDFS 读取测试成功"
        else
            log_error "HDFS 读取测试失败"
        fi
        
        # 清理测试文件
        hdfs dfs -rm "$test_file" >/dev/null 2>&1
    else
        log_error "HDFS 写入测试失败"
        return 1
    fi
    
    return 0
}

# 7. 生成诊断报告
generate_report() {
    log_info "7. 生成诊断报告..."
    
    local report_file="$HOME/hadoop_client_diagnosis_$(date +%Y%m%d_%H%M%S).txt"
    
    cat > "$report_file" << EOF
Hadoop 远程客户端诊断报告
========================

基本信息:
- 学生 ID: $STUDENT_ID
- 集群地址: $CLUSTER_MASTER
- 诊断时间: $(date)
- 操作系统: $(uname -s) $(uname -r)

环境变量:
- HADOOP_HOME: ${HADOOP_HOME:-未设置}
- HADOOP_CONF_DIR: ${HADOOP_CONF_DIR:-未设置}
- JAVA_HOME: ${JAVA_HOME:-未设置}
- HADOOP_USER_NAME: ${HADOOP_USER_NAME:-未设置}

Java 版本:
$(java -version 2>&1 || echo "Java 未安装")

Hadoop 版本:
$(hadoop version 2>/dev/null | head -3 || echo "Hadoop 命令不可用")

网络连接测试:
$(for port in 9000 8088 9870; do
    if timeout 3 bash -c "echo >/dev/tcp/$CLUSTER_MASTER/$port" 2>/dev/null; then
        echo "- 端口 $port: 连接成功"
    else
        echo "- 端口 $port: 连接失败"
    fi
done)

HDFS 连接测试:
$(export HADOOP_USER_NAME="$STUDENT_ID"; hdfs dfs -ls / >/dev/null 2>&1 && echo "成功" || echo "失败")

个人目录访问:
$(export HADOOP_USER_NAME="$STUDENT_ID"; hdfs dfs -ls "/users/$STUDENT_ID" >/dev/null 2>&1 && echo "成功" || echo "失败")

YARN 连接测试:
$(yarn application -list >/dev/null 2>&1 && echo "成功" || echo "失败")

配置文件状态:
$(for file in core-site.xml yarn-site.xml mapred-site.xml; do
    if [[ -f "${HADOOP_CONF_DIR:-$HOME/.hadoop/conf}/$file" ]]; then
        echo "- $file: 存在"
    else
        echo "- $file: 缺失"
    fi
done)

建议操作:
EOF

    # 添加建议
    if ! command -v hadoop >/dev/null 2>&1; then
        echo "- 安装 Hadoop 并配置环境变量" >> "$report_file"
    fi
    
    if [[ ! -d "${HADOOP_CONF_DIR:-$HOME/.hadoop/conf}" ]]; then
        echo "- 运行 05-setup_student_client.sh 配置客户端环境" >> "$report_file"
    fi
    
    if ! timeout 3 bash -c "echo >/dev/tcp/$CLUSTER_MASTER/9000" 2>/dev/null; then
        echo "- 检查网络连接和防火墙设置" >> "$report_file"
    fi
    
    export HADOOP_USER_NAME="$STUDENT_ID"
    if ! hdfs dfs -ls "/users/$STUDENT_ID" >/dev/null 2>&1; then
        echo "- 联系管理员创建用户账户和目录" >> "$report_file"
    fi
    
    log_success "诊断报告已保存到: $report_file"
    
    if [[ "$VERBOSE" == "true" ]]; then
        echo
        log_info "报告内容:"
        cat "$report_file"
    fi
}

# 主函数
main() {
    local exit_code=0
    
    # 执行诊断步骤
    check_basic_environment || exit_code=1
    echo
    
    check_network_connectivity || exit_code=1
    echo
    
    check_configuration_files || exit_code=1
    echo
    
    check_environment_variables || exit_code=1
    echo
    
    test_hadoop_connectivity || exit_code=1
    echo
    
    if [[ $exit_code -eq 0 ]]; then
        performance_test || exit_code=1
        echo
    fi
    
    generate_report
    echo
    
    # 总结
    if [[ $exit_code -eq 0 ]]; then
        log_success "=== 诊断完成：客户端配置正常 ==="
        log_info "您的 Hadoop 客户端已正确配置并可以正常使用"
    else
        log_error "=== 诊断完成：发现问题 ==="
        log_info "请根据上述错误信息和诊断报告进行修复"
        log_info "常用修复命令:"
        log_info "  - 配置客户端: 05-setup_student_client.sh $STUDENT_ID $CLUSTER_MASTER"
        log_info "  - 重置环境: $0 --reset-env"
        log_info "  - 自动修复: $0 --fix-config"
    fi
    
    return $exit_code
}

# 执行主函数
main "$@"