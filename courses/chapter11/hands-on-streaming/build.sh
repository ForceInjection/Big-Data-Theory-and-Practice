#!/bin/bash

# 流式计算动手实践系列构建脚本
# 使用 Docker Maven 镜像构建所有示例项目

set -e  # 遇到错误时退出

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

# 项目根目录
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLES_DIR="${PROJECT_ROOT}/examples"
BUILD_DIR="${PROJECT_ROOT}/build"
TARGET_DIR="${PROJECT_ROOT}/target"

# Maven 镜像配置
MAVEN_IMAGE="maven:3.8.6-openjdk-11"
CONTAINER_NAME="streaming-examples-build"

# 示例项目列表
EXAMPLES=(
    "wordcount"
    "user-behavior" 
    "fraud-detection"
    "iot-monitoring"
    "realtime-etl"
)

# 清理函数
cleanup() {
    log_info "清理构建环境..."
    rm -rf "${BUILD_DIR}/logs"
    docker rm -f ${CONTAINER_NAME} 2>/dev/null || true
}

# 设置清理陷阱
trap cleanup EXIT

# 检查 Docker 环境
check_docker() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker 未安装或不可用，请先安装 Docker"
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        log_error "Docker 服务未运行，请启动 Docker"
        exit 1
    fi
}

# 拉取 Maven 镜像
pull_maven_image() {
    if docker image inspect "${MAVEN_IMAGE}" >/dev/null 2>&1; then
        log_info "Maven 镜像 ${MAVEN_IMAGE} 已存在，跳过拉取。"
    else
        log_info "拉取 Maven 镜像..."
        docker pull ${MAVEN_IMAGE}
    fi
}

# 构建单个示例项目
build_example() {
    local example_name="$1"
    local example_dir="${EXAMPLES_DIR}/${example_name}"
    
    if [ ! -d "${example_dir}" ]; then
        log_error "示例项目目录不存在: ${example_dir}"
        return 1
    fi
    
    if [ ! -f "${example_dir}/pom.xml" ]; then
        log_error "未找到 pom.xml 文件: ${example_dir}/pom.xml"
        return 1
    fi
    
    log_info "构建示例项目: ${example_name}"
    
    # 运行 Maven 构建（强制更新依赖）
    docker run --rm \
        --name "${CONTAINER_NAME}-${example_name}" \
        -v "${example_dir}":/workspace \
        -v "${HOME}/.m2":/root/.m2 \
        -w /workspace \
        ${MAVEN_IMAGE} \
        mvn clean compile package -U -DskipTests=true \
        | tee "${BUILD_DIR}/logs/${example_name}-build-$(date +%Y%m%d-%H%M%S).log"
    
    # 获取 Maven 命令的实际退出状态
    local maven_exit_code=${PIPESTATUS[0]}
    if [ ${maven_exit_code} -eq 0 ]; then
        log_success "示例项目 ${example_name} 构建成功！"
        
        # 检查构建产物
        local jar_file="${example_dir}/target/*.jar"
        if ls ${jar_file} 1> /dev/null 2>&1; then
            log_info "构建产物: $(ls ${jar_file})"
            # 复制到统一的目标目录
            mkdir -p "${TARGET_DIR}/${example_name}"
            cp ${jar_file} "${TARGET_DIR}/${example_name}/"
        else
            log_warning "未找到jar文件，检查target目录..."
            ls -la "${example_dir}/target/" || true
        fi
        
        return 0
    else
        log_error "示例项目 ${example_name} 构建失败！"
        return 1
    fi
}

# 构建所有示例项目
build_all_examples() {
    log_info "开始构建所有流式计算示例项目..."
    
    # 检查项目根目录
    if [ ! -f "${PROJECT_ROOT}/docker-compose.yml" ]; then
        log_error "未找到 docker-compose.yml 文件，请确认在正确的项目目录中"
        exit 1
    fi
    
    # 检查 Docker 环境
    check_docker
    
    # 拉取 Maven 镜像
    pull_maven_image
    
    # 创建构建输出目录
    mkdir -p "${BUILD_DIR}/logs"
    mkdir -p "${TARGET_DIR}"
    
    local success_count=0
    local total_count=${#EXAMPLES[@]}
    
    # 构建每个示例项目
    for example in "${EXAMPLES[@]}"; do
        if build_example "${example}"; then
            ((success_count++))
        else
            log_warning "项目 ${example} 构建失败，继续构建其他项目..."
        fi
    done
    
    # 输出构建结果
    if [ ${success_count} -eq ${total_count} ]; then
        log_success "所有 ${total_count} 个示例项目构建成功！"
    else
        log_warning "构建完成: ${success_count}/${total_count} 个项目成功"
        if [ ${success_count} -eq 0 ]; then
            log_error "所有项目构建失败！"
            exit 1
        fi
    fi
    
    # 显示构建信息
    log_info "构建信息:"
    echo "  - Java 版本: OpenJDK 11"
    echo "  - Maven 版本: 3.8.6"
    echo "  - 构建时间: $(date)"
    echo "  - 构建日志: ${BUILD_DIR}/logs/"
    echo "  - 构建产物: ${TARGET_DIR}/"
    echo "  - 成功项目: ${success_count}/${total_count}"
    
    # 显示构建产物列表
    log_info "构建产物列表:"
    find "${TARGET_DIR}" -name "*.jar" -type f | while read jar_file; do
        echo "  - $(basename "${jar_file}") ($(du -h "${jar_file}" | cut -f1))"
    done
}

# 运行单个示例项目的测试
run_example_tests() {
    local example_name="$1"
    local example_dir="${EXAMPLES_DIR}/${example_name}"
    
    log_info "运行示例项目测试: ${example_name}"
    
    docker run --rm \
        --name "${CONTAINER_NAME}-${example_name}-test" \
        -v "${example_dir}":/workspace \
        -v "${HOME}/.m2":/root/.m2 \
        -w /workspace \
        ${MAVEN_IMAGE} \
        mvn test \
        | tee "${BUILD_DIR}/logs/${example_name}-test-$(date +%Y%m%d-%H%M%S).log"
    
    if [ $? -eq 0 ]; then
        log_success "示例项目 ${example_name} 测试通过！"
        return 0
    else
        log_error "示例项目 ${example_name} 测试失败！"
        return 1
    fi
}

# 运行所有示例项目的测试
run_all_tests() {
    log_info "开始运行所有示例项目测试..."
    
    check_docker
    pull_maven_image
    mkdir -p "${BUILD_DIR}/logs"
    
    local success_count=0
    local total_count=${#EXAMPLES[@]}
    
    for example in "${EXAMPLES[@]}"; do
        if run_example_tests "${example}"; then
            ((success_count++))
        fi
    done
    
    if [ ${success_count} -eq ${total_count} ]; then
        log_success "所有 ${total_count} 个示例项目测试通过！"
    else
        log_warning "测试完成: ${success_count}/${total_count} 个项目通过"
        if [ ${success_count} -eq 0 ]; then
            log_error "所有项目测试失败！"
            exit 1
        fi
    fi
}

# 清理构建产物
clean_build() {
    log_info "清理所有构建产物..."
    
    for example in "${EXAMPLES[@]}"; do
        local example_dir="${EXAMPLES_DIR}/${example}"
        if [ -d "${example_dir}" ]; then
            log_info "清理示例项目: ${example}"
            docker run --rm \
                -v "${example_dir}":/workspace \
                -v "${HOME}/.m2":/root/.m2 \
                -w /workspace \
                ${MAVEN_IMAGE} \
                mvn clean 2>/dev/null || true
        fi
    done
    
    rm -rf "${BUILD_DIR}"
    rm -rf "${TARGET_DIR}"
    
    log_success "所有构建产物清理完成"
}

# 显示帮助信息
show_help() {
    echo "流式计算动手实践系列构建脚本"
    echo ""
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  build [项目] 构建所有示例项目（默认），或指定单个项目"
    echo "  test       运行所有测试"
    echo "  clean      清理构建产物"
    echo "  list       列出所有示例项目"
    echo "  help       显示帮助信息"
    echo ""
    echo "示例项目列表:"
    for example in "${EXAMPLES[@]}"; do
        echo "  - ${example}"
    done
    echo ""
    echo "示例:"
    echo "  $0 build    # 构建所有项目"
    echo "  $0 test     # 运行所有测试"
    echo "  $0 clean    # 清理构建产物"
    echo "  $0 list     # 列出示例项目"
}

# 列出所有示例项目
list_examples() {
    echo "可用的流式计算示例项目:"
    echo ""
    for example in "${EXAMPLES[@]}"; do
        local example_dir="${EXAMPLES_DIR}/${example}"
        if [ -d "${example_dir}" ]; then
            if [ -f "${example_dir}/pom.xml" ]; then
                echo "  ✓ ${example}"
            else
                echo "  ✗ ${example} (缺少pom.xml)"
            fi
        else
            echo "  ? ${example} (目录不存在)"
        fi
    done
}

# 主程序
main() {
    case "${1:-build}" in
        "build")
            if [ -n "$2" ]; then
                # 检查是否为示例项目名称
                local is_example=false
                for ex in "${EXAMPLES[@]}"; do
                    if [ "$ex" == "$2" ]; then
                        is_example=true
                        break
                    fi
                done
                
                if [ "$is_example" = true ]; then
                    check_docker
                    pull_maven_image
                    mkdir -p "${BUILD_DIR}/logs"
                    mkdir -p "${TARGET_DIR}"
                    build_example "$2"
                else
                    log_error "未知示例项目: $2"
                    list_examples
                    exit 1
                fi
            else
                build_all_examples
            fi
            ;;
        "test")
            run_all_tests
            ;;
        "clean")
            clean_build
            ;;
        "list")
            list_examples
            ;;
        "help"|"-h"|"--help")
            show_help
            ;;
        *)
            # 检查是否为示例项目名称
            local is_example=false
            for ex in "${EXAMPLES[@]}"; do
                if [ "$ex" == "$1" ]; then
                    is_example=true
                    break
                fi
            done
            
            if [ "$is_example" = true ]; then
                check_docker
                pull_maven_image
                mkdir -p "${BUILD_DIR}/logs"
                mkdir -p "${TARGET_DIR}"
                build_example "$1"
            else
                log_error "未知选项: $1"
                show_help
                exit 1
            fi
            ;;
    esac
}

# 执行主程序
main "$@"