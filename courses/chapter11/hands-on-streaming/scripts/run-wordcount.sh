#!/bin/bash

# 实时词频统计示例运行脚本

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}📊 启动实时词频统计示例...${NC}"

# 1. 检查环境
echo -e "${YELLOW}🔍 检查运行环境...${NC}"
if ! docker ps | grep -q flink-jobmanager; then
    echo -e "${RED}❌ Flink 环境未启动，请先运行 ./scripts/start-environment.sh${NC}"
    exit 1
fi

# 2. 编译项目
JAR_FILE="$PROJECT_ROOT/target/wordcount/wordcount-1.0.0.jar"

if [ -f "$JAR_FILE" ]; then
    echo -e "${GREEN}✅ 检测到 JAR 包已存在，跳过编译步骤...${NC}"
    echo -e "   JAR路径: $JAR_FILE"
else
    echo -e "${YELLOW}🔨 正在编译 WordCount 模块...${NC}"
    cd "$PROJECT_ROOT"
    ./build.sh wordcount

    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ 编译失败${NC}"
        exit 1
    fi
fi

# 清理之前的输出数据
echo -e "${YELLOW}🧹 清理之前的输出数据...${NC}"
rm -rf "$PROJECT_ROOT/data/output/wordcount-result"

# 3. 准备输入数据
INPUT_FILE="$PROJECT_ROOT/data/input/wordcount-input.txt"
mkdir -p "$PROJECT_ROOT/data/input"

if [ ! -f "$INPUT_FILE" ]; then
    echo -e "${YELLOW}📝 生成默认测试数据...${NC}"
    cat > "$INPUT_FILE" << 'EOF'
Apache Flink is a framework and distributed processing engine for stateful computations over unbounded and bounded data streams
Flink has been designed to run in all common cluster environments perform computations at in-memory speed and at any scale
Flink provides results that are consistent and easy to understand thanks to its well-thought-out APIs and support for different levels of abstraction
Flink guarantees exactly-once state consistency in case of failures thanks to its lightweight snapshotting mechanism
EOF
    echo -e "${GREEN}已生成示例数据到 $INPUT_FILE${NC}"
fi

# 4. 启动数据源容器
echo -e "${YELLOW}🌐 启动数据源容器...${NC}"
docker rm -f wordcount-source 2>/dev/null || true

# 获取 Docker 网络名称 (兼容 docker-compose 项目名称前缀)
NETWORK_NAME=$(docker network ls --filter name=streaming-network --format "{{.Name}}" | head -n 1)
if [ -z "$NETWORK_NAME" ]; then
    echo -e "${RED}❌ 未找到 streaming-network 网络${NC}"
    exit 1
fi
echo -e "使用 Docker 网络: $NETWORK_NAME"

# 启动一个 Alpine 容器
if ! docker run -d --name wordcount-source --network "$NETWORK_NAME" alpine:latest tail -f /dev/null; then
    echo -e "${RED}❌ 启动数据源容器失败${NC}"
    exit 1
fi

# 复制数据文件
docker cp "$INPUT_FILE" wordcount-source:/input.txt

# 在容器内启动 Netcat 服务
# 使用 (cat; sleep) 模式确保发送完数据后连接保持打开
# 修改为 sleep 20: 发送完数据后保持连接 20 秒，让 Flink 有足够时间处理和 Checkpoint，然后断开连接使作业正常结束
docker exec -d wordcount-source sh -c "while true; do (cat /input.txt; sleep 20) | nc -l -p 9999; sleep 1; done"

echo -e "${GREEN}✅ 数据源容器已启动 (wordcount-source:9999)${NC}"

# 等待服务器启动
sleep 2

# 5. 提交 Flink 作业
echo -e "${YELLOW}🚀 提交 Flink 作业...${NC}"
# 注意：容器内的路径是 /opt/flink/usrlib/wordcount/wordcount-1.0.0.jar
JOB_JAR="/opt/flink/usrlib/wordcount/wordcount-1.0.0.jar"
MAIN_CLASS="com.streaming.practice.wordcount.WordCountExample"

# 连接到 wordcount-source 容器
docker exec flink-jobmanager flink run \
    -d \
    -c "$MAIN_CLASS" \
    "$JOB_JAR" \
    wordcount-source 9999

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ 作业提交成功！${NC}"
    echo -e "📊 可以在 Flink Web UI (http://localhost:8081) 查看作业运行情况"

    # 7. 查看作业输出
    echo -e "${YELLOW}📋 查看作业输出...${NC}"
    OUTPUT_DIR="$PROJECT_ROOT/data/output/wordcount-result"
    
    # 轮询等待结果文件生成 (最多等待 120 秒)
    echo -e "${YELLOW}⏳ 等待结果文件生成 (最多 120 秒)...${NC}"
    FOUND_DATA=false
    for ((i=1; i<=24; i++)); do
        if [ -d "$OUTPUT_DIR" ] && find "$OUTPUT_DIR" -type f \( -name "part-*" -o -name "*.inprogress" \) 2>/dev/null | grep -q .; then
            FOUND_DATA=true
            break
        fi
        sleep 5
        echo -n "."
    done
    echo ""
    
    # 6. 查看作业状态
    echo -e "${YELLOW}� 查看作业状态...${NC}"
    docker exec flink-jobmanager flink list
    
    if [ "$FOUND_DATA" = true ]; then
        echo -e "输出目录: $OUTPUT_DIR"
        
        # 查找所有结果文件 (包括 .inprogress)
        RESULT_FILES=$(find "$OUTPUT_DIR" -type f \( -name "part-*" -o -name "*.inprogress" \) | sort)
        
        echo -e "${BLUE}📄 结果文件列表:${NC}"
        echo "$RESULT_FILES"
        
        echo -e "${BLUE}📄 文件内容 (前20行):${NC}"
        # 使用 while read 循环逐行处理文件列表，避免文件名空格问题 (虽然 Flink 生成的文件名通常无空格)
        echo "$RESULT_FILES" | while read file; do
             if [ -f "$file" ]; then
                 echo -e "${BLUE}=== $(basename "$file") ===${NC}"
                 head -n 20 "$file"
             fi
        done
    else
         echo -e "${RED}❌ 未找到输出目录或文件 $OUTPUT_DIR${NC}"
         echo "⚠️ 尚未生成输出文件，可能作业仍在初始化或无数据输出"
    fi
    
    # 8. 清理
    echo -e "${YELLOW}🧹 清理临时资源...${NC}"
    docker rm -f wordcount-source 2>/dev/null || true
    
    echo -e "${GREEN}✅ 词频统计示例运行完成！${NC}"
else
    echo -e "${RED}❌ 作业提交失败${NC}"
    # 清理资源
    docker rm -f wordcount-source 2>/dev/null || true
    exit 1
fi
