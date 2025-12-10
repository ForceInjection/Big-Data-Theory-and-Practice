#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${YELLOW}🔄 启动实时数据 ETL 示例...${NC}"

echo -e "${YELLOW}🔍 检查运行环境...${NC}"
if ! docker ps | grep -q flink-jobmanager; then
    echo -e "${RED}❌ Flink 环境未启动，请先运行 ./scripts/start-environment.sh${NC}"
    exit 1
fi

if ! docker ps | grep -q kafka; then
    echo -e "${RED}❌ Kafka 服务未启动，请先运行 ./scripts/start-environment.sh${NC}"
    exit 1
fi

JAR_FILE="$PROJECT_ROOT/target/realtime-etl/realtime-etl-1.0.0.jar"

if [ -f "$JAR_FILE" ]; then
    echo -e "${GREEN}✅ 检测到 JAR 包已存在，跳过编译步骤...${NC}"
    echo -e "   JAR路径: $JAR_FILE"
else
    echo -e "${YELLOW}🔨 正在编译 Real-time ETL 模块...${NC}"
    cd "$PROJECT_ROOT"
    ./build.sh realtime-etl
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ 编译失败${NC}"
        exit 1
    fi
fi

echo -e "${YELLOW}⚙️ 准备 Kafka Topics...${NC}"
docker exec kafka kafka-topics --create \
    --topic user-events \
    --bootstrap-server kafka:9092 \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

docker exec kafka kafka-topics --create \
    --topic order-events \
    --bootstrap-server kafka:9092 \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

echo -e "${YELLOW}🧹 清理之前的输出数据...${NC}"
rm -rf "$PROJECT_ROOT/data/output/realtime-etl"

echo -e "${YELLOW}📝 生成并发送 ETL 测试数据到 Kafka...${NC}"
INPUT_USER="$PROJECT_ROOT/data/input/etl-user-input.csv"
INPUT_ORDER="$PROJECT_ROOT/data/input/etl-order-input.csv"
mkdir -p "$PROJECT_ROOT/data/input"

CURRENT_TIME=$(date +%s)
echo "u-1001,user-alice,alice@example.com,$(($CURRENT_TIME * 1000))" > "$INPUT_USER"
echo "u-1002,user-bob,bob@example.com,$(($CURRENT_TIME * 1000 + 1000))" >> "$INPUT_USER"
echo "u-1003,user-carol,carol@example.com,$(($CURRENT_TIME * 1000 + 2000))" >> "$INPUT_USER"

echo "o-2001,u-1001,199.99,PAID,$(($CURRENT_TIME * 1000 + 3000))" > "$INPUT_ORDER"
echo "o-2002,u-1002,89.50,PAID,$(($CURRENT_TIME * 1000 + 3500))" >> "$INPUT_ORDER"
echo "o-2003,u-1003,12.00,CANCELLED,$(($CURRENT_TIME * 1000 + 3600))" >> "$INPUT_ORDER"

cat "$INPUT_USER" | docker exec -i kafka kafka-console-producer \
    --topic user-events \
    --bootstrap-server kafka:9092

cat "$INPUT_ORDER" | docker exec -i kafka kafka-console-producer \
    --topic order-events \
    --bootstrap-server kafka:9092

echo -e "${GREEN}✅ 数据已发送到 Kafka Topics 'user-events' 和 'order-events'${NC}"

echo -e "${YELLOW}🚀 提交 Flink 作业...${NC}"
JOB_JAR="/opt/flink/usrlib/realtime-etl/realtime-etl-1.0.0.jar"
MAIN_CLASS="com.streaming.practice.etl.RealTimeETL"

docker exec flink-jobmanager flink run \
    -d \
    -c "$MAIN_CLASS" \
    "$JOB_JAR"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ 作业提交成功！${NC}"
    echo -e "📊 可以在 Flink Web UI (http://localhost:8081) 查看作业运行情况"

    echo -e "${YELLOW}📋 等待文件输出生成 (最多 120 秒)...${NC}"
    OUTPUT_DIR="$PROJECT_ROOT/data/output/realtime-etl"
    FOUND_DATA=false
    for ((i=1; i<=24; i++)); do
        if [ -d "$OUTPUT_DIR" ] && find "$OUTPUT_DIR" -type f \( -name "part-*" -o -name "*.inprogress" -o -name ".part-*" \) 2>/dev/null | grep -q .; then
            FOUND_DATA=true
            break
        fi
        sleep 5
        echo -n "."
    done
    echo ""

    echo -e "${YELLOW}🔍 查看作业状态...${NC}"
    docker exec flink-jobmanager flink list

    if [ "$FOUND_DATA" = true ]; then
        echo -e "输出目录: $OUTPUT_DIR"
        RESULT_FILES=$(find "$OUTPUT_DIR" -type f \( -name "part-*" -o -name "*.inprogress" -o -name ".part-*" \) | sort)
        echo -e "${BLUE}📄 结果文件列表:${NC}"
        echo "$RESULT_FILES"
        echo -e "${BLUE}📄 文件内容 (前20行):${NC}"
        echo "$RESULT_FILES" | while read file; do
             if [ -f "$file" ]; then
                 echo -e "${BLUE}=== $(basename "$file") ===${NC}"
                 head -n 20 "$file"
             fi
        done
        echo -e "${GREEN}✅ ETL 示例运行完成！${NC}"
    else
        echo -e "${YELLOW}⚠️ 未找到输出目录或结果文件 $OUTPUT_DIR${NC}"
        echo -e "${YELLOW}ℹ️ 可在 TaskManager 日志或 Flink Web UI 查看控制台输出${NC}"
        echo -e "${GREEN}✅ ETL 示例运行完成！${NC}"
    fi
else
    echo -e "${RED}❌ 作业提交失败${NC}"
    exit 1
fi

