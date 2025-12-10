#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${YELLOW}🛡️ 启动金融交易实时风控示例...${NC}"

echo -e "${YELLOW}🔍 检查运行环境...${NC}"
if ! docker ps | grep -q flink-jobmanager; then
    echo -e "${RED}❌ Flink 环境未启动，请先运行 ./scripts/start-environment.sh${NC}"
    exit 1
fi

if ! docker ps | grep -q kafka; then
    echo -e "${RED}❌ Kafka 服务未启动，请先运行 ./scripts/start-environment.sh${NC}"
    exit 1
fi

JAR_FILE="$PROJECT_ROOT/target/fraud-detection/fraud-detection-1.0.0.jar"

if [ -f "$JAR_FILE" ]; then
    echo -e "${GREEN}✅ 检测到 JAR 包已存在，跳过编译步骤...${NC}"
    echo -e "   JAR路径: $JAR_FILE"
else
    echo -e "${YELLOW}🔨 正在编译 Fraud Detection 模块...${NC}"
    cd "$PROJECT_ROOT"
    ./build.sh fraud-detection
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ 编译失败${NC}"
        exit 1
    fi
fi

echo -e "${YELLOW}⚙️ 准备 Kafka Topic...${NC}"
docker exec kafka kafka-topics --create \
    --topic transactions \
    --bootstrap-server kafka:9092 \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

INPUT_FILE="$PROJECT_ROOT/data/input/transactions-input.csv"
mkdir -p "$PROJECT_ROOT/data/input"

echo -e "${YELLOW}📝 生成并发送测试交易数据到 Kafka...${NC}"
CURRENT_TIME=$(date +%s)
echo "tx-001,userA,4111111111111111,120.50,merchantX,CityA,$(($CURRENT_TIME * 1000)),SUCCESS" > "$INPUT_FILE"
echo "tx-002,userA,4111111111111111,80.00,merchantX,CityA,$(($CURRENT_TIME * 1000 + 30000)),SUCCESS" >> "$INPUT_FILE"
echo "tx-003,userA,4111111111111111,60.00,merchantX,CityA,$(($CURRENT_TIME * 1000 + 60000)),SUCCESS" >> "$INPUT_FILE"
echo "tx-004,userB,4222222222222222,8000.00,merchantY,CityB,$(($CURRENT_TIME * 1000 + 90000)),SUCCESS" >> "$INPUT_FILE"
echo "tx-005,userC,4333333333333333,50.00,merchantZ,CityC,$(($CURRENT_TIME * 1000 + 120000)),FAILED" >> "$INPUT_FILE"

echo -e "${GREEN}已生成示例数据到 $INPUT_FILE${NC}"

cat "$INPUT_FILE" | docker exec -i kafka kafka-console-producer \
    --topic transactions \
    --bootstrap-server kafka:9092

echo -e "${GREEN}✅ 数据已发送到 Kafka Topic 'transactions'${NC}"

echo -e "${YELLOW}🚀 提交 Flink 作业...${NC}"
JOB_JAR="/opt/flink/usrlib/fraud-detection/fraud-detection-1.0.0.jar"
MAIN_CLASS="com.streaming.practice.fraud.FraudDetection"

docker exec flink-jobmanager flink run \
    -d \
    -c "$MAIN_CLASS" \
    "$JOB_JAR"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ 作业提交成功！${NC}"
    echo -e "📊 可以在 Flink Web UI (http://localhost:8081) 查看作业运行情况"

    echo -e "${YELLOW}🔍 查看作业状态...${NC}"
    docker exec flink-jobmanager flink list

    echo -e "${YELLOW}📋 等待文件输出生成 (最多 120 秒)...${NC}"
    OUTPUT_DIR="$PROJECT_ROOT/data/output/fraud-alerts"
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
        echo -e "${GREEN}✅ 风控示例运行完成！${NC}"
    else
        echo -e "${YELLOW}⚠️ 未找到输出目录或结果文件 $OUTPUT_DIR${NC}"
        echo -e "${YELLOW}ℹ️ 可在 TaskManager 日志或 Flink Web UI 查看控制台输出${NC}"
        echo -e "${GREEN}✅ 风控示例运行完成！${NC}"
    fi
else
    echo -e "${RED}❌ 作业提交失败${NC}"
    exit 1
fi
