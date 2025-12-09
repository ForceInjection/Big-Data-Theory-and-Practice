#!/bin/bash

# 实时用户行为分析示例运行脚本

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}📊 启动实时用户行为分析示例...${NC}"

# 1. 检查环境
echo -e "${YELLOW}🔍 检查运行环境...${NC}"
if ! docker ps | grep -q flink-jobmanager; then
    echo -e "${RED}❌ Flink 环境未启动，请先运行 ./scripts/start-environment.sh${NC}"
    exit 1
fi

if ! docker ps | grep -q kafka; then
    echo -e "${RED}❌ Kafka 服务未启动，请先运行 ./scripts/start-environment.sh${NC}"
    exit 1
fi

# 2. 编译项目
JAR_FILE="$PROJECT_ROOT/target/user-behavior/user-behavior-1.0.0.jar"

if [ -f "$JAR_FILE" ]; then
    echo -e "${GREEN}✅ 检测到 JAR 包已存在，跳过编译步骤...${NC}"
    echo -e "   JAR路径: $JAR_FILE"
else
    echo -e "${YELLOW}🔨 正在编译 User Behavior Analysis 模块...${NC}"
    cd "$PROJECT_ROOT"
    ./build.sh user-behavior

    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ 编译失败${NC}"
        exit 1
    fi
fi

# 3. 准备 Kafka Topic
echo -e "${YELLOW}⚙️ 准备 Kafka Topic...${NC}"
docker exec kafka kafka-topics --create \
    --topic user-behavior-events \
    --bootstrap-server kafka:9092 \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists

# 清理之前的输出数据
echo -e "${YELLOW}🧹 清理之前的输出数据...${NC}"
rm -rf "$PROJECT_ROOT/data/output/user-behavior-result"

# 4. 生成并发送数据
INPUT_FILE="$PROJECT_ROOT/data/input/user-behavior-input.csv"
mkdir -p "$PROJECT_ROOT/data/input"

echo -e "${YELLOW}📝 生成并发送测试数据到 Kafka...${NC}"

# 生成模拟数据 (每次运行都重新生成，保证时间戳是最新的)
CURRENT_TIME=$(date +%s)
echo "user1,view,product1,$(($CURRENT_TIME * 1000))" > "$INPUT_FILE"
echo "user1,click,product1,$(($CURRENT_TIME * 1000 + 1000))" >> "$INPUT_FILE"
echo "user2,view,product2,$(($CURRENT_TIME * 1000 + 2000))" >> "$INPUT_FILE"
echo "user1,add_to_cart,product1,$(($CURRENT_TIME * 1000 + 3000))" >> "$INPUT_FILE"
echo "user1,purchase,product1,$(($CURRENT_TIME * 1000 + 4000))" >> "$INPUT_FILE"
echo "user3,view,product1,$(($CURRENT_TIME * 1000 + 5000))" >> "$INPUT_FILE"
echo "user2,view,product1,$(($CURRENT_TIME * 1000 + 6000))" >> "$INPUT_FILE"
echo "user2,click,product1,$(($CURRENT_TIME * 1000 + 7000))" >> "$INPUT_FILE"

# 发送一条未来的数据以触发之前的窗口计算 (窗口1分钟，允许迟到10秒)
# 需要发送时间戳 > 窗口结束时间 + 10秒 的数据
# 假设当前时间是窗口开始附近，加 75秒 足够触发窗口关闭
FUTURE_TIME=$(( ($CURRENT_TIME + 75) * 1000 ))
echo "dummy_user,view,dummy_product,$FUTURE_TIME" >> "$INPUT_FILE"

echo -e "${GREEN}已生成示例数据到 $INPUT_FILE${NC}"

# 发送数据到 Kafka
cat "$INPUT_FILE" | docker exec -i kafka kafka-console-producer \
    --topic user-behavior-events \
    --bootstrap-server kafka:9092

echo -e "${GREEN}✅ 数据已发送到 Kafka Topic 'user-behavior-events'${NC}"

# 5. 提交作业
echo -e "${YELLOW}🚀 提交 Flink 作业...${NC}"
# 注意：容器内的路径是 /opt/flink/usrlib/user-behavior/user-behavior-1.0.0.jar
# 因为 docker-compose 挂载的是 ./target:/opt/flink/usrlib
# 而 build.sh 将 jar 复制到了 target/user-behavior/
JOB_JAR="/opt/flink/usrlib/user-behavior/user-behavior-1.0.0.jar"
MAIN_CLASS="com.streaming.practice.userbehavior.UserBehaviorAnalysis"

docker exec flink-jobmanager flink run \
    -d \
    -c "$MAIN_CLASS" \
    "$JOB_JAR"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ 作业提交成功！${NC}"
    echo -e "📊 可以在 Flink Web UI (http://localhost:8081) 查看作业运行情况"
    
    # 7. 查看作业输出
    echo -e "${YELLOW}📋 查看作业输出...${NC}"
    OUTPUT_DIR="$PROJECT_ROOT/data/output/user-behavior-result"
    
    # 轮询等待结果文件生成 (最多等待 120 秒)
    echo -e "${YELLOW}⏳ 等待结果文件生成 (最多 120 秒)...${NC}"
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
    
    # 6. 查看作业状态
    echo -e "${YELLOW}🔍 查看作业状态...${NC}"
    docker exec flink-jobmanager flink list
    
    if [ "$FOUND_DATA" = true ]; then
        echo -e "输出目录: $OUTPUT_DIR"
        
        # 查找所有结果文件 (包括 .inprogress 和 .part-*)
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
    else
         echo -e "${RED}❌ 未找到输出目录或文件 $OUTPUT_DIR${NC}"
         echo "⚠️ 尚未生成输出文件，可能作业仍在初始化或无数据输出"
    fi
    
    echo -e "${GREEN}✅ 用户行为分析示例运行完成！${NC}"
else
    echo -e "${RED}❌ 作业提交失败${NC}"
    exit 1
fi
