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

# 尝试删除旧 Topic 以确保数据纯净
echo -e "${YELLOW}🧹 尝试删除旧 Topic (如果存在)...${NC}"
docker exec kafka kafka-topics --delete --topic user-behavior-events --bootstrap-server kafka:9092 2>/dev/null || true
# 等待一会儿确保删除完成
sleep 2

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

# 生成模拟数据，模拟 Watermark 直观解释文档中的场景
# 窗口: [00, 05)秒, L=2s, AL=1s
# 场景: E01(01s), E03(03s), EX(07s), E02(02s, Late), E_Final(09s), E_SuperLate(02s, SideOutput)

# 使用当前时间的分钟整点作为 00秒 基准，方便观察
# 获取当前时间戳（秒）
NOW_SEC=$(python3 -c 'import time; print(int(time.time()))')
# 计算当前分钟整点时间戳（毫秒）
BASE_TIME=$(python3 -c "print(int($NOW_SEC // 60 * 60 * 1000))")

echo "基准时间 (XX:XX:00): $(date -r $(($BASE_TIME/1000)) '+%Y-%m-%d %H:%M:%S')"

# 定义时间偏移 (秒 -> 毫秒)
function time_offset() {
    echo $(($BASE_TIME + $1 * 1000))
}

# E01: 01s (正常数据)
T_E01=$(time_offset 1)
# E03: 03s (乱序数据)
T_E03=$(time_offset 3)
# EX: 07s (推进 Watermark 到 05s，触发窗口计算)
T_EX=$(time_offset 7)
# E02: 02s (迟到数据，Watermark=05s, 窗口[00,05)已触发但未关闭(AL=1s)，应更新结果)
T_E02=$(time_offset 2)
# E_Final: 09s (推进 Watermark 到 07s > 05+1，窗口彻底关闭)
T_FINAL=$(time_offset 9)
# E_SuperLate: 02s (超迟到数据，窗口已彻底关闭，应进入 Side Output)
T_SUPER_LATE=$(time_offset 2)

# 清空文件
> "$INPUT_FILE"

# 1. 发送 E01 (01s) -> Watermark -01s (实际上是负数或0，取决于实现，但肯定 < 05)
echo "user1,view,E01,$T_E01" >> "$INPUT_FILE"
# 2. 发送 E03 (03s) -> Watermark 01s
echo "user1,view,E03,$T_E03" >> "$INPUT_FILE"
# 3. 发送 EX (07s) -> Watermark 05s -> 触发窗口 [00, 05) 计算 (Count=2: E01, E03)
echo "user1,view,EX,$T_EX" >> "$INPUT_FILE"
# 4. 发送 E02 (02s) -> Watermark 05s -> 迟到但 AL 内 -> 更新窗口 [00, 05) (Count=3: E01, E03, E02)
echo "user1,view,E02_Late,$T_E02" >> "$INPUT_FILE"
# 5. 发送 E_Final (09s) -> Watermark 07s (> 05+1) -> 关闭窗口 [00, 05)
echo "user1,view,E_Final,$T_FINAL" >> "$INPUT_FILE"
# 6. 发送 E_SuperLate (02s) -> Watermark 07s -> 超迟到 -> Side Output
echo "user1,view,E_SuperLate,$T_SUPER_LATE" >> "$INPUT_FILE"

echo -e "${GREEN}已生成示例数据到 $INPUT_FILE${NC}"
cat "$INPUT_FILE"

# 5. 提交作业 (先提交作业，确保 Flink 准备好接收数据)
echo -e "${YELLOW}🚀 提交 Flink 作业...${NC}"
# 注意：容器内的路径是 /opt/flink/usrlib/user-behavior/user-behavior-1.0.0.jar
JOB_JAR="/opt/flink/usrlib/user-behavior/user-behavior-1.0.0.jar"
MAIN_CLASS="com.streaming.practice.userbehavior.UserBehaviorAnalysis"

docker exec flink-jobmanager flink run \
    -d \
    -c "$MAIN_CLASS" \
    "$JOB_JAR"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ 作业提交成功！${NC}"
    echo -e "📊 可以在 Flink Web UI (http://localhost:8081) 查看作业运行情况"
    
    echo "Waiting 10s for job to initialize..."
    sleep 10

    # 6. 发送数据到 Kafka (逐行发送，确保顺序和 Watermark 生成)
    echo -e "${YELLOW}📝 发送数据到 Kafka...${NC}"
    cat "$INPUT_FILE" | while read line; do
        echo "Sending: $line"
        echo "$line" | docker exec -i kafka kafka-console-producer \
            --topic user-behavior-events \
            --bootstrap-server kafka:9092
        
        echo "Sent. Waiting 2s..."
        sleep 2 # 等待 2 秒，确保 Watermark 推进
    done
    echo -e "${GREEN}✅ 数据已发送到 Kafka Topic 'user-behavior-events'${NC}"
    
    echo -e "${YELLOW}💡 提示：由于 Flink 处理需要时间，建议在 Flink UI 或日志中观察输出顺序${NC}"
    echo -e "   预期行为："
    echo -e "   1. 收到 EX 后，输出窗口 [00, 05) 结果 (Count=2)"
    echo -e "   2. 收到 E02_Late 后，再次输出窗口 [00, 05) 结果 (Count=3)"
    echo -e "   3. 收到 E_SuperLate 后，Side Output 输出该条数据"
    
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
        
        echo -e "${BLUE}📄 原始文件内容 (前20行):${NC}"
        echo "$RESULT_FILES" | while read file; do
             if [ -f "$file" ]; then
                 echo -e "${BLUE}=== $(basename "$file") ===${NC}"
                 head -n 20 "$file"
             fi
        done

        echo -e "\n${BLUE}📊 Watermark 机制验证报告:${NC}"
        echo -e "${CYAN}--------------------------------------------------------------------------------${NC}"
        echo -e "${CYAN}| 类别          | 窗口/事件时间       | 统计值 | 触发说明                      |${NC}"
        echo -e "${CYAN}|---------------|---------------------|--------|-------------------------------|${NC}"

        # 1. 解析主窗口输出
        # 格式: Window[11:43:00, 11:43:05) count=2, processTime=11:43:34
        cat $RESULT_FILES | sort | while read line; do
            if [[ "$line" == *"Window["* ]]; then
                # 提取窗口信息 Window[HH:mm:ss, HH:mm:ss)
                WINDOW=$(echo "$line" | grep -o "Window\[[^)]*)" | sed 's/Window//')
                # 提取 Count
                COUNT=$(echo "$line" | grep -o "count=[0-9]*" | cut -d= -f2)
                
                DESC="首次触发 (Watermark >= End)"
                if [ "$COUNT" -gt 2 ]; then
                    DESC="迟到更新 (Allowed Lateness)"
                fi
                
                printf "| %-13s | %-19s | %-6s | %-29s |\n" "窗口输出" "$WINDOW" "$COUNT" "$DESC"
            fi
        done

        # 2. 解析 Side Output (从 TaskManager 日志获取)
        TMS=$(docker ps --format '{{.Names}}' | grep taskmanager)
        FOUND_LATE=false
        for tm in $TMS; do
            # 格式: Late Data (Side Output)> UserBehaviorEvent{..., eventTime=2025-12-12T03:43:02}
            LATE_LOGS=$(docker logs $tm 2>&1 | grep "Late Data (Side Output)")
            if [ ! -z "$LATE_LOGS" ]; then
                echo "$LATE_LOGS" | while read log; do
                     # 提取事件时间 T03:43:02
                     RAW_TIME=$(echo "$log" | grep -o "eventTime=[^}]*" | cut -d= -f2)
                     # 截取 HH:mm:ss (最后8位)
                     EVENT_TIME=${RAW_TIME: -8}
                     printf "| %-13s | EventTime: %-8s | %-6s | %-29s |\n" "侧输出流" "$EVENT_TIME" "Drop" "严重迟到 (Side Output)"
                done
                FOUND_LATE=true
            fi
        done
        
        if [ "$FOUND_LATE" = false ]; then
             printf "| %-13s | %-19s | %-6s | %-29s |\n" "侧输出流" "N/A" "N/A" "未检测到侧输出数据 (可能尚未触发)"
        fi
        
        echo -e "${CYAN}--------------------------------------------------------------------------------${NC}"
    else
         echo -e "${RED}❌ 未找到输出目录或文件 $OUTPUT_DIR${NC}"
         echo "⚠️ 尚未生成输出文件，可能作业仍在初始化或无数据输出"
    fi
    
    echo -e "${GREEN}✅ 用户行为分析示例运行完成！${NC}"
else
    echo -e "${RED}❌ 作业提交失败${NC}"
    exit 1
fi
