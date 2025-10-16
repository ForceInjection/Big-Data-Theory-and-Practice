#!/bin/bash

# Hadoop 集群自动化备份配置脚本
# 用法: ./11-setup_backup.sh

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_debug() {
    echo -e "${BLUE}[DEBUG]${NC} $1"
}

# 检查是否为 root 用户
check_root() {
    if [[ $EUID -ne 0 ]]; then
        log_error "此脚本需要 root 权限运行"
        exit 1
    fi
}

# 检查 Hadoop 环境
check_hadoop_env() {
    if [[ -z "$HADOOP_HOME" ]]; then
        log_error "HADOOP_HOME 环境变量未设置"
        exit 1
    fi
    
    if [[ ! -d "$HADOOP_HOME" ]]; then
        log_error "Hadoop 安装目录不存在: $HADOOP_HOME"
        exit 1
    fi
    
    log_info "Hadoop 环境检查通过: $HADOOP_HOME"
}

# 创建备份目录结构
create_backup_dirs() {
    local backup_base="/opt/hadoop-backup"
    local dirs=(
        "$backup_base"
        "$backup_base/config"
        "$backup_base/hdfs-snapshots"
        "$backup_base/logs"
        "$backup_base/scripts"
    )
    
    for dir in "${dirs[@]}"; do
        if [[ ! -d "$dir" ]]; then
            mkdir -p "$dir"
            log_info "创建备份目录: $dir"
        fi
    done
    
    # 设置权限
    chown -R hadoop:hadoop "$backup_base"
    chmod -R 755 "$backup_base"
}

# 创建配置文件备份脚本
create_config_backup_script() {
    local script_path="/opt/hadoop-backup/scripts/backup_config.sh"
    
    cat > "$script_path" << 'EOF'
#!/bin/bash

# Hadoop 配置文件备份脚本
# 自动备份 Hadoop 配置文件

BACKUP_DIR="/opt/hadoop-backup/config"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
CONFIG_BACKUP_DIR="$BACKUP_DIR/config_$TIMESTAMP"

# 创建时间戳目录
mkdir -p "$CONFIG_BACKUP_DIR"

# 备份配置文件
if [[ -n "$HADOOP_HOME" && -d "$HADOOP_HOME/etc/hadoop" ]]; then
    cp -r "$HADOOP_HOME/etc/hadoop"/* "$CONFIG_BACKUP_DIR/"
    echo "$(date): 配置文件备份完成 -> $CONFIG_BACKUP_DIR" >> "$BACKUP_DIR/backup.log"
else
    echo "$(date): 错误 - HADOOP_HOME 未设置或配置目录不存在" >> "$BACKUP_DIR/backup.log"
    exit 1
fi

# 清理超过 30 天的备份
find "$BACKUP_DIR" -name "config_*" -type d -mtime +30 -exec rm -rf {} \; 2>/dev/null || true

echo "配置文件备份完成: $CONFIG_BACKUP_DIR"
EOF

    chmod +x "$script_path"
    chown hadoop:hadoop "$script_path"
    log_info "创建配置文件备份脚本: $script_path"
}

# 创建 HDFS 快照备份脚本
create_hdfs_snapshot_script() {
    local script_path="/opt/hadoop-backup/scripts/backup_hdfs_snapshots.sh"
    
    cat > "$script_path" << 'EOF'
#!/bin/bash

# HDFS 快照备份脚本
# 自动创建和管理 HDFS 快照

BACKUP_DIR="/opt/hadoop-backup/hdfs-snapshots"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$BACKUP_DIR/snapshot.log"

# 检查 HDFS 是否运行
if ! hdfs dfsadmin -report &>/dev/null; then
    echo "$(date): 错误 - HDFS 服务未运行" >> "$LOG_FILE"
    exit 1
fi

# 创建根目录快照（如果启用了快照功能）
if hdfs dfsadmin -allowSnapshot / 2>/dev/null; then
    SNAPSHOT_NAME="root_snapshot_$TIMESTAMP"
    if hdfs dfs -createSnapshot / "$SNAPSHOT_NAME" 2>/dev/null; then
        echo "$(date): 根目录快照创建成功: $SNAPSHOT_NAME" >> "$LOG_FILE"
    else
        echo "$(date): 根目录快照创建失败" >> "$LOG_FILE"
    fi
fi

# 为用户目录创建快照
USER_DIRS=$(hdfs dfs -ls /user 2>/dev/null | grep "^d" | awk '{print $8}' || true)
for user_dir in $USER_DIRS; do
    if hdfs dfsadmin -allowSnapshot "$user_dir" 2>/dev/null; then
        user_name=$(basename "$user_dir")
        SNAPSHOT_NAME="${user_name}_snapshot_$TIMESTAMP"
        if hdfs dfs -createSnapshot "$user_dir" "$SNAPSHOT_NAME" 2>/dev/null; then
            echo "$(date): 用户目录快照创建成功: $user_dir -> $SNAPSHOT_NAME" >> "$LOG_FILE"
        fi
    fi
done

# 清理超过 7 天的快照
CUTOFF_DATE=$(date -d "7 days ago" +"%Y%m%d")
hdfs dfs -ls /.snapshot 2>/dev/null | grep "snapshot_" | while read -r line; do
    snapshot_path=$(echo "$line" | awk '{print $8}')
    snapshot_name=$(basename "$snapshot_path")
    snapshot_date=$(echo "$snapshot_name" | grep -o '[0-9]\{8\}' | head -1)
    
    if [[ "$snapshot_date" < "$CUTOFF_DATE" ]]; then
        if hdfs dfs -deleteSnapshot / "$snapshot_name" 2>/dev/null; then
            echo "$(date): 清理旧快照: $snapshot_name" >> "$LOG_FILE"
        fi
    fi
done

echo "HDFS 快照备份完成"
EOF

    chmod +x "$script_path"
    chown hadoop:hadoop "$script_path"
    log_info "创建 HDFS 快照备份脚本: $script_path"
}

# 创建日志备份脚本
create_log_backup_script() {
    local script_path="/opt/hadoop-backup/scripts/backup_logs.sh"
    
    cat > "$script_path" << 'EOF'
#!/bin/bash

# Hadoop 日志备份脚本
# 自动备份和压缩 Hadoop 日志文件

BACKUP_DIR="/opt/hadoop-backup/logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_BACKUP_DIR="$BACKUP_DIR/logs_$TIMESTAMP"

# 创建备份目录
mkdir -p "$LOG_BACKUP_DIR"

# 备份 Hadoop 日志
if [[ -n "$HADOOP_HOME" && -d "$HADOOP_HOME/logs" ]]; then
    # 只备份 .log 文件，排除 .out 文件
    find "$HADOOP_HOME/logs" -name "*.log" -mtime +1 -exec cp {} "$LOG_BACKUP_DIR/" \;
    
    # 压缩备份
    cd "$BACKUP_DIR"
    tar -czf "logs_$TIMESTAMP.tar.gz" "logs_$TIMESTAMP"
    rm -rf "$LOG_BACKUP_DIR"
    
    echo "$(date): 日志备份完成 -> logs_$TIMESTAMP.tar.gz" >> "$BACKUP_DIR/backup.log"
else
    echo "$(date): 错误 - Hadoop 日志目录不存在" >> "$BACKUP_DIR/backup.log"
    exit 1
fi

# 清理超过 14 天的日志备份
find "$BACKUP_DIR" -name "logs_*.tar.gz" -mtime +14 -delete 2>/dev/null || true

echo "日志备份完成: logs_$TIMESTAMP.tar.gz"
EOF

    chmod +x "$script_path"
    chown hadoop:hadoop "$script_path"
    log_info "创建日志备份脚本: $script_path"
}

# 创建主备份脚本
create_main_backup_script() {
    local script_path="/opt/hadoop-backup/scripts/full_backup.sh"
    
    cat > "$script_path" << 'EOF'
#!/bin/bash

# Hadoop 集群完整备份脚本
# 执行所有备份任务

SCRIPT_DIR="/opt/hadoop-backup/scripts"
LOG_FILE="/opt/hadoop-backup/full_backup.log"

echo "$(date): 开始完整备份" >> "$LOG_FILE"

# 执行配置文件备份
if [[ -x "$SCRIPT_DIR/backup_config.sh" ]]; then
    echo "$(date): 执行配置文件备份" >> "$LOG_FILE"
    "$SCRIPT_DIR/backup_config.sh" >> "$LOG_FILE" 2>&1
fi

# 执行 HDFS 快照备份
if [[ -x "$SCRIPT_DIR/backup_hdfs_snapshots.sh" ]]; then
    echo "$(date): 执行 HDFS 快照备份" >> "$LOG_FILE"
    sudo -u hadoop "$SCRIPT_DIR/backup_hdfs_snapshots.sh" >> "$LOG_FILE" 2>&1
fi

# 执行日志备份
if [[ -x "$SCRIPT_DIR/backup_logs.sh" ]]; then
    echo "$(date): 执行日志备份" >> "$LOG_FILE"
    "$SCRIPT_DIR/backup_logs.sh" >> "$LOG_FILE" 2>&1
fi

echo "$(date): 完整备份结束" >> "$LOG_FILE"
echo "完整备份任务完成"
EOF

    chmod +x "$script_path"
    chown hadoop:hadoop "$script_path"
    log_info "创建主备份脚本: $script_path"
}

# 设置定时任务
setup_cron_jobs() {
    local cron_file="/tmp/hadoop_backup_cron"
    
    # 创建 cron 任务
    cat > "$cron_file" << EOF
# Hadoop 集群自动备份任务
# 每天凌晨 2 点执行完整备份
0 2 * * * /opt/hadoop-backup/scripts/full_backup.sh >/dev/null 2>&1

# 每 6 小时执行配置文件备份
0 */6 * * * /opt/hadoop-backup/scripts/backup_config.sh >/dev/null 2>&1

# 每天凌晨 3 点执行 HDFS 快照备份
0 3 * * * sudo -u hadoop /opt/hadoop-backup/scripts/backup_hdfs_snapshots.sh >/dev/null 2>&1
EOF

    # 安装 cron 任务
    crontab "$cron_file"
    rm -f "$cron_file"
    
    log_info "定时备份任务已设置："
    log_info "  - 完整备份：每天凌晨 2:00"
    log_info "  - 配置备份：每 6 小时"
    log_info "  - HDFS 快照：每天凌晨 3:00"
}

# 创建备份状态检查脚本
create_status_script() {
    local script_path="/opt/hadoop-backup/scripts/backup_status.sh"
    
    cat > "$script_path" << 'EOF'
#!/bin/bash

# 备份状态检查脚本

BACKUP_DIR="/opt/hadoop-backup"

echo "=== Hadoop 集群备份状态 ==="
echo

# 检查备份目录
echo "备份目录状态："
du -sh "$BACKUP_DIR"/* 2>/dev/null || echo "  无备份数据"
echo

# 检查最近的配置备份
echo "最近的配置备份："
ls -lt "$BACKUP_DIR/config" 2>/dev/null | head -5 || echo "  无配置备份"
echo

# 检查最近的日志备份
echo "最近的日志备份："
ls -lt "$BACKUP_DIR/logs"/*.tar.gz 2>/dev/null | head -5 || echo "  无日志备份"
echo

# 检查 HDFS 快照
echo "HDFS 快照状态："
if command -v hdfs >/dev/null 2>&1; then
    sudo -u hadoop hdfs dfs -ls /.snapshot 2>/dev/null | head -10 || echo "  无快照或快照功能未启用"
else
    echo "  HDFS 命令不可用"
fi
echo

# 检查定时任务
echo "定时备份任务："
crontab -l 2>/dev/null | grep -E "(backup|hadoop)" || echo "  无定时备份任务"
EOF

    chmod +x "$script_path"
    chown hadoop:hadoop "$script_path"
    log_info "创建备份状态检查脚本: $script_path"
}

# 主函数
main() {
    log_info "开始配置 Hadoop 集群自动化备份..."
    
    # 检查权限和环境
    check_root
    check_hadoop_env
    
    # 创建备份目录结构
    create_backup_dirs
    
    # 创建各种备份脚本
    create_config_backup_script
    create_hdfs_snapshot_script
    create_log_backup_script
    create_main_backup_script
    create_status_script
    
    # 设置定时任务
    setup_cron_jobs
    
    log_info "自动化备份配置完成！"
    echo
    log_info "可用的备份命令："
    log_info "  - 完整备份：/opt/hadoop-backup/scripts/full_backup.sh"
    log_info "  - 配置备份：/opt/hadoop-backup/scripts/backup_config.sh"
    log_info "  - HDFS 快照：/opt/hadoop-backup/scripts/backup_hdfs_snapshots.sh"
    log_info "  - 日志备份：/opt/hadoop-backup/scripts/backup_logs.sh"
    log_info "  - 状态检查：/opt/hadoop-backup/scripts/backup_status.sh"
    echo
    log_info "备份目录：/opt/hadoop-backup"
    log_info "定时任务已设置，可使用 'crontab -l' 查看"
}

# 执行主函数
main "$@"