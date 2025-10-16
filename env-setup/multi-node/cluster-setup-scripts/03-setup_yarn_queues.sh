#!/bin/bash
# 配置 YARN 资源队列脚本
# 用法: ./03-setup_yarn_queues.sh

HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
CAPACITY_SCHEDULER_FILE="$HADOOP_CONF_DIR/capacity-scheduler.xml"

# 检查 Hadoop 配置目录
if [ ! -d "$HADOOP_CONF_DIR" ]; then
    echo "错误: Hadoop 配置目录不存在: $HADOOP_CONF_DIR"
    echo "请设置正确的 HADOOP_CONF_DIR 环境变量"
    exit 1
fi

echo "=== 开始配置 YARN 资源队列 ==="
echo "配置文件: $CAPACITY_SCHEDULER_FILE"
echo

# 备份原始配置文件
if [ -f "$CAPACITY_SCHEDULER_FILE" ]; then
    echo "备份原始配置文件..."
    cp "$CAPACITY_SCHEDULER_FILE" "${CAPACITY_SCHEDULER_FILE}.backup.$(date +%Y%m%d_%H%M%S)"
    echo "备份完成: ${CAPACITY_SCHEDULER_FILE}.backup.$(date +%Y%m%d_%H%M%S)"
fi

# 创建新的 capacity-scheduler.xml 配置
echo "创建新的队列配置..."
cat > "$CAPACITY_SCHEDULER_FILE" << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <!-- 队列定义 -->
  <property>
    <name>yarn.scheduler.capacity.root.queues</name>
    <value>default,students</value>
    <description>根队列下的子队列列表</description>
  </property>

  <!-- 默认队列配置 -->
  <property>
    <name>yarn.scheduler.capacity.root.default.capacity</name>
    <value>30</value>
    <description>默认队列容量百分比</description>
  </property>

  <property>
    <name>yarn.scheduler.capacity.root.default.maximum-capacity</name>
    <value>50</value>
    <description>默认队列最大容量百分比</description>
  </property>

  <property>
    <name>yarn.scheduler.capacity.root.default.state</name>
    <value>RUNNING</value>
    <description>队列状态</description>
  </property>

  <property>
    <name>yarn.scheduler.capacity.root.default.acl_submit_applications</name>
    <value>*</value>
    <description>允许提交应用的用户</description>
  </property>

  <property>
    <name>yarn.scheduler.capacity.root.default.acl_administer_queue</name>
    <value>*</value>
    <description>允许管理队列的用户</description>
  </property>

  <!-- 学生队列配置 -->
  <property>
    <name>yarn.scheduler.capacity.root.students.capacity</name>
    <value>70</value>
    <description>学生队列容量百分比</description>
  </property>

  <property>
    <name>yarn.scheduler.capacity.root.students.maximum-capacity</name>
    <value>80</value>
    <description>学生队列最大容量百分比</description>
  </property>

  <property>
    <name>yarn.scheduler.capacity.root.students.state</name>
    <value>RUNNING</value>
    <description>队列状态</description>
  </property>

  <property>
    <name>yarn.scheduler.capacity.root.students.acl_submit_applications</name>
    <value>students</value>
    <description>只允许 students 组用户提交应用</description>
  </property>

  <property>
    <name>yarn.scheduler.capacity.root.students.acl_administer_queue</name>
    <value>hadoop</value>
    <description>只允许 hadoop 用户管理队列</description>
  </property>

  <!-- 用户限制配置 -->
  <property>
    <name>yarn.scheduler.capacity.root.students.user-limit-factor</name>
    <value>2</value>
    <description>用户资源限制因子</description>
  </property>

  <property>
    <name>yarn.scheduler.capacity.root.students.maximum-applications</name>
    <value>100</value>
    <description>队列最大应用数量</description>
  </property>

  <property>
    <name>yarn.scheduler.capacity.root.students.maximum-am-resource-percent</name>
    <value>0.3</value>
    <description>ApplicationMaster 最大资源百分比</description>
  </property>

  <!-- 全局配置 -->
  <property>
    <name>yarn.scheduler.capacity.maximum-applications</name>
    <value>10000</value>
    <description>系统最大应用数量</description>
  </property>

  <property>
    <name>yarn.scheduler.capacity.maximum-am-resource-percent</name>
    <value>0.1</value>
    <description>全局 ApplicationMaster 最大资源百分比</description>
  </property>

  <property>
    <name>yarn.scheduler.capacity.resource-calculator</name>
    <value>org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator</value>
    <description>资源计算器</description>
  </property>

  <property>
    <name>yarn.scheduler.capacity.node-locality-delay</name>
    <value>40</value>
    <description>节点本地性延迟</description>
  </property>

  <property>
    <name>yarn.scheduler.capacity.rack-locality-additional-delay</name>
    <value>-1</value>
    <description>机架本地性额外延迟</description>
  </property>
</configuration>
EOF

echo "队列配置文件创建完成"
echo

# 验证配置文件格式
echo "验证配置文件格式..."
if command -v xmllint >/dev/null 2>&1; then
    if xmllint --noout "$CAPACITY_SCHEDULER_FILE" 2>/dev/null; then
        echo "配置文件格式验证通过"
    else
        echo "警告: 配置文件格式可能有问题"
    fi
else
    echo "提示: 未安装 xmllint，跳过格式验证"
fi

echo
echo "=== YARN 队列配置完成 ==="
echo "配置摘要:"
echo "- default 队列: 30% 容量，最大 50%"
echo "- students 队列: 70% 容量，最大 80%"
echo "- students 队列仅允许 students 组用户使用"
echo
echo "重启 YARN 服务以应用配置:"
echo "stop-yarn.sh"
echo "start-yarn.sh"
echo
echo "验证队列配置:"
echo "yarn queue -list"