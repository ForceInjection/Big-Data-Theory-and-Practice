#!/usr/bin/env python3
"""
多 broker 环境故障恢复演示
演示真正的副本复制、Leader 选举和故障转移
"""

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
import json
import time
import threading
import random

def create_ha_topic():
    """创建高可用性 topic"""
    print("=== 创建高可用性 Topic ===")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094']
        )
        
        topic = NewTopic(
            name='ha-test-topic',
            num_partitions=3,
            replication_factor=3,  # 3个副本
            topic_configs={
                'min.insync.replicas': '2'  # 最少需要2个同步副本
            }
        )
        
        admin_client.create_topics([topic])
        print("✓ 创建高可用 topic 成功 (3分区, 3副本, min.insync.replicas=2)")
        return True
        
    except TopicAlreadyExistsError:
        print("✓ 高可用 topic 已存在")
        return True
    except Exception as e:
        print(f"✗ 创建 topic 失败: {e}")
        return False

def describe_topic():
    """查看 topic 详情，显示副本分布"""
    print("\n=== Topic 副本分布 ===")
    
    try:
        # 使用 kafka-topics 命令获取更详细的信息
        import subprocess
        result = subprocess.run([
            'docker', 'exec', 'kafka-broker1', 
            'kafka-topics', '--describe',
            '--topic', 'ha-test-topic',
            '--bootstrap-server', 'localhost:9092'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print(result.stdout)
            return True
        else:
            print("✗ 无法获取 topic 详情")
            return False
            
    except Exception as e:
        print(f"✗ 获取 topic 详情失败: {e}")
        return False

def producer_thread():
    """生产者线程 - 持续发送消息"""
    print("启动生产者线程...")
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'  # 需要所有副本确认
    )
    
    message_count = 0
    while True:
        try:
            message = {
                'id': message_count,
                'content': f'Message {message_count}',
                'timestamp': time.time()
            }
            
            producer.send('ha-test-topic', value=message)
            print(f"生产者发送: Message {message_count}")
            message_count += 1
            
            time.sleep(1)  # 每秒发送一条消息
            
        except Exception as e:
            print(f"生产者错误: {e}")
            time.sleep(2)

def consumer_thread():
    """消费者线程 - 持续消费消息"""
    print("启动消费者线程...")
    
    consumer = KafkaConsumer(
        'ha-test-topic',
        bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='ha-test-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    last_message_id = -1
    
    for message in consumer:
        print(f"消费者收到: ID={message.value['id']}, Content={message.value['content']}")
        
        # 检查消息连续性
        current_id = message.value['id']
        if last_message_id != -1 and current_id != last_message_id + 1:
            print(f"⚠️  消息不连续: 期望 {last_message_id + 1}, 收到 {current_id}")
        
        last_message_id = current_id

def simulate_broker_failure():
    """模拟 broker 故障"""
    print("\n=== 模拟 Broker 故障 ===")
    time.sleep(5)
    
    # 随机选择一个 broker 停止
    broker_to_stop = random.choice(['kafka-broker1', 'kafka-broker2', 'kafka-broker3'])
    print(f"停止 broker: {broker_to_stop}")
    
    import subprocess
    subprocess.run(['docker', 'stop', broker_to_stop], capture_output=True)
    
    print(f"Broker {broker_to_stop} 已停止，观察故障转移...")
    time.sleep(10)
    
    # 查看新的 Leader 分配
    describe_topic()
    
    # 等待一段时间后恢复 broker
    time.sleep(15)
    print(f"恢复 broker: {broker_to_stop}")
    subprocess.run(['docker', 'start', broker_to_stop], capture_output=True)
    
    print("Broker 已恢复，观察重新平衡...")
    time.sleep(10)
    describe_topic()

def main():
    """主函数"""
    print("多 broker Kafka 故障恢复演示")
    print("=" * 50)
    
    # 检查 broker 连接
    print("检查 broker 连接...")
    try:
        admin = KafkaAdminClient(bootstrap_servers=['localhost:9092'], request_timeout_ms=3000)
        admin.list_topics()
        print("✓ 成功连接到 Kafka broker")
    except Exception as e:
        print(f"✗ 无法连接到 Kafka broker: {e}")
        print("请先运行: ./startup_scripts/docker_start_multibroker.sh")
        return
    
    # 创建高可用 topic
    if not create_ha_topic():
        return
    
    # 查看初始副本分布
    describe_topic()
    
    # 启动生产者和消费者线程
    producer = threading.Thread(target=producer_thread, daemon=True)
    consumer = threading.Thread(target=consumer_thread, daemon=True)
    
    producer.start()
    consumer.start()
    
    # 等待初始消息流动
    time.sleep(3)
    
    # 模拟故障
    failure_simulator = threading.Thread(target=simulate_broker_failure)
    failure_simulator.start()
    
    # 等待演示完成
    try:
        failure_simulator.join()
        print("\n=== 演示完成 ===")
        print("故障恢复演示已完成，观察以下内容:")
        print("1. Leader 自动选举")
        print("2. 副本同步状态")
        print("3. 消息生产的连续性")
        print("4. 消费者无中断消费")
        
    except KeyboardInterrupt:
        print("\n演示被用户中断")

if __name__ == "__main__":
    main()