#!/usr/bin/env python3
"""
多 broker 环境事务测试脚本
用于在完整的多 broker Kafka 集群中测试事务功能
"""

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
import json
import time

def test_transactional_producer():
    """测试事务生产者"""
    print("=== 测试事务生产者 ===")
    
    try:
        # 配置事务生产者
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            transactional_id='test-transactional-producer',
            retries=5,
            acks='all'
        )
        
        # 初始化事务
        producer.init_transactions()
        print("✓ 事务初始化成功")
        
        # 开始事务
        producer.begin_transaction()
        print("✓ 事务开始")
        
        # 发送消息
        for i in range(5):
            message = {
                'transaction_id': f'tx-{i}',
                'value': f'message-{i}',
                'timestamp': time.time()
            }
            producer.send('transaction-test', value=message)
            print(f"✓ 发送消息: {message['transaction_id']}")
        
        # 提交事务
        producer.commit_transaction()
        print("✓ 事务提交成功")
        
        producer.close()
        return True
        
    except Exception as e:
        print(f"✗ 事务测试失败: {e}")
        return False

def test_transactional_consumer():
    """测试事务消费者"""
    print("\n=== 测试事务消费者 ===")
    
    try:
        consumer = KafkaConsumer(
            'transaction-test',
            bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id='transaction-test-group',
            isolation_level='read_committed',  # 只读取已提交的消息
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        print("等待接收事务消息...")
        
        # 读取消息
        received_messages = []
        for i, message in enumerate(consumer):
            if i >= 5:  # 只读取5条消息
                break
            received_messages.append(message.value)
            print(f"✓ 收到消息: {message.value['transaction_id']}")
        
        consumer.close()
        
        if len(received_messages) == 5:
            print("✓ 成功接收到所有事务消息")
            return True
        else:
            print(f"✗ 只收到 {len(received_messages)} 条消息，预期 5 条")
            return False
            
    except Exception as e:
        print(f"✗ 消费者测试失败: {e}")
        return False

def create_test_topic():
    """创建测试 topic"""
    print("=== 创建测试 Topic ===")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094']
        )
        
        topic = NewTopic(
            name='transaction-test',
            num_partitions=3,
            replication_factor=3,  # 多副本
            topic_configs={
                'min.insync.replicas': '2'
            }
        )
        
        admin_client.create_topics([topic])
        print("✓ 创建测试 topic 成功 (3分区, 3副本)")
        return True
        
    except TopicAlreadyExistsError:
        print("✓ 测试 topic 已存在")
        return True
    except NoBrokersAvailable:
        print("✗ 无法连接到 Kafka broker，请先启动多 broker 集群")
        return False
    except Exception as e:
        print(f"✗ 创建 topic 失败: {e}")
        return False

def main():
    """主函数"""
    print("多 broker Kafka 事务功能测试")
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
    
    # 创建测试 topic
    if not create_test_topic():
        return
    
    # 测试事务生产者
    producer_success = test_transactional_producer()
    
    # 等待消息提交
    time.sleep(2)
    
    # 测试事务消费者
    consumer_success = test_transactional_consumer()
    
    print("\n" + "=" * 50)
    if producer_success and consumer_success:
        print("🎉 事务功能测试成功!")
        print("多 broker 环境支持完整的事务语义")
    else:
        print("❌ 事务功能测试失败")
        print("请检查多 broker 集群状态")

if __name__ == "__main__":
    main()