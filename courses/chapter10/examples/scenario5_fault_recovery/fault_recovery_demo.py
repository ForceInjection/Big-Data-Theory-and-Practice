#!/usr/bin/env python3
"""
故障恢复演示
演示 Kafka 的高可用性和自动故障恢复
"""

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError
import json
import time
import threading
import signal
import sys

# 配置参数
MAX_MESSAGES = 50  # 最大消息数量
PRODUCER_SLEEP = 0.5  # 生产者发送间隔(秒)
CONSUMER_SLEEP = 0.3  # 消费者处理间隔(秒)
DEMO_TIMEOUT = 120  # 演示超时时间(秒)

# 全局控制变量
stop_demo = False
message_count = 0
consumed_count = 0

def setup_test_environment():
    """设置测试环境"""
    
    admin_client = KafkaAdminClient(
        bootstrap_servers='localhost:9092'
    )
    
    # 创建测试 Topic (单broker环境)
    topic = NewTopic(
        name='ha-test-topic',
        num_partitions=3,
        replication_factor=1,  # 单broker环境
        topic_configs={}
    )
    
    try:
        admin_client.create_topics([topic])
        print("创建高可用测试 Topic 成功")
        
        # 查看 Topic 详情
        topic_info = admin_client.describe_topics(['ha-test-topic'])
        print(f"Topic 配置: {topic_info}")
        
    except Exception as e:
        print(f"创建 Topic 失败: {e}")
    finally:
        admin_client.close()

def signal_handler(sig, frame):
    """信号处理函数"""
    global stop_demo
    print("\n接收到停止信号，正在优雅停止...")
    stop_demo = True

def continuous_producer():
    """持续生产者，模拟实时数据流"""
    global message_count
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',  # 需要所有副本确认
        retries=10   # 重试次数
    )
    
    print(f"启动持续生产者 (最多发送 {MAX_MESSAGES} 条消息)...")
    
    try:
        while not stop_demo and message_count < MAX_MESSAGES:
            message = {
                'id': message_count + 1,
                'content': f'测试消息 {message_count + 1}',
                'timestamp': int(time.time()),
                'sequence': message_count
            }
            
            future = producer.send('ha-test-topic', value=message)
            
            # 获取发送结果
            metadata = future.get(timeout=10)
            
            message_count += 1
            
            if message_count % 10 == 0:
                print(f"已发送 {message_count}/{MAX_MESSAGES} 条消息 (最新分区: {metadata.partition})")
            elif message_count == MAX_MESSAGES:
                print(f"✓ 已完成 {MAX_MESSAGES} 条消息发送")
            
            time.sleep(PRODUCER_SLEEP)  # 控制发送速率
            
    except KeyboardInterrupt:
        print(f"\n生产者被中断，总共发送 {message_count} 条消息")
    except Exception as e:
        print(f"生产者错误: {e}")
    finally:
        producer.close()
        print("生产者已关闭")

def resilient_consumer():
    """弹性消费者，演示故障恢复"""
    global consumed_count, stop_demo
    
    consumer = KafkaConsumer(
        'ha-test-topic',
        bootstrap_servers=['localhost:9092'],
        group_id='fault-tolerance-group',
        auto_offset_reset='earliest',
        enable_auto_commit=False,  # 手动提交以演示精确控制
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=1000  # 添加超时以避免无限阻塞
    )
    
    print("启动弹性消费者...")
    
    try:
        for message in consumer:
            if stop_demo:
                break
                
            consumed_count += 1
            
            print(f"消费消息: 序列 {message.value['sequence']} | "
                  f"分区 {message.partition} | 偏移量 {message.offset}")
            
            # 模拟消息处理
            time.sleep(CONSUMER_SLEEP)
            
            # 手动提交偏移量
            consumer.commit()
            
            if consumed_count % 5 == 0:
                print(f"已成功处理 {consumed_count} 条消息")
            
            # 如果所有消息都已消费且生产者已停止，则退出
            if consumed_count >= message_count and stop_demo:
                break
                
    except KeyboardInterrupt:
        print(f"\n消费者被中断，总共处理 {consumed_count} 条消息")
    except Exception as e:
        print(f"消费者错误: {e}")
        # 这里可以添加重试逻辑
    finally:
        consumer.close()
        print("消费者已关闭")

def demonstrate_fault_recovery():
    """演示故障恢复过程"""
    global stop_demo
    
    print("=" * 60)
    print("Kafka 故障恢复演示")
    print("=" * 60)
    print(f"配置: {MAX_MESSAGES} 条消息 | {DEMO_TIMEOUT} 秒超时")
    
    # 注册信号处理
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 设置测试环境
    setup_test_environment()
    
    # 启动生产者和消费者
    producer_thread = threading.Thread(target=continuous_producer)
    consumer_thread = threading.Thread(target=resilient_consumer)
    
    start_time = time.time()
    producer_thread.start()
    time.sleep(2)  # 让生产者先发送一些消息
    consumer_thread.start()
    
    print("\n系统运行中...")
    print("可以尝试停止 Kafka Broker 来模拟故障")
    print(f"按 Ctrl+C 停止演示 (或等待 {MAX_MESSAGES} 条消息完成)")
    
    # 主循环，监控超时和完成状态
    try:
        while not stop_demo:
            # 检查是否超时
            if time.time() - start_time > DEMO_TIMEOUT:
                print(f"\n⚠️  演示超时 ({DEMO_TIMEOUT} 秒)，正在停止...")
                stop_demo = True
                break
            
            # 检查是否所有消息都已发送和消费
            if message_count >= MAX_MESSAGES and consumed_count >= MAX_MESSAGES:
                print(f"\n✓ 所有 {MAX_MESSAGES} 条消息已成功发送和消费")
                stop_demo = True
                break
            
            time.sleep(1)  # 每秒检查一次
            
    except KeyboardInterrupt:
        print("\n演示被用户中断")
    
    # 等待线程结束
    stop_demo = True
    producer_thread.join(timeout=5)
    consumer_thread.join(timeout=5)
    
    # 打印最终统计
    print("\n" + "=" * 40)
    print("演示结果统计:")
    print(f"发送消息: {message_count}/{MAX_MESSAGES}")
    print(f"消费消息: {consumed_count}/{MAX_MESSAGES}")
    print(f"运行时间: {time.time() - start_time:.1f} 秒")
    print("=" * 40)
    
    if message_count == MAX_MESSAGES and consumed_count == MAX_MESSAGES:
        print("✓ 演示成功完成!")
    else:
        print("⚠️  演示未完全完成")

if __name__ == "__main__":
    demonstrate_fault_recovery()