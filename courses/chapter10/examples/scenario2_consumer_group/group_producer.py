#!/usr/bin/env python3
"""
消费者组演示 - 生产者
发送大量消息供多个消费者消费
"""

from kafka import KafkaProducer
import json
import time

# 配置 Kafka 生产者
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_test_messages():
    """生成测试消息"""
    messages = []
    
    # 生成 100 条测试消息
    for i in range(100):
        message = {
            'message_id': i + 1,
            'content': f'测试消息 {i + 1}',
            'timestamp': int(time.time()),
            'partition_hint': f'partition_{i % 4}'  # 提示消息可能的分区
        }
        messages.append(message)
    
    return messages

def send_messages():
    """发送测试消息"""
    messages = generate_test_messages()
    
    print(f"开始发送 {len(messages)} 条测试消息...")
    
    for i, message in enumerate(messages):
        future = producer.send('test-topic', value=message)
        
        # 每10条消息显示进度
        if (i + 1) % 10 == 0:
            metadata = future.get(timeout=10)
            print(f"已发送 {i + 1} 条消息 (最新分区: {metadata.partition})")
    
    print("所有消息发送完成!")
    print("现在可以启动多个消费者实例来演示负载均衡")

if __name__ == "__main__":
    try:
        send_messages()
    except Exception as e:
        print(f"错误: {e}")
    finally:
        producer.close()