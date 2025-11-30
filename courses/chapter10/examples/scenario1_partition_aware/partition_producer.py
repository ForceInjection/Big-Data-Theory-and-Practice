#!/usr/bin/env python3
"""
分区感知生产者示例
演示如何使用消息键控制消息路由到特定分区
"""

from kafka import KafkaProducer
import json
import time

# 配置 Kafka 生产者
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

def send_messages_with_keys():
    """发送带不同键的消息到不同分区"""
    
    # 用户数据 - 相同用户ID的消息会进入相同分区
    users = [
        {'user_id': 'user1', 'action': 'login', 'timestamp': int(time.time())},
        {'user_id': 'user2', 'action': 'purchase', 'timestamp': int(time.time())},
        {'user_id': 'user1', 'action': 'logout', 'timestamp': int(time.time())},
        {'user_id': 'user3', 'action': 'view', 'timestamp': int(time.time())},
        {'user_id': 'user2', 'action': 'logout', 'timestamp': int(time.time())}
    ]
    
    print("开始发送带键的消息...")
    
    for i, user_data in enumerate(users):
        # 使用 user_id 作为消息键
        key = user_data['user_id']
        
        # 发送消息
        future = producer.send(
            'user-behavior', 
            key=key,
            value=user_data
        )
        
        # 获取消息元数据（包括分区信息）
        metadata = future.get(timeout=10)
        
        print(f"消息 {i+1}: 用户 {key} -> 分区 {metadata.partition}")
    
    print("所有消息发送完成!")

if __name__ == "__main__":
    try:
        send_messages_with_keys()
    except Exception as e:
        print(f"错误: {e}")
    finally:
        producer.close()