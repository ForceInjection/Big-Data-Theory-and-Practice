#!/usr/bin/env python3
"""
Kafka Streams 数据生成器
为流处理示例生成测试数据
"""

from kafka import KafkaProducer
import json
import time
import random

def generate_stream_data():
    """生成流处理测试数据"""
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    user_actions = ['click', 'view', 'purchase', 'login', 'logout']
    user_ids = ['user1', 'user2', 'user3', 'user4', 'user5']
    
    print("生成 Kafka Streams 测试数据...")
    print("运行Java Streams应用来处理这些数据")
    
    try:
        for i in range(100):
            event = {
                'user_id': random.choice(user_ids),
                'action': random.choice(user_actions),
                'timestamp': int(time.time() * 1000),
                'value': random.randint(1, 1000),
                'sequence': i
            }
            
            # 发送到 user-events topic
            producer.send('user-events', value=event)
            
            if (i + 1) % 20 == 0:
                print(f"已生成 {i + 1} 个事件")
                
            time.sleep(0.2)  # 控制生成速率
            
    except KeyboardInterrupt:
        print("\n数据生成中断")
    except Exception as e:
        print(f"错误: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    generate_stream_data()