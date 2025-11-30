#!/usr/bin/env python3
"""
分区感知消费者示例
演示如何从特定分区消费消息
"""

from kafka import KafkaConsumer
import json

# 配置 Kafka 消费者
consumer = KafkaConsumer(
    'user-behavior',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # 从最早的消息开始消费
    enable_auto_commit=True,
    group_id='partition-demo-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    key_deserializer=lambda x: x.decode('utf-8') if x else None
)

def consume_from_all_partitions():
    """从所有分区消费消息"""
    print("开始消费消息 (所有分区)...")
    print("=" * 50)
    
    try:
        for message in consumer:
            print(f"分区 {message.partition} | "
                  f"偏移量 {message.offset} | "
                  f"键: {message.key} | "
                  f"值: {message.value}")
            print("-" * 50)
            
    except KeyboardInterrupt:
        print("\n消费中断")

if __name__ == "__main__":
    try:
        consume_from_all_partitions()
    except Exception as e:
        print(f"错误: {e}")
    finally:
        consumer.close()