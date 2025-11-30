#!/usr/bin/env python3
"""
消费者组演示 - 消费者
多个实例可以同时运行来演示负载均衡
"""

from kafka import KafkaConsumer
import json
import sys
import time

# 获取消费者实例标识
consumer_id = sys.argv[1] if len(sys.argv) > 1 else "consumer-1"

# 配置 Kafka 消费者
consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='load-balance-demo-group',  # 所有实例使用相同的组ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def consume_messages():
    """消费消息并显示消费者分配信息"""
    
    # 获取当前分配的分区
    assignment = consumer.assignment()
    print(f"[{consumer_id}] 分配到的分区: {[p.partition for p in assignment]}")
    
    print(f"[{consumer_id}] 开始消费消息...")
    print("=" * 60)
    
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            
            print(f"[{consumer_id}] 分区 {message.partition} | "
                  f"偏移量 {message.offset} | "
                  f"内容: {message.value['content']}")
            
            # 模拟处理时间
            time.sleep(0.1)
            
            # 每消费10条消息显示状态
            if message_count % 10 == 0:
                print(f"[{consumer_id}] 已消费 {message_count} 条消息")
                
    except KeyboardInterrupt:
        print(f"\n[{consumer_id}] 消费中断，总共消费 {message_count} 条消息")

if __name__ == "__main__":
    try:
        consume_messages()
    except Exception as e:
        print(f"[{consumer_id}] 错误: {e}")
    finally:
        consumer.close()