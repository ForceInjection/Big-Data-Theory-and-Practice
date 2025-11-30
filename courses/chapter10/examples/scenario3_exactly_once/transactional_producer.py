#!/usr/bin/env python3
"""
事务性生产者示例
演示 Kafka 的精确一次语义实现
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

# 配置事务性生产者
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    
    # 启用幂等性和事务支持
    enable_idempotence=True,
    transactional_id='eos-demo-producer',
    
    # 重要配置参数
    retries=5,  # 重试次数
    acks='all',  # 需要所有副本确认
    max_in_flight_requests_per_connection=1  # 保证顺序
)

def transactional_demo():
    """事务性生产演示"""
    
    # 初始化事务
    producer.init_transactions()
    
    print("开始事务性生产演示...")
    
    try:
        # 开始事务
        producer.begin_transaction()
        
        # 模拟业务操作 - 发送多条消息
        messages = [
            {'type': 'order_created', 'order_id': '1001', 'amount': 199.99},
            {'type': 'payment_processed', 'order_id': '1001', 'status': 'success'},
            {'type': 'inventory_updated', 'order_id': '1001', 'items': 2}
        ]
        
        for i, message in enumerate(messages):
            # 发送到不同 Topic 模拟分布式事务
            if message['type'] == 'order_created':
                future = producer.send('orders', value=message)
            elif message['type'] == 'payment_processed':
                future = producer.send('payments', value=message)
            else:
                future = producer.send('inventory', value=message)
            
            # 获取发送结果
            metadata = future.get(timeout=10)
            print(f"消息 {i+1} 发送成功: {message['type']} -> 分区 {metadata.partition}")
        
        # 提交事务（所有消息要么全部成功，要么全部失败）
        producer.commit_transaction()
        print("事务提交成功! 所有消息已原子性提交")
        
    except KafkaError as e:
        # 回滚事务
        producer.abort_transaction()
        print(f"事务失败，已回滚: {e}")
        
    except Exception as e:
        producer.abort_transaction()
        print(f"系统错误，事务已回滚: {e}")

if __name__ == "__main__":
    try:
        transactional_demo()
    except Exception as e:
        print(f"错误: {e}")
    finally:
        producer.close()