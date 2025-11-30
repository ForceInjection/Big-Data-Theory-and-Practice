#!/usr/bin/env python3
"""
简化的Kafka Streams处理器
使用基本的Kafka消费者和生产者来模拟流处理功能
"""

from kafka import KafkaConsumer, KafkaProducer
import json
import time
from collections import defaultdict

def simple_stream_processor():
    """简单的流处理器，模拟Kafka Streams功能"""
    
    print("启动简化的Kafka Streams处理器...")
    
    # 创建消费者读取user-events主题
    consumer = KafkaConsumer(
        'user-events',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='stream-processor-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    # 创建生产者写入处理结果
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # 维护处理状态
    user_purchase_counts = defaultdict(int)
    user_purchase_totals = defaultdict(int)
    
    print("开始处理用户事件流...")
    print("按Ctrl+C停止处理")
    
    try:
        for message in consumer:
            event = message.value
            user_id = event.get('user_id')
            action = event.get('action')
            value = event.get('value', 0)
            
            # 只处理购买事件
            if action == 'purchase':
                user_purchase_counts[user_id] += 1
                user_purchase_totals[user_id] += value
                
                # 输出处理结果
                result = {
                    'user_id': user_id,
                    'purchase_count': user_purchase_counts[user_id],
                    'total_amount': user_purchase_totals[user_id],
                    'timestamp': int(time.time() * 1000)
                }
                
                # 发送到结果主题
                producer.send('user-purchase-stats', value=result)
                
                print(f"处理购买事件: 用户 {user_id}, 金额 {value}, 总次数 {user_purchase_counts[user_id]}, 总金额 {user_purchase_totals[user_id]}")
            
            # 每处理10个事件输出一次统计信息
            if sum(user_purchase_counts.values()) % 10 == 0:
                print("\n当前统计:")
                for uid, count in user_purchase_counts.items():
                    if count > 0:
                        print(f"  用户 {uid}: {count}次购买, 总金额 {user_purchase_totals[uid]}")
                print()
    
    except KeyboardInterrupt:
        print("\n停止流处理...")
    
    finally:
        consumer.close()
        producer.close()
        print("流处理器已关闭")

if __name__ == "__main__":
    simple_stream_processor()