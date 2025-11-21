"""
Parquet 高级特性练习模块

提供嵌套数据、元数据操作、流式处理等高级特性的演示。
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import os
import json
import time
from typing import Dict, Any, List, Optional, Iterator

from ..utils import DataGenerator, PerformanceAnalyzer


class ParquetAdvancedExercise:
    """Parquet 高级特性练习类"""
    
    def __init__(self, output_dir: str = "output"):
        """
        初始化高级特性练习
        
        Args:
            output_dir: 输出目录
        """
        self.output_dir = output_dir
        self.data_generator = DataGenerator()
        self.performance_analyzer = PerformanceAnalyzer()
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
    
    def test_nested_data_structures(self, num_records: int = 1000) -> Dict[str, Any]:
        """
        测试嵌套数据结构
        
        Args:
            num_records: 记录数量
            
        Returns:
            嵌套数据测试结果
        """
        print("=" * 60)
        print("测试嵌套数据结构")
        print("=" * 60)
        
        # 生成嵌套数据
        nested_data = self.data_generator.generate_nested_data(num_records)
        
        # 定义 PyArrow Schema
        schema = pa.schema([
            ('UserID', pa.int64()),
            ('Username', pa.string()),
            ('Age', pa.int64()),
            ('Contacts', pa.list_(pa.struct([
                ('type', pa.string()),
                ('value', pa.string())
            ]))),
            ('Address', pa.struct([
                ('province', pa.string()),
                ('city', pa.string()),
                ('district', pa.string()),
                ('street', pa.string())
            ])),
            ('Tags', pa.list_(pa.string()))
        ])
        
        # 转换为 PyArrow Table
        table = pa.Table.from_pandas(nested_data, schema=schema)
        
        # 保存为 Parquet
        nested_file = os.path.join(self.output_dir, 'nested_data.parquet')
        pq.write_table(table, nested_file)
        
        # 读取并验证
        table_read = pq.read_table(nested_file)
        df_read = table_read.to_pandas()
        
        # 性能指标
        file_size = self.performance_analyzer.get_file_size(nested_file)
        
        print(f"嵌套数据文件大小: {file_size:.2f} MB")
        print(f"原始数据形状: {nested_data.shape}")
        print(f"读取数据形状: {df_read.shape}")
        print("✅ 嵌套数据结构测试完成！")
        
        return {
            'file_size_mb': file_size,
            'original_shape': nested_data.shape,
            'read_shape': df_read.shape
        }
    
    def test_metadata_operations(self, filename: str) -> Dict[str, Any]:
        """
        测试元数据操作
        
        Args:
            filename: Parquet 文件路径
            
        Returns:
            元数据操作结果
        """
        print("=" * 60)
        print("测试元数据操作")
        print("=" * 60)
        
        # 读取元数据
        parquet_file = pq.ParquetFile(filename)
        metadata = parquet_file.metadata
        
        # 获取元数据信息
        result = {
            'num_rows': metadata.num_rows,
            'num_columns': metadata.num_columns,
            'num_row_groups': metadata.num_row_groups,
            'created_by': metadata.created_by,
            'schema': str(metadata.schema),
            'column_stats': {}
        }
        
        # 获取列统计信息
        for i in range(metadata.num_columns):
            col_name = metadata.schema.column(i).name
            result['column_stats'][col_name] = {
                'type': str(metadata.schema.column(i).physical_type),
                'compression': metadata.row_group(0).column(i).compression,
                'encodings': [str(enc) for enc in metadata.row_group(0).column(i).encodings]
            }
        
        print(f"文件行数: {result['num_rows']}")
        print(f"文件列数: {result['num_columns']}")
        print(f"行组数量: {result['num_row_groups']}")
        print("✅ 元数据操作测试完成！")
        
        return result
    
    def test_streaming_processing(self, filename: str, batch_size: int = 1000) -> Dict[str, Any]:
        """
        测试流式处理
        
        Args:
            filename: Parquet 文件路径
            batch_size: 批处理大小
            
        Returns:
            流式处理结果
        """
        print("=" * 60)
        print("测试流式处理")
        print("=" * 60)
        
        parquet_file = pq.ParquetFile(filename)
        total_rows = parquet_file.metadata.num_rows
        
        # 流式读取
        start_time = time.time()
        processed_rows = 0
        
        for i, batch in enumerate(parquet_file.iter_batches(batch_size=batch_size)):
            df_batch = batch.to_pandas()
            processed_rows += len(df_batch)
            
            # 模拟处理操作
            df_batch['processed'] = df_batch['Age'] * 2
            
            if (i + 1) % 10 == 0:
                print(f"已处理批次 {i + 1}, 行数: {processed_rows}/{total_rows}")
        
        processing_time = time.time() - start_time
        
        print(f"流式处理完成，总时间: {processing_time:.3f} 秒")
        print(f"处理速率: {total_rows / processing_time:.0f} 行/秒")
        print("✅ 流式处理测试完成！")
        
        return {
            'total_rows': total_rows,
            'processing_time': processing_time,
            'rows_per_second': total_rows / processing_time
        }
    
    def run_comprehensive_test(self, num_records: int = 5000):
        """
        运行综合测试
        
        Args:
            num_records: 记录数量
        """
        print("=" * 60)
        print("Parquet 高级特性综合测试")
        print("=" * 60)
        
        results = {}
        
        # 1. 测试嵌套数据结构
        results['nested_data'] = self.test_nested_data_structures(num_records)
        
        # 2. 测试元数据操作
        nested_file = os.path.join(self.output_dir, 'nested_data.parquet')
        results['metadata'] = self.test_metadata_operations(nested_file)
        
        # 3. 测试流式处理
        results['streaming'] = self.test_streaming_processing(nested_file)
        
        print("\n" + "=" * 60)
        print("综合测试完成！")
        print("=" * 60)
        
        # 保存测试结果
        results_file = os.path.join(self.output_dir, 'advanced_test_results.json')
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        print(f"测试结果已保存到: {results_file}")
        
        return results

    def run_advanced_exercise(self, num_records: int = 5000) -> Dict[str, Any]:
        """
        运行高级特性练习
        
        Args:
            num_records: 记录数量
            
        Returns:
            高级特性练习结果
        """
        return self.run_comprehensive_test(num_records)

    def cleanup(self):
        """清理临时文件"""
        from ..utils import cleanup_files
        patterns = [
            os.path.join(self.output_dir, 'nested_data.*'),
            os.path.join(self.output_dir, 'advanced_test_results.json')
        ]
        cleanup_files(patterns)


def main():
    """主函数"""
    exercise = ParquetAdvancedExercise()
    results = exercise.run_comprehensive_test(5000)
    
    # 打印摘要结果
    print("\n=== 测试结果摘要 ===")
    print(f"嵌套数据文件大小: {results['nested_data']['file_size_mb']:.2f} MB")
    print(f"流式处理速率: {results['streaming']['rows_per_second']:.0f} 行/秒")
    print(f"元数据列数: {results['metadata']['num_columns']}")


if __name__ == "__main__":
    main()