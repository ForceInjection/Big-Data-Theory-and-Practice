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
from typing import Dict, Any, List, Optional, Iterator

from .utils import DataGenerator, PerformanceAnalyzer


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
        
        # 转换为 PyArrow Table
        table = pa.Table.from_pandas(nested_data)
        
        print("嵌套数据结构:")
        print(table.schema)
        
        # 保存嵌套数据
        nested_file = os.path.join(self.output_dir, 'nested_data.parquet')
        
        def write_nested():
            pq.write_table(table, nested_file)
        
        _, write_time = self.performance_analyzer.measure_time(write_nested)
        
        print(f"写入嵌套数据: {write_time:.4f} 秒")
        
        # 读取嵌套数据
        def read_nested():
            return pq.read_table(nested_file).to_pandas()
        
        df_read, read_time = self.performance_analyzer.measure_time(read_nested)
        
        print(f"读取嵌套数据: {read_time:.4f} 秒")
        
        # 验证数据完整性
        original_rows = len(nested_data)
        read_rows = len(df_read)
        data_integrity = original_rows == read_rows
        
        print(f"数据完整性: {'✓' if data_integrity else '✗'}")
        print(f"原始行数: {original_rows}, 读取行数: {read_rows}")
        
        # 分析嵌套列
        nested_columns = []
        for col in df_read.columns:
            if df_read[col].dtype == 'object':
                sample_value = df_read[col].iloc[0]
                if isinstance(sample_value, (list, dict)):
                    nested_columns.append(col)
        
        print(f"嵌套列: {nested_columns}")
        
        # 文件大小
        file_size = self.performance_analyzer.get_file_size(nested_file)
        print(f"文件大小: {file_size:.2f} MB")
        
        return {
            'write_time': write_time,
            'read_time': read_time,
            'data_integrity': data_integrity,
            'original_rows': original_rows,
            'read_rows': read_rows,
            'nested_columns': nested_columns,
            'file_size_mb': file_size
        }
    
    def test_metadata_operations(self) -> Dict[str, Any]:
        """
        测试元数据操作
        
        Returns:
            元数据操作测试结果
        """
        print("\n" + "=" * 60)
        print("测试元数据操作")
        print("=" * 60)
        
        # 创建测试数据
        df = self.data_generator.generate_user_data(1000)
        table = pa.Table.from_pandas(df)
        
        # 添加自定义元数据
        metadata = {
            'created_by': 'Parquet Practice Exercise',
            'version': '1.0',
            'description': 'Test data for metadata operations',
            'schema_version': '2023.1'
        }
        
        # 将元数据添加到 schema
        existing_metadata = table.schema.metadata or {}
        updated_metadata = {**existing_metadata}
        for key, value in metadata.items():
            updated_metadata[key.encode()] = value.encode()
        
        schema_with_metadata = table.schema.with_metadata(updated_metadata)
        table_with_metadata = table.cast(schema_with_metadata)
        
        # 保存带元数据的文件
        metadata_file = os.path.join(self.output_dir, 'with_metadata.parquet')
        pq.write_table(table_with_metadata, metadata_file)
        
        print("已添加自定义元数据:")
        for key, value in metadata.items():
            print(f"• {key}: {value}")
        
        # 读取并检查元数据
        parquet_file = pq.ParquetFile(metadata_file)
        
        print("\n文件元数据信息:")
        print(f"• 行数: {parquet_file.metadata.num_rows:,}")
        print(f"• 列数: {parquet_file.metadata.num_columns}")
        print(f"• 行组数: {parquet_file.metadata.num_row_groups}")
        print(f"• 创建者: {parquet_file.metadata.created_by or 'Unknown'}")
        
        # 读取自定义元数据
        custom_metadata = {}
        try:
            # 尝试从 schema 的 pandas metadata 中获取自定义元数据
            schema_metadata = parquet_file.schema_arrow.metadata
            if schema_metadata:
                for key, value in schema_metadata.items():
                    try:
                        key_str = key.decode() if isinstance(key, bytes) else str(key)
                        value_str = value.decode() if isinstance(value, bytes) else str(value)
                        if not key_str.startswith('pandas'):  # 跳过 pandas 内部元数据
                            custom_metadata[key_str] = value_str
                    except (UnicodeDecodeError, AttributeError):
                        pass
        except AttributeError:
            # 如果没有元数据，继续执行
            pass
        
        print("\n自定义元数据:")
        for key, value in custom_metadata.items():
            print(f"• {key}: {value}")
        
        # 列级元数据
        print("\n列信息:")
        for i, column in enumerate(parquet_file.schema):
            print(f"• 列 {i}: {column.name} ({column.physical_type})")
        
        return {
            'file_metadata': {
                'num_rows': parquet_file.metadata.num_rows,
                'num_columns': parquet_file.metadata.num_columns,
                'num_row_groups': parquet_file.metadata.num_row_groups,
                'created_by': parquet_file.metadata.created_by or 'Unknown'
            },
            'custom_metadata': custom_metadata,
            'schema_info': [
                {'name': col.name, 'type': str(col.physical_type)}
                for col in parquet_file.schema
            ]
        }
    
    def test_streaming_operations(self, total_records: int = 10000, batch_size: int = 1000) -> Dict[str, Any]:
        """
        测试流式操作
        
        Args:
            total_records: 总记录数
            batch_size: 批次大小
            
        Returns:
            流式操作测试结果
        """
        print("\n" + "=" * 60)
        print("测试流式操作")
        print("=" * 60)
        
        streaming_file = os.path.join(self.output_dir, 'streaming_data.parquet')
        
        # 流式写入
        print(f"流式写入 {total_records:,} 条记录 (批次大小: {batch_size:,})")
        
        def streaming_write():
            writer = None
            batches_written = 0
            
            try:
                for i in range(0, total_records, batch_size):
                    # 生成批次数据
                    current_batch_size = min(batch_size, total_records - i)
                    batch_df = self.data_generator.generate_user_data(current_batch_size)
                    batch_table = pa.Table.from_pandas(batch_df)
                    
                    if writer is None:
                        # 创建写入器
                        writer = pq.ParquetWriter(streaming_file, batch_table.schema)
                    
                    # 写入批次
                    writer.write_table(batch_table)
                    batches_written += 1
                    
                    if batches_written % 5 == 0:
                        print(f"已写入 {batches_written} 个批次...")
                
            finally:
                if writer:
                    writer.close()
            
            return batches_written
        
        batches_written, write_time = self.performance_analyzer.measure_time(streaming_write)
        
        print(f"流式写入完成: {write_time:.4f} 秒, {batches_written} 个批次")
        
        # 流式读取
        print("\n流式读取数据:")
        
        def streaming_read():
            parquet_file = pq.ParquetFile(streaming_file)
            total_rows = 0
            batches_read = 0
            
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                batch_df = batch.to_pandas()
                total_rows += len(batch_df)
                batches_read += 1
                
                if batches_read % 5 == 0:
                    print(f"已读取 {batches_read} 个批次, {total_rows:,} 行...")
            
            return total_rows, batches_read
        
        (total_rows, batches_read), read_time = self.performance_analyzer.measure_time(streaming_read)
        
        print(f"流式读取完成: {read_time:.4f} 秒")
        print(f"总行数: {total_rows:,}, 批次数: {batches_read}")
        
        # 验证数据完整性
        data_integrity = total_rows == total_records
        print(f"数据完整性: {'✓' if data_integrity else '✗'}")
        
        # 文件大小
        file_size = self.performance_analyzer.get_file_size(streaming_file)
        print(f"文件大小: {file_size:.2f} MB")
        
        return {
            'total_records': total_records,
            'batch_size': batch_size,
            'write_time': write_time,
            'read_time': read_time,
            'batches_written': batches_written,
            'batches_read': batches_read,
            'total_rows_read': total_rows,
            'data_integrity': data_integrity,
            'file_size_mb': file_size
        }
    
    def test_schema_evolution(self) -> Dict[str, Any]:
        """
        测试 Schema 演进
        
        Returns:
            Schema 演进测试结果
        """
        print("\n" + "=" * 60)
        print("测试 Schema 演进")
        print("=" * 60)
        
        # 创建初始 schema
        initial_data = pd.DataFrame({
            'id': range(100),
            'name': [f'User_{i}' for i in range(100)],
            'age': np.random.randint(18, 80, 100)
        })
        
        initial_file = os.path.join(self.output_dir, 'schema_v1.parquet')
        initial_table = pa.Table.from_pandas(initial_data)
        pq.write_table(initial_table, initial_file)
        
        print("初始 Schema (v1):")
        print(initial_table.schema)
        
        # 演进 schema - 添加新列
        evolved_data = initial_data.copy()
        evolved_data['email'] = [f'user_{i}@example.com' for i in range(100)]
        evolved_data['city'] = np.random.choice(['北京', '上海', '广州'], 100)
        
        evolved_file = os.path.join(self.output_dir, 'schema_v2.parquet')
        evolved_table = pa.Table.from_pandas(evolved_data)
        pq.write_table(evolved_table, evolved_file)
        
        print("\n演进后 Schema (v2):")
        print(evolved_table.schema)
        
        # 测试兼容性读取
        print("\n测试 Schema 兼容性:")
        
        # 读取 v1 文件
        v1_table = pq.read_table(initial_file)
        print(f"v1 文件列数: {len(v1_table.schema)}")
        
        # 读取 v2 文件
        v2_table = pq.read_table(evolved_file)
        print(f"v2 文件列数: {len(v2_table.schema)}")
        
        # 尝试用 v1 schema 读取 v2 文件的部分列
        try:
            v2_partial = pq.read_table(evolved_file, columns=['id', 'name', 'age'])
            compatibility_test = len(v2_partial.schema) == len(v1_table.schema)
            print(f"向后兼容性: {'✓' if compatibility_test else '✗'}")
        except Exception as e:
            print(f"向后兼容性: ✗ ({str(e)})")
            compatibility_test = False
        
        # 合并不同 schema 的数据
        print("\n合并不同 Schema 的数据:")
        try:
            # 为 v1 数据添加缺失列
            v1_df = v1_table.to_pandas()
            v1_df['email'] = None
            v1_df['city'] = None
            
            v2_df = v2_table.to_pandas()
            
            # 合并数据
            combined_df = pd.concat([v1_df, v2_df], ignore_index=True)
            
            combined_file = os.path.join(self.output_dir, 'schema_combined.parquet')
            combined_table = pa.Table.from_pandas(combined_df)
            pq.write_table(combined_table, combined_file)
            
            print(f"合并成功: {len(combined_df)} 行, {len(combined_df.columns)} 列")
            merge_success = True
            
        except Exception as e:
            print(f"合并失败: {str(e)}")
            merge_success = False
        
        return {
            'initial_schema': str(initial_table.schema),
            'evolved_schema': str(evolved_table.schema),
            'v1_columns': len(v1_table.schema),
            'v2_columns': len(v2_table.schema),
            'backward_compatibility': compatibility_test,
            'merge_success': merge_success
        }
    
    def test_data_types_and_encoding(self) -> Dict[str, Any]:
        """
        测试数据类型和编码
        
        Returns:
            数据类型和编码测试结果
        """
        print("\n" + "=" * 60)
        print("测试数据类型和编码")
        print("=" * 60)
        
        # 创建包含各种数据类型的数据
        data = {
            'int8_col': np.random.randint(-128, 127, 1000, dtype=np.int8),
            'int16_col': np.random.randint(-32768, 32767, 1000, dtype=np.int16),
            'int32_col': np.random.randint(-2147483648, 2147483647, 1000, dtype=np.int32),
            'int64_col': np.random.randint(-9223372036854775808, 9223372036854775807, 1000, dtype=np.int64),
            'float32_col': np.random.random(1000).astype(np.float32),
            'float64_col': np.random.random(1000).astype(np.float64),
            'bool_col': np.random.choice([True, False], 1000),
            'string_col': [f'String_{i}' for i in range(1000)],
            'category_col': np.random.choice(['A', 'B', 'C', 'D'], 1000),
            'datetime_col': pd.date_range('2023-01-01', periods=1000, freq='H'),
            'decimal_col': np.round(np.random.random(1000) * 1000, 2)
        }
        
        df = pd.DataFrame(data)
        
        # 转换为 PyArrow 表并指定精确的数据类型
        schema = pa.schema([
            ('int8_col', pa.int8()),
            ('int16_col', pa.int16()),
            ('int32_col', pa.int32()),
            ('int64_col', pa.int64()),
            ('float32_col', pa.float32()),
            ('float64_col', pa.float64()),
            ('bool_col', pa.bool_()),
            ('string_col', pa.string()),
            ('category_col', pa.dictionary(pa.int32(), pa.string())),
            ('datetime_col', pa.timestamp('ns')),
            ('decimal_col', pa.float64())  # 使用 float64 而不是 decimal128
        ])
        
        table = pa.Table.from_pandas(df, schema=schema)
        
        print("数据类型 Schema:")
        for field in table.schema:
            print(f"• {field.name}: {field.type}")
        
        # 保存文件
        types_file = os.path.join(self.output_dir, 'data_types.parquet')
        pq.write_table(table, types_file)
        
        # 读取并验证类型
        read_table = pq.read_table(types_file)
        
        print("\n读取后的数据类型:")
        for field in read_table.schema:
            print(f"• {field.name}: {field.type}")
        
        # 类型保持性检查
        type_preservation = str(table.schema) == str(read_table.schema)
        print(f"\n数据类型保持性: {'✓' if type_preservation else '✗'}")
        
        # 文件大小分析
        file_size = self.performance_analyzer.get_file_size(types_file)
        print(f"文件大小: {file_size:.2f} MB")
        
        # 分析编码效果
        parquet_file = pq.ParquetFile(types_file)
        encoding_info = []
        
        for rg in range(parquet_file.metadata.num_row_groups):
            row_group = parquet_file.metadata.row_group(rg)
            for col in range(row_group.num_columns):
                column = row_group.column(col)
                encoding_info.append({
                    'column': column.path_in_schema,
                    'encoding': str(column.encodings),
                    'compression': str(column.compression),
                    'total_byte_size': getattr(column, 'total_byte_size', 0),
                    'total_compressed_size': getattr(column, 'total_compressed_size', 0)
                })
        
        print("\n编码信息:")
        for info in encoding_info[:5]:  # 显示前5列的信息
            compression_ratio = info['total_byte_size'] / info['total_compressed_size'] if info['total_compressed_size'] > 0 else 1
            print(f"• {info['column']}: {info['encoding']}, 压缩比: {compression_ratio:.2f}")
        
        return {
            'schema': str(table.schema),
            'type_preservation': type_preservation,
            'file_size_mb': file_size,
            'encoding_info': encoding_info
        }
    
    def run_advanced_exercise(self) -> Dict[str, Any]:
        """
        运行完整的高级特性练习
        
        Returns:
            所有高级特性测试的结果
        """
        print("=" * 60)
        print("开始 Parquet 高级练习")
        print("=" * 60)
        
        results = {}
        
        # 1. Nested data structure test
        results['nested_data'] = self.test_nested_data_structures()
        
        # 2. Metadata operations test
        results['metadata'] = self.test_metadata_operations()
        
        # 3. Streaming operations test
        results['streaming'] = self.test_streaming_operations()
        
        # 4. Schema evolution test
        results['schema_evolution'] = self.test_schema_evolution()
        
        # 5. Data types and encoding test
        results['data_types'] = self.test_data_types_and_encoding()
        
        # 6. Compression algorithm test
        results['compression'] = self.test_compression_algorithms()
        
        # Display summary
        self.display_advanced_summary(results)
        
        # 保存结果
        results_file = os.path.join(self.output_dir, 'advanced_results.json')
        self.performance_analyzer.save_results(results, results_file)
        
        return results
    
    def test_compression_algorithms(self) -> Dict[str, Any]:
        """
        测试压缩算法对比
        
        Returns:
            压缩算法测试结果
        """
        print("\n" + "=" * 60)
        print("测试压缩算法对比")
        print("=" * 60)
        
        compression_types = ['snappy', 'gzip', 'brotli', 'lz4']
        results = {}
        
        # 生成测试数据
        test_data = self.data_generator.generate_user_data(5000)
        table = pa.Table.from_pandas(test_data)
        
        for compression in compression_types:
            print(f"\n测试压缩算法: {compression}")
            
            filename = os.path.join(self.output_dir, f'data_{compression}.parquet')
            
            # 保存
            def save_with_compression():
                pq.write_table(table, filename, compression=compression)
            
            _, save_time = self.performance_analyzer.measure_time(save_with_compression)
            
            # 读取
            def read_compressed():
                return pq.read_table(filename)
            
            _, read_time = self.performance_analyzer.measure_time(read_compressed)
            
            # 文件大小
            file_size = self.performance_analyzer.get_file_size(filename)
            
            results[compression] = {
                'save_time': save_time,
                'read_time': read_time,
                'file_size': file_size
            }
            
            print(f"保存时间: {save_time:.4f} 秒")
            print(f"读取时间: {read_time:.4f} 秒")
            print(f"文件大小: {file_size:.2f} MB")
        
        # 找出最佳压缩算法
        best_compression = min(results.keys(), key=lambda x: results[x]['file_size'])
        fastest_save = min(results.keys(), key=lambda x: results[x]['save_time'])
        fastest_read = min(results.keys(), key=lambda x: results[x]['read_time'])
        
        print(f"\n压缩算法对比结果:")
        print(f"• 最佳压缩率: {best_compression} ({results[best_compression]['file_size']:.2f} MB)")
        print(f"• 最快保存: {fastest_save} ({results[fastest_save]['save_time']:.4f} 秒)")
        print(f"• 最快读取: {fastest_read} ({results[fastest_read]['read_time']:.4f} 秒)")
        
        return {
            'compression_results': results,
            'best_compression': best_compression,
            'fastest_save': fastest_save,
            'fastest_read': fastest_read
        }
    
    def display_advanced_summary(self, results: Dict[str, Any]) -> None:
        """
        显示高级特性总结
        
        Args:
            results: 高级特性测试结果
        """
        print("\n" + "=" * 60)
        print("高级特性练习总结")
        print("=" * 60)
        
        print("🎯 高级特性验证:")
        
        if 'nested_data' in results:
            integrity = results['nested_data'].get('data_integrity', False)
            print(f"• 嵌套数据处理: {'✓' if integrity else '✗'}")
        
        if 'metadata' in results:
            metadata_count = len(results['metadata'].get('custom_metadata', {}))
            print(f"• 元数据操作: ✓ ({metadata_count} 个自定义字段)")
        
        if 'streaming' in results:
            streaming_integrity = results['streaming'].get('data_integrity', False)
            print(f"• 流式处理: {'✓' if streaming_integrity else '✗'}")
        
        if 'schema_evolution' in results:
            compatibility = results['schema_evolution'].get('backward_compatibility', False)
            print(f"• Schema 演进: {'✓' if compatibility else '✗'}")
        
        if 'data_types' in results:
            type_preservation = results['data_types'].get('type_preservation', False)
            print(f"• 数据类型保持: {'✓' if type_preservation else '✗'}")
        
        if 'compression' in results:
            best_compression = results['compression'].get('best_compression', 'N/A')
            print(f"• 压缩算法测试: ✓ (最佳: {best_compression})")
        
        print("\n💡 高级特性应用场景:")
        print("• 嵌套数据: JSON、XML 等半结构化数据存储")
        print("• 元数据: 数据血缘、版本控制、质量标记")
        print("• 流式处理: 大数据集的内存友好处理")
        print("• Schema 演进: 数据模型的平滑升级")
        print("• 数据类型: 精确的数据表示和存储优化")
        print("• 压缩算法: 存储空间与性能的平衡")
    
    def cleanup(self):
        """清理临时文件"""
        from .utils import cleanup_files
        patterns = [
            os.path.join(self.output_dir, 'nested_data.parquet'),
            os.path.join(self.output_dir, 'with_metadata.parquet'),
            os.path.join(self.output_dir, 'streaming_data.parquet'),
            os.path.join(self.output_dir, 'schema_v1.parquet'),
            os.path.join(self.output_dir, 'schema_v2.parquet'),
            os.path.join(self.output_dir, 'schema_combined.parquet'),
            os.path.join(self.output_dir, 'data_types.parquet'),
            os.path.join(self.output_dir, 'data_*.parquet')
        ]
        cleanup_files(patterns)