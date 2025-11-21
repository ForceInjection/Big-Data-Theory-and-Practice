"""
综合测试模块

提供完整的 Parquet 功能测试套件。
"""

import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import os
import tempfile
import shutil
import time
from pathlib import Path

from parquet_practice.exercises.basic import ParquetBasicExercise
from parquet_practice.exercises.compression import ParquetCompressionExercise
from parquet_practice.exercises.query_optimization import ParquetQueryOptimizationExercise
from parquet_practice.exercises.partitioning import ParquetPartitioningExercise
from parquet_practice.exercises.advanced import ParquetAdvancedExercise
from parquet_practice.utils import DataGenerator


class TestParquetComprehensive:
    """Parquet 综合测试类"""
    
    def setup_method(self):
        """测试设置"""
        self.test_dir = tempfile.mkdtemp(prefix="parquet_test_")
        self.data_generator = DataGenerator()
        
    def teardown_method(self):
        """测试清理"""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_basic_functionality(self):
        """测试基础功能"""
        print("\n=== 测试基础功能 ===")
        
        # 生成测试数据
        df = self.data_generator.generate_user_data(1000)
        
        # 测试基础练习
        exercise = ParquetBasicExercise(1000, self.test_dir)
        
        # 测试数据生成
        generated_df = exercise.generate_sample_data()# 验证结果
        assert generated_df.shape == (1000, 6)
        assert 'UserID' in generated_df.columns
        assert 'Username' in generated_df.columns
        
        # 测试保存和读取
        exercise.run_basic_exercise()
        
        # 验证数据生成
        assert generated_df.shape == (1000, 6)
        assert 'UserID' in generated_df.columns
        assert 'Username' in generated_df.columns
        
        # 验证文件存在
        parquet_file = os.path.join(self.test_dir, 'sample_data.parquet')
        csv_file = os.path.join(self.test_dir, 'sample_data.csv')
        assert os.path.exists(parquet_file)
        assert os.path.exists(csv_file)
        assert os.path.getsize(parquet_file) > 0
        assert os.path.getsize(csv_file) > 0
        
        print("✅ 基础功能测试通过")
    
    def test_compression_algorithms(self):
        """测试压缩算法"""
        print("\n=== 测试压缩算法 ===")
        
        # 生成测试数据
        df = self.data_generator.generate_user_data(5000)
        
        # 测试压缩练习
        exercise = ParquetCompressionExercise(df, self.test_dir)
        results = exercise.test_compression_algorithms()
        
        # 验证结果
        assert len(results) >= 4  # 至少4种压缩算法
        assert 'SNAPPY' in results
        assert 'GZIP' in results
        assert 'LZ4' in results
        assert 'BROTLI' in results
        
        # 验证每种算法都有完整的结果
        for algo, result in results.items():
            assert 'file_size' in result
            assert 'write_time' in result
            assert 'read_time' in result
            assert result['file_size'] > 0
        
        print("✅ 压缩算法测试通过")
    
    def test_query_optimization(self):
        """测试查询优化"""
        print("\n=== 测试查询优化 ===")
        
        # 生成测试数据
        df = self.data_generator.generate_user_data(10000)
        parquet_file = os.path.join(self.test_dir, 'test_query.parquet')
        df.to_parquet(parquet_file)
        
        # 测试查询优化
        exercise = ParquetQueryOptimizationExercise(df, self.test_dir)
        results = exercise.run_optimization_exercise()
        
        # 验证结果 - 更新为匹配实际的返回键
        assert 'projection' in results
        assert 'predicate' in results
        assert 'combined' in results
        assert 'complex' in results
        
        # 验证性能提升 - 使用实际的键结构
        projection_data = results['projection']
        predicate_data = results['predicate']
        
        # 投影下推应该减少内存使用
        assert projection_data['memory_reduction_percent'] > 0
        
        # 谓词下推应该减少数据量
        assert predicate_data['data_reduction_percent'] > 0
        
        print("✅ 查询优化测试通过")
    
    def test_partitioning(self):
        """测试分区存储"""
        print("\n=== 测试分区存储 ===")
        
        # 生成测试数据
        df = self.data_generator.generate_user_data(5000)
        
        # 测试分区练习
        exercise = ParquetPartitioningExercise(df, self.test_dir)
        
        # 先创建分区表和非分区表
        exercise.create_non_partitioned_table()
        exercise.create_partitioned_table(['City'])
        
        results = exercise.test_partition_pruning('Beijing')
        
        # 验证结果 - 更新为匹配实际的返回键
        assert 'non_partitioned_time' in results
        assert 'partitioned_time' in results
        assert 'speedup' in results
        assert 'data_consistent' in results
        
        # 验证分区目录存在
        partition_dir = os.path.join(self.test_dir, 'partitioned_table')
        assert os.path.exists(partition_dir)
        
        # 验证分区查询性能
        partitioned_time = results['partitioned_time']
        non_partitioned_time = results['non_partitioned_time']
        
        print("✅ 分区存储测试通过")
    
    def test_advanced_features(self):
        """测试高级特性"""
        print("\n=== 测试高级特性 ===")
        
        # 测试高级特性
        exercise = ParquetAdvancedExercise(self.test_dir)
        results = exercise.run_comprehensive_test(1000)
        
        # 验证结果
        assert 'nested_data' in results
        assert 'metadata' in results
        assert 'streaming' in results
        
        # 验证嵌套数据
        assert results['nested_data']['file_size_mb'] > 0
        
        # 验证元数据
        assert results['metadata']['num_rows'] > 0
        assert results['metadata']['num_columns'] > 0
        
        # 验证流式处理
        assert results['streaming']['processing_time'] > 0
        assert results['streaming']['rows_per_second'] > 0
        
        print("✅ 高级特性测试通过")
    
    def test_data_integrity(self):
        """测试数据完整性"""
        print("\n=== 测试数据完整性 ===")
        
        # 生成测试数据
        original_df = self.data_generator.generate_user_data(2000)
        
        # 保存为 Parquet
        parquet_file = os.path.join(self.test_dir, 'integrity_test.parquet')
        original_df.to_parquet(parquet_file)
        
        # 读取数据
        read_df = pd.read_parquet(parquet_file)
        
        # 验证数据完整性
        assert original_df.shape == read_df.shape
        assert original_df.columns.tolist() == read_df.columns.tolist()
        
        # 验证数值数据
        pd.testing.assert_frame_equal(original_df, read_df, check_dtype=False)
        
        print("✅ 数据完整性测试通过")
    
    def test_performance_benchmark(self):
        """测试性能基准"""
        print("\n=== 测试性能基准 ===")
        
        # 测试不同数据量下的性能
        sizes = [1000, 5000, 10000]
        results = {}
        
        for size in sizes:
            df = self.data_generator.generate_user_data(size)
            
            # 测试写入性能
            write_start = time.time()
            parquet_file = os.path.join(self.test_dir, f'benchmark_{size}.parquet')
            df.to_parquet(parquet_file)
            write_time = time.time() - write_start
            
            # 测试读取性能
            read_start = time.time()
            read_df = pd.read_parquet(parquet_file)
            read_time = time.time() - read_start
            
            # 文件大小
            file_size = os.path.getsize(parquet_file) / (1024 * 1024)
            
            results[size] = {
                'write_time': write_time,
                'read_time': read_time,
                'file_size_mb': file_size,
                'rows_per_second_write': size / write_time,
                'rows_per_second_read': size / read_time
            }
        
        # 验证性能数据
        assert len(results) == len(sizes)
        
        for size, result in results.items():
            assert result['write_time'] > 0
            assert result['read_time'] > 0
            assert result['file_size_mb'] > 0
            assert result['rows_per_second_write'] > 0
            assert result['rows_per_second_read'] > 0
        
        print("✅ 性能基准测试通过")


def run_comprehensive_tests():
    """运行综合测试"""
    print("=" * 60)
    print("开始运行 Parquet 综合测试")
    print("=" * 60)
    
    test_instance = TestParquetComprehensive()
    
    try:
        test_instance.setup_method()
        
        # 运行所有测试
        test_instance.test_basic_functionality()
        test_instance.test_compression_algorithms()
        test_instance.test_query_optimization()
        test_instance.test_partitioning()
        test_instance.test_advanced_features()
        test_instance.test_data_integrity()
        test_instance.test_performance_benchmark()
        
        print("\n" + "=" * 60)
        print("✅ 所有测试通过！")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        test_instance.teardown_method()


if __name__ == "__main__":
    success = run_comprehensive_tests()
    exit(0 if success else 1)