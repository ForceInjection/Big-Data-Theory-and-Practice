"""
高级特性测试模块

测试 Parquet 高级特性功能，包括嵌套数据结构、元数据操作和流式处理。
"""

import pytest
import os
import tempfile
import shutil
from pathlib import Path

from src.parquet_practice.exercises.advanced import ParquetAdvancedExercise


class TestParquetAdvancedExercise:
    """Parquet 高级特性测试类"""
    
    def setup_method(self):
        """测试设置"""
        # 创建临时目录
        self.test_dir = tempfile.mkdtemp(prefix="parquet_advanced_test_")
        self.exercise = ParquetAdvancedExercise(output_dir=self.test_dir)
    
    def teardown_method(self):
        """测试清理"""
        # 清理临时目录
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_nested_data_structures_basic(self):
        """测试基本嵌套数据结构功能"""
        # 执行嵌套数据结构测试
        result = self.exercise.test_nested_data_structures(num_records=100)
        
        # 验证结果
        assert 'file_size_mb' in result
        assert 'original_shape' in result
        assert 'read_shape' in result
        
        # 验证文件大小合理
        assert result['file_size_mb'] > 0
        
        # 验证数据形状一致
        assert result['original_shape'] == result['read_shape']
        
        # 验证文件存在
        nested_file = os.path.join(self.test_dir, 'nested_data.parquet')
        assert os.path.exists(nested_file)
    
    def test_nested_data_structures_large(self):
        """测试大数据量的嵌套数据结构"""
        # 执行嵌套数据结构测试（较大数据量）
        result = self.exercise.test_nested_data_structures(num_records=1000)
        
        # 验证结果
        assert result['file_size_mb'] > 0
        assert result['original_shape'][0] == 1000  # 1000 条记录
        assert result['original_shape'] == result['read_shape']
    
    def test_metadata_operations_basic(self):
        """测试基本元数据操作功能"""
        # 首先创建嵌套数据文件
        self.exercise.test_nested_data_structures(num_records=100)
        nested_file = os.path.join(self.test_dir, 'nested_data.parquet')
        
        # 执行元数据操作测试
        result = self.exercise.test_metadata_operations(nested_file)
        
        # 验证元数据信息
        assert 'num_rows' in result
        assert 'num_columns' in result
        assert 'num_row_groups' in result
        assert 'created_by' in result
        assert 'schema' in result
        assert 'column_stats' in result
        
        # 验证具体数值
        assert result['num_rows'] == 100
        assert result['num_columns'] > 0
        assert result['num_row_groups'] >= 1
        
        # 验证列统计信息
        assert len(result['column_stats']) == result['num_columns']
    
    def test_metadata_operations_invalid_file(self):
        """测试对无效文件的元数据操作"""
        # 创建不存在的文件路径
        invalid_file = os.path.join(self.test_dir, 'nonexistent.parquet')
        
        # 应该抛出异常
        with pytest.raises(Exception):
            self.exercise.test_metadata_operations(invalid_file)
    
    def test_streaming_processing_basic(self):
        """测试基本流式处理功能"""
        # 首先创建嵌套数据文件
        self.exercise.test_nested_data_structures(num_records=500)
        nested_file = os.path.join(self.test_dir, 'nested_data.parquet')
        
        # 执行流式处理测试
        result = self.exercise.test_streaming_processing(nested_file, batch_size=100)
        
        # 验证结果
        assert 'total_rows' in result
        assert 'processing_time' in result
        assert 'rows_per_second' in result
        
        # 验证具体数值
        assert result['total_rows'] == 500
        assert result['processing_time'] > 0
        assert result['rows_per_second'] > 0
    
    def test_streaming_processing_small_batch(self):
        """测试小批量流式处理"""
        # 首先创建嵌套数据文件
        self.exercise.test_nested_data_structures(num_records=300)
        nested_file = os.path.join(self.test_dir, 'nested_data.parquet')
        
        # 执行流式处理测试（小批量）
        result = self.exercise.test_streaming_processing(nested_file, batch_size=50)
        
        # 验证结果
        assert result['total_rows'] == 300
        assert result['processing_time'] > 0
        # 小批量应该仍然有合理的处理速度
        assert result['rows_per_second'] > 10
    
    def test_streaming_processing_large_batch(self):
        """测试大批量流式处理"""
        # 首先创建嵌套数据文件
        self.exercise.test_nested_data_structures(num_records=1000)
        nested_file = os.path.join(self.test_dir, 'nested_data.parquet')
        
        # 执行流式处理测试（大批量）
        result = self.exercise.test_streaming_processing(nested_file, batch_size=500)
        
        # 验证结果
        assert result['total_rows'] == 1000
        assert result['processing_time'] > 0
        # 大批量应该比小批量更快
        assert result['rows_per_second'] > 50
    
    def test_comprehensive_test_basic(self):
        """测试综合测试功能"""
        # 执行综合测试
        result = self.exercise.run_comprehensive_test(num_records=200)
        
        # 验证结果包含所有测试部分
        assert 'nested_data' in result
        assert 'metadata' in result
        assert 'streaming' in result
        
        # 验证嵌套数据结果
        assert result['nested_data']['file_size_mb'] > 0
        assert result['nested_data']['original_shape'][0] == 200
        
        # 验证元数据结果
        assert result['metadata']['num_rows'] == 200
        
        # 验证流式处理结果
        assert result['streaming']['total_rows'] == 200
        assert result['streaming']['processing_time'] > 0
        
        # 验证结果文件存在
        results_file = os.path.join(self.test_dir, 'advanced_test_results.json')
        assert os.path.exists(results_file)
    
    def test_comprehensive_test_large(self):
        """测试大数据量的综合测试"""
        # 执行综合测试（较大数据量）
        result = self.exercise.run_comprehensive_test(num_records=1000)
        
        # 验证结果
        assert result['nested_data']['original_shape'][0] == 1000
        assert result['metadata']['num_rows'] == 1000
        assert result['streaming']['total_rows'] == 1000
        
        # 验证流式处理性能
        assert result['streaming']['rows_per_second'] > 0
    
    def test_cleanup(self):
        """测试清理功能"""
        # 首先创建一些文件
        self.exercise.run_comprehensive_test(num_records=100)
        
        # 验证文件存在
        nested_file = os.path.join(self.test_dir, 'nested_data.parquet')
        results_file = os.path.join(self.test_dir, 'advanced_test_results.json')
        assert os.path.exists(nested_file)
        assert os.path.exists(results_file)
        
        # 执行清理
        self.exercise.cleanup()
        
        # 验证文件已被清理
        assert not os.path.exists(nested_file)
        assert not os.path.exists(results_file)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])