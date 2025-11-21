"""
查询优化测试模块
"""

import pytest
import pandas as pd
import tempfile
import os
import shutil
from pathlib import Path

# 添加项目根目录到 Python 路径
import sys
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from parquet_practice.exercises.query_optimization import ParquetQueryOptimizationExercise
from parquet_practice.utils import DataGenerator


class TestParquetQueryOptimizationExercise:
    """查询优化练习测试"""
    
    def setup_method(self):
        """测试设置"""
        self.test_dir = tempfile.mkdtemp(prefix="query_optimization_test_")
        self.data_generator = DataGenerator(seed=42)
        
    def teardown_method(self):
        """测试清理"""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_init(self):
        """测试初始化"""
        test_df = self.data_generator.generate_user_data(100)
        exercise = ParquetQueryOptimizationExercise(data_df=test_df, output_dir=self.test_dir)
        
        assert exercise.df.equals(test_df)
        assert exercise.output_dir == self.test_dir
        assert hasattr(exercise, 'performance_analyzer')
        assert os.path.exists(exercise.filename)
    
    def test_projection_pushdown_basic(self):
        """测试基本投影下推功能"""
        test_df = self.data_generator.generate_user_data(200)
        exercise = ParquetQueryOptimizationExercise(data_df=test_df, output_dir=self.test_dir)
        
        # 测试默认列选择
        result = exercise.test_projection_pushdown()
        
        assert 'all_columns_time' in result
        assert 'selected_columns_time' in result
        assert 'speedup' in result
        assert 'all_columns_count' in result
        assert 'selected_columns_count' in result
        assert 'memory_reduction_percent' in result
        
        # 验证性能提升
        assert result['selected_columns_time'] < result['all_columns_time']
        assert result['speedup'] > 1.0
        assert result['selected_columns_count'] < result['all_columns_count']
    
    def test_projection_pushdown_custom_columns(self):
        """测试自定义列选择的投影下推"""
        test_df = self.data_generator.generate_user_data(150)
        exercise = ParquetQueryOptimizationExercise(data_df=test_df, output_dir=self.test_dir)
        
        custom_columns = ['UserID', 'Age', 'Income']
        result = exercise.test_projection_pushdown(selected_columns=custom_columns)
        
        assert result['selected_columns_count'] == len(custom_columns)
        assert result['memory_reduction_percent'] > 0
    
    def test_predicate_pushdown_basic(self):
        """测试基本谓词下推功能"""
        test_df = self.data_generator.generate_user_data(300)
        exercise = ParquetQueryOptimizationExercise(data_df=test_df, output_dir=self.test_dir)
        
        result = exercise.test_predicate_pushdown()
        
        assert 'memory_filter_time' in result
        assert 'parquet_filter_time' in result
        assert 'speedup' in result
        assert 'filtered_rows' in result
        assert 'original_rows' in result
        assert 'data_reduction_percent' in result
        
        # 验证过滤效果
        assert result['filtered_rows'] < result['original_rows']
        assert result['data_reduction_percent'] > 0
    
    def test_predicate_pushdown_custom_filters(self):
        """测试自定义过滤条件的谓词下推"""
        test_df = self.data_generator.generate_user_data(250)
        exercise = ParquetQueryOptimizationExercise(data_df=test_df, output_dir=self.test_dir)
        
        custom_filters = [('Income', '>', 50000)]
        result = exercise.test_predicate_pushdown(filters=custom_filters)
        
        assert result['filtered_rows'] > 0
        assert result['data_reduction_percent'] > 0
    
    def test_combined_optimization(self):
        """测试组合优化功能"""
        test_df = self.data_generator.generate_user_data(400)
        exercise = ParquetQueryOptimizationExercise(data_df=test_df, output_dir=self.test_dir)
        
        result = exercise.test_combined_optimization()
        
        assert 'optimized_time' in result
        assert 'full_scan_time' in result
        assert 'speedup' in result
        assert 'result_rows' in result
        assert 'result_columns' in result
        
        # 验证组合优化效果（由于数据量较小，性能提升可能不明显）
        assert result['result_rows'] > 0
        assert result['result_columns'] > 0
        # 对于小数据量，组合优化可能不如全表扫描快，但结果应该正确
    
    def test_complex_queries(self):
        """测试复杂查询场景"""
        test_df = self.data_generator.generate_user_data(500)
        exercise = ParquetQueryOptimizationExercise(data_df=test_df, output_dir=self.test_dir)
        
        result = exercise.test_complex_queries()
        
        assert 'range_query' in result
        assert 'multi_condition_query' in result
        assert 'in_query' in result
        
        # 验证每个查询场景都有时间记录
        assert 'time' in result['range_query']
        assert 'time' in result['multi_condition_query']
        assert 'time' in result['in_query']
    
    def test_run_optimization_exercise(self):
        """测试完整优化练习运行"""
        test_df = self.data_generator.generate_user_data(100)
        exercise = ParquetQueryOptimizationExercise(data_df=test_df, output_dir=self.test_dir)
        
        result = exercise.run_optimization_exercise()
        
        assert 'projection' in result
        assert 'predicate' in result
        assert 'combined' in result
        assert 'complex' in result
        
        # 验证结果文件存在
        results_file = os.path.join(self.test_dir, 'query_optimization_results.json')
        assert os.path.exists(results_file)
    
    def test_cleanup(self):
        """测试清理功能"""
        test_df = self.data_generator.generate_user_data(50)
        exercise = ParquetQueryOptimizationExercise(data_df=test_df, output_dir=self.test_dir)
        
        # 确保文件存在
        assert os.path.exists(exercise.filename)
        
        # 执行清理
        exercise.cleanup()
        
        # 验证文件已清理
        assert not os.path.exists(exercise.filename)