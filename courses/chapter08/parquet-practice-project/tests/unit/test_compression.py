"""
压缩算法测试模块
"""

import pandas as pd
import tempfile
import os
import shutil
from pathlib import Path

# 添加项目根目录到 Python 路径
import sys
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from parquet_practice.exercises.compression import ParquetCompressionExercise
from parquet_practice.utils import DataGenerator


class TestParquetCompressionExercise:
    """压缩算法练习测试"""
    
    def setup_method(self):
        """测试设置"""
        self.test_dir = tempfile.mkdtemp(prefix="compression_test_")
        self.data_generator = DataGenerator(seed=42)
        
    def teardown_method(self):
        """测试清理"""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_init(self):
        """测试初始化"""
        # 生成测试数据
        test_data = self.data_generator.generate_user_data(1000)
        exercise = ParquetCompressionExercise(test_data, output_dir=self.test_dir)
        assert exercise.output_dir == self.test_dir
        assert hasattr(exercise, 'performance_analyzer')
    
    def test_generate_test_data(self):
        """测试数据生成"""
        # 生成测试数据
        test_data = self.data_generator.generate_user_data(500)
        exercise = ParquetCompressionExercise(test_data, output_dir=self.test_dir)
        
        # 验证数据
        assert isinstance(exercise.df, pd.DataFrame)
        assert len(exercise.df) == 500
        assert len(exercise.df.columns) == 6  # UserID, Username, Age, City, RegisterTime, Income
    
    def test_test_compression_algorithms(self):
        """测试压缩算法比较"""
        # 生成测试数据
        test_data = self.data_generator.generate_user_data(100)
        exercise = ParquetCompressionExercise(test_data, output_dir=self.test_dir)
        results = exercise.test_compression_algorithms()
        
        # 检查结果结构
        assert isinstance(results, dict)
        expected_algorithms = ['SNAPPY', 'GZIP', 'LZ4', 'BROTLI', 'NONE']
        for algo in expected_algorithms:
            assert algo in results
            assert 'file_size' in results[algo]
            assert 'write_time' in results[algo]
            assert 'read_time' in results[algo]
        
        # 检查文件是否创建
        for algo in expected_algorithms:
            file_path = os.path.join(self.test_dir, f'data_{algo.lower()}.parquet')
            assert os.path.exists(file_path)
    
    def test_analyze_compression_tradeoffs(self):
        """测试压缩权衡分析"""
        # 生成测试数据
        test_data = self.data_generator.generate_user_data(100)
        exercise = ParquetCompressionExercise(test_data, output_dir=self.test_dir)
        
        # 先生成测试数据（包含 compression_ratio 字段）
        test_results = {
            'SNAPPY': {'file_size': 0.1, 'write_time': 0.05, 'read_time': 0.02, 'compression_ratio': 1.5},
            'GZIP': {'file_size': 0.08, 'write_time': 0.08, 'read_time': 0.03, 'compression_ratio': 1.88},
            'LZ4': {'file_size': 0.12, 'write_time': 0.04, 'read_time': 0.02, 'compression_ratio': 1.25},
            'BROTLI': {'file_size': 0.07, 'write_time': 0.10, 'read_time': 0.04, 'compression_ratio': 2.14},
            'NONE': {'file_size': 0.15, 'write_time': 0.03, 'read_time': 0.01, 'compression_ratio': 1.0}
        }
        
        # 注意：analyze_compression_tradeoffs 方法不返回结果，只打印分析信息
        # 我们只需要验证方法可以正常调用而不出错
        try:
            exercise.analyze_compression_tradeoffs(test_results)
            # 如果方法执行没有抛出异常，测试就通过了
            assert True
        except Exception as e:
            assert False, f"analyze_compression_tradeoffs 方法执行失败: {e}"
    
    def test_run_compression_exercise(self):
        """测试完整压缩练习"""
        # 生成测试数据
        test_data = self.data_generator.generate_user_data(200)
        exercise = ParquetCompressionExercise(test_data, output_dir=self.test_dir)
        results = exercise.run_compression_exercise()
        
        # 检查返回结果结构
        assert isinstance(results, dict)
        expected_algorithms = ['SNAPPY', 'GZIP', 'LZ4', 'BROTLI', 'NONE']
        for algo in expected_algorithms:
            assert algo in results
            assert 'file_size' in results[algo]
            assert 'write_time' in results[algo]
            assert 'read_time' in results[algo]
        
        # 检查结果文件是否创建
        results_file = os.path.join(self.test_dir, 'compression_results.json')
        assert os.path.exists(results_file)
    
    def test_cleanup(self):
        """测试清理功能"""
        # 生成测试数据
        test_data = self.data_generator.generate_user_data(50)
        exercise = ParquetCompressionExercise(test_data, output_dir=self.test_dir)
        
        # 先运行练习创建文件
        exercise.run_compression_exercise()
        
        # 检查文件存在
        parquet_files = [f for f in os.listdir(self.test_dir) if f.endswith('.parquet')]
        assert len(parquet_files) > 0
        
        # 执行清理
        exercise.cleanup()
        
        # 检查文件是否被清理
        parquet_files_after = [f for f in os.listdir(self.test_dir) if f.endswith('.parquet')]
        assert len(parquet_files_after) == 0