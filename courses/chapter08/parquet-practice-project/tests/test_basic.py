"""
基础功能测试模块
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path

# 添加项目根目录到 Python 路径
import sys
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from parquet_practice import (
    DataGenerator,
    PerformanceAnalyzer,
    ParquetBasicExercise,
    verify_data_integrity,
    cleanup_files
)


class TestDataGenerator:
    """数据生成器测试"""
    
    def test_init_with_seed(self):
        """测试带种子的初始化"""
        generator = DataGenerator(seed=42)
        assert generator is not None
    
    def test_init_without_seed(self):
        """测试不带种子的初始化"""
        generator = DataGenerator()
        assert generator is not None
    
    def test_generate_user_data_default(self):
        """测试默认参数生成用户数据"""
        generator = DataGenerator(seed=42)
        df = generator.generate_user_data()
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10000  # 默认记录数
        assert len(df.columns) == 8  # 预期列数
        
        # 检查必要的列
        expected_columns = ['user_id', 'name', 'email', 'age', 'salary', 'department', 'join_date', 'is_active']
        assert all(col in df.columns for col in expected_columns)
    
    def test_generate_user_data_custom_records(self):
        """测试自定义记录数"""
        generator = DataGenerator(seed=42)
        df = generator.generate_user_data(records=1000)
        
        assert len(df) == 1000
    
    def test_generate_user_data_with_nulls(self):
        """测试包含空值的数据生成"""
        generator = DataGenerator(seed=42)
        df = generator.generate_user_data(records=1000, include_nulls=True, null_probability=0.1)
        
        # 检查是否有空值
        has_nulls = df.isnull().any().any()
        assert has_nulls
    
    def test_generate_user_data_without_nulls(self):
        """测试不包含空值的数据生成"""
        generator = DataGenerator(seed=42)
        df = generator.generate_user_data(records=1000, include_nulls=False)
        
        # 检查是否没有空值
        has_nulls = df.isnull().any().any()
        assert not has_nulls
    
    def test_generate_nested_data(self):
        """测试嵌套数据生成"""
        generator = DataGenerator(seed=42)
        df = generator.generate_nested_data(records=100)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 100
        
        # 检查嵌套列
        expected_columns = ['id', 'profile', 'tags', 'metrics']
        assert all(col in df.columns for col in expected_columns)
    
    def test_reproducibility_with_seed(self):
        """测试使用种子的可重现性"""
        generator1 = DataGenerator(seed=42)
        generator2 = DataGenerator(seed=42)
        
        df1 = generator1.generate_user_data(records=100)
        df2 = generator2.generate_user_data(records=100)
        
        pd.testing.assert_frame_equal(df1, df2)


class TestPerformanceAnalyzer:
    """性能分析器测试"""
    
    def test_init(self):
        """测试初始化"""
        analyzer = PerformanceAnalyzer()
        assert analyzer is not None
    
    def test_measure_time(self):
        """测试时间测量"""
        analyzer = PerformanceAnalyzer()
        
        with analyzer.measure_time("test_operation"):
            # 模拟一些操作
            sum(range(1000))
        
        # 检查是否记录了时间
        assert hasattr(analyzer, 'times')
        assert 'test_operation' in analyzer.times
        assert analyzer.times['test_operation'] > 0
    
    def test_get_file_size_existing_file(self):
        """测试获取存在文件的大小"""
        analyzer = PerformanceAnalyzer()
        
        # 创建临时文件
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(b"test content")
            tmp_path = tmp.name
        
        try:
            size = analyzer.get_file_size(tmp_path)
            assert size > 0
        finally:
            os.unlink(tmp_path)
    
    def test_get_file_size_nonexistent_file(self):
        """测试获取不存在文件的大小"""
        analyzer = PerformanceAnalyzer()
        
        with pytest.raises(FileNotFoundError):
            analyzer.get_file_size("nonexistent_file.txt")
    
    def test_compare_performance(self):
        """测试性能对比"""
        analyzer = PerformanceAnalyzer()
        
        results = {
            "Format1": {"write_time": 1.0, "read_time": 0.5, "file_size": 1024},
            "Format2": {"write_time": 2.0, "read_time": 1.0, "file_size": 2048}
        }
        
        # 这个方法主要是打印输出，我们只测试它不会抛出异常
        try:
            analyzer.compare_performance(results)
        except Exception as e:
            pytest.fail(f"compare_performance raised an exception: {e}")
    
    def test_save_results_json(self):
        """测试保存结果为 JSON"""
        analyzer = PerformanceAnalyzer()
        
        results = {"test": "data", "number": 42}
        
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            analyzer.save_results(results, tmp_path)
            assert os.path.exists(tmp_path)
            assert os.path.getsize(tmp_path) > 0
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


class TestParquetBasicExercise:
    """Parquet 基础练习测试"""
    
    def setup_method(self):
        """测试前设置"""
        self.temp_dir = tempfile.mkdtemp()
        self.exercise = ParquetBasicExercise(output_dir=self.temp_dir)
    
    def teardown_method(self):
        """测试后清理"""
        # 清理临时文件
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_init(self):
        """测试初始化"""
        assert self.exercise is not None
        assert self.exercise.output_dir == self.temp_dir
    
    def test_run_exercise_with_generated_data(self):
        """测试使用生成数据运行练习"""
        results = self.exercise.run_exercise(records=100)
        
        assert isinstance(results, dict)
        assert 'performance' in results
        assert 'compression_ratio' in results
        assert 'speed_improvement' in results
        assert 'data_integrity' in results
        
        # 检查性能数据结构
        performance = results['performance']
        assert 'parquet' in performance
        assert 'csv' in performance
        
        for format_data in performance.values():
            assert 'write_time' in format_data
            assert 'read_time' in format_data
            assert 'file_size' in format_data
    
    def test_run_exercise_with_custom_data(self):
        """测试使用自定义数据运行练习"""
        # 创建自定义数据
        generator = DataGenerator(seed=42)
        custom_data = generator.generate_user_data(records=50)
        
        results = self.exercise.run_exercise(data=custom_data)
        
        assert isinstance(results, dict)
        assert results['data_integrity'] is True
    
    def test_run_exercise_data_integrity(self):
        """测试数据完整性验证"""
        results = self.exercise.run_exercise(records=50)
        
        # 数据完整性应该为 True
        assert results['data_integrity'] is True
    
    def test_run_exercise_compression_ratio(self):
        """测试压缩比计算"""
        results = self.exercise.run_exercise(records=100)
        
        compression_ratio = results['compression_ratio']
        assert isinstance(compression_ratio, float)
        assert compression_ratio > 0  # 压缩比应该大于 0


class TestUtilityFunctions:
    """工具函数测试"""
    
    def test_verify_data_integrity_identical_data(self):
        """测试相同数据的完整性验证"""
        generator = DataGenerator(seed=42)
        df1 = generator.generate_user_data(records=100)
        df2 = df1.copy()
        
        result = verify_data_integrity(df1, df2)
        assert result is True
    
    def test_verify_data_integrity_different_data(self):
        """测试不同数据的完整性验证"""
        generator = DataGenerator(seed=42)
        df1 = generator.generate_user_data(records=100)
        df2 = generator.generate_user_data(records=100)  # 不同的数据
        
        result = verify_data_integrity(df1, df2)
        assert result is False
    
    def test_verify_data_integrity_modified_data(self):
        """测试修改后数据的完整性验证"""
        generator = DataGenerator(seed=42)
        df1 = generator.generate_user_data(records=100)
        df2 = df1.copy()
        df2.iloc[0, 0] = "modified"  # 修改一个值
        
        result = verify_data_integrity(df1, df2)
        assert result is False
    
    def test_cleanup_files(self):
        """测试文件清理功能"""
        # 创建临时文件
        temp_files = []
        for i in range(3):
            with tempfile.NamedTemporaryFile(suffix=".test", delete=False) as tmp:
                temp_files.append(tmp.name)
        
        # 确认文件存在
        for file_path in temp_files:
            assert os.path.exists(file_path)
        
        # 清理文件
        cleanup_files(temp_files)
        
        # 确认文件已删除
        for file_path in temp_files:
            assert not os.path.exists(file_path)


class TestIntegration:
    """集成测试"""
    
    def test_full_workflow(self):
        """测试完整工作流程"""
        # 1. 生成数据
        generator = DataGenerator(seed=42)
        data = generator.generate_user_data(records=100)
        
        # 2. 创建临时目录
        temp_dir = tempfile.mkdtemp()
        
        try:
            # 3. 运行基础练习
            exercise = ParquetBasicExercise(output_dir=temp_dir)
            results = exercise.run_exercise(data=data)
            
            # 4. 验证结果
            assert isinstance(results, dict)
            assert results['data_integrity'] is True
            assert results['compression_ratio'] > 0
            
            # 5. 检查生成的文件
            parquet_file = os.path.join(temp_dir, "test_data.parquet")
            csv_file = os.path.join(temp_dir, "test_data.csv")
            
            # 文件应该存在（如果练习没有自动清理的话）
            # 这取决于具体的实现
            
        finally:
            # 清理临时目录
            import shutil
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])