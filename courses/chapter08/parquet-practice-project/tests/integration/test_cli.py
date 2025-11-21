"""
CLI 接口测试模块

测试 Parquet 实践项目的命令行接口功能。
"""

import pytest
import os
import sys
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from parquet_practice.cli.main import ParquetPracticeRunner, main


class TestParquetPracticeRunner:
    """Parquet 实践运行器测试类"""
    
    def setup_method(self):
        """测试设置"""
        # 创建临时目录
        self.test_dir = tempfile.mkdtemp(prefix="parquet_cli_test_")
        self.runner = ParquetPracticeRunner(output_dir=self.test_dir)
    
    def teardown_method(self):
        """测试清理"""
        # 清理临时目录
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_runner_initialization(self):
        """测试运行器初始化"""
        assert self.runner.output_dir == self.test_dir
        assert os.path.exists(self.test_dir)
        assert hasattr(self.runner, 'data_generator')
    
    @patch('parquet_practice.cli.main.ParquetBasicExercise')
    def test_run_basic_exercise(self, mock_basic_exercise):
        """测试基础练习运行"""
        # 设置模拟
        mock_df = MagicMock()
        
        # Mock data_generator 的 generate_user_data 方法
        with patch.object(self.runner.data_generator, 'generate_user_data', return_value=mock_df) as mock_generate:
            mock_instance = MagicMock()
            mock_instance.run_basic_exercise.return_value = {'test': 'result'}
            mock_basic_exercise.return_value = mock_instance
            
            # 执行测试
            result = self.runner.run_basic_exercise(100)
            
            # 验证调用
            mock_generate.assert_called_once_with(100)
            mock_basic_exercise.assert_called_once_with(num_records=100, output_dir=self.test_dir)
            mock_instance.run_basic_exercise.assert_called_once()
            mock_instance.cleanup.assert_called_once()
            assert result == {'test': 'result'}
    
    @patch('parquet_practice.cli.main.ParquetCompressionExercise')
    def test_run_compression_exercise(self, mock_compression_exercise):
        """测试压缩练习运行"""
        # 设置模拟
        mock_df = MagicMock()
        
        # Mock data_generator 的 generate_user_data 方法
        with patch.object(self.runner.data_generator, 'generate_user_data', return_value=mock_df) as mock_generate:
            mock_instance = MagicMock()
            mock_instance.run_compression_exercise.return_value = {'compression': 'result'}
            mock_compression_exercise.return_value = mock_instance
            
            # 执行测试
            result = self.runner.run_compression_exercise(200)
            
            # 验证调用
            mock_generate.assert_called_once_with(200)
            mock_compression_exercise.assert_called_once_with(mock_df, self.test_dir)
            mock_instance.run_compression_exercise.assert_called_once()
            mock_instance.cleanup.assert_called_once()
            assert result == {'compression': 'result'}
    
    @patch('parquet_practice.cli.main.ParquetQueryOptimizationExercise')
    def test_run_query_optimization_exercise(self, mock_query_exercise):
        """测试查询优化练习运行"""
        # 设置模拟
        mock_df = MagicMock()
        
        # Mock data_generator 的 generate_user_data 方法
        with patch.object(self.runner.data_generator, 'generate_user_data', return_value=mock_df) as mock_generate:
            mock_instance = MagicMock()
            mock_instance.run_optimization_exercise.return_value = {'query': 'result'}
            mock_query_exercise.return_value = mock_instance
            
            # 执行测试
            result = self.runner.run_query_optimization_exercise(300)
            
            # 验证调用
            mock_generate.assert_called_once_with(300)
            mock_query_exercise.assert_called_once_with(mock_df, self.test_dir)
            mock_instance.run_optimization_exercise.assert_called_once()
            mock_instance.cleanup.assert_called_once()
            assert result == {'query': 'result'}
    
    @patch('parquet_practice.cli.main.ParquetPartitioningExercise')
    @patch('parquet_practice.cli.main.DataGenerator')
    @patch('parquet_practice.cli.main.PerformanceAnalyzer')
    def test_run_partitioning_exercise(self, mock_analyzer, mock_data_gen, mock_partition_exercise):
        """测试分区练习运行"""
        # 设置模拟
        mock_df = MagicMock()
        mock_data_gen_instance = MagicMock()
        mock_data_gen_instance.generate_user_data.return_value = mock_df
        mock_data_gen.return_value = mock_data_gen_instance
        
        mock_instance = MagicMock()
        mock_instance.run_partitioning_exercise.return_value = {'partition': 'result'}
        mock_partition_exercise.return_value = mock_instance
        
        mock_analyzer_instance = MagicMock()
        mock_analyzer.return_value = mock_analyzer_instance
        
        # 执行测试
        result = self.runner.run_partitioning_exercise(400)
        
        # 验证调用
        mock_data_gen_instance.generate_user_data.assert_called_once_with(400)
        mock_partition_exercise.assert_called_once_with(mock_df, self.test_dir)
        mock_instance.run_partitioning_exercise.assert_called_once()
        mock_analyzer_instance.compare_performance.assert_called_once()
        mock_instance.cleanup.assert_called_once()
        assert result == {'partition': 'result'}
    
    @patch('parquet_practice.cli.main.ParquetAdvancedExercise')
    def test_run_advanced_exercise(self, mock_advanced_exercise):
        """测试高级特性练习运行"""
        # 设置模拟
        mock_instance = MagicMock()
        mock_instance.run_advanced_exercise.return_value = {'advanced': 'result'}
        mock_advanced_exercise.return_value = mock_instance
        
        # 执行测试
        result = self.runner.run_advanced_exercise()
        
        # 验证调用
        mock_advanced_exercise.assert_called_once_with(self.test_dir)
        mock_instance.run_advanced_exercise.assert_called_once()
        mock_instance.cleanup.assert_called_once()
        assert result == {'advanced': 'result'}
    
    @patch('parquet_practice.cli.main.ParquetPracticeRunner.run_basic_exercise')
    @patch('parquet_practice.cli.main.ParquetPracticeRunner.run_compression_exercise')
    @patch('parquet_practice.cli.main.ParquetPracticeRunner.run_query_optimization_exercise')
    @patch('parquet_practice.cli.main.ParquetPracticeRunner.run_partitioning_exercise')
    @patch('parquet_practice.cli.main.ParquetPracticeRunner.run_advanced_exercise')
    @patch('parquet_practice.cli.main.ParquetPracticeRunner.display_final_summary')
    def test_run_all_exercises(self, mock_display_summary, mock_advanced, mock_partition, mock_query, mock_compression, mock_basic):
        """测试运行所有练习"""
        # 设置模拟返回值
        mock_basic.return_value = {'basic': 'result'}
        mock_compression.return_value = {'compression': 'result'}
        mock_query.return_value = {'query': 'result'}
        mock_partition.return_value = {'partition': 'result'}
        mock_advanced.return_value = {'advanced': 'result'}
        
        # 执行测试
        result = self.runner.run_all_exercises(500)
        
        # 验证调用
        mock_basic.assert_called_once_with(500)
        mock_compression.assert_called_once_with(500)
        mock_query.assert_called_once_with(500)
        mock_partition.assert_called_once_with(500)
        mock_advanced.assert_called_once()
        
        # 验证总结显示被调用
        expected_results = {
            'basic': {'basic': 'result'},
            'compression': {'compression': 'result'},
            'query_optimization': {'query': 'result'},
            'partitioning': {'partition': 'result'},
            'advanced': {'advanced': 'result'}
        }
        mock_display_summary.assert_called_once_with(expected_results)
        
        # 验证方法返回 None（不返回结果）
        assert result is None
    
    @patch('builtins.input', side_effect=['1000'])
    def test_get_record_count_valid(self, mock_input):
        """测试获取有效的记录数量"""
        result = self.runner.get_record_count()
        assert result == 1000
    
    @patch('builtins.input', side_effect=[''])
    def test_get_record_count_default(self, mock_input):
        """测试获取默认记录数量"""
        result = self.runner.get_record_count()
        assert result == 10000
    
    @patch('builtins.input', side_effect=['invalid', '500'])
    def test_get_record_count_invalid_then_valid(self, mock_input):
        """测试先无效后有效的记录数量输入"""
        result = self.runner.get_record_count()
        assert result == 500
    
    @patch('builtins.input', side_effect=['-100', '200'])
    def test_get_record_count_negative_then_valid(self, mock_input):
        """测试先负数后有效的记录数量输入"""
        result = self.runner.get_record_count()
        assert result == 200


class TestCLIArgumentParsing:
    """CLI 参数解析测试类"""
    
    def test_help_argument(self):
        """测试帮助参数"""
        with patch('sys.argv', ['main.py', '--help']):
            with pytest.raises(SystemExit):
                main()
    
    @patch('parquet_practice.cli.main.ParquetPracticeRunner.run_interactive')
    def test_interactive_mode(self, mock_run_interactive):
        """测试交互式模式"""
        with patch('sys.argv', ['main.py', '--interactive']):
            main()
            mock_run_interactive.assert_called_once()
    
    @patch('parquet_practice.cli.main.ParquetPracticeRunner.run_basic_exercise')
    def test_basic_exercise_mode(self, mock_run_basic):
        """测试基础练习模式"""
        with patch('sys.argv', ['main.py', '--exercise', 'basic', '--records', '1000']):
            main()
            mock_run_basic.assert_called_once_with(1000)
    
    @patch('parquet_practice.cli.main.ParquetPracticeRunner.run_compression_exercise')
    def test_compression_exercise_mode(self, mock_run_compression):
        """测试压缩练习模式"""
        with patch('sys.argv', ['main.py', '--exercise', 'compression', '--records', '2000']):
            main()
            mock_run_compression.assert_called_once_with(2000)
    
    @patch('parquet_practice.cli.main.ParquetPracticeRunner.run_query_optimization_exercise')
    def test_query_exercise_mode(self, mock_run_query):
        """测试查询优化练习模式"""
        with patch('sys.argv', ['main.py', '--exercise', 'query', '--records', '3000']):
            main()
            mock_run_query.assert_called_once_with(3000)
    
    @patch('parquet_practice.cli.main.ParquetPracticeRunner.run_partitioning_exercise')
    def test_partition_exercise_mode(self, mock_run_partition):
        """测试分区练习模式"""
        with patch('sys.argv', ['main.py', '--exercise', 'partition', '--records', '4000']):
            main()
            mock_run_partition.assert_called_once_with(4000)
    
    @patch('parquet_practice.cli.main.ParquetPracticeRunner.run_advanced_exercise')
    def test_advanced_exercise_mode(self, mock_run_advanced):
        """测试高级特性练习模式"""
        with patch('sys.argv', ['main.py', '--exercise', 'advanced']):
            main()
            mock_run_advanced.assert_called_once()
    
    @patch('parquet_practice.cli.main.ParquetPracticeRunner.run_all_exercises')
    def test_all_exercises_mode(self, mock_run_all):
        """测试所有练习模式"""
        with patch('sys.argv', ['main.py', '--exercise', 'all', '--records', '5000']):
            main()
            mock_run_all.assert_called_once_with(5000)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])