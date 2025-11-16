"""
Parquet 文件格式实践练习项目

这个包提供了一系列用于学习和实践 Parquet 文件格式的工具和练习。

主要功能：
- Parquet 文件的基本读写操作
- 压缩算法性能比较
- 查询优化技术演示
- 分区存储实现
- 嵌套数据结构处理
"""

__version__ = "1.0.0"
__author__ = "Parquet Practice Project"
__email__ = "example@example.com"

from .basic_exercise import ParquetBasicExercise
from .compression_exercise import ParquetCompressionExercise
from .query_optimization_exercise import ParquetQueryOptimizationExercise
from .partitioning_exercise import ParquetPartitioningExercise
from .advanced_exercise import ParquetAdvancedExercise
from .utils import DataGenerator, PerformanceAnalyzer

__all__ = [
    'ParquetBasicExercise',
    'ParquetCompressionExercise', 
    'ParquetQueryOptimizationExercise',
    'ParquetPartitioningExercise',
    'ParquetAdvancedExercise',
    'DataGenerator',
    'PerformanceAnalyzer'
]