"""
Parquet 基础练习模块

提供 Parquet 文件的基本读写操作练习。
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import time
import os
from typing import Dict, Any, Tuple

from .utils import DataGenerator, PerformanceAnalyzer, verify_data_integrity


class ParquetBasicExercise:
    """Parquet 基础练习类"""
    
    def __init__(self, num_records: int = 100000, output_dir: str = "output"):
        """
        初始化基础练习
        
        Args:
            num_records: 记录数量
            output_dir: 输出目录
        """
        self.num_records = num_records
        self.output_dir = output_dir
        self.data_generator = DataGenerator()
        self.performance_analyzer = PerformanceAnalyzer()
        self.df = None
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
    def generate_sample_data(self) -> pd.DataFrame:
        """
        生成示例数据
        
        Returns:
            生成的 DataFrame
        """
        self.df = self.data_generator.generate_user_data(self.num_records)
        return self.df
    
    def save_to_parquet(self, filename: str = None) -> Tuple[float, float]:
        """
        保存数据为 Parquet 格式
        
        Args:
            filename: 文件名，默认为 sample_data.parquet
            
        Returns:
            (保存时间, 文件大小MB)
        """
        if filename is None:
            filename = os.path.join(self.output_dir, 'sample_data.parquet')
        
        print(f"正在保存数据到 {filename}...")
        
        # Convert to PyArrow table
        table = pa.Table.from_pandas(self.df)
        
        # Measure save time
        _, save_time = self.performance_analyzer.measure_time(
            pq.write_table, table, filename
        )
        
        # Get file size
        file_size = self.performance_analyzer.get_file_size(filename)
        
        print(f"Parquet 文件保存成功！")
        print(f"保存时间：{save_time:.2f} 秒")
        print(f"文件大小：{file_size:.2f} MB")
        
        return save_time, file_size
    
    def save_to_csv(self, filename: str = None) -> Tuple[float, float]:
        """
        保存数据为 CSV 格式（用于对比）
        
        Args:
            filename: 文件名，默认为 sample_data.csv
            
        Returns:
            (保存时间, 文件大小MB)
        """
        if filename is None:
            filename = os.path.join(self.output_dir, 'sample_data.csv')
        
        print(f"正在保存数据到 {filename}...")
        
        # 测量保存时间
        _, save_time = self.performance_analyzer.measure_time(
            self.df.to_csv, filename, index=False
        )
        
        # 获取文件大小
        file_size = self.performance_analyzer.get_file_size(filename)
        
        print(f"CSV 文件保存完成！")
        print(f"保存时间: {save_time:.2f} 秒")
        print(f"文件大小: {file_size:.2f} MB")
        
        return save_time, file_size
    
    def read_from_parquet(self, filename: str = None) -> Tuple[pd.DataFrame, float]:
        """
        从 Parquet 文件读取数据
        
        Args:
            filename: 文件名
            
        Returns:
            (读取的 DataFrame, 读取时间)
        """
        if filename is None:
            filename = os.path.join(self.output_dir, 'sample_data.parquet')
        
        print(f"正在从 {filename} 读取数据...")
        
        def read_parquet():
            table = pq.read_table(filename)
            return table.to_pandas()
        
        df_read, read_time = self.performance_analyzer.measure_time(read_parquet)
        
        print(f"数据读取成功！")
        print(f"读取时间：{read_time:.2f} 秒")
        print(f"数据形状：{df_read.shape}")
        
        # Verify data integrity
        if verify_data_integrity(self.df, df_read):
            print("✅ 数据完整性验证通过")
        else:
            print("❌ 数据完整性验证失败")
        
        return df_read, read_time
    
    def read_from_csv(self, filename: str = None) -> Tuple[pd.DataFrame, float]:
        """
        从 CSV 文件读取数据（用于对比）
        
        Args:
            filename: 文件名
            
        Returns:
            (读取的 DataFrame, 读取时间)
        """
        if filename is None:
            filename = os.path.join(self.output_dir, 'sample_data.csv')
        
        print(f"正在从 {filename} 读取数据...")
        
        df_read, read_time = self.performance_analyzer.measure_time(
            pd.read_csv, filename
        )
        
        print(f"CSV 文件读取完成！")
        print(f"读取时间: {read_time:.2f} 秒")
        print(f"读取行数: {len(df_read)}")
        
        return df_read, read_time
    
    def run_basic_exercise(self) -> Dict[str, Any]:
        """
        运行基础练习
        
        Returns:
            练习结果字典
        """
        print("=" * 60)
        print("开始 Parquet 基础练习")
        print("=" * 60)
        
        # 1. Generate data
        if self.df is None:
            self.generate_sample_data()
        
        # 2. Save as Parquet and CSV
        parquet_save_time, parquet_size = self.save_to_parquet()
        csv_save_time, csv_size = self.save_to_csv()
        
        # 3. Read data
        df_parquet, parquet_read_time = self.read_from_parquet()
        df_csv, csv_read_time = self.read_from_csv()
        
        # 4. Verify data integrity
        parquet_integrity = verify_data_integrity(self.df, df_parquet)
        csv_integrity = verify_data_integrity(self.df, df_csv)
        
        # 5. Performance comparison
        results = {
            'Parquet': {
                'save_time': parquet_save_time,
                'read_time': parquet_read_time,
                'file_size': parquet_size,
                'integrity': parquet_integrity
            },
            'CSV': {
                'save_time': csv_save_time,
                'read_time': csv_read_time,
                'file_size': csv_size,
                'integrity': csv_integrity
            }
        }
        
        # Display comparison results
        self.performance_analyzer.compare_performance(
            {k: {key: val for key, val in v.items() if key != 'integrity'} 
             for k, v in results.items()},
            "Parquet vs CSV Performance Comparison"
        )
        
        # Calculate compression ratio
        compression_ratio = csv_size / parquet_size if parquet_size > 0 else 0
        print(f"\n压缩比：{compression_ratio:.2f}x")
        print(f"节省存储空间：{((csv_size - parquet_size) / csv_size * 100):.1f}%")
        
        # Save results
        results_file = os.path.join(self.output_dir, 'basic_exercise_results.json')
        self.performance_analyzer.save_results(results, results_file)
        
        return results
    
    def cleanup(self):
        """清理临时文件"""
        from .utils import cleanup_files
        patterns = [
            os.path.join(self.output_dir, 'sample_data.*')
        ]
        cleanup_files(patterns)