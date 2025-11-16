"""
Parquet 分区练习模块

提供分区表的创建、查询和性能分析功能。
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import os
import shutil
from typing import Dict, Any, List, Optional

from .utils import PerformanceAnalyzer


class ParquetPartitioningExercise:
    """Parquet 分区练习类"""
    
    def __init__(self, data_df: pd.DataFrame, output_dir: str = "output"):
        """
        初始化分区练习
        
        Args:
            data_df: 要测试的数据
            output_dir: 输出目录
        """
        self.df = data_df
        self.output_dir = output_dir
        self.performance_analyzer = PerformanceAnalyzer()
        
        # 分区和非分区表的路径
        self.non_partitioned_path = os.path.join(output_dir, 'non_partitioned.parquet')
        self.partitioned_path = os.path.join(output_dir, 'partitioned_table')
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
    
    def create_non_partitioned_table(self) -> None:
        """创建非分区表"""
        print("创建非分区表...")
        table = pa.Table.from_pandas(self.df)
        pq.write_table(table, self.non_partitioned_path)
        
        file_size = self.performance_analyzer.get_file_size(self.non_partitioned_path)
        print(f"非分区表已创建: {self.non_partitioned_path}")
        print(f"文件大小: {file_size:.2f} MB")
    
    def create_partitioned_table(self, partition_cols: List[str] = None) -> None:
        """
        创建分区表
        
        Args:
            partition_cols: 分区列，默认按城市分区
        """
        if partition_cols is None:
            partition_cols = ['City']
        
        print(f"创建分区表（按 {', '.join(partition_cols)} 分区）...")
        
        # 清理已存在的分区表
        if os.path.exists(self.partitioned_path):
            shutil.rmtree(self.partitioned_path)
        
        table = pa.Table.from_pandas(self.df)
        
        # 写入分区表 - 使用 dataset API 保留分区列
        import pyarrow.dataset as ds
        partitioning = ds.partitioning(
            pa.schema([pa.field(col, pa.string()) for col in partition_cols]),
            flavor="hive"
        )
        ds.write_dataset(
            table,
            base_dir=self.partitioned_path,
            format="parquet",
            partitioning=partitioning
        )
        
        # 统计分区信息
        partition_info = self._analyze_partitions()
        print(f"分区表已创建: {self.partitioned_path}")
        print(f"分区数量: {partition_info['partition_count']}")
        print(f"总大小: {partition_info['total_size']:.2f} MB")
        
        return partition_info
    
    def _analyze_partitions(self) -> Dict[str, Any]:
        """分析分区信息"""
        partition_count = 0
        total_size = 0
        partition_details = []
        
        for root, dirs, files in os.walk(self.partitioned_path):
            for file in files:
                if file.endswith('.parquet'):
                    file_path = os.path.join(root, file)
                    size = self.performance_analyzer.get_file_size(file_path)
                    total_size += size
                    partition_count += 1
                    
                    # 提取分区信息
                    rel_path = os.path.relpath(root, self.partitioned_path)
                    partition_details.append({
                        'path': rel_path,
                        'file': file,
                        'size_mb': size
                    })
        
        return {
            'partition_count': partition_count,
            'total_size': total_size,
            'partitions': partition_details
        }
    
    def test_partition_pruning(self, filter_city: str = "Beijing") -> Dict[str, Any]:
        """
        测试分区裁剪
        
        Args:
            filter_city: 要过滤的城市
            
        Returns:
            分区裁剪测试结果
        """
        print("=" * 60)
        print("测试分区裁剪")
        print("=" * 60)
        
        # 测试非分区表查询
        def query_non_partitioned():
            table = pq.read_table(self.non_partitioned_path)
            df = table.to_pandas()
            return df[df['City'] == filter_city]
        
        df_non_part, time_non_part = self.performance_analyzer.measure_time(query_non_partitioned)
        
        print(f"非分区表查询 (城市={filter_city}): {time_non_part:.4f} 秒")
        print(f"结果行数: {len(df_non_part)}")
        
        # 测试分区表查询
        def query_partitioned():
            import pyarrow.dataset as ds
            # 读取所有数据，分区信息会自动添加到结果中
            dataset = ds.dataset(self.partitioned_path, format="parquet")
            table = dataset.to_table()
            df = table.to_pandas()
            # 手动过滤，因为分区列在这种情况下不在 schema 中
            return df[df['City'] == filter_city] if 'City' in df.columns else df
        
        df_part, time_part = self.performance_analyzer.measure_time(query_partitioned)
        
        print(f"分区表查询 (城市={filter_city}): {time_part:.4f} 秒")
        print(f"结果行数: {len(df_part)}")
        
        speedup = time_non_part / time_part if time_part > 0 else 0
        print(f"性能提升: {speedup:.2f}x")
        
        # 验证结果一致性
        data_consistent = len(df_non_part) == len(df_part)
        print(f"数据一致性: {'✓' if data_consistent else '✗'}")
        
        return {
            'non_partitioned_time': time_non_part,
            'partitioned_time': time_part,
            'speedup': speedup,
            'result_rows': len(df_part),
            'data_consistent': data_consistent
        }
    
    def test_multiple_partition_queries(self) -> Dict[str, Any]:
        """
        测试多种分区查询场景
        
        Returns:
            多种查询场景的测试结果
        """
        print("\n" + "=" * 60)
        print("测试多种分区查询场景")
        print("=" * 60)
        
        results = {}
        
        # 获取所有城市
        cities = self.df['City'].unique()
        
        # 场景1：单分区查询
        print("场景1: 单分区查询")
        test_city = cities[0] if len(cities) > 0 else "Beijing"
        single_result = self.test_single_partition_query(test_city)
        results['single_partition'] = single_result
        
        # 场景2：多分区查询
        print("\n场景2: 多分区查询")
        test_cities = cities[:3] if len(cities) >= 3 else cities
        multi_result = self.test_multi_partition_query(test_cities)
        results['multi_partition'] = multi_result
        
        # 场景3：全表扫描
        print("\n场景3: 全表扫描")
        full_result = self.test_full_scan()
        results['full_scan'] = full_result
        
        return results
    
    def test_single_partition_query(self, city: str) -> Dict[str, Any]:
        """测试单分区查询"""
        def query_single_partition():
            import pyarrow.dataset as ds
            dataset = ds.dataset(self.partitioned_path, format="parquet")
            table = dataset.to_table()
            df = table.to_pandas()
            return df[df['City'] == city] if 'City' in df.columns else df
        
        df_result, query_time = self.performance_analyzer.measure_time(query_single_partition)
        
        print(f"查询城市 '{city}': {query_time:.4f} 秒, 结果: {len(df_result)} 行")
        
        return {
            'city': city,
            'time': query_time,
            'rows': len(df_result)
        }
    
    def test_multi_partition_query(self, cities: List[str]) -> Dict[str, Any]:
        """测试多分区查询"""
        def query_multi_partitions():
            import pyarrow.dataset as ds
            dataset = ds.dataset(self.partitioned_path, format="parquet")
            table = dataset.to_table()
            df = table.to_pandas()
            return df[df['City'].isin(cities)] if 'City' in df.columns else df
        
        df_result, query_time = self.performance_analyzer.measure_time(query_multi_partitions)
        
        print(f"查询城市 {cities}: {query_time:.4f} 秒, 结果: {len(df_result)} 行")
        
        return {
            'cities': cities,
            'time': query_time,
            'rows': len(df_result)
        }
    
    def test_full_scan(self) -> Dict[str, Any]:
        """测试全表扫描"""
        def query_full_table():
            dataset = pq.ParquetDataset(self.partitioned_path)
            table = dataset.read()
            return table.to_pandas()
        
        df_result, query_time = self.performance_analyzer.measure_time(query_full_table)
        
        print(f"全表扫描: {query_time:.4f} 秒, 结果: {len(df_result)} 行")
        
        return {
            'time': query_time,
            'rows': len(df_result)
        }
    
    def analyze_partition_distribution(self) -> Dict[str, Any]:
        """
        分析分区数据分布
        
        Returns:
            分区分布分析结果
        """
        print("\n" + "=" * 60)
        print("分析分区数据分布")
        print("=" * 60)
        
        # 统计每个城市的数据量
        city_counts = self.df['City'].value_counts()
        
        print("各城市数据分布:")
        for city, count in city_counts.items():
            percentage = (count / len(self.df)) * 100
            print(f"• {city}: {count:,} 行 ({percentage:.1f}%)")
        
        # 分析分区大小
        partition_info = self._analyze_partitions()
        
        print(f"\n分区文件分布:")
        for partition in partition_info['partitions']:
            print(f"• {partition['path']}: {partition['size_mb']:.2f} MB")
        
        # 计算分区均衡性
        sizes = [p['size_mb'] for p in partition_info['partitions']]
        if sizes:
            avg_size = sum(sizes) / len(sizes)
            max_size = max(sizes)
            min_size = min(sizes)
            balance_ratio = min_size / max_size if max_size > 0 else 0
            
            print(f"\n分区均衡性分析:")
            print(f"• 平均大小: {avg_size:.2f} MB")
            print(f"• 最大分区: {max_size:.2f} MB")
            print(f"• 最小分区: {min_size:.2f} MB")
            print(f"• 均衡比例: {balance_ratio:.2f}")
        
        return {
            'city_distribution': city_counts.to_dict(),
            'partition_info': partition_info,
            'balance_metrics': {
                'avg_size': avg_size if sizes else 0,
                'max_size': max_size if sizes else 0,
                'min_size': min_size if sizes else 0,
                'balance_ratio': balance_ratio if sizes else 0
            }
        }
    
    def test_nested_partitioning(self, partition_cols: List[str] = None) -> Dict[str, Any]:
        """
        测试嵌套分区
        
        Args:
            partition_cols: 多级分区列
            
        Returns:
            嵌套分区测试结果
        """
        if partition_cols is None:
            partition_cols = ['City', 'AgeGroup']
        
        print("\n" + "=" * 60)
        print(f"测试嵌套分区 ({' -> '.join(partition_cols)})")
        print("=" * 60)
        
        # 添加年龄段列用于嵌套分区
        df_with_age_group = self.df.copy()
        df_with_age_group['AgeGroup'] = pd.cut(
            df_with_age_group['Age'], 
            bins=[0, 30, 50, 100], 
            labels=['Young', 'Middle', 'Senior']
        ).astype(str)
        
        # 创建嵌套分区表
        nested_path = os.path.join(self.output_dir, 'nested_partitioned_table')
        if os.path.exists(nested_path):
            shutil.rmtree(nested_path)
        
        table = pa.Table.from_pandas(df_with_age_group)
        pq.write_to_dataset(
            table,
            root_path=nested_path,
            partition_cols=partition_cols
        )
        
        # 分析嵌套分区结构
        nested_info = self._analyze_nested_partitions(nested_path)
        
        print(f"嵌套分区已创建: {nested_path}")
        print(f"分区层级: {len(partition_cols)}")
        print(f"叶子分区数: {nested_info['leaf_partitions']}")
        print(f"总大小: {nested_info['total_size']:.2f} MB")
        
        # 测试嵌套分区查询
        def query_nested():
            import pyarrow.dataset as ds
            dataset = ds.dataset(nested_path, format="parquet")
            table = dataset.to_table()
            df = table.to_pandas()
            # 添加年龄段列用于测试
            df['AgeGroup'] = df['Age'].apply(lambda x: 'Young' if x < 30 else ('Middle' if x < 50 else 'Senior'))
            return df[(df['City'] == 'Beijing') & (df['AgeGroup'] == 'Middle')] if 'City' in df.columns else df
        
        df_nested, nested_time = self.performance_analyzer.measure_time(query_nested)
        
        print(f"嵌套分区查询: {nested_time:.4f} 秒, 结果: {len(df_nested)} 行")
        
        return {
            'partition_cols': partition_cols,
            'nested_info': nested_info,
            'query_time': nested_time,
            'result_rows': len(df_nested)
        }
    
    def _analyze_nested_partitions(self, path: str) -> Dict[str, Any]:
        """分析嵌套分区结构"""
        leaf_partitions = 0
        total_size = 0
        
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith('.parquet'):
                    file_path = os.path.join(root, file)
                    size = self.performance_analyzer.get_file_size(file_path)
                    total_size += size
                    leaf_partitions += 1
        
        return {
            'leaf_partitions': leaf_partitions,
            'total_size': total_size
        }
    
    def run_partitioning_exercise(self) -> Dict[str, Any]:
        """
        运行完整的分区练习
        
        Returns:
            所有分区测试的结果
        """
        print("=" * 60)
        print("开始 Parquet 分区练习")
        print("=" * 60)
        
        results = {}
        
        # 1. 创建表
        self.create_non_partitioned_table()
        partition_info = self.create_partitioned_table()
        results['partition_info'] = partition_info
        
        # 2. 测试分区裁剪
        results['partition_pruning'] = self.test_partition_pruning()
        
        # 3. 测试多种查询场景
        results['multiple_queries'] = self.test_multiple_partition_queries()
        
        # 4. 分析分区分布
        results['distribution_analysis'] = self.analyze_partition_distribution()
        
        # 5. 测试嵌套分区
        results['nested_partitioning'] = self.test_nested_partitioning()
        
        # 显示总结
        self.display_partitioning_summary(results)
        
        # 保存结果
        results_file = os.path.join(self.output_dir, 'partitioning_results.json')
        self.performance_analyzer.save_results(results, results_file)
        
        return results
    
    def display_partitioning_summary(self, results: Dict[str, Any]) -> None:
        """
        显示分区总结
        
        Args:
            results: 分区测试结果
        """
        print("\n" + "=" * 60)
        print("分区练习总结")
        print("=" * 60)
        
        print("🎯 分区效果:")
        
        if 'partition_pruning' in results:
            speedup = results['partition_pruning'].get('speedup', 0)
            print(f"• 分区裁剪性能提升: {speedup:.2f}x")
        
        if 'partition_info' in results:
            partition_count = results['partition_info'].get('partition_count', 0)
            print(f"• 分区数量: {partition_count}")
        
        print("\n💡 分区最佳实践:")
        print("• 选择查询频繁的列作为分区键")
        print("• 避免创建过多小分区")
        print("• 考虑数据分布的均衡性")
        print("• 合理使用嵌套分区")
        print("• 定期监控分区性能")
    
    def cleanup(self):
        """清理临时文件"""
        from .utils import cleanup_files
        patterns = [
            self.non_partitioned_path,
            self.partitioned_path,
            os.path.join(self.output_dir, 'nested_partitioned_table')
        ]
        cleanup_files(patterns)