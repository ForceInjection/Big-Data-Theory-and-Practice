"""
Parquet 查询优化练习模块

提供投影下推和谓词下推等查询优化技术的演示。
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import os
from typing import Dict, Any, List, Tuple, Optional

from ..utils import PerformanceAnalyzer


class ParquetQueryOptimizationExercise:
    """Parquet 查询优化练习类"""
    
    def __init__(self, data_df: pd.DataFrame, output_dir: str = "output"):
        """
        初始化查询优化练习
        
        Args:
            data_df: 要测试的数据
            output_dir: 输出目录
        """
        self.df = data_df
        self.output_dir = output_dir
        self.performance_analyzer = PerformanceAnalyzer()
        self.filename = os.path.join(output_dir, 'optimization_test.parquet')
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 保存数据为 Parquet 文件
        self._prepare_test_data()
    
    def _prepare_test_data(self) -> None:
        """准备测试数据"""
        print("准备查询优化测试数据...")
        table = pa.Table.from_pandas(self.df)
        pq.write_table(table, self.filename)
        print(f"测试数据已保存到: {self.filename}")
    
    def test_projection_pushdown(self, selected_columns: List[str] = None) -> Dict[str, Any]:
        """
        测试投影下推（列裁剪）
        
        Args:
            selected_columns: 要选择的列，默认选择部分列
            
        Returns:
            投影下推测试结果
        """
        print("=" * 60)
        print("测试投影下推（列裁剪）")
        print("=" * 60)
        
        if selected_columns is None:
            selected_columns = ['UserID', 'Username', 'City']
        
        # 测试读取所有列
        def read_all_columns():
            table = pq.read_table(self.filename)
            return table.to_pandas()
        
        df_all, time_all_columns = self.performance_analyzer.measure_time(read_all_columns)
        
        print(f"读取所有列 ({len(df_all.columns)} 列): {time_all_columns:.4f} 秒")
        
        # 测试只读取部分列
        def read_selected_columns():
            table = pq.read_table(self.filename, columns=selected_columns)
            return table.to_pandas()
        
        df_selected, time_selected_columns = self.performance_analyzer.measure_time(read_selected_columns)
        
        print(f"读取选定列 ({len(selected_columns)} 列): {time_selected_columns:.4f} 秒")
        
        speedup = time_all_columns / time_selected_columns if time_selected_columns > 0 else 0
        print(f"性能提升: {speedup:.2f}x")
        
        # 计算数据量减少
        memory_reduction = (1 - len(selected_columns) / len(df_all.columns)) * 100
        print(f"内存使用减少: {memory_reduction:.1f}%")
        
        return {
            'all_columns_time': time_all_columns,
            'selected_columns_time': time_selected_columns,
            'speedup': speedup,
            'all_columns_count': len(df_all.columns),
            'selected_columns_count': len(selected_columns),
            'memory_reduction_percent': memory_reduction
        }
    
    def test_predicate_pushdown(self, filters: List[Tuple] = None) -> Dict[str, Any]:
        """
        测试谓词下推（过滤下推）
        
        Args:
            filters: 过滤条件，默认使用年龄过滤
            
        Returns:
            谓词下推测试结果
        """
        print("\n" + "=" * 60)
        print("测试谓词下推（过滤下推）")
        print("=" * 60)
        
        if filters is None:
            filters = [('Age', '>', 50)]
        
        # 测试不使用过滤器（内存过滤）
        def memory_filter():
            table = pq.read_table(self.filename)
            df = table.to_pandas()
            # 应用过滤条件
            for column, op, value in filters:
                if op == '>':
                    df = df[df[column] > value]
                elif op == '<':
                    df = df[df[column] < value]
                elif op == '>=':
                    df = df[df[column] >= value]
                elif op == '<=':
                    df = df[df[column] <= value]
                elif op == '==':
                    df = df[df[column] == value]
                elif op == '!=':
                    df = df[df[column] != value]
                elif op == 'in':
                    df = df[df[column].isin(value)]
            return df
        
        df_filtered_memory, time_memory_filter = self.performance_analyzer.measure_time(memory_filter)
        
        print(f"内存过滤: {time_memory_filter:.4f} 秒, 结果行数: {len(df_filtered_memory)}")
        
        # 测试使用 Parquet 过滤器
        def parquet_filter():
            table = pq.read_table(self.filename, filters=filters)
            return table.to_pandas()
        
        df_filtered_parquet, time_parquet_filter = self.performance_analyzer.measure_time(parquet_filter)
        
        print(f"Parquet 过滤: {time_parquet_filter:.4f} 秒, 结果行数: {len(df_filtered_parquet)}")
        
        speedup = time_memory_filter / time_parquet_filter if time_parquet_filter > 0 else 0
        print(f"性能提升: {speedup:.2f}x")
        
        # 计算数据量减少
        data_reduction = (1 - len(df_filtered_parquet) / len(self.df)) * 100
        print(f"数据量减少: {data_reduction:.1f}%")
        
        return {
            'memory_filter_time': time_memory_filter,
            'parquet_filter_time': time_parquet_filter,
            'speedup': speedup,
            'filtered_rows': len(df_filtered_parquet),
            'original_rows': len(self.df),
            'data_reduction_percent': data_reduction
        }
    
    def test_combined_optimization(self, 
                                 selected_columns: List[str] = None,
                                 filters: List[Tuple] = None) -> Dict[str, Any]:
        """
        测试组合优化（投影 + 谓词下推）
        
        Args:
            selected_columns: 要选择的列
            filters: 过滤条件
            
        Returns:
            组合优化测试结果
        """
        print("\n" + "=" * 60)
        print("测试组合优化（投影 + 谓词下推）")
        print("=" * 60)
        
        if selected_columns is None:
            selected_columns = ['UserID', 'Username', 'Age', 'City']
        
        if filters is None:
            filters = [('Age', '>', 30), ('City', 'in', ['Beijing', 'Shanghai', 'Guangzhou'])]
        
        # 组合优化：只读取需要的列 + 过滤
        def optimized_query():
            table = pq.read_table(
                self.filename,
                columns=selected_columns,
                filters=filters
            )
            return table.to_pandas()
        
        df_optimized, time_optimized = self.performance_analyzer.measure_time(optimized_query)
        
        print(f"组合优化查询: {time_optimized:.4f} 秒")
        print(f"结果行数: {len(df_optimized)}")
        print(f"结果列数: {len(df_optimized.columns)}")
        
        # 与全表扫描对比
        def full_scan():
            table = pq.read_table(self.filename)
            df = table.to_pandas()
            # 应用过滤条件
            for column, op, value in filters:
                if op == '>':
                    df = df[df[column] > value]
                elif op == '<':
                    df = df[df[column] < value]
                elif op == 'in':
                    df = df[df[column].isin(value)]
                # 可以添加更多操作符
            # 选择列
            return df[selected_columns]
        
        df_full_scan, time_full_scan = self.performance_analyzer.measure_time(full_scan)
        
        print(f"全表扫描 + 内存过滤: {time_full_scan:.4f} 秒")
        
        speedup = time_full_scan / time_optimized if time_optimized > 0 else 0
        print(f"性能提升: {speedup:.2f}x")
        
        return {
            'optimized_time': time_optimized,
            'full_scan_time': time_full_scan,
            'speedup': speedup,
            'result_rows': len(df_optimized),
            'result_columns': len(df_optimized.columns)
        }
    
    def test_complex_queries(self) -> Dict[str, Any]:
        """
        测试复杂查询场景
        
        Returns:
            复杂查询测试结果
        """
        print("\n" + "=" * 60)
        print("测试复杂查询场景")
        print("=" * 60)
        
        results = {}
        
        # 场景1：范围查询
        print("场景1: 年龄范围查询 (25-45岁)")
        range_filters = [('Age', '>=', 25), ('Age', '<=', 45)]
        range_columns = ['UserID', 'Username', 'Age', 'Income']
        
        def range_query():
            return pq.read_table(
                self.filename, 
                columns=range_columns, 
                filters=range_filters
            ).to_pandas()
        
        _, range_time = self.performance_analyzer.measure_time(range_query)
        results['range_query'] = {'time': range_time}
        print(f"范围查询时间: {range_time:.4f} 秒")
        
        # 场景2：多条件查询
        print("\n场景2: 多条件查询 (高收入用户)")
        multi_filters = [('Age', '>', 30), ('Income', '>', 60000)]
        multi_columns = ['UserID', 'Username', 'Age', 'City', 'Income']
        
        def multi_condition_query():
            return pq.read_table(
                self.filename,
                columns=multi_columns,
                filters=multi_filters
            ).to_pandas()
        
        _, multi_time = self.performance_analyzer.measure_time(multi_condition_query)
        results['multi_condition_query'] = {'time': multi_time}
        print(f"多条件查询时间: {multi_time:.4f} 秒")
        
        # 场景3：IN 查询
        print("\n场景3: IN 查询 (特定城市)")
        in_filters = [('City', 'in', ['Beijing', 'Shanghai', 'Shenzhen'])]
        in_columns = ['UserID', 'Username', 'City', 'Income']
        
        def in_query():
            return pq.read_table(
                self.filename,
                columns=in_columns,
                filters=in_filters
            ).to_pandas()
        
        _, in_time = self.performance_analyzer.measure_time(in_query)
        results['in_query'] = {'time': in_time}
        print(f"IN 查询时间: {in_time:.4f} 秒")
        
        return results
    
    def run_optimization_exercise(self) -> Dict[str, Any]:
        """
        运行完整的查询优化练习
        
        Returns:
            所有优化测试的结果
        """
        print("=" * 60)
        print("开始 Parquet 查询优化练习")
        print("=" * 60)
        
        results = {}
        
        # 1. Projection pushdown test
        results['projection'] = self.test_projection_pushdown()
        
        # 2. Predicate pushdown test
        results['predicate'] = self.test_predicate_pushdown()
        
        # 3. Combined optimization test
        results['combined'] = self.test_combined_optimization()
        
        # 4. Complex query test
        results['complex'] = self.test_complex_queries()
        
        # Display summary
        self.display_optimization_summary(results)
        
        # 保存结果
        results_file = os.path.join(self.output_dir, 'query_optimization_results.json')
        self.performance_analyzer.save_results(results, results_file)
        
        return results
    
    def display_optimization_summary(self, results: Dict[str, Any]) -> None:
        """
        显示优化总结
        
        Args:
            results: 优化测试结果
        """
        print("\n" + "=" * 60)
        print("查询优化总结")
        print("=" * 60)
        
        print("🎯 优化效果:")
        
        if 'projection' in results:
            proj_speedup = results['projection'].get('speedup', 0)
            print(f"• 投影下推: {proj_speedup:.2f}x 性能提升")
        
        if 'predicate' in results:
            pred_speedup = results['predicate'].get('speedup', 0)
            print(f"• 谓词下推: {pred_speedup:.2f}x 性能提升")
        
        if 'combined' in results:
            comb_speedup = results['combined'].get('speedup', 0)
            print(f"• 组合优化: {comb_speedup:.2f}x 性能提升")
        
        print("\n💡 优化建议:")
        print("• 只读取需要的列（投影下推）")
        print("• 在存储层面进行数据过滤（谓词下推）")
        print("• 结合使用多种优化技术")
        print("• 根据查询模式设计合适的分区策略")
    
    def cleanup(self):
        """清理临时文件"""
        from ..utils import cleanup_files
        patterns = [
            self.filename
        ]
        cleanup_files(patterns)