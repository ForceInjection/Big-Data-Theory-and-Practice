"""
Parquet 压缩算法比较练习模块

提供不同压缩算法的性能比较功能。
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import os
from typing import Dict, Any, List, Optional

from ..utils import PerformanceAnalyzer


class ParquetCompressionExercise:
    """Parquet 压缩算法比较练习类"""
    
    def __init__(self, data_df: pd.DataFrame, output_dir: str = "output"):
        """
        初始化压缩算法比较练习
        
        Args:
            data_df: 要测试的数据
            output_dir: 输出目录
        """
        self.df = data_df
        self.output_dir = output_dir
        self.performance_analyzer = PerformanceAnalyzer()
        self.compression_algorithms = ['SNAPPY', 'GZIP', 'LZ4', 'BROTLI', None]
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
    def test_single_compression(self, compression: Optional[str]) -> Dict[str, float]:
        """
        测试单个压缩算法的性能
        
        Args:
            compression: 压缩算法名称
            
        Returns:
            性能指标字典
        """
        compression_name = compression or 'NONE'
        print(f"正在测试压缩算法：{compression_name}")
        print("-" * 40)
        
        filename = os.path.join(self.output_dir, f'data_{compression_name.lower()}.parquet')
        
        # 转换为 PyArrow 表
        table = pa.Table.from_pandas(self.df)
        
        # 测试写入性能
        _, write_time = self.performance_analyzer.measure_time(
            pq.write_table, table, filename, compression=compression
        )
        
        # 获取文件大小
        file_size = self.performance_analyzer.get_file_size(filename)
        
        # 测试读取性能
        def read_table():
            return pq.read_table(filename)
        
        _, read_time = self.performance_analyzer.measure_time(read_table)
        
        print(f"写入时间：{write_time:.4f} 秒")
        print(f"读取时间：{read_time:.4f} 秒")
        print(f"文件大小：{file_size:.2f} MB")
        
        return {
            'write_time': write_time,
            'read_time': read_time,
            'file_size': file_size,
            'filename': filename
        }
    
    def run_compression_exercise(self) -> Dict[str, Dict[str, float]]:
        """
        运行压缩算法练习的主方法
        
        Returns:
            所有压缩算法的性能指标
        """
        print("🔄 开始压缩算法练习...")
        print("=" * 60)
        
        # 运行所有压缩算法测试
        results = self.test_compression_algorithms()
        
        # 绘制对比图表
        self.plot_compression_comparison(results)
        
        print("✅ 压缩算法练习完成！")
        return results
    
    def test_compression_algorithms(self, algorithms: List[str] = None) -> Dict[str, Dict[str, float]]:
        """
        测试多个压缩算法的性能
        
        Args:
            algorithms: 要测试的压缩算法列表，默认测试所有
            
        Returns:
            所有算法的性能结果
        """
        if algorithms is None:
            algorithms = self.compression_algorithms
            
        print("=" * 60)
        print("开始压缩算法比较练习")
        print("=" * 60)
        
        results = {}
        
        for compression in algorithms:
            compression_name = compression or 'NONE'
            try:
                result = self.test_single_compression(compression)
                results[compression_name] = result
                print()  # 空行分隔
            except Exception as e:
                print(f"❌ 测试 {compression_name} 时出错: {e}")
                print()
        
        # 显示对比结果
        self.display_compression_results(results)
        
        # 保存结果
        results_file = os.path.join(self.output_dir, 'compression_results.json')
        self.performance_analyzer.save_results(results, results_file)
        
        return results
    
    def display_compression_results(self, results: Dict[str, Dict[str, float]]) -> None:
        """
        显示压缩算法对比结果
        
        Args:
            results: 压缩算法结果字典
        """
        print("=" * 80)
        print("压缩算法性能对比")
        print("=" * 80)
        
        # 准备显示数据
        display_results = {}
        base_size = results.get('NONE', {}).get('file_size', 0)
        
        for algo, metrics in results.items():
            compression_ratio = base_size / metrics['file_size'] if metrics['file_size'] > 0 else 0
            display_results[algo] = {
                'write_time': metrics['write_time'],
                'read_time': metrics['read_time'],
                'file_size': metrics['file_size'],
                'compression_ratio': compression_ratio
            }
        
        # 使用性能分析器显示结果
        self.performance_analyzer.compare_performance(display_results, "压缩算法性能对比")
        
        # 显示额外的分析
        self.analyze_compression_tradeoffs(display_results)
    
    def analyze_compression_tradeoffs(self, results: Dict[str, Dict[str, float]]) -> None:
        """
        分析压缩算法的权衡
        
        Args:
            results: 压缩算法结果字典
        """
        print("\n" + "=" * 60)
        print("压缩算法权衡分析")
        print("=" * 60)
        
        # 找出最佳算法
        best_compression = max(results.keys(), 
                             key=lambda x: results[x].get('compression_ratio', 0))
        fastest_write = min(results.keys(), 
                           key=lambda x: results[x].get('write_time', float('inf')))
        fastest_read = min(results.keys(), 
                          key=lambda x: results[x].get('read_time', float('inf')))
        
        print(f"🏆 最佳压缩比: {best_compression} "
              f"({results[best_compression]['compression_ratio']:.2f}x)")
        print(f"⚡ 最快写入: {fastest_write} "
              f"({results[fastest_write]['write_time']:.4f}s)")
        print(f"⚡ 最快读取: {fastest_read} "
              f"({results[fastest_read]['read_time']:.4f}s)")
        
        # 推荐算法
        print(f"\n📋 算法推荐:")
        print(f"• 存储优先: {best_compression} (最高压缩比)")
        print(f"• 写入优先: {fastest_write} (最快写入)")
        print(f"• 读取优先: {fastest_read} (最快读取)")
        
        # 平衡推荐
        balanced_scores = {}
        for algo, metrics in results.items():
            if algo == 'NONE':
                continue
            # 综合评分：压缩比权重0.4，写入速度权重0.3，读取速度权重0.3
            compression_score = metrics.get('compression_ratio', 0) / max(
                r.get('compression_ratio', 1) for r in results.values()
            )
            write_score = (1 / metrics.get('write_time', 1)) / max(
                1 / r.get('write_time', 1) for r in results.values()
            )
            read_score = (1 / metrics.get('read_time', 1)) / max(
                1 / r.get('read_time', 1) for r in results.values()
            )
            
            balanced_scores[algo] = (
                0.4 * compression_score + 0.3 * write_score + 0.3 * read_score
            )
        
        if balanced_scores:
            best_balanced = max(balanced_scores.keys(), key=lambda x: balanced_scores[x])
            print(f"• 综合最佳: {best_balanced} (平衡性能)")
    
    def plot_compression_comparison(self, results: Dict[str, Dict[str, float]]) -> None:
        """
        绘制压缩算法对比图
        
        Args:
            results: 压缩算法结果字典
        """
        try:
            import matplotlib.pyplot as plt
            
            # 准备数据
            algorithms = list(results.keys())
            write_times = [results[algo]['write_time'] for algo in algorithms]
            read_times = [results[algo]['read_time'] for algo in algorithms]
            file_sizes = [results[algo]['file_size'] for algo in algorithms]
            
            # 创建子图
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
            
            # 写入时间对比
            ax1.bar(algorithms, write_times, alpha=0.7, color='skyblue')
            ax1.set_title('Write Time Comparison')
            ax1.set_ylabel('Time (seconds)')
            ax1.tick_params(axis='x', rotation=45)
            
            # 读取时间对比
            ax2.bar(algorithms, read_times, alpha=0.7, color='lightgreen')
            ax2.set_title('Read Time Comparison')
            ax2.set_ylabel('Time (seconds)')
            ax2.tick_params(axis='x', rotation=45)
            
            # 文件大小对比
            ax3.bar(algorithms, file_sizes, alpha=0.7, color='salmon')
            ax3.set_title('File Size Comparison')
            ax3.set_ylabel('Size (MB)')
            ax3.tick_params(axis='x', rotation=45)
            
            # 压缩比对比
            base_size = results.get('NONE', {}).get('file_size', max(file_sizes))
            compression_ratios = [base_size / size if size > 0 else 0 for size in file_sizes]
            ax4.bar(algorithms, compression_ratios, alpha=0.7, color='gold')
            ax4.set_title('Compression Ratio Comparison')
            ax4.set_ylabel('Compression Ratio')
            ax4.tick_params(axis='x', rotation=45)
            
            plt.tight_layout()
            
            # 保存图表
            plot_file = os.path.join(self.output_dir, 'compression_comparison.png')
            plt.savefig(plot_file, dpi=300, bbox_inches='tight')
            print(f"对比图表已保存到: {plot_file}")
            
            plt.show()
            
        except ImportError:
            print("⚠️  matplotlib 未安装，跳过图表绘制")
    
    def cleanup(self):
        """清理临时文件"""
        from ..utils import cleanup_files
        patterns = [
            os.path.join(self.output_dir, 'data_*.parquet')
        ]
        cleanup_files(patterns)