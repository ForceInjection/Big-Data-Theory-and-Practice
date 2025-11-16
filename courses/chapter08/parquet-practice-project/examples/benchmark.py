#!/usr/bin/env python3
"""
Parquet 性能基准测试

这个脚本提供了全面的 Parquet 性能基准测试，
包括不同数据量、不同压缩算法的性能对比。
"""

import os
import sys
import time
import pandas as pd
import matplotlib.pyplot as plt
from typing import List, Dict, Any

# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(project_root, 'src'))

from parquet_practice import (
    DataGenerator, 
    ParquetCompressionExercise,
    PerformanceAnalyzer
)


class ParquetBenchmark:
    """Parquet 性能基准测试"""
    
    def __init__(self, output_dir: str = "benchmark_results"):
        """
        初始化基准测试
        
        Args:
            output_dir: 输出目录
        """
        self.output_dir = output_dir
        self.data_generator = DataGenerator()
        self.performance_analyzer = PerformanceAnalyzer()
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        print("🎯 Parquet 性能基准测试")
        print("=" * 50)
    
    def benchmark_data_sizes(self, sizes: List[int] = None) -> Dict[str, Any]:
        """
        测试不同数据量的性能
        
        Args:
            sizes: 数据量列表
            
        Returns:
            测试结果
        """
        if sizes is None:
            sizes = [1000, 5000, 10000, 50000, 100000]
        
        print("📊 测试不同数据量的性能...")
        
        results = {
            'sizes': sizes,
            'parquet_write_times': [],
            'parquet_read_times': [],
            'csv_write_times': [],
            'csv_read_times': [],
            'parquet_sizes': [],
            'csv_sizes': []
        }
        
        for size in sizes:
            print(f"  测试 {size:,} 条记录...")
            
            # 生成数据
            data = self.data_generator.generate_user_data(size)
            df = pd.DataFrame(data)
            
            # 文件路径
            parquet_file = os.path.join(self.output_dir, f"benchmark_{size}.parquet")
            csv_file = os.path.join(self.output_dir, f"benchmark_{size}.csv")
            
            # 测试 Parquet 写入
            start_time = time.time()
            df.to_parquet(parquet_file, compression='snappy')
            parquet_write_time = time.time() - start_time
            results['parquet_write_times'].append(parquet_write_time)
            
            # 测试 CSV 写入
            start_time = time.time()
            df.to_csv(csv_file, index=False)
            csv_write_time = time.time() - start_time
            results['csv_write_times'].append(csv_write_time)
            
            # 测试 Parquet 读取
            start_time = time.time()
            pd.read_parquet(parquet_file)
            parquet_read_time = time.time() - start_time
            results['parquet_read_times'].append(parquet_read_time)
            
            # 测试 CSV 读取
            start_time = time.time()
            pd.read_csv(csv_file)
            csv_read_time = time.time() - start_time
            results['csv_read_times'].append(csv_read_time)
            
            # 文件大小
            parquet_size = self.performance_analyzer.get_file_size(parquet_file)
            csv_size = self.performance_analyzer.get_file_size(csv_file)
            results['parquet_sizes'].append(parquet_size)
            results['csv_sizes'].append(csv_size)
            
            # 清理文件
            os.remove(parquet_file)
            os.remove(csv_file)
        
        return results
    
    def benchmark_compression_algorithms(self, num_records: int = 10000) -> Dict[str, Any]:
        """
        测试不同压缩算法的性能
        
        Args:
            num_records: 记录数量
            
        Returns:
            测试结果
        """
        print(f"🗜️ 测试不同压缩算法的性能 ({num_records:,} 条记录)...")
        
        # 生成数据
        data = self.data_generator.generate_user_data(num_records)
        
        # 创建压缩练习实例
        exercise = ParquetCompressionExercise(data, self.output_dir)
        
        # 运行压缩测试
        results = exercise.run_compression_exercise()
        
        # 清理
        exercise.cleanup()
        
        return results
    
    def plot_size_benchmark(self, results: Dict[str, Any]) -> None:
        """绘制数据量基准测试图表"""
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        
        sizes = results['sizes']
        
        # 写入时间对比
        ax1.plot(sizes, results['parquet_write_times'], 'o-', label='Parquet', color='blue')
        ax1.plot(sizes, results['csv_write_times'], 's-', label='CSV', color='red')
        ax1.set_xlabel('记录数量')
        ax1.set_ylabel('写入时间 (秒)')
        ax1.set_title('写入性能对比')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # 读取时间对比
        ax2.plot(sizes, results['parquet_read_times'], 'o-', label='Parquet', color='blue')
        ax2.plot(sizes, results['csv_read_times'], 's-', label='CSV', color='red')
        ax2.set_xlabel('记录数量')
        ax2.set_ylabel('读取时间 (秒)')
        ax2.set_title('读取性能对比')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # 文件大小对比
        ax3.plot(sizes, [s/1024/1024 for s in results['parquet_sizes']], 'o-', label='Parquet', color='blue')
        ax3.plot(sizes, [s/1024/1024 for s in results['csv_sizes']], 's-', label='CSV', color='red')
        ax3.set_xlabel('记录数量')
        ax3.set_ylabel('文件大小 (MB)')
        ax3.set_title('文件大小对比')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # 读取速度提升倍数
        speedups = [csv_time / parquet_time for csv_time, parquet_time in 
                   zip(results['csv_read_times'], results['parquet_read_times'])]
        ax4.plot(sizes, speedups, 'o-', color='green')
        ax4.set_xlabel('记录数量')
        ax4.set_ylabel('速度提升倍数')
        ax4.set_title('Parquet 读取速度提升')
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # 保存图表
        chart_path = os.path.join(self.output_dir, 'size_benchmark.png')
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        print(f"📊 数据量基准测试图表已保存: {chart_path}")
        
        plt.show()
    
    def run_full_benchmark(self) -> None:
        """运行完整的基准测试"""
        print("🚀 开始完整基准测试...")
        
        # 1. 数据量基准测试
        size_results = self.benchmark_data_sizes()
        
        # 2. 压缩算法基准测试
        compression_results = self.benchmark_compression_algorithms()
        
        # 3. 生成报告
        self.generate_benchmark_report(size_results, compression_results)
        
        # 4. 绘制图表
        self.plot_size_benchmark(size_results)
        
        print("✅ 完整基准测试完成！")
    
    def generate_benchmark_report(self, size_results: Dict[str, Any], 
                                compression_results: Dict[str, Any]) -> None:
        """生成基准测试报告"""
        report_path = os.path.join(self.output_dir, 'benchmark_report.md')
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# Parquet 性能基准测试报告\n\n")
            f.write(f"测试时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # 数据量测试结果
            f.write("## 数据量性能测试\n\n")
            f.write("| 记录数量 | Parquet写入(s) | CSV写入(s) | Parquet读取(s) | CSV读取(s) | 读取提升 | Parquet大小(MB) | CSV大小(MB) | 压缩率 |\n")
            f.write("|---------|---------------|-----------|---------------|-----------|---------|----------------|-------------|--------|\n")
            
            for i, size in enumerate(size_results['sizes']):
                parquet_write = size_results['parquet_write_times'][i]
                csv_write = size_results['csv_write_times'][i]
                parquet_read = size_results['parquet_read_times'][i]
                csv_read = size_results['csv_read_times'][i]
                speedup = csv_read / parquet_read
                parquet_size = size_results['parquet_sizes'][i] / 1024 / 1024
                csv_size = size_results['csv_sizes'][i] / 1024 / 1024
                compression_ratio = (1 - parquet_size / csv_size) * 100
                
                f.write(f"| {size:,} | {parquet_write:.3f} | {csv_write:.3f} | {parquet_read:.3f} | {csv_read:.3f} | {speedup:.1f}x | {parquet_size:.2f} | {csv_size:.2f} | {compression_ratio:.1f}% |\n")
            
            # 压缩算法测试结果
            f.write("\n## 压缩算法性能测试\n\n")
            if 'compression_results' in compression_results:
                f.write("| 压缩算法 | 写入时间(s) | 读取时间(s) | 文件大小(MB) | 压缩比 |\n")
                f.write("|---------|------------|------------|-------------|--------|\n")
                
                for result in compression_results['compression_results']:
                    algorithm = result['algorithm']
                    write_time = result['write_time']
                    read_time = result['read_time']
                    file_size = result['file_size'] / 1024 / 1024
                    compression_ratio = result['compression_ratio']
                    
                    f.write(f"| {algorithm} | {write_time:.3f} | {read_time:.3f} | {file_size:.2f} | {compression_ratio:.1f} |\n")
            
            # 总结
            f.write("\n## 测试总结\n\n")
            f.write("### 主要发现\n\n")
            f.write("1. **读取性能**: Parquet 格式在读取性能上显著优于 CSV 格式\n")
            f.write("2. **存储效率**: Parquet 格式具有更好的压缩效果，节省存储空间\n")
            f.write("3. **压缩算法**: 不同压缩算法在性能和压缩比之间有不同的权衡\n")
            f.write("4. **扩展性**: 随着数据量增加，Parquet 的优势更加明显\n\n")
            
            f.write("### 建议\n\n")
            f.write("- 对于大数据分析场景，推荐使用 Parquet 格式\n")
            f.write("- 根据具体需求选择合适的压缩算法\n")
            f.write("- 在数据管道中考虑使用 Parquet 提升整体性能\n")
        
        print(f"📄 基准测试报告已生成: {report_path}")


def main():
    """主函数"""
    # 创建基准测试实例
    benchmark = ParquetBenchmark()
    
    # 运行完整基准测试
    benchmark.run_full_benchmark()


if __name__ == '__main__':
    main()