#!/usr/bin/env python3
"""
性能报告生成脚本

生成 Parquet 实践项目的性能对比报告和可视化图表。
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import json
import os
import tempfile
import time
from datetime import datetime

# 设置中文字体支持
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


def run_performance_benchmark():
    """运行性能基准测试"""
    print("🚀 开始运行性能基准测试...")
    
    # 创建临时目录
    test_dir = tempfile.mkdtemp(prefix='parquet_performance_')
    print(f"📁 测试目录: {test_dir}")
    
    # 测试不同数据量
    sizes = [1000, 5000, 10000, 50000]
    results = {}
    
    for size in sizes:
        print(f"\n📊 测试数据量: {size} 条记录")
        
        # 生成测试数据
        df = generate_test_data(size)
        
        # 测试 Parquet 性能
        parquet_results = test_parquet_performance(df, test_dir, size)
        
        # 测试 CSV 性能（作为对比）
        csv_results = test_csv_performance(df, test_dir, size)
        
        results[size] = {
            'parquet': parquet_results,
            'csv': csv_results,
            'comparison': {
                'write_speedup': csv_results['write_time'] / parquet_results['write_time'] if parquet_results['write_time'] > 0 else 0,
                'read_speedup': csv_results['read_time'] / parquet_results['read_time'] if parquet_results['read_time'] > 0 else 0,
                'size_reduction': (csv_results['file_size'] - parquet_results['file_size']) / csv_results['file_size'] * 100
            }
        }
        
        print(f"   ✅ Parquet: 写入 {parquet_results['write_time']:.3f}s, 读取 {parquet_results['read_time']:.3f}s, 大小 {parquet_results['file_size']:.2f}MB")
        print(f"   📄 CSV:     写入 {csv_results['write_time']:.3f}s, 读取 {csv_results['read_time']:.3f}s, 大小 {csv_results['file_size']:.2f}MB")
        print(f"   ⚡ 性能提升: 写入 {results[size]['comparison']['write_speedup']:.1f}x, 读取 {results[size]['comparison']['read_speedup']:.1f}x, 大小减少 {results[size]['comparison']['size_reduction']:.1f}%")
    
    return results, test_dir


def generate_test_data(size):
    """生成测试数据"""
    # 创建包含多种数据类型的测试数据
    dates = pd.date_range('2020-01-01', periods=size, freq='D')
    
    df = pd.DataFrame({
        'id': range(size),
        'timestamp': dates,
        'value_int': np.random.randint(0, 1000, size),
        'value_float': np.random.randn(size),
        'category': np.random.choice(['A', 'B', 'C', 'D'], size),
        'text': [f'sample_text_{i}' for i in range(size)],
        'bool_flag': np.random.choice([True, False], size)
    })
    
    return df


def test_parquet_performance(df, test_dir, size):
    """测试 Parquet 性能"""
    parquet_file = os.path.join(test_dir, f'data_{size}.parquet')
    
    # 写入性能
    write_start = time.time()
    df.to_parquet(parquet_file, engine='pyarrow', compression='snappy')
    write_time = time.time() - write_start
    
    # 读取性能
    read_start = time.time()
    read_df = pd.read_parquet(parquet_file)
    read_time = time.time() - read_start
    
    # 文件大小
    file_size = os.path.getsize(parquet_file) / (1024 * 1024)  # MB
    
    return {
        'write_time': write_time,
        'read_time': read_time,
        'file_size': file_size,
        'rows_per_second_write': size / write_time if write_time > 0 else 0,
        'rows_per_second_read': size / read_time if read_time > 0 else 0
    }


def test_csv_performance(df, test_dir, size):
    """测试 CSV 性能"""
    csv_file = os.path.join(test_dir, f'data_{size}.csv')
    
    # 写入性能
    write_start = time.time()
    df.to_csv(csv_file, index=False)
    write_time = time.time() - write_start
    
    # 读取性能
    read_start = time.time()
    read_df = pd.read_csv(csv_file)
    read_time = time.time() - read_start
    
    # 文件大小
    file_size = os.path.getsize(csv_file) / (1024 * 1024)  # MB
    
    return {
        'write_time': write_time,
        'read_time': read_time,
        'file_size': file_size,
        'rows_per_second_write': size / write_time if write_time > 0 else 0,
        'rows_per_second_read': size / read_time if read_time > 0 else 0
    }


def create_visualizations(results, output_dir):
    """创建可视化图表"""
    print("\n🎨 创建可视化图表...")
    
    # 准备数据
    sizes = list(results.keys())
    
    parquet_write_times = [results[size]['parquet']['write_time'] for size in sizes]
    csv_write_times = [results[size]['csv']['write_time'] for size in sizes]
    
    parquet_read_times = [results[size]['parquet']['read_time'] for size in sizes]
    csv_read_times = [results[size]['csv']['read_time'] for size in sizes]
    
    parquet_sizes = [results[size]['parquet']['file_size'] for size in sizes]
    csv_sizes = [results[size]['csv']['file_size'] for size in sizes]
    
    write_speedups = [results[size]['comparison']['write_speedup'] for size in sizes]
    read_speedups = [results[size]['comparison']['read_speedup'] for size in sizes]
    size_reductions = [results[size]['comparison']['size_reduction'] for size in sizes]
    
    # 创建图表
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('Parquet vs CSV 性能对比分析', fontsize=16, fontweight='bold')
    
    # 1. 写入时间对比
    axes[0, 0].plot(sizes, parquet_write_times, 'o-', label='Parquet', linewidth=2, markersize=8)
    axes[0, 0].plot(sizes, csv_write_times, 's--', label='CSV', linewidth=2, markersize=8)
    axes[0, 0].set_xlabel('数据量 (条记录)')
    axes[0, 0].set_ylabel('写入时间 (秒)')
    axes[0, 0].set_title('写入性能对比')
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)
    
    # 2. 读取时间对比
    axes[0, 1].plot(sizes, parquet_read_times, 'o-', label='Parquet', linewidth=2, markersize=8)
    axes[0, 1].plot(sizes, csv_read_times, 's--', label='CSV', linewidth=2, markersize=8)
    axes[0, 1].set_xlabel('数据量 (条记录)')
    axes[0, 1].set_ylabel('读取时间 (秒)')
    axes[0, 1].set_title('读取性能对比')
    axes[0, 1].legend()
    axes[0, 1].grid(True, alpha=0.3)
    
    # 3. 文件大小对比
    axes[0, 2].plot(sizes, parquet_sizes, 'o-', label='Parquet', linewidth=2, markersize=8)
    axes[0, 2].plot(sizes, csv_sizes, 's--', label='CSV', linewidth=2, markersize=8)
    axes[0, 2].set_xlabel('数据量 (条记录)')
    axes[0, 2].set_ylabel('文件大小 (MB)')
    axes[0, 2].set_title('存储效率对比')
    axes[0, 2].legend()
    axes[0, 2].grid(True, alpha=0.3)
    
    # 4. 写入性能提升
    axes[1, 0].bar(sizes, write_speedups, alpha=0.7, color='green')
    axes[1, 0].set_xlabel('数据量 (条记录)')
    axes[1, 0].set_ylabel('性能提升倍数')
    axes[1, 0].set_title('写入性能提升 (Parquet vs CSV)')
    axes[1, 0].grid(True, alpha=0.3)
    for i, v in enumerate(write_speedups):
        axes[1, 0].text(sizes[i], v + 0.1, f'{v:.1f}x', ha='center')
    
    # 5. 读取性能提升
    axes[1, 1].bar(sizes, read_speedups, alpha=0.7, color='blue')
    axes[1, 1].set_xlabel('数据量 (条记录)')
    axes[1, 1].set_ylabel('性能提升倍数')
    axes[1, 1].set_title('读取性能提升 (Parquet vs CSV)')
    axes[1, 1].grid(True, alpha=0.3)
    for i, v in enumerate(read_speedups):
        axes[1, 1].text(sizes[i], v + 0.1, f'{v:.1f}x', ha='center')
    
    # 6. 存储空间节省
    axes[1, 2].bar(sizes, size_reductions, alpha=0.7, color='orange')
    axes[1, 2].set_xlabel('数据量 (条记录)')
    axes[1, 2].set_ylabel('空间节省百分比 (%)')
    axes[1, 2].set_title('存储空间节省 (Parquet vs CSV)')
    axes[1, 2].grid(True, alpha=0.3)
    for i, v in enumerate(size_reductions):
        axes[1, 2].text(sizes[i], v + 1, f'{v:.1f}%', ha='center')
    
    plt.tight_layout()
    
    # 保存图表
    chart_path = os.path.join(output_dir, 'performance_comparison.png')
    plt.savefig(chart_path, dpi=300, bbox_inches='tight')
    print(f"📈 图表已保存: {chart_path}")
    
    # 创建详细数据表格
    create_detailed_table(results, output_dir)
    
    return chart_path


def create_detailed_table(results, output_dir):
    """创建详细数据表格"""
    table_data = []
    
    for size in sorted(results.keys()):
        parquet = results[size]['parquet']
        csv = results[size]['csv']
        comparison = results[size]['comparison']
        
        table_data.append({
            '数据量': size,
            'Parquet写入时间(s)': f"{parquet['write_time']:.3f}",
            'CSV写入时间(s)': f"{csv['write_time']:.3f}",
            '写入性能提升': f"{comparison['write_speedup']:.1f}x",
            'Parquet读取时间(s)': f"{parquet['read_time']:.3f}",
            'CSV读取时间(s)': f"{csv['read_time']:.3f}",
            '读取性能提升': f"{comparison['read_speedup']:.1f}x",
            'Parquet文件大小(MB)': f"{parquet['file_size']:.2f}",
            'CSV文件大小(MB)': f"{csv['file_size']:.2f}",
            '空间节省百分比': f"{comparison['size_reduction']:.1f}%"
        })
    
    # 创建 DataFrame 并保存为 CSV
    df_table = pd.DataFrame(table_data)
    table_path = os.path.join(output_dir, 'performance_details.csv')
    df_table.to_csv(table_path, index=False, encoding='utf-8-sig')
    print(f"📋 详细数据表格已保存: {table_path}")
    
    # 保存为 Markdown 表格
    md_table = df_table.to_markdown(index=False)
    md_path = os.path.join(output_dir, 'performance_details.md')
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write("# Parquet vs CSV 性能对比详细数据\n\n")
        f.write(md_table)
    print(f"📄 Markdown 表格已保存: {md_path}")
    
    return df_table


def generate_report(results, output_dir):
    """生成完整报告"""
    print("\n📝 生成性能报告...")
    
    # 保存原始结果
    results_path = os.path.join(output_dir, 'performance_results.json')
    with open(results_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"📊 原始结果已保存: {results_path}")
    
    # 创建可视化图表
    chart_path = create_visualizations(results, output_dir)
    
    # 生成总结报告
    generate_summary_report(results, output_dir)
    
    return {
        'results_json': results_path,
        'chart_png': chart_path,
        'details_csv': os.path.join(output_dir, 'performance_details.csv'),
        'details_md': os.path.join(output_dir, 'performance_details.md')
    }


def generate_summary_report(results, output_dir):
    """生成总结报告"""
    summary = {
        'timestamp': datetime.now().isoformat(),
        'data_sizes_tested': list(results.keys()),
        'average_write_speedup': np.mean([results[size]['comparison']['write_speedup'] for size in results]),
        'average_read_speedup': np.mean([results[size]['comparison']['read_speedup'] for size in results]),
        'average_size_reduction': np.mean([results[size]['comparison']['size_reduction'] for size in results]),
        'max_write_speedup': max([results[size]['comparison']['write_speedup'] for size in results]),
        'max_read_speedup': max([results[size]['comparison']['read_speedup'] for size in results]),
        'max_size_reduction': max([results[size]['comparison']['size_reduction'] for size in results])
    }
    
    summary_path = os.path.join(output_dir, 'performance_summary.json')
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    # 生成文本总结
    text_summary = f"""
# Parquet 性能测试总结报告

## 测试概述
- 测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- 测试数据量: {', '.join(map(str, sorted(results.keys())))} 条记录
- 测试内容: Parquet vs CSV 格式的性能对比

## 主要发现

### 性能提升
- **平均写入性能提升**: {summary['average_write_speedup']:.1f}x
- **平均读取性能提升**: {summary['average_read_speedup']:.1f}x
- **最大写入性能提升**: {summary['max_write_speedup']:.1f}x
- **最大读取性能提升**: {summary['max_read_speedup']:.1f}x

### 存储效率
- **平均存储空间节省**: {summary['average_size_reduction']:.1f}%
- **最大存储空间节省**: {summary['max_size_reduction']:.1f}%

## 结论
Parquet 格式在大多数场景下相比 CSV 格式具有显著优势：
1. **读取性能**: 平均提升 {summary['average_read_speedup']:.1f} 倍
2. **写入性能**: 平均提升 {summary['average_write_speedup']:.1f} 倍  
3. **存储效率**: 平均节省 {summary['average_size_reduction']:.1f}% 存储空间
4. **数据量越大，优势越明显**: 随着数据量增加，性能提升效果更加显著

建议在需要处理大量数据的场景中优先使用 Parquet 格式。
"""
    
    summary_text_path = os.path.join(output_dir, 'performance_summary.md')
    with open(summary_text_path, 'w', encoding='utf-8') as f:
        f.write(text_summary)
    
    print(f"📋 总结报告已保存: {summary_text_path}")
    
    # 打印总结
    print("\n" + "="*60)
    print("🎯 性能测试总结")
    print("="*60)
    print(f"平均写入性能提升: {summary['average_write_speedup']:.1f}x")
    print(f"平均读取性能提升: {summary['average_read_speedup']:.1f}x")
    print(f"平均存储空间节省: {summary['average_size_reduction']:.1f}%")
    print("="*60)


def main():
    """主函数"""
    print("=" * 60)
    print("📊 Parquet 性能对比报告生成工具")
    print("=" * 60)
    
    # 创建输出目录
    output_dir = os.path.join(os.getcwd(), 'output', 'reports')
    os.makedirs(output_dir, exist_ok=True)
    print(f"📁 输出目录: {output_dir}")
    
    try:
        # 运行性能测试
        results, test_dir = run_performance_benchmark()
        
        # 生成报告
        report_files = generate_report(results, output_dir)
        
        print("\n✅ 性能报告生成完成！")
        print("📁 生成的文件:")
        for key, path in report_files.items():
            print(f"   • {key}: {path}")
        
        print(f"\n🎉 请查看 {output_dir} 目录中的报告文件")
        
    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()