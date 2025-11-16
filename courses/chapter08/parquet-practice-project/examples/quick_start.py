#!/usr/bin/env python3
"""
Parquet 实践练习 - 快速开始示例

这个脚本演示了如何快速开始使用 Parquet 实践练习项目。
"""

import os
import sys

# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(project_root, 'src'))

from parquet_practice import DataGenerator, ParquetBasicExercise


def quick_demo():
    """快速演示 Parquet 的基本功能"""
    print("🚀 Parquet 快速演示")
    print("=" * 50)
    
    # 1. 生成示例数据
    print("📊 生成示例数据...")
    data_generator = DataGenerator()
    data = data_generator.generate_user_data(1000)  # 生成 1000 条记录
    print(f"✅ 生成了 {len(data)} 条用户数据")
    
    # 2. 创建输出目录
    output_dir = os.path.join(project_root, "examples", "output")
    os.makedirs(output_dir, exist_ok=True)
    
    # 3. 运行基础练习
    print("\n🔄 运行基础练习...")
    exercise = ParquetBasicExercise(num_records=1000, output_dir=output_dir)
    exercise.df = data  # 设置已生成的数据
    results = exercise.run_basic_exercise()
    
    # 4. 显示结果
    print("\n📈 性能对比结果:")
    if 'performance_comparison' in results:
        perf = results['performance_comparison']
        print(f"• Parquet 写入时间: {perf['parquet_write_time']:.3f} 秒")
        print(f"• CSV 写入时间: {perf['csv_write_time']:.3f} 秒")
        print(f"• Parquet 读取时间: {perf['parquet_read_time']:.3f} 秒")
        print(f"• CSV 读取时间: {perf['csv_read_time']:.3f} 秒")
        print(f"• 读取速度提升: {perf['read_speedup']:.1f}x")
        print(f"• 文件大小压缩: {perf['size_reduction']:.1f}%")
    
    # 5. 清理
    exercise.cleanup()
    
    print("\n✅ 快速演示完成！")
    print("\n💡 接下来你可以:")
    print("• 运行 python main.py 进入交互式模式")
    print("• 运行 python main.py --exercise all 运行所有练习")
    print("• 查看 examples/ 目录下的其他示例")


if __name__ == '__main__':
    quick_demo()