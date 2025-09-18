#!/usr/bin/env python3
"""
Parquet 实践练习项目主程序

提供交互式菜单，让用户选择不同的练习模块。
"""

import os
import sys
import argparse
from typing import Optional

# 添加 src 目录到 Python 路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from parquet_practice import (
    DataGenerator,
    ParquetBasicExercise,
    ParquetCompressionExercise,
    ParquetQueryOptimizationExercise,
    ParquetPartitioningExercise,
    ParquetAdvancedExercise
)
from parquet_practice.utils import PerformanceAnalyzer


class ParquetPracticeRunner:
    """Parquet 实践练习运行器"""
    
    def __init__(self, output_dir: str = "output"):
        """
        Initialize runner
        
        Args:
            output_dir: Output directory
        """
        self.output_dir = output_dir
        self.data_generator = DataGenerator()
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
    
    def show_menu(self) -> None:
        """Display main menu"""
        print("📋 练习菜单：")
        print("1. 基础练习 - Parquet 文件读写和性能对比")
        print("2. 压缩算法练习 - 不同压缩算法的性能分析")
        print("3. 查询优化练习 - 投影下推和谓词下推")
        print("4. 分区练习 - 分区表创建和查询优化")
        print("5. 高级特性练习 - 嵌套数据、元数据、流式处理")
        print("6. 运行所有练习")
        print("0. 退出")
        print()
    
    def run_basic_exercise(self, num_records: int = 10000) -> None:
        """运行基础练习"""
        print("🚀 开始基础练习...")
        
        # 生成测试数据
        data = self.data_generator.generate_user_data(num_records)
        
        # 创建练习实例
        exercise = ParquetBasicExercise(num_records=num_records, output_dir=self.output_dir)
        exercise.df = data  # 设置已生成的数据
        
        # 运行练习
        results = exercise.run_basic_exercise()
        
        # 清理
        exercise.cleanup()
        
        print("✅ 基础练习完成！")
        return results
    
    def run_compression_exercise(self, num_records: int = 10000) -> None:
        """运行压缩算法练习"""
        print("🚀 开始压缩算法练习...")
        
        # 生成测试数据
        data = self.data_generator.generate_user_data(num_records)
        
        # 创建练习实例
        exercise = ParquetCompressionExercise(data, self.output_dir)
        
        # 运行练习
        results = exercise.run_compression_exercise()
        
        # 清理
        exercise.cleanup()
        
        print("✅ 压缩算法练习完成！")
        return results
    
    def run_query_optimization_exercise(self, num_records: int = 10000) -> None:
        """Run query optimization exercises"""
        print("🔍 开始查询优化练习...")
        
        # Generate test data
        data = self.data_generator.generate_user_data(num_records)
        
        # Run query optimization exercises
        exercise = ParquetQueryOptimizationExercise(data, self.output_dir)
        results = exercise.run_optimization_exercise()
        
        # Cleanup
        exercise.cleanup()
        
        print("✅ 查询优化练习完成!")
        return results
    
    def run_partitioning_exercise(self, num_records: int = 10000) -> None:
        """Run partitioning exercises"""
        print("📂 开始分区练习...")
        
        # Generate test data
        generator = DataGenerator()
        df = generator.generate_user_data(num_records)
        
        # Initialize exercise
        exercise = ParquetPartitioningExercise(df, self.output_dir)
        
        # Run exercises
        results = exercise.run_partitioning_exercise()
        
        # Display results
        analyzer = PerformanceAnalyzer()
        analyzer.compare_performance(results, "Partitioning Performance")
        
        # Cleanup
        exercise.cleanup()
        
        print("✅ 分区练习完成！")
        return results
    
    def run_advanced_exercise(self) -> None:
        """Run advanced feature exercises"""
        print("🚀 开始高级特性练习...")
        
        # Run advanced exercises
        exercise = ParquetAdvancedExercise(self.output_dir)
        results = exercise.run_advanced_exercise()
        
        # Cleanup
        exercise.cleanup()
        
        print("✅ 高级特性练习完成！")
        return results
    
    def run_all_exercises(self, num_records: int = 5000) -> None:
        """运行所有练习"""
        print("🚀 开始运行所有练习...")
        print(f"使用 {num_records:,} 条记录进行测试")
        print()
        
        all_results = {}
        
        try:
            # 1. 基础练习
            all_results['basic'] = self.run_basic_exercise(num_records)
            print()
            
            # 2. 压缩算法练习
            all_results['compression'] = self.run_compression_exercise(num_records)
            print()
            
            # 3. 查询优化练习
            all_results['query_optimization'] = self.run_query_optimization_exercise(num_records)
            print()
            
            # 4. 分区练习
            all_results['partitioning'] = self.run_partitioning_exercise(num_records)
            print()
            
            # 5. 高级特性练习
            all_results['advanced'] = self.run_advanced_exercise()
            print()
            
            # 显示总结
            self.display_final_summary(all_results)
            
        except KeyboardInterrupt:
            print("\n⚠️ 练习被用户中断")
        except Exception as e:
            print(f"\n❌ 练习过程中出现错误: {e}")
    
    def display_final_summary(self, results: dict) -> None:
        """显示最终总结"""
        print("=" * 60)
        print("🎉 所有练习完成 - 总结")
        print("=" * 60)
        
        print("📊 练习结果概览:")
        
        if 'basic' in results and results['basic'] is not None:
            basic = results['basic']
            if isinstance(basic, dict) and 'performance_comparison' in basic:
                perf = basic['performance_comparison']
                if isinstance(perf, dict):
                    print(f"• 基础练习: Parquet vs CSV 读取速度提升 {perf.get('read_speedup', 0):.1f}x")
        
        if 'compression' in results and results['compression'] is not None:
            compression = results['compression']
            if isinstance(compression, dict) and 'best_algorithm' in compression:
                best = compression['best_algorithm']
                if isinstance(best, dict):
                    print(f"• 压缩练习: 最佳算法 {best.get('algorithm', 'N/A')} (压缩比 {best.get('compression_ratio', 0):.1f})")
        
        if 'query_optimization' in results and results['query_optimization'] is not None:
            query = results['query_optimization']
            if isinstance(query, dict) and 'combined' in query:
                combined = query['combined']
                if isinstance(combined, dict):
                    print(f"• 查询优化: 组合优化性能提升 {combined.get('speedup', 0):.1f}x")
        
        if 'partitioning' in results and results['partitioning'] is not None:
            partition = results['partitioning']
            if isinstance(partition, dict) and 'partition_pruning' in partition:
                pruning = partition['partition_pruning']
                if isinstance(pruning, dict):
                    print(f"• 分区练习: 分区裁剪性能提升 {pruning.get('speedup', 0):.1f}x")
        
        if 'advanced' in results and results['advanced'] is not None:
            print("• 高级特性: 嵌套数据、元数据、流式处理等功能验证完成")
        
        print("\n🎯 学习收获:")
        print("• 掌握了 Parquet 文件格式的核心优势")
        print("• 理解了列式存储的性能优化原理")
        print("• 学会了选择合适的压缩算法")
        print("• 掌握了查询优化技术")
        print("• 理解了分区策略的重要性")
        print("• 了解了 Parquet 的高级特性")
        
        print("\n💡 实际应用建议:")
        print("• 在大数据分析场景中优先考虑 Parquet 格式")
        print("• 根据数据特征选择合适的压缩算法")
        print("• 设计合理的分区策略提升查询性能")
        print("• 利用投影下推和谓词下推优化查询")
        print("• 在数据管道中使用流式处理技术")
    
    def run_interactive(self) -> None:
        """Run interactive mode"""
        print("\n" + "="*60)
        print("🎯 欢迎使用 Parquet 实践项目交互模式！")
        print("="*60)
        
        while True:
            self.show_menu()
            try:
                choice = input("Please select exercise type (0-6): ").strip()
                
                if choice == '0':
                    print("👋 感谢使用 Parquet 实践项目！")
                    break
                elif choice == '1':
                    print("\n🚀 开始基础练习...")
                    num_records = self.get_record_count()
                    self.run_basic_exercise(num_records)
                elif choice == '2':
                    print("\n🚀 开始压缩算法练习...")
                    num_records = self.get_record_count()
                    self.run_compression_exercise(num_records)
                elif choice == '3':
                    print("\n🚀 开始查询优化练习...")
                    num_records = self.get_record_count()
                    self.run_query_optimization_exercise(num_records)
                elif choice == '4':
                    print("\n🚀 开始分区练习...")
                    num_records = self.get_record_count()
                    self.run_partitioning_exercise(num_records)
                elif choice == '5':
                    print("\n🚀 开始高级特性练习...")
                    self.run_advanced_exercise()
                elif choice == '6':
                    print("\n🚀 开始所有练习...")
                    num_records = self.get_record_count()
                    self.run_all_exercises(num_records)
                else:
                    print("❌ 无效选择，请输入 0-6 之间的数字")
                    
                if choice != '0':
                    input("\nPress Enter to continue...")
                    
            except KeyboardInterrupt:
                print("\n\n👋 程序被用户中断，再见！")
                break
            except Exception as e:
                print(f"❌ 发生错误：{e}")
                input("\nPress Enter to continue...")
    
    def get_record_count(self) -> int:
        """Get record count"""
        while True:
            try:
                count_input = input("Enter number of test records (default 10000): ").strip()
                if not count_input:
                    return 10000
                
                count = int(count_input)
                if count <= 0:
                    print("❌ 记录数量必须大于 0")
                    continue
                
                if count > 100000:
                    print("⚠️ 大量记录可能需要较长时间")
                    confirm = input("Continue? (y/N): ").strip().lower()
                    if confirm != 'y':
                        continue
                
                return count
                
            except ValueError:
                print("❌ 请输入有效的数字")
            except KeyboardInterrupt:
                return 10000


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Parquet 文件格式练习项目 - 学习和实践 Apache Parquet 列式存储格式',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
练习模块说明：
  basic       基础练习 - 学习 Parquet 文件的读写操作、数据类型支持
  compression 压缩练习 - 比较不同压缩算法（SNAPPY、GZIP、LZ4、BROTLI）的性能
  query       查询优化练习 - 学习列式存储的查询优化技术和谓词下推
  partition   分区练习 - 掌握 Parquet 文件分区策略和性能优化
  advanced    高级特性练习 - 探索嵌套数据结构、复杂数据类型处理
  all         运行所有练习 - 完整体验所有功能模块

使用示例：
  python main.py                           # 交互式模式
  python main.py -e basic -r 5000         # 运行基础练习，生成 5000 条记录
  python main.py -e all -r 10000          # 运行所有练习，每个模块 10000 条记录
  python main.py -i                       # 强制进入交互式模式
        """)
    parser.add_argument('--exercise', '-e', 
                       choices=['basic', 'compression', 'query', 'partition', 'advanced', 'all'],
                       help='直接运行指定的练习模块')
    parser.add_argument('--records', '-r', type=int, default=10000,
                       help='测试记录数量（默认：10000）')
    parser.add_argument('--output', '-o', default='output',
                       help='输出目录路径（默认：output）')
    parser.add_argument('--interactive', '-i', action='store_true',
                       help='启用交互式模式')
    
    args = parser.parse_args()
    
    # Create runner
    runner = ParquetPracticeRunner(args.output)
    
    if args.interactive or not args.exercise:
        # Interactive mode
        runner.run_interactive()
    else:
        # Command line mode
        if args.exercise == 'basic':
            runner.run_basic_exercise(args.records)
        elif args.exercise == 'compression':
            runner.run_compression_exercise(args.records)
        elif args.exercise == 'query':
            runner.run_query_optimization_exercise(args.records)
        elif args.exercise == 'partition':
            runner.run_partitioning_exercise(args.records)
        elif args.exercise == 'advanced':
            runner.run_advanced_exercise()
        elif args.exercise == 'all':
            runner.run_all_exercises(args.records)


if __name__ == '__main__':
    main()