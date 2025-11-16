"""
工具模块

提供数据生成和性能分析的通用功能。
"""

import pandas as pd
import numpy as np
import time
import os
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional


class DataGenerator:
    """数据生成器类"""
    
    def __init__(self, seed: int = 42):
        """
        初始化数据生成器
        
        Args:
            seed: 随机种子，确保结果可重现
        """
        self.seed = seed
        np.random.seed(seed)
        self.cities = ['Beijing', 'Shanghai', 'Guangzhou', 'Shenzhen', 'Hangzhou', 'Nanjing', 'Chengdu', 'Wuhan', 'Xian', 'Chongqing']
        
    def generate_user_data(self, num_records: int = 100000) -> pd.DataFrame:
        """
        生成用户数据
        
        Args:
            num_records: 记录数量
            
        Returns:
            包含用户数据的 DataFrame
        """
        print(f"正在生成 {num_records} 条用户记录...")
        
        data = {
            'UserID': range(1, num_records + 1),
            'Username': [f'User_{i:06d}' for i in range(1, num_records + 1)],
            'Age': np.random.randint(18, 80, num_records),
            'City': np.random.choice(self.cities, num_records),
            'RegisterTime': [
                datetime.now() - timedelta(days=np.random.randint(0, 365))
                for _ in range(num_records)
            ],
            'Income': np.random.normal(50000, 20000, num_records).round(2)
        }
        
        df = pd.DataFrame(data)
        print("数据生成完成！")
        return df
    
    def generate_nested_data(self, num_records: int = 10000) -> pd.DataFrame:
        """
        生成包含嵌套结构的数据
        
        Args:
            num_records: 记录数量
            
        Returns:
            包含嵌套数据的 DataFrame
        """
        print(f"正在生成 {num_records} 条嵌套数据记录...")
        
        data = []
        for i in range(1, num_records + 1):
            # Generate contact list
            num_contacts = np.random.randint(1, 4)  # 1-3 contacts
            contacts = []
            for _ in range(num_contacts):
                contacts.append({
                    'type': np.random.choice(['Mobile', 'Email', 'WeChat']),
                    'value': f'contact_{np.random.randint(10000, 99999)}'
                })
            
            # Generate address information
            address = {
                'province': np.random.choice(['Beijing', 'Shanghai', 'Guangdong', 'Zhejiang', 'Jiangsu']),
                'city': np.random.choice(self.cities[:5]),  # Use first 5 cities
                'district': f'District_{np.random.randint(1, 10)}',
                'street': f'Street_{np.random.randint(1, 100)}'
            }
            
            data.append({
                'UserID': i,
                'Username': f'User_{i:06d}',
                'Age': np.random.randint(18, 80),
                'Contacts': contacts,
                'Address': address,
                'Tags': np.random.choice(['VIP', 'Regular', 'New'], size=np.random.randint(1, 3)).tolist()
            })
        
        df = pd.DataFrame(data)
        print("嵌套数据生成完成！")
        return df


class PerformanceAnalyzer:
    """性能分析器类"""
    
    def __init__(self):
        """初始化性能分析器"""
        self.results = {}
        
    def measure_time(self, func, *args, **kwargs) -> tuple:
        """
        测量函数执行时间
        
        Args:
            func: 要测量的函数
            *args: 函数参数
            **kwargs: 函数关键字参数
            
        Returns:
            (结果, 执行时间)
        """
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = time.time() - start_time
        return result, execution_time
    
    def get_file_size(self, filename: str) -> float:
        """
        获取文件大小（MB）
        
        Args:
            filename: 文件名
            
        Returns:
            文件大小（MB）
        """
        if os.path.exists(filename):
            return os.path.getsize(filename) / (1024 * 1024)
        return 0.0
    
    def compare_performance(self, results: Dict[str, Dict[str, float]], 
                          title: str = "Performance Comparison") -> None:
        """
        显示性能对比结果
        
        Args:
            results: 性能结果字典
            title: 对比标题
        """
        print(f"\n{'=' * 80}")
        print(f"{title}")
        print(f"{'=' * 80}")
        
        # 确定列宽
        max_name_len = max(len(name) for name in results.keys())
        name_width = max(max_name_len, 10)
        
        # 打印表头
        header = f"{'方法':<{name_width}}"
        for key in next(iter(results.values())).keys():
            if 'time' in key.lower():
                header += f" {key + '(秒)':<12}"
            elif 'size' in key.lower():
                header += f" {key + '(MB)':<12}"
            else:
                header += f" {key:<12}"
        print(header)
        print("-" * len(header))
        
        # 打印数据
        for name, metrics in results.items():
            row = f"{name:<{name_width}}"
            for value in metrics.values():
                if isinstance(value, float):
                    row += f" {value:<12.4f}"
                elif isinstance(value, list):
                    row += f" {len(value):<12}"
                else:
                    row += f" {str(value):<12}"
            print(row)
    
    def plot_performance_comparison(self, results: Dict[str, Dict[str, float]], 
                                  metric: str, title: str = "Performance Comparison") -> None:
        """
        绘制性能对比图
        
        Args:
            results: 性能结果字典
            metric: 要绘制的指标
            title: 图表标题
        """
        plt.figure(figsize=(10, 6))
        
        methods = list(results.keys())
        values = [results[method][metric] for method in methods]
        
        bars = plt.bar(methods, values, alpha=0.7)
        plt.title(title)
        plt.ylabel(metric)
        plt.xticks(rotation=45)
        
        # 在柱状图上显示数值
        for bar, value in zip(bars, values):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(values)*0.01,
                    f'{value:.4f}', ha='center', va='bottom')
        
        plt.tight_layout()
        plt.show()
    
    def save_results(self, results: Dict[str, Any], filename: str) -> None:
        """
        保存结果到文件
        
        Args:
            results: 结果字典
            filename: 保存文件名
        """
        import json
        
        # 转换 numpy 类型为 Python 原生类型
        def convert_numpy(obj):
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, dict):
                return {key: convert_numpy(value) for key, value in obj.items()}
            elif isinstance(obj, list):
                return [convert_numpy(item) for item in obj]
            return obj
        
        converted_results = convert_numpy(results)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(converted_results, f, ensure_ascii=False, indent=2)
        
        print(f"结果已保存到：{filename}")


def verify_data_integrity(original_df: pd.DataFrame, read_df: pd.DataFrame) -> bool:
    """
    验证数据完整性
    
    Args:
        original_df: 原始数据
        read_df: 读取的数据
        
    Returns:
        是否完整
    """
    print("正在验证数据完整性...")
    
    # Check row count
    if len(original_df) != len(read_df):
        print(f"❌ 行数不匹配：原始 {len(original_df)} 行，读取 {len(read_df)} 行")
        return False
    
    # Check column count
    if len(original_df.columns) != len(read_df.columns):
        print(f"❌ 列数不匹配：原始 {len(original_df.columns)} 列，读取 {len(read_df.columns)} 列")
        return False
    
    # Check column names
    for col in original_df.columns:
        if col not in read_df.columns:
            print(f"❌ 缺少列 '{col}'")
            return False
    
    print("✅ 数据完整性验证通过！")
    return True


def cleanup_files(file_patterns: List[str]) -> None:
    """
    Clean up temporary files
    
    Args:
        file_patterns: List of file patterns
    """
    import glob
    
    for pattern in file_patterns:
        files = glob.glob(pattern)
        for file in files:
            try:
                if os.path.isfile(file):
                    os.remove(file)
                    print(f"已删除文件：{file}")
                elif os.path.isdir(file):
                    import shutil
                    shutil.rmtree(file)
                    print(f"已删除目录：{file}")
            except Exception as e:
                print(f"删除 {file} 时出错：{e}")