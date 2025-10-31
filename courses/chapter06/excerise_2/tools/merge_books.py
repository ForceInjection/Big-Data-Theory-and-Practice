#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
书籍合并脚本 - 将data目录下的所有书籍文件合并为一个大文件
用于MapReduce Word Count性能测试
"""

import os
from pathlib import Path
import time

def merge_books():
    """合并所有书籍文件到一个大文件"""
    
    # 设置路径
    data_dir = Path("../data")
    books_dir = data_dir / "books"
    merged_file = data_dir / "all_books_merged.txt"
    
    # 确保数据目录存在
    if not data_dir.exists():
        print(f"❌ 错误: 数据目录 {data_dir} 不存在!")
        return False
    
    # 确保books子目录存在
    if not books_dir.exists():
        print(f"❌ 错误: books目录 {books_dir} 不存在!")
        return False
    
    # 获取books目录下的所有txt文件
    txt_files = sorted(books_dir.glob("*.txt"))
    
    if not txt_files:
        print("❌ 错误: 在data/books目录中没有找到任何txt文件!")
        return False
    
    print(f"📚 找到 {len(txt_files)} 个书籍文件")
    print("🔄 开始合并...")
    print("=" * 60)
    
    # 删除旧的合并文件（如果存在）
    if merged_file.exists():
        print(f"🗑️  删除旧的合并文件: {merged_file.name}")
        merged_file.unlink()
    
    # 统计信息
    total_books = 0
    total_size = 0
    start_time = time.time()
    
    # 合并文件
    with open(merged_file, 'w', encoding='utf-8') as outfile:
        for i, txt_file in enumerate(txt_files, 1):
            try:
                # 添加书籍分隔符
                book_marker = f"=== {txt_file.name} ==="
                outfile.write(f"\n\n{book_marker}\n")
                
                # 读取并写入书籍内容
                with open(txt_file, 'r', encoding='utf-8') as infile:
                    content = infile.read()
                    outfile.write(content)
                    
                    # 统计信息
                    file_size = len(content.encode('utf-8'))
                    total_size += file_size
                    total_books += 1
                    
                    # 显示进度
                    progress = (i / len(txt_files)) * 100
                    print(f"  [{progress:5.1f}%] 已合并: {txt_file.name} ({file_size/1024:.1f} KB)")
                    
            except Exception as e:
                print(f"  ❌ 错误读取 {txt_file.name}: {e}")
                continue
    
    # 计算处理时间
    end_time = time.time()
    processing_time = end_time - start_time
    
    # 显示最终结果
    print("\n" + "=" * 60)
    print("✅ 合并完成!")
    print(f"📊 统计信息:")
    print(f"   - 合并书籍数量: {total_books} 本")
    print(f"   - 总文件大小: {total_size/1024/1024:.1f} MB")
    print(f"   - 处理时间: {processing_time:.2f} 秒")
    print(f"   - 输出文件: {merged_file}")
    
    # 验证输出文件
    if merged_file.exists():
        actual_size = merged_file.stat().st_size / 1024 / 1024  # MB
        print(f"   - 实际文件大小: {actual_size:.1f} MB")
        
        # 检查是否达到目标大小
        target_size = 300  # MB
        if actual_size >= target_size:
            print(f"🎉 恭喜! 已达到 {target_size}MB 目标!")
        else:
            remaining = target_size - actual_size
            print(f"⚠️  距离 {target_size}MB 目标还差 {remaining:.1f}MB")
    
    return True

def verify_merged_file():
    """验证合并后的文件"""
    merged_file = Path("data/all_books_merged.txt")
    
    if not merged_file.exists():
        print("❌ 合并文件不存在!")
        return False
    
    print("\n🔍 验证合并文件...")
    
    # 统计书籍数量
    with open(merged_file, 'r', encoding='utf-8') as f:
        content = f.read()
        book_count = content.count("=== ") 
    
    # 文件大小
    file_size = merged_file.stat().st_size / 1024 / 1024  # MB
    
    # 行数统计
    with open(merged_file, 'r', encoding='utf-8') as f:
        line_count = sum(1 for _ in f)
    
    # 词数估算（简单统计）
    word_count = len(content.split())
    
    print(f"📈 验证结果:")
    print(f"   - 文件大小: {file_size:.1f} MB")
    print(f"   - 包含书籍: {book_count} 本")
    print(f"   - 总行数: {line_count:,} 行")
    print(f"   - 估算词数: {word_count:,} 个词")
    
    return True

def main():
    """主函数"""
    print("📖 书籍合并脚本")
    print("用途: 将data目录下的所有书籍文件合并为一个大文件，用于MapReduce测试")
    print("=" * 60)
    
    # 执行合并
    if merge_books():
        # 验证结果
        verify_merged_file()
        print("\n✅ 所有操作完成!")
    else:
        print("\n❌ 合并操作失败!")

if __name__ == "__main__":
    main()