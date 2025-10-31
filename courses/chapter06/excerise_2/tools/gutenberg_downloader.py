#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Project Gutenberg 统一书籍下载器
支持从配置文件和在线源获取书籍列表，用于MapReduce Word Count性能测试

特性:
- 支持JSON配置文件管理书籍列表
- 支持多种书籍集合（essential, extended, mega, large_works）
- 支持从网站动态获取书籍列表
- 智能重试和错误处理
- 进度显示和统计信息
- 自动合并功能
"""

import os
import json
import urllib.request
import urllib.error
import time
import sys
import argparse
import re
from pathlib import Path
from urllib.parse import urljoin
from typing import Dict, List, Optional, Tuple

class GutenbergDownloader:
    """Project Gutenberg 书籍下载器"""
    
    def __init__(self, config_file: str = "book_catalog.json"):
        """初始化下载器
        
        Args:
            config_file: 配置文件路径
        """
        self.config_file = config_file
        self.config = self._load_config()
        self.download_settings = self.config.get("download_settings", {})
        self.stats = {
            "total": 0,
            "downloaded": 0,
            "failed": 0,
            "skipped": 0,
            "total_size": 0
        }
    
    def _load_config(self) -> Dict:
        """加载配置文件"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"❌ 配置文件 {self.config_file} 不存在!")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"❌ 配置文件格式错误: {e}")
            sys.exit(1)
    
    def _create_output_directory(self) -> Path:
        """创建输出目录"""
        output_dir = Path(self.download_settings.get("output_directory", "data/books"))
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir
    
    def _clean_filename(self, text: str) -> str:
        """清理文件名，移除特殊字符"""
        # 移除或替换特殊字符
        text = re.sub(r'[<>:"/\\|?*]', '', text)
        text = re.sub(r'[^\w\s-]', '', text)
        text = re.sub(r'[-\s]+', '_', text)
        return text.strip('_')
    
    def _download_book(self, book_info: Dict, output_dir: Path) -> Tuple[bool, int]:
        """下载单本书籍
        
        Args:
            book_info: 书籍信息字典
            output_dir: 输出目录
            
        Returns:
            (是否成功, 文件大小)
        """
        book_id = book_info["id"]
        title = book_info["title"]
        author = book_info["author"]
        
        # 生成文件名
        clean_title = self._clean_filename(title)
        clean_author = self._clean_filename(author)
        filename = f"{book_id}_{clean_title}_{clean_author}.txt"
        filepath = output_dir / filename
        
        # 检查文件是否已存在
        if filepath.exists():
            file_size = filepath.stat().st_size
            print(f"  ⏭️  跳过已存在: {filename} ({file_size/1024:.1f} KB)")
            self.stats["skipped"] += 1
            return True, file_size
        
        # 尝试下载
        urls = [
            self.download_settings["base_url"].format(id=book_id),
            self.download_settings["fallback_url"].format(id=book_id)
        ]
        
        retry_attempts = self.download_settings.get("retry_attempts", 3)
        
        for attempt in range(retry_attempts):
            for url in urls:
                try:
                    print(f"  📥 下载中: {title} (尝试 {attempt + 1}/{retry_attempts})")
                    
                    with urllib.request.urlopen(url, timeout=30) as response:
                        content = response.read().decode('utf-8', errors='ignore')
                        
                        # 写入文件
                        with open(filepath, 'w', encoding='utf-8') as f:
                            f.write(content)
                        
                        file_size = len(content.encode('utf-8'))
                        print(f"  ✅ 完成: {filename} ({file_size/1024:.1f} KB)")
                        self.stats["downloaded"] += 1
                        self.stats["total_size"] += file_size
                        
                        # 下载间隔
                        delay = self.download_settings.get("delay_between_downloads", 1)
                        if delay > 0:
                            time.sleep(delay)
                        
                        return True, file_size
                        
                except urllib.error.HTTPError as e:
                    if e.code == 404:
                        print(f"  ⚠️  URL不存在 (404): {url}")
                        continue
                    else:
                        print(f"  ❌ HTTP错误 {e.code}: {url}")
                        
                except Exception as e:
                    print(f"  ❌ 下载失败: {e}")
                    
                # 重试前等待
                if attempt < retry_attempts - 1:
                    time.sleep(2)
        
        print(f"  ❌ 下载失败: {title}")
        self.stats["failed"] += 1
        return False, 0
    
    def download_collection(self, collection_name: str) -> bool:
        """下载指定的书籍集合
        
        Args:
            collection_name: 集合名称 (essential, extended, mega, large_works)
            
        Returns:
            是否成功
        """
        collections = self.config.get("book_collections", {})
        
        if collection_name not in collections:
            print(f"❌ 未找到集合: {collection_name}")
            print(f"可用集合: {list(collections.keys())}")
            return False
        
        collection = collections[collection_name]
        books = collection.get("books", [])
        
        if not books:
            print(f"❌ 集合 {collection_name} 中没有书籍")
            return False
        
        print(f"📚 开始下载集合: {collection_name}")
        print(f"📖 描述: {collection.get('description', '')}")
        print(f"🎯 目标大小: {collection.get('target_size_mb', 0)} MB")
        print(f"📊 书籍数量: {len(books)} 本")
        print("=" * 60)
        
        # 创建输出目录
        output_dir = self._create_output_directory()
        
        # 重置统计
        self.stats = {
            "total": len(books),
            "downloaded": 0,
            "failed": 0,
            "skipped": 0,
            "total_size": 0
        }
        
        start_time = time.time()
        
        # 下载书籍
        for i, book in enumerate(books, 1):
            progress = (i / len(books)) * 100
            print(f"[{progress:5.1f}%] ({i}/{len(books)})")
            
            success, file_size = self._download_book(book, output_dir)
            
            if not success:
                continue
        
        # 显示统计信息
        end_time = time.time()
        processing_time = end_time - start_time
        
        print("\n" + "=" * 60)
        print("✅ 下载完成!")
        print(f"📊 统计信息:")
        print(f"   - 总书籍数: {self.stats['total']} 本")
        print(f"   - 成功下载: {self.stats['downloaded']} 本")
        print(f"   - 跳过已存在: {self.stats['skipped']} 本")
        print(f"   - 下载失败: {self.stats['failed']} 本")
        print(f"   - 总大小: {self.stats['total_size']/1024/1024:.1f} MB")
        print(f"   - 处理时间: {processing_time:.2f} 秒")
        print(f"   - 输出目录: {output_dir}")
        
        return True
    
    def merge_books(self, output_file: Optional[str] = None) -> bool:
        """合并所有下载的书籍为一个文件
        
        Args:
            output_file: 输出文件路径，默认使用配置文件中的设置
            
        Returns:
            是否成功
        """
        if output_file is None:
            output_file = self.download_settings.get("merge_output", "data/all_books_merged.txt")
        
        output_path = Path(output_file)
        books_dir = Path(self.download_settings.get("output_directory", "data/books"))
        
        if not books_dir.exists():
            print(f"❌ 书籍目录不存在: {books_dir}")
            return False
        
        # 获取所有txt文件
        txt_files = sorted(books_dir.glob("*.txt"))
        
        if not txt_files:
            print(f"❌ 在 {books_dir} 中没有找到任何txt文件")
            return False
        
        print(f"📚 开始合并 {len(txt_files)} 个书籍文件...")
        print(f"📁 输出文件: {output_path}")
        print("=" * 60)
        
        # 确保输出目录存在
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 删除旧文件
        if output_path.exists():
            output_path.unlink()
        
        total_size = 0
        start_time = time.time()
        
        # 合并文件
        with open(output_path, 'w', encoding='utf-8') as outfile:
            for i, txt_file in enumerate(txt_files, 1):
                try:
                    # 添加书籍分隔符
                    book_marker = f"=== {txt_file.name} ==="
                    outfile.write(f"\n\n{book_marker}\n")
                    
                    # 读取并写入内容
                    with open(txt_file, 'r', encoding='utf-8') as infile:
                        content = infile.read()
                        outfile.write(content)
                        
                        file_size = len(content.encode('utf-8'))
                        total_size += file_size
                        
                        progress = (i / len(txt_files)) * 100
                        print(f"  [{progress:5.1f}%] 已合并: {txt_file.name} ({file_size/1024:.1f} KB)")
                        
                except Exception as e:
                    print(f"  ❌ 错误处理 {txt_file.name}: {e}")
                    continue
        
        # 显示结果
        end_time = time.time()
        processing_time = end_time - start_time
        actual_size = output_path.stat().st_size / 1024 / 1024  # MB
        
        print("\n" + "=" * 60)
        print("✅ 合并完成!")
        print(f"📊 统计信息:")
        print(f"   - 合并书籍: {len(txt_files)} 本")
        print(f"   - 文件大小: {actual_size:.1f} MB")
        print(f"   - 处理时间: {processing_time:.2f} 秒")
        print(f"   - 输出文件: {output_path}")
        
        return True
    
    def list_collections(self):
        """列出所有可用的书籍集合"""
        collections = self.config.get("book_collections", {})
        
        print("📚 可用的书籍集合:")
        print("=" * 60)
        
        for name, info in collections.items():
            books_count = len(info.get("books", []))
            target_size = info.get("target_size_mb", 0)
            description = info.get("description", "")
            
            print(f"🔖 {name}")
            print(f"   描述: {description}")
            print(f"   书籍数量: {books_count} 本")
            print(f"   目标大小: {target_size} MB")
            print()
    
    def update_from_online(self, source_name: str = "gutenberg_popular") -> bool:
        """从在线源更新书籍列表（预留功能）
        
        Args:
            source_name: 在线源名称
            
        Returns:
            是否成功
        """
        print(f"🌐 从在线源更新书籍列表: {source_name}")
        print("⚠️  此功能正在开发中...")
        return False

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="Project Gutenberg 统一书籍下载器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  %(prog)s --list                    # 列出所有可用集合
  %(prog)s --download essential      # 下载基础集合
  %(prog)s --download mega           # 下载大型集合
  %(prog)s --merge                   # 合并所有书籍
  %(prog)s --download essential --merge  # 下载并合并
        """
    )
    
    parser.add_argument("--config", "-c", 
                       default="book_catalog.json",
                       help="配置文件路径 (默认: book_catalog.json)")
    
    parser.add_argument("--list", "-l", 
                       action="store_true",
                       help="列出所有可用的书籍集合")
    
    parser.add_argument("--download", "-d",
                       metavar="COLLECTION",
                       help="下载指定的书籍集合 (essential, extended, mega, large_works)")
    
    parser.add_argument("--merge", "-m",
                       action="store_true", 
                       help="合并所有下载的书籍为一个文件")
    
    parser.add_argument("--output", "-o",
                       metavar="FILE",
                       help="合并输出文件路径")
    
    args = parser.parse_args()
    
    # 创建下载器
    try:
        downloader = GutenbergDownloader(args.config)
    except SystemExit:
        return 1
    
    # 执行操作
    if args.list:
        downloader.list_collections()
        return 0
    
    if args.download:
        success = downloader.download_collection(args.download)
        if not success:
            return 1
    
    if args.merge:
        success = downloader.merge_books(args.output)
        if not success:
            return 1
    
    if not any([args.list, args.download, args.merge]):
        parser.print_help()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())