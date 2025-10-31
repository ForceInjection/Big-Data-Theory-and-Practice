# Project Gutenberg 书籍下载系统

这是一个用于 MapReduce Word Count 性能测试的统一书籍下载和管理系统。

## 文件结构

```text
├── gutenberg_downloader.py    # 统一下载器（主程序）
├── book_catalog.json         # 书籍列表配置文件
├── merge_books.py            # 独立的合并脚本（备用）
├── data/                     # 数据目录
│   ├── books/               # 下载的书籍文件
│   └── all_books_merged.txt # 合并后的大文件
└── old_scripts/             # 旧版本脚本备份
    ├── download_books.py
    ├── download_books_extended.py
    ├── download_books_mega.py
    ├── download_final_books.py
    └── download_large_books.py
```

## 快速开始

### 1. 查看可用的书籍集合

```bash
python3 gutenberg_downloader.py --list
```

### 2. 下载书籍集合

```bash
# 下载基础集合（约100MB）
python3 gutenberg_downloader.py --download essential

# 下载扩展集合（约200MB）
python3 gutenberg_downloader.py --download extended

# 下载大型集合（约300MB）
python3 gutenberg_downloader.py --download mega

# 下载大型文学作品
python3 gutenberg_downloader.py --download large_works
```

### 3. 合并所有书籍

```bash
python3 gutenberg_downloader.py --merge
```

### 4. 一键下载并合并

```bash
python3 gutenberg_downloader.py --download mega --merge
```

## 书籍集合说明

| 集合名称    | 描述               | 书籍数量 | 目标大小 |
| ----------- | ------------------ | -------- | -------- |
| essential   | 最受欢迎的经典作品 | ~10 本   | 100MB    |
| extended    | 扩展书籍集合       | ~300 本  | 200MB    |
| mega        | 大型书籍集合       | ~600 本  | 300MB    |
| large_works | 大型文学作品       | ~5 本    | 50MB     |

## 配置文件

书籍列表和下载设置都在 `book_catalog.json` 中配置：

```json
{
  "metadata": {
    "name": "Project Gutenberg Book Catalog",
    "version": "1.0.0",
    "description": "用于MapReduce Word Count性能测试的书籍目录"
  },
  "download_settings": {
    "base_url": "https://www.gutenberg.org/files/{id}/{id}-0.txt",
    "fallback_url": "https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt",
    "output_directory": "data/books",
    "merge_output": "data/all_books_merged.txt",
    "retry_attempts": 3,
    "delay_between_downloads": 1
  },
  "book_collections": {
    "essential": {
      "description": "最受欢迎的经典作品（约100本）",
      "target_size_mb": 100,
      "books": [...]
    }
  }
}
```

## 高级用法

### 自定义配置文件

```bash
python3 gutenberg_downloader.py --config my_catalog.json --download essential
```

### 自定义输出文件

```bash
python3 gutenberg_downloader.py --merge --output my_merged_books.txt
```

### 查看帮助

```bash
python3 gutenberg_downloader.py --help
```

## 功能特性

- ✅ **统一管理**: 所有下载脚本合并为一个
- ✅ **配置驱动**: 书籍列表通过 JSON 文件管理
- ✅ **智能重试**: 自动重试失败的下载
- ✅ **进度显示**: 实时显示下载进度
- ✅ **文件去重**: 自动跳过已存在的文件
- ✅ **错误处理**: 完善的错误处理和日志
- ✅ **统计信息**: 详细的下载和合并统计
- ✅ **命令行界面**: 友好的命令行参数

## 在线书籍列表支持

系统预留了从网站动态获取书籍列表的功能：

```json
"online_sources": {
  "gutenberg_popular": {
    "url": "https://www.gutenberg.org/browse/scores/top",
    "description": "Project Gutenberg 热门书籍"
  },
  "gutenberg_recent": {
    "url": "https://www.gutenberg.org/browse/recent/last1",
    "description": "Project Gutenberg 最新书籍"
  }
}
```

## 性能优化

- 并发下载支持（可配置）
- 智能文件名清理
- 内存优化的文件合并
- 断点续传支持（计划中）

## 使用示例

```bash
# 完整的工作流程
python3 gutenberg_downloader.py --list                    # 查看集合
python3 gutenberg_downloader.py --download mega           # 下载大型集合
python3 gutenberg_downloader.py --merge                   # 合并所有书籍
ls -lh data/all_books_merged.txt                         # 检查结果
```

## 故障排除

1. **下载失败**: 检查网络连接，系统会自动重试
2. **文件权限**: 确保对 `data/` 目录有写权限
3. **配置错误**: 检查 `book_catalog.json` 格式是否正确
4. **依赖问题**: 确保 Python 3.6+ 和网络访问正常

---

**注意**: 此系统专为 MapReduce Word Count 性能测试设计，下载的书籍来自 Project Gutenberg 公共领域作品。
