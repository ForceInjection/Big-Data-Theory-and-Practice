# Project Gutenberg 书籍下载系统

这是一个用于 MapReduce Word Count 性能测试的书籍下载和管理系统，包含两个主要脚本：

- **gutenberg_downloader.py**: 统一的书籍下载器，支持配置文件管理、多种书籍集合下载和自动合并功能
- **merge_books.py**: 独立的书籍合并脚本，专门用于将已下载的书籍文件合并为一个大文件

两个脚本可以独立使用，也可以配合使用，为 MapReduce 性能测试提供不同大小的数据集。

## 1. 文件结构

```text
├── gutenberg_downloader.py    # 主下载器 - 支持配置文件管理和自动合并
├── merge_books.py            # 独立合并脚本 - 专门用于合并已下载的书籍
├── book_catalog.json         # 书籍列表配置文件（gutenberg_downloader.py 使用）
├── README.md                 # 项目说明文档
└── data/                     # 数据目录（自动创建）
    ├── books/               # 下载的书籍文件存储目录
    └── all_books_merged.txt # 合并后的大文件
```

## 2. 快速开始

### 2.1 方法一：使用 gutenberg_downloader.py（推荐）

#### 2.1.1 查看可用的书籍集合

```bash
python3 gutenberg_downloader.py --list
# 或使用短参数
python3 gutenberg_downloader.py -l
```

#### 2.1.2 下载书籍集合

```bash
# 下载基础集合（约100MB）
python3 gutenberg_downloader.py --download essential

# 下载扩展集合（约200MB）
python3 gutenberg_downloader.py --download extended

# 下载大型集合（约300MB）
python3 gutenberg_downloader.py --download mega

# 下载大型文学作品
python3 gutenberg_downloader.py --download large_works

# 使用短参数
python3 gutenberg_downloader.py -d mega
```

#### 2.1.3 合并所有书籍

```bash
# 使用默认输出文件
python3 gutenberg_downloader.py --merge

# 指定输出文件
python3 gutenberg_downloader.py --merge --output my_books.txt

# 使用短参数
python3 gutenberg_downloader.py -m -o my_books.txt
```

#### 2.1.4 一键下载并合并

```bash
python3 gutenberg_downloader.py --download mega --merge
```

### 2.2 方法二：使用 merge_books.py（独立合并）

如果已经有下载的书籍文件，可以直接使用独立的合并脚本：

```bash
python3 merge_books.py
```

此脚本会：

- 自动查找 `data/books/` 目录下的所有 `.txt` 文件
- 将它们合并到 `data/all_books_merged.txt`
- 显示详细的统计信息和验证结果

## 3. 书籍集合说明

| 集合名称    | 描述               | 书籍数量 | 目标大小 |
| ----------- | ------------------ | -------- | -------- |
| essential   | 最受欢迎的经典作品 | ~10 本   | 100MB    |
| extended    | 扩展书籍集合       | ~300 本  | 200MB    |
| mega        | 大型书籍集合       | ~600 本  | 300MB    |
| large_works | 大型文学作品       | ~5 本    | 50MB     |

## 4. 配置文件

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

## 5. 脚本详细说明

### 5.1 gutenberg_downloader.py

这是主要的下载器脚本，提供完整的书籍下载和管理功能。

**主要功能：**

- 从 Project Gutenberg 下载书籍
- 支持多种预设的书籍集合
- 自动文件名清理和去重
- 智能重试机制
- 内置合并功能

**命令行参数：**

```bash
python3 gutenberg_downloader.py [选项]

选项:
  -h, --help            显示帮助信息
  -c CONFIG, --config CONFIG
                        配置文件路径 (默认: book_catalog.json)
  -l, --list            列出所有可用的书籍集合
  -d COLLECTION, --download COLLECTION
                        下载指定的书籍集合 (essential, extended, mega, large_works)
  -m, --merge           合并所有下载的书籍为一个文件
  -o FILE, --output FILE
                        合并输出文件路径
```

**使用示例：**

```bash
# 查看帮助
python3 gutenberg_downloader.py --help

# 列出可用集合
python3 gutenberg_downloader.py --list

# 下载并合并
python3 gutenberg_downloader.py --download mega --merge

# 使用自定义配置和输出
python3 gutenberg_downloader.py --config my_config.json --download essential --merge --output my_books.txt
```

### 5.2 merge_books.py

这是独立的合并脚本，专门用于合并已下载的书籍文件。

**主要功能：**

- 扫描 `data/books/` 目录下的所有 txt 文件
- 将文件合并为一个大文件
- 添加书籍分隔标记
- 显示详细统计和验证信息

**使用方法：**

```bash
# 直接运行，无需参数
python3 merge_books.py
```

**输出信息：**

- 合并进度显示
- 文件大小统计
- 处理时间
- 验证结果（行数、词数等）

## 6. 高级用法

### 6.1 自定义配置文件

```bash
python3 gutenberg_downloader.py --config my_catalog.json --download essential
```

### 6.2 自定义输出文件

```bash
python3 gutenberg_downloader.py --merge --output my_merged_books.txt
```

### 6.3 查看帮助

```bash
python3 gutenberg_downloader.py --help
```

## 7. 功能特性

### 7.1 gutenberg_downloader.py 特性

- ✅ **配置驱动**: 通过 JSON 文件管理书籍列表和下载设置
- ✅ **多集合支持**: 支持 essential、extended、mega、large_works 四种预设集合
- ✅ **智能重试**: 自动重试失败的下载，支持多个 URL 备选
- ✅ **进度显示**: 实时显示下载进度和百分比
- ✅ **文件去重**: 自动跳过已存在的文件，避免重复下载
- ✅ **错误处理**: 完善的错误处理和异常捕获
- ✅ **统计信息**: 详细的下载统计（成功、失败、跳过、总大小、处理时间）
- ✅ **命令行界面**: 支持长短参数，友好的帮助信息
- ✅ **文件名清理**: 自动清理特殊字符，生成合法的文件名
- ✅ **自动合并**: 内置合并功能，可与下载一键完成
- ✅ **自定义输出**: 支持自定义配置文件和输出文件路径

### 7.2 merge_books.py 特性

- ✅ **独立运行**: 无需配置文件，直接处理已下载的书籍
- ✅ **自动发现**: 自动扫描 `data/books/` 目录下的所有 txt 文件
- ✅ **书籍分隔**: 在合并文件中添加清晰的书籍分隔标记
- ✅ **详细统计**: 显示合并书籍数量、文件大小、处理时间等信息
- ✅ **文件验证**: 合并完成后自动验证文件并显示统计信息
- ✅ **目标检查**: 检查是否达到预设的文件大小目标（300MB）
- ✅ **错误容错**: 单个文件读取失败不影响整体合并过程

## 8. 在线书籍列表支持

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

## 9. 性能优化

- 并发下载支持（可配置）
- 智能文件名清理
- 内存优化的文件合并
- 断点续传支持（计划中）

## 10. 使用示例

```bash
# 完整的工作流程
python3 gutenberg_downloader.py --list                    # 查看集合
python3 gutenberg_downloader.py --download mega           # 下载大型集合
python3 gutenberg_downloader.py --merge                   # 合并所有书籍
ls -lh data/all_books_merged.txt                         # 检查结果
```

## 11. 故障排除

1. **下载失败**: 检查网络连接，系统会自动重试
2. **文件权限**: 确保对 `data/` 目录有写权限
3. **配置错误**: 检查 `book_catalog.json` 格式是否正确
4. **依赖问题**: 确保 Python 3.6+ 和网络访问正常

---

**注意**: 此系统专为 MapReduce Word Count 性能测试设计，下载的书籍来自 Project Gutenberg 公共领域作品。
