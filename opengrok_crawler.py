"""
OpenGrok 代码爬虫
爬取 http://192.168.1.57:8189 上的代码，保留完整目录层级。
"""

import os
import sys
import time
import argparse
from urllib.parse import urljoin, quote
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ============ 配置 ============
BASE_URL = "http://192.168.1.57:8189"
XREF_PREFIX = "/source/xref/"
DOWNLOAD_PREFIX = "/source/download/"

# 项目根路径，比如 T813/v_sys/frameworks/base
DEFAULT_PROJECT_PATH = "T813/v_sys/frameworks/base"

# 并发数（文件下载）
MAX_WORKERS = 8

# 请求间隔（秒），避免打爆服务器
REQUEST_DELAY = 0.15

# 超时
TIMEOUT = 30

# 跳过的目录/文件名（大小写不敏感）
SKIP_NAMES = {
    ".git", ".hg", ".svn",
    "out", "gen",
}


# ============ 工具函数 ============

def make_session():
    """创建 requests session，带重试"""
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        max_retries=3,
        pool_connections=MAX_WORKERS + 4,
        pool_maxsize=MAX_WORKERS + 4,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_page(session, url, timeout=TIMEOUT):
    """GET 请求，返回 text 或 None"""
    try:
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"  [ERROR] 请求失败 {url}: {e}", file=sys.stderr)
        return None


def download_file(session, url, timeout=TIMEOUT):
    """下载原始文件，返回 bytes 或 None"""
    try:
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.content
    except Exception as e:
        print(f"  [ERROR] 下载失败 {url}: {e}", file=sys.stderr)
        return None


# ============ 目录解析 ============

def parse_directory(html):
    """
    解析 OpenGrok 目录列表页 HTML。
    返回: (folders: list[str], files: list[str])
    folders 和 files 都是相对路径，比如 "core/java"，"Android.bp"
    """
    soup = BeautifulSoup(html, "html.parser")
    folders = []
    files = []

    # OpenGrok 的目录列表在 <table class="dirlist"> 里
    # 每一行一个条目，第一列是图标/空，第二列是名称链接
    table = soup.find("table", class_="dirlist")
    if not table:
        # 备选：找包含文件列表的 table
        table = soup.find("table")

    if not table:
        print("  [WARN] 未找到目录表格", file=sys.stderr)
        return folders, files

    tbody = table.find("tbody")
    rows = tbody.find_all("tr") if tbody else table.find_all("tr")

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 2:
            continue

        # 第二个 td 是名称列
        name_cell = cells[1]
        link = name_cell.find("a")

        if not link:
            # ".." 之类的文本链接
            text = name_cell.get_text(strip=True)
            if text == "..":
                continue
            continue

        name = link.get("href", "")
        display_name = link.get_text(strip=True)

        # 跳过 ..
        if display_name == "..":
            continue

        # 判断是文件夹还是文件：
        # 文件夹链接以 / 结尾（或 href 以 / 结尾）
        # 文件有 [D] 下载链接
        is_folder = name.endswith("/") or display_name.endswith("/")

        # 也通过同行有没有 [D] download 链接来判断
        if not is_folder:
            name_cell_text = name_cell.get_text()
            if "[D]" in name_cell_text or "/a=true" in str(name_cell):
                is_folder = False
            # 再看一下 href：如果是 xref 链接且带 /，就是文件夹
            href = link.get("href", "")
            if "history" in href or href.endswith("/"):
                is_folder = True

        # 清理名称
        clean_name = display_name.rstrip("/")

        # 跳过
        if clean_name.lower() in SKIP_NAMES:
            continue

        # 从 href 提取路径
        # href 可能是相对路径，如 "core/java/" 或 "Android.bp"
        # 也可能是绝对路径，如 "/source/xref/T813/..."
        href = link.get("href", "")
        if href.startswith("/"):
            # 绝对路径，提取 xref 后面的部分
            if XREF_PREFIX in href:
                rel = href.split(XREF_PREFIX, 1)[1]
                # 去掉项目前缀
                # rel 类似 "T813/v_sys/frameworks/base/core/java/"
                # 但我们只想要相对当前目录的部分
                # 实际上 href 在 dirlist 里通常是相对路径
                pass
            # 尝试从 href 末尾提取相对名
            # 对于绝对路径，直接用 display_name
            if is_folder:
                folders.append(clean_name)
            else:
                files.append(clean_name)
        else:
            # 相对路径
            if is_folder:
                folders.append(clean_name)
            else:
                files.append(clean_name)

    return folders, files


# ============ 主爬虫 ============

class OpenGrokCrawler:
    def __init__(self, project_path, output_dir, max_workers=MAX_WORKERS, delay=REQUEST_DELAY):
        self.project_path = project_path.strip("/")
        self.output_dir = Path(output_dir)
        self.session = make_session()
        self.max_workers = max_workers
        self.delay = delay
        self.stats = {
            "dirs": 0,
            "files": 0,
            "errors": 0,
            "skipped": 0,
        }

    def xref_url(self, rel_path=""):
        """构建 xref 目录列表 URL"""
        path = f"{self.project_path}/{rel_path}" if rel_path else self.project_path
        return f"{BASE_URL}{XREF_PREFIX}{path}/"

    def download_url(self, file_rel_path):
        """构建文件下载 URL"""
        return f"{BASE_URL}{DOWNLOAD_PREFIX}{self.project_path}/{file_rel_path}"

    def local_path(self, rel_path):
        """项目内相对路径 → 本地输出路径"""
        return self.output_dir / rel_path

    def crawl_directory(self, rel_path=""):
        """递归爬取一个目录，返回 (sub_dirs, files) 列表"""
        url = self.xref_url(rel_path)
        html = get_page(self.session, url)
        if html is None:
            self.stats["errors"] += 1
            return [], []

        folders, files = parse_directory(html)
        self.stats["dirs"] += 1

        if rel_path:
            print(f"📂 {rel_path}/ ({len(folders)} dirs, {len(files)} files)")
        else:
            print(f"📂 / ({len(folders)} dirs, {len(files)} files)")

        return folders, files

    def crawl_file(self, file_rel_path):
        """下载单个文件并保存"""
        url = self.download_url(file_rel_path)
        content = download_file(self.session, url)
        if content is None:
            self.stats["errors"] += 1
            return

        local = self.local_path(file_rel_path)
        local.parent.mkdir(parents=True, exist_ok=True)

        try:
            local.write_bytes(content)
            self.stats["files"] += 1
        except Exception as e:
            print(f"  ❌ 写入失败 {file_rel_path}: {e}", file=sys.stderr)
            self.stats["errors"] += 1

    def run(self):
        """主入口：BFS 遍历目录，边扫描边并发下载文件"""
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 下载线程池
        download_executor = ThreadPoolExecutor(max_workers=self.max_workers)
        # 统计锁
        stats_lock = threading.Lock()
        pending_futures = []

        def submit_file(file_rel_path):
            pending_futures.append(
                download_executor.submit(self.crawl_file, file_rel_path)
            )

        # BFS 队列
        dir_queue = [""]  # 空字符串 = 项目根
        total_files = 0

        print(f"🚀 开始爬取: {BASE_URL}{XREF_PREFIX}{self.project_path}/")
        print(f"📁 输出目录: {self.output_dir}")
        print(f"{'='*60}")

        try:
            while dir_queue:
                rel_path = dir_queue.pop(0)
                url = self.xref_url(rel_path)
                html = get_page(self.session, url)
                if html is None:
                    with stats_lock:
                        self.stats["errors"] += 1
                    continue

                folders, files = parse_directory(html)
                with stats_lock:
                    self.stats["dirs"] += 1

                # 立即提交文件下载
                for file_name in files:
                    file_path = f"{rel_path}/{file_name}" if rel_path else file_name
                    submit_file(file_path)
                    total_files += 1

                # 子目录加入队列
                for folder in folders:
                    child_path = f"{rel_path}/{folder}" if rel_path else folder
                    dir_queue.append(child_path)

                # 实时进度
                if self.stats["dirs"] % 100 == 0:
                    dl_done = self.stats["files"]
                    print(f"  已扫描 {self.stats['dirs']} 个目录, 发现 {total_files} 文件, 已下载 {dl_done}")

                time.sleep(self.delay)
        finally:
            # 等待所有下载完成
            print(f"\n{'='*60}")
            print(f"📂 目录扫描完成: {self.stats['dirs']}")
            print(f"📄 待下载文件: {total_files}")
            print(f"⬇️ 等待剩余下载完成...")

            done = 0
            for f in as_completed(pending_futures):
                done += 1
                if done % 100 == 0:
                    print(f"  下载进度: {done}/{total_files}")

            download_executor.shutdown(wait=True)

        # 总结
        print(f"\n{'='*60}")
        print(f"✅ 爬取完成!")
        print(f"  📂 目录: {self.stats['dirs']}")
        print(f"  📄 文件: {self.stats['files']}")
        print(f"  ❌ 错误: {self.stats['errors']}")
        print(f"  📁 输出: {self.output_dir.resolve()}")


# ============ CLI ============

def main():
    parser = argparse.ArgumentParser(
        description="OpenGrok 代码爬虫 - 爬取指定项目的完整源码",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 爬取默认项目
  python opengrok_crawler.py

  # 爬取指定项目路径
  python opengrok_crawler.py -p T813/v_sys/frameworks/base/core

  # 指定输出目录和并发数
  python opengrok_crawler.py -o ./my_code -w 16
        """,
    )
    parser.add_argument(
        "-p", "--project",
        default=DEFAULT_PROJECT_PATH,
        help=f"项目路径 (默认: {DEFAULT_PROJECT_PATH})",
    )
    parser.add_argument(
        "-o", "--output",
        default="./opengrok_output",
        help="输出目录 (默认: ./opengrok_output)",
    )
    parser.add_argument(
        "-w", "--workers",
        type=int,
        default=MAX_WORKERS,
        help=f"并发下载线程数 (默认: {MAX_WORKERS})",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=REQUEST_DELAY,
        help=f"请求间隔秒数 (默认: {REQUEST_DELAY})",
    )

    args = parser.parse_args()

    crawler = OpenGrokCrawler(
        project_path=args.project,
        output_dir=args.output,
        max_workers=args.workers,
        delay=args.delay,
    )
    crawler.run()


if __name__ == "__main__":
    main()
