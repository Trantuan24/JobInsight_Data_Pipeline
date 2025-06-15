import asyncio
import pandas as pd
import time
import os
import sys

# Thêm đường dẫn gốc dự án vào sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import modules
try:
    from src.utils.logger import get_logger
    from src.crawler.backup import backup_html_pages
    from src.crawler.parser import parse_html_files
    from src.crawler.crawler_utils import parse_last_update
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)
    from backup import backup_html_pages
    from parser import parse_html_files
    from src.crawler.crawler_utils import parse_last_update

logger = get_logger("crawler.main")

def crawl_jobs(num_pages=5):
    """Wrapper function để maintain compatibility với code cũ"""
    # Backup HTML
    asyncio.run(backup_html_pages(num_pages))
    
    # Parse HTML files
    return parse_html_files()

def get_job_listing(num_pages=5):
    """Wrapper function để maintain compatibility"""
    return crawl_jobs(num_pages)

if __name__ == "__main__":
    start_time = time.time()
    
    # Test backup và parse riêng biệt
    print("Phase 1: Backing up HTML pages...")
    backup_results = asyncio.run(backup_html_pages(5))
    
    print("\nPhase 2: Parsing HTML files...")
    df_results = parse_html_files()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    if not df_results.empty:
        print(f"\nCrawled {len(df_results)} job listings in {elapsed_time:.2f} seconds")
        print(f"Sample data:\n{df_results[['job_id', 'title', 'company_name']].head()}")
    else:
        print("No data crawled") 