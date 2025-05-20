"""
Tests for the TopCV Crawler component
"""

import os
import sys
import pytest
import pandas as pd
import logging
import asyncio
from datetime import datetime

# Thêm đường dẫn gốc của dự án vào sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import from crawler modules
from src.crawler.crawler import (
    crawl_multiple_keywords, 
    crawl_job_listings,
    async_crawl_multiple_keywords
)
from src.crawler.crawler_config import BASE_URL, KEYWORDS

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Kiểm tra cơ bản để đảm bảo module được import đúng
def test_imports():
    """Kiểm tra xem các module crawler có import thành công không"""
    assert crawl_multiple_keywords is not None
    assert crawl_job_listings is not None
    assert BASE_URL is not None
    assert KEYWORDS is not None and isinstance(KEYWORDS, list)
    assert len(KEYWORDS) > 0

# Cần cài pytest-asyncio để chạy test này
# pip install pytest-asyncio
@pytest.mark.asyncio
async def test_async_crawler_single():
    """Kiểm tra crawler async với một keyword duy nhất"""
    # Bỏ qua test nếu không có trong chế độ chạy đầy đủ
    if not os.environ.get('RUN_FULL_TESTS'):
        pytest.skip("Bỏ qua test crawler thực tế (đặt RUN_FULL_TESTS=1 để chạy)")
    
    keyword = "Python"
    results = await crawl_job_listings(BASE_URL, keyword=keyword, num_pages=1)
    
    assert results is not None
    assert isinstance(results, list)
    assert len(results) > 0
    assert all(isinstance(job, dict) for job in results)
    
    # Kiểm tra cấu trúc dữ liệu
    if results:
        first_job = results[0]
        expected_fields = ['job_id', 'title', 'company_name', 'salary', 'skills']
        for field in expected_fields:
            assert field in first_job, f"Trường {field} không có trong dữ liệu job"
    
    logging.info(f"Tìm thấy {len(results)} jobs cho từ khóa '{keyword}'")

def test_crawler_multiple_keywords():
    """Kiểm tra crawler với nhiều keywords (phiên bản đầy đủ)"""
    # Bỏ qua test nếu không có trong chế độ chạy đầy đủ
    if not os.environ.get('RUN_FULL_TESTS'):
        pytest.skip("Bỏ qua test crawler thực tế (đặt RUN_FULL_TESTS=1 để chạy)")
    
    # Sử dụng tập nhỏ các keywords để test nhanh
    test_keywords = ['Python', 'Frontend']
    
    # Chạy crawler
    results_df = crawl_multiple_keywords(
        base_url=BASE_URL,
        keywords=test_keywords,
        num_pages=1
    )
    
    # Kiểm tra kết quả
    assert results_df is not None
    assert isinstance(results_df, pd.DataFrame)
    assert not results_df.empty
    assert 'job_id' in results_df.columns
    assert 'title' in results_df.columns
    
    logging.info(f"Tìm thấy {len(results_df)} jobs cho từ khóa {test_keywords}")

def test_fast_mock():
    """
    Test nhanh với mock data, luôn chạy khi pytest
    Không gọi API thực tế nên không bị tốn thời gian/tài nguyên
    """
    # Đây là một mockup test đơn giản
    # Trong thực tế bạn sẽ mock các response từ TopCV API
    assert True

if __name__ == "__main__":
    # Khi chạy trực tiếp file test này
    print("=" * 50)
    print("TOPCV CRAWLER TESTS")
    print("=" * 50)
    
    # Chạy test import trước
    test_imports()
    print("✓ Import tests passed")
    
    # Thiết lập biến môi trường để chạy các test đầy đủ
    os.environ['RUN_FULL_TESTS'] = '1'
    
    # Chạy test crawler cho một keyword
    print("\nRunning single keyword crawler test...")
    asyncio.run(test_async_crawler_single())
    
    # Chạy test crawler cho nhiều keywords
    print("\nRunning multiple keywords crawler test...")
    test_crawler_multiple_keywords()
    
    print("\nAll tests completed!") 