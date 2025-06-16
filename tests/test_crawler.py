"""
Tests for the JobInsight Crawler component
"""

import os
import sys
import pytest
import pandas as pd
import logging
import asyncio
from datetime import datetime
from pathlib import Path

# Thêm đường dẫn gốc của dự án vào sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import from crawler modules
from src.crawler.crawler import crawl_jobs, get_job_listing
from src.crawler.backup import backup_html_pages
from src.crawler.parser import parse_html_files
from src.crawler.crawler_config import BASE_URL, BACKUP_DIR
from src.crawler.crawler_utils import parse_last_update

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Mock data setup for testing
def setup_mock_html():
    """Tạo mock HTML file để test parse_html_files mà không cần chạy crawler thực tế"""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    mock_html = """
    <html>
    <body>
        <div class="job-item-2" data-job-id="123456">
            <h3 class="title"><a href="/job-detail/python-developer-123456.html"><span data-original-title="Python Developer">Python Developer</span></a></h3>
            <a class="company" href="/company/test-company">Test Company</a>
            <label class="address" data-original-title="Hà Nội, Quận Cầu Giấy">Hà Nội</label>
            <label class="title-salary">15-20 triệu</label>
            <div class="skills">
                <label class="item">Python</label>
                <label class="item">Django</label>
                <label class="item">SQL</label>
            </div>
        </div>
    </body>
    </html>
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    mock_file = BACKUP_DIR / f"it_p1_{timestamp}_mock.html"
    with open(mock_file, 'w', encoding='utf-8') as f:
        f.write(mock_html)
    return mock_file

# Kiểm tra cơ bản để đảm bảo module được import đúng
def test_imports():
    """Kiểm tra xem các module crawler có import thành công không"""
    assert crawl_jobs is not None
    assert get_job_listing is not None
    assert backup_html_pages is not None
    assert parse_html_files is not None
    assert BASE_URL is not None
    assert BACKUP_DIR is not None
    assert isinstance(BACKUP_DIR, Path)

def test_parse_last_update():
    """Kiểm tra hàm phân tích văn bản thời gian cập nhật"""
    # Test các trường hợp thời gian khác nhau
    assert parse_last_update("Cập nhật 2 phút trước") == 2 * 60
    assert parse_last_update("Cập nhật 3 giờ trước") == 3 * 60 * 60
    assert parse_last_update("Cập nhật 1 ngày trước") == 1 * 24 * 60 * 60
    assert parse_last_update("Cập nhật 2 tuần trước") == 2 * 7 * 24 * 60 * 60
    assert parse_last_update("Cập nhật 1 tháng trước") == 1 * 30 * 24 * 60 * 60
    
    # Test các trường hợp đặc biệt
    assert parse_last_update("") == 0
    assert parse_last_update(None) == 0
    assert parse_last_update("không xác định") == 0

# Cần cài pytest-asyncio để chạy test này
# pip install pytest-asyncio
@pytest.mark.asyncio
async def test_backup_html_pages():
    """Kiểm tra chức năng backup HTML pages"""
    # Bỏ qua test nếu không có trong chế độ chạy đầy đủ
    if not os.environ.get('RUN_FULL_TESTS'):
        pytest.skip("Bỏ qua test crawler thực tế (đặt RUN_FULL_TESTS=1 để chạy)")
    
    # Chỉ test 1 trang để tránh mất thời gian
    num_pages = 1
    results = await backup_html_pages(num_pages)
    
    assert results is not None
    assert isinstance(results, list)
    assert len(results) == num_pages
    
    # Kiểm tra cấu trúc kết quả
    first_result = results[0]
    assert 'success' in first_result
    assert 'page' in first_result
    
    if first_result['success']:
        assert 'filename' in first_result
        assert os.path.exists(first_result['filename'])
    else:
        assert 'error' in first_result
    
    logging.info(f"Backup test completed for {num_pages} pages")

def test_parse_html_files():
    """Kiểm tra chức năng parse HTML files"""
    # Tạo mock HTML file để test
    mock_file = setup_mock_html()
    
    # Parse mock file
    df_results = parse_html_files()
    
    # Xóa mock file sau khi test
    os.remove(mock_file)
    
    # Kiểm tra kết quả
    assert isinstance(df_results, pd.DataFrame)
    assert not df_results.empty
    
    # Kiểm tra cấu trúc dữ liệu
    expected_columns = ['job_id', 'title', 'company_name']
    for col in expected_columns:
        assert col in df_results.columns
    
    # Kiểm tra dữ liệu cụ thể từ mock
    assert '123456' in df_results['job_id'].values
    assert 'Python Developer' in df_results['title'].values
    assert 'Test Company' in df_results['company_name'].values
    
    logging.info(f"Parsed {len(df_results)} job records from mock HTML")

def test_crawl_jobs():
    """Kiểm tra wrapper function crawl_jobs"""
    # Bỏ qua test nếu không có trong chế độ chạy đầy đủ
    if not os.environ.get('RUN_FULL_TESTS'):
        pytest.skip("Bỏ qua test crawler thực tế (đặt RUN_FULL_TESTS=1 để chạy)")
    
    # Test với số trang ít để nhanh
    num_pages = 1
    
    # Tạo mock HTML trước để đảm bảo có gì đó để parse
    mock_file = setup_mock_html()
    
    # Chạy crawler
    results_df = crawl_jobs(num_pages=num_pages)
    
    # Kiểm tra kết quả
    assert results_df is not None
    assert isinstance(results_df, pd.DataFrame)
    assert not results_df.empty
    assert 'job_id' in results_df.columns
    assert 'title' in results_df.columns
    
    logging.info(f"Tìm thấy {len(results_df)} jobs từ crawl_jobs")
    
    # Dọn dẹp
    try:
        os.remove(mock_file)
    except:
        pass  # Bỏ qua lỗi xóa file

def test_get_job_listing():
    """Kiểm tra function get_job_listing (alias của crawl_jobs)"""
    # Bỏ qua test nếu không có trong chế độ chạy đầy đủ
    if not os.environ.get('RUN_FULL_TESTS'):
        pytest.skip("Bỏ qua test crawler thực tế (đặt RUN_FULL_TESTS=1 để chạy)")
    
    # Tạo mock HTML trước
    mock_file = setup_mock_html()
    
    # Chạy get_job_listing - chỉ cần kiểm tra rằng nó không bị lỗi và trả về DataFrame
    results_df = get_job_listing(num_pages=1)
    assert isinstance(results_df, pd.DataFrame)
    assert not results_df.empty
    
    # Dọn dẹp
    try:
        os.remove(mock_file)
    except:
        pass  # Bỏ qua lỗi xóa file

def test_fast_mock():
    """
    Test nhanh với mock data, luôn chạy khi pytest
    Không gọi API thực tế nên không bị tốn thời gian/tài nguyên
    """
    # Kiểm tra imports
    assert crawl_jobs is not None
    assert get_job_listing is not None
    assert backup_html_pages is not None
    assert parse_html_files is not None
    
    # Kiểm tra parse_last_update
    assert parse_last_update("Cập nhật 5 phút trước") == 5 * 60
    assert parse_last_update("") == 0
    
    # Tạo mock HTML và kiểm tra parse
    mock_file = setup_mock_html()
    
    # Chạy parse trên mock data
    try:
        df = parse_html_files()
        assert isinstance(df, pd.DataFrame)
        # Nếu parse thành công, df sẽ không rỗng
        if not df.empty:
            assert 'job_id' in df.columns
    except Exception as e:
        assert False, f"Parse test failed with error: {str(e)}"
    finally:
        # Dọn dẹp
        try:
            os.remove(mock_file)
        except:
            pass
    
    print("✓ Fast mock tests passed")
    return True

if __name__ == "__main__":
    # Khi chạy trực tiếp file test này
    print("=" * 50)
    print("CRAWLER TESTS")
    print("=" * 50)
    
    # Chạy test import trước
    test_imports()
    print("✓ Import tests passed")
    
    # Chạy test utils
    test_parse_last_update()
    print("✓ Utils tests passed")
    
    # Thiết lập biến môi trường để chạy các test đầy đủ
    os.environ['RUN_FULL_TESTS'] = '1'
    
    # Chạy test backup HTML
    print("\nRunning backup HTML test...")
    asyncio.run(test_backup_html_pages())
    print("✓ Backup test passed")
    
    # Chạy test parse HTML
    print("\nRunning parse HTML test...")
    test_parse_html_files()
    print("✓ Parse test passed")
    
    # Chạy test crawler
    print("\nRunning crawl_jobs test...")
    test_crawl_jobs()
    print("✓ Crawl test passed")
    
    # Chạy test get_job_listing
    print("\nRunning get_job_listing test...")
    test_get_job_listing()
    print("✓ Get job listing test passed")
    
    # Chạy test fast mock
    print("\nRunning fast mock test...")
    test_fast_mock()
    print("✓ Fast mock test passed")
    
    print("\nAll tests completed!") 