import os
import logging

# Tạo thư mục data nếu chưa có
os.makedirs('data', exist_ok=True)
os.makedirs('data/job_tracking', exist_ok=True)
os.makedirs('data/job_tracking/backups', exist_ok=True)

# Thiết lập logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    filename=f'logs/topcv_crawler.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Danh sách keywords cụ thể
KEYWORDS = [
    'Backend', 'Frontend', 'Fullstack', 'DevOps', 'QA', 
    'Python', 'Java', 'JavaScript', 'React', 'NodeJS',
    'Data', 'AI', 'Machine Learning', 'Senior', 'Junior'
]

# Danh sách user agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
]

# Trường quan trọng để kiểm tra thay đổi
IMPORTANT_FIELDS = ['title', 'salary', 'deadline', 'last_update', 'location', 'company_name']

# Số instance browser tối đa cho đa luồng
MAX_CONCURRENT_BROWSERS = 3

# URL cơ bản
BASE_URL = "https://www.topcv.vn/viec-lam-it"

# Cấu hình timeout (milliseconds)
PAGE_LOAD_TIMEOUT = 60000
SELECTOR_TIMEOUT = 30000
DEFAULT_TIMEOUT = 30000

# Cấu hình chờ giữa các request
MIN_DELAY_BETWEEN_PAGES = 2
MAX_DELAY_BETWEEN_PAGES = 4
MIN_DELAY_BETWEEN_GROUPS = 8
MAX_DELAY_BETWEEN_GROUPS = 12
GROUP_TIMEOUT = 120  # seconds 