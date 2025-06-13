import os
import logging
import random

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

# Danh sách user agents mở rộng cho đa dạng hơn
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
]

# Trường quan trọng để kiểm tra thay đổi
IMPORTANT_FIELDS = ['title', 'salary', 'deadline', 'last_update', 'location', 'company_name']

# URL cơ bản
BASE_URL = "https://www.topcv.vn/viec-lam-it"

# Cấu hình timeout (milliseconds)
PAGE_LOAD_TIMEOUT = 90000  # Tăng lên 90 giây để tránh timeout
SELECTOR_TIMEOUT = 60000   # Tăng lên 60 giây
DEFAULT_TIMEOUT = 60000    # Tăng lên 60 giây

# Cấu hình chờ giữa các request
MIN_DELAY_BETWEEN_PAGES = 5  # Tăng để giảm nguy cơ bị phát hiện
MAX_DELAY_BETWEEN_PAGES = 10  # Tăng để giảm nguy cơ bị phát hiện

# Cấu hình anti-bot
SCROLL_MIN_DELAY = 0.5  # Thời gian tối thiểu giữa các lần scroll (giây)
SCROLL_MAX_DELAY = 2.0  # Thời gian tối đa giữa các lần scroll (giây)
HUMAN_LIKE_SCROLL_STEPS = 5  # Số bước scroll để đến cuối trang

# Cấu hình headless mode
USE_HEADLESS = True  # True cho môi trường production, False để debug

# Tùy chọn cho việc ẩn dấu vết bot
STEALTH_MODE = True

def get_random_user_agent():
    """Trả về user agent ngẫu nhiên từ danh sách"""
    return random.choice(USER_AGENTS)