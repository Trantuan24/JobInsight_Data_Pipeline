"""
JobInsight: Hệ thống thu thập, phân tích và trực quan hóa dữ liệu việc làm từ các trang tuyển dụng.

Dự án này bao gồm các thành phần chính:
- Crawler: Thu thập dữ liệu từ các trang tuyển dụng
- Ingestion: Chuẩn hóa và lưu trữ dữ liệu 
- ETL: Chuyển đổi dữ liệu từ raw sang data warehouse
- Analysis: Phân tích dữ liệu và trích xuất insights
"""

__version__ = "1.0.0"
__author__ = "Trantuan24"

# Import các package chính
from . import crawler
from . import ingestion
from . import etl
from . import utils
from . import db
