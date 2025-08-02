# Thư mục ingestion chứa các module liên quan đến việc nạp dữ liệu vào database

# Core ingestion functionality after optimization
from src.db.bulk_operations import DBBulkOperations
from src.ingestion.cdc import save_cdc_record

__all__ = [
    'DBBulkOperations',
    'save_cdc_record'
]
