# Thư mục ingestion chứa các module liên quan đến việc nạp dữ liệu vào database

# Đảm bảo thứ tự import đúng để tránh circular import
from src.ingestion.data_processor import prepare_job_data, validate_utf8
from src.db.bulk_operations import DBBulkOperations
from src.ingestion.db_operations import batch_insert_records, insert_record, ensure_table_exists
from src.ingestion.cdc import save_cdc_record
from src.ingestion.ingest import ingest_dataframe, bulk_upsert_jobs, replay_cdc_records, list_cdc_files

__all__ = [
    'prepare_job_data',
    'validate_utf8',
    'DBBulkOperations',
    'batch_insert_records',
    'insert_record',
    'ensure_table_exists',
    'save_cdc_record',
    'ingest_dataframe',
    'bulk_upsert_jobs',
    'replay_cdc_records',
    'list_cdc_files'
]
