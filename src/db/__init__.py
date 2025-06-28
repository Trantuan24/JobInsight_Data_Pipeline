"""
Database package cho JobInsight.
Chứa các module xử lý tương tác với cơ sở dữ liệu.
"""

from src.db.core import get_connection, execute_query, get_dataframe
from src.db.bulk_operations import DBBulkOperations

__all__ = [
    'get_connection',
    'execute_query',
    'get_dataframe',
    'DBBulkOperations',
] 