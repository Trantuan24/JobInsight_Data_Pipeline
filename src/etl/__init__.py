#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ETL package cho việc chuyển dữ liệu từ Staging sang Data Warehouse
"""

# Import các module chính
from src.etl.etl_main import (
    run_staging_to_dwh_etl, 
    run_incremental_etl,
    get_staging_batch, 
    verify_etl_integrity,
    backup_dwh_database,
    restore_dwh_from_backup
)
from src.etl.etl_utils import get_duckdb_connection, setup_duckdb_schema
from src.etl.dimension_handler import DimensionHandler
from src.etl.fact_handler import FactHandler
from src.etl.partitioning import PartitionManager
from src.etl.raw_to_staging import run_etl

__all__ = [
    'run_staging_to_dwh_etl',
    'run_incremental_etl',
    'get_duckdb_connection',
    'setup_duckdb_schema',
    'get_staging_batch',
    'verify_etl_integrity',
    'DimensionHandler',
    'FactHandler',
    'PartitionManager',
    'run_etl',
    'backup_dwh_database',
    'restore_dwh_from_backup'
]

__version__ = "1.1.0"
__author__ = "JobInsight ETL Team"
