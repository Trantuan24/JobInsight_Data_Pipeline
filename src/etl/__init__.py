#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ETL package cho việc chuyển dữ liệu từ Staging sang Data Warehouse
"""

# Import các module chính
from src.etl.etl_main import run_staging_to_dwh_etl, get_staging_batch, verify_etl_integrity
from src.etl.etl_utils import get_duckdb_connection, setup_duckdb_schema
from src.etl.dimension_handler import DimensionHandler
from src.etl.fact_handler import FactHandler
from src.etl.partitioning import PartitionManager

__all__ = [
    'run_staging_to_dwh_etl',
    'get_duckdb_connection',
    'setup_duckdb_schema',
    'get_staging_batch',
    'verify_etl_integrity',
    'DimensionHandler',
    'FactHandler',
    'PartitionManager'
]

__version__ = "1.0.0"
__author__ = "JobInsight ETL Team"
