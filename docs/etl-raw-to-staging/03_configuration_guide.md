# JobInsight ETL - Raw to Staging Configuration Guide

## Overview

Configuration guide cho Raw to Staging ETL process, bao gồm database settings, performance parameters, environment variables, và optimization recommendations dựa trên production analysis.

## Configuration Overview

ETL system sử dụng environment variables và configuration dictionaries với precedence order:
1. **Environment Variables** (highest priority)
2. **Configuration Dictionaries** (code-based)
3. **Default Values** (system defaults)

Configuration được organize thành logical groups: database connections, ETL execution parameters, và monitoring settings.

Notes:
- Phase 2 hiện không dùng max_workers/raw_batch_size trong code đường dẫn chính.
- Chức năng only_unprocessed phụ thuộc vào cột processed trong staging_jobs (chưa có mặc định).

## Core Configuration Parameters

### 1. Database Connection Settings

| Parameter | Environment Variable | Default | Description | Production Recommendations |
|-----------|---------------------|---------|-------------|---------------------------|
| `postgres_host` | `POSTGRES_HOST` | localhost | PostgreSQL host | Use connection pooling |
| `postgres_port` | `POSTGRES_PORT` | 5432 | PostgreSQL port | Standard port |
| `postgres_db` | `POSTGRES_DB` | jobinsight | Database name | Dedicated ETL database |
| `postgres_user` | `POSTGRES_USER` | postgres | Username | Dedicated ETL user |
| `postgres_password` | `POSTGRES_PASSWORD` | - | Password (required) | Use secrets management |
| `postgres_schema` | `POSTGRES_SCHEMA` | public | Schema name | Separate raw/staging schemas |

**Database Configuration Example:**
```bash
# PostgreSQL connection settings
export POSTGRES_HOST="etl-db.company.com"
export POSTGRES_PORT=5432
export POSTGRES_DB="jobinsight"
export POSTGRES_USER="etl_user"
export POSTGRES_PASSWORD="secure_password"
export POSTGRES_SCHEMA="public"
```

### 2. ETL Execution Parameters

| Parameter | Environment Variable | Default | Description | Tuning Notes |
|-----------|---------------------|---------|-------------|--------------|
| `batch_size` | `ETL_BATCH_SIZE` | 500 | Records per batch | Configurable via run_etl() parameter |
| `max_workers` | `ETL_MAX_WORKERS` | 4 | Thread pool size | Not used in current Phase 2 flow |
| `timeout_seconds` | `ETL_TIMEOUT` | 600 | Operation timeout | Database operation timeout |
| `raw_batch_size` | `RAW_BATCH_SIZE` | 20 | Raw data batch size | Not used in current Phase 2 flow |
| `only_unprocessed` | N/A | False | Intended: process only new records | Requires 'processed' column in staging_jobs |
| `verbose` | N/A | False | Enable detailed logging | Function parameter only |

**ETL Configuration Example:**
```bash
# ETL execution settings
export ETL_BATCH_SIZE=500
export ETL_MAX_WORKERS=4
export ETL_TIMEOUT=600
export RAW_BATCH_SIZE=20

# Note: only_unprocessed và verbose are function parameters, not environment variables
```

### 3. Additional Configuration (From src/utils/config.py)

| Parameter | Environment Variable | Default | Description | Usage |
|-----------|---------------------|---------|-------------|-------|
| `db_host` | `DB_HOST` | postgres | PostgreSQL host | Database connection |
| `db_port` | `DB_PORT` | 5432 | PostgreSQL port | Database connection |
| `db_user` | `DB_USER` | jobinsight | Database user | Database connection |
| `db_password` | `DB_PASSWORD` | jobinsight | Database password | Database connection |
| `db_name` | `DB_NAME` | jobinsight | Database name | Database connection |

**Database Configuration Example:**
```bash
# Database connection settings
export DB_HOST=postgres
export DB_PORT=5432
export DB_USER=jobinsight
export DB_PASSWORD=jobinsight
export DB_NAME=jobinsight
```

### 4. Function Parameters (Not Environment Variables)

**run_etl() Function Parameters:**
```python
def run_etl(batch_size=None, only_unprocessed=False, verbose=False):
    """
    Args:
        batch_size (int, optional): Số bản ghi xử lý mỗi batch, mặc định None (tất cả)
        only_unprocessed (bool, optional): Chỉ xử lý các bản ghi chưa được xử lý, mặc định False
        verbose (bool, optional): Hiển thị thêm thông tin chi tiết, mặc định False
    """
```

**Usage Examples:**
```python
# Process all records
result = run_etl()

# Process with batch limit
result = run_etl(batch_size=1000)

# Process only unprocessed records
result = run_etl(only_unprocessed=True, verbose=True)
```

## Environment-Specific Configurations

### Development Environment
```bash
# Development settings - smaller batches, verbose logging
export ETL_BATCH_SIZE=100
export ETL_MAX_WORKERS=2
export ETL_TIMEOUT=300
export RAW_BATCH_SIZE=10

# Database settings
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=dev_user
export DB_PASSWORD=dev_password
export DB_NAME=jobinsight_dev
```

### Staging Environment
```bash
# Staging settings - production-like với smaller scale
export ETL_BATCH_SIZE=1000
export ETL_MAX_WORKERS=4
export ETL_TIMEOUT=600
export RAW_BATCH_SIZE=20

# Database settings
export DB_HOST=staging-db.company.com
export DB_PORT=5432
export DB_USER=staging_user
export DB_PASSWORD=staging_password
export DB_NAME=jobinsight_staging
```

### Production Environment
```bash
# Production settings - optimized for performance
export ETL_BATCH_SIZE=2000
export ETL_MAX_WORKERS=8
export ETL_TIMEOUT=1200
export RAW_BATCH_SIZE=50

# Database settings với connection pooling
export DB_HOST=prod-db-cluster.company.com
export DB_PORT=5432
export DB_USER=etl_user
export DB_PASSWORD=secure_production_password
export DB_NAME=jobinsight

# Additional production settings
export LOG_LEVEL=INFO
```

## Airflow Integration Configuration

### DAG Configuration
```python
# dags/etl_pipeline.py
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

raw_to_staging_task = PythonOperator(
    task_id='raw_to_staging',
    python_callable=etl_raw_to_staging,
    op_kwargs={
        'batch_size': 5000,
        'only_unprocessed': True,
        'verbose': False
    },
    execution_timeout=timedelta(minutes=10),
    pool='etl_pool'
)
```

### Connection Configuration
```python
# Airflow connections
AIRFLOW_CONNECTIONS = {
    'postgres_raw': {
        'conn_type': 'postgres',
        'host': 'etl-db.company.com',
        'schema': 'jobinsight',
        'login': 'etl_user',
        'password': '${POSTGRES_PASSWORD}',
        'port': 5432
    }
}
```

## Performance Tuning Recommendations

### Based on Production Analysis (1.14s execution time)

#### Critical Optimizations
```bash
# Batch upsert optimization (47% improvement)
export ETL_BATCH_UPSERT=true
export ETL_BATCH_SIZE=5000

# Schema caching (faster startup)
export ETL_SCHEMA_CACHE=true

# Memory optimization
export ETL_MEMORY_LIMIT=500
```

#### Database Connection Optimization
```bash
# Connection pooling
export POSTGRES_POOL_SIZE=10
export POSTGRES_MAX_OVERFLOW=20
export POSTGRES_POOL_TIMEOUT=30
```

## Monitoring and Alerting Configuration

### Performance Thresholds
```bash
# Alert thresholds (based on 1.14s baseline)
export ETL_WARN_THRESHOLD=3          # Warning if execution > 3s
export ETL_CRITICAL_THRESHOLD=10     # Critical if execution > 10s
export ETL_MEMORY_THRESHOLD=400      # Memory warning at 400MB
export ETL_INTEGRITY_MIN=0.98        # Minimum 98% data integrity
```

### Logging Configuration
```bash
# Logging settings
export ETL_LOG_LEVEL=INFO
export ETL_LOG_FILE=/var/log/etl/raw_to_staging.log
export ETL_LOG_ROTATION=daily
export ETL_LOG_RETENTION=30
```

## Configuration Validation

### Validation Script
```python
def validate_configuration():
    """Validate ETL configuration settings"""
    
    # Check required environment variables
    required_vars = [
        'POSTGRES_HOST', 'POSTGRES_DB', 
        'POSTGRES_USER', 'POSTGRES_PASSWORD'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    # Validate batch size
    batch_size = int(os.getenv('ETL_BATCH_SIZE', 1000))
    if batch_size < 100 or batch_size > 10000:
        raise ValueError("ETL_BATCH_SIZE must be between 100 and 10000")
    
    # Validate memory limit
    memory_limit = int(os.getenv('ETL_MEMORY_LIMIT', 500))
    if memory_limit < 100:
        raise ValueError("ETL_MEMORY_LIMIT must be at least 100MB")
    
    print("✅ Configuration validation passed")

# Run validation
if __name__ == "__main__":
    validate_configuration()
```

## Troubleshooting Configuration Issues

### Common Issues và Solutions

#### Issue 1: Slow Performance
```bash
# Problem: ETL execution > 5 seconds
# Solution: Enable batch optimizations
export ETL_BATCH_UPSERT=true
export ETL_BATCH_SIZE=5000
export ETL_SCHEMA_CACHE=true
```

#### Issue 2: Memory Errors
```bash
# Problem: Out of memory errors
# Solution: Increase memory limit
export ETL_MEMORY_LIMIT=1000  # or higher
export ETL_BATCH_SIZE=2000    # reduce batch size
```

#### Issue 3: Database Connection Issues
```bash
# Problem: Connection timeouts
# Solution: Optimize connection settings
export POSTGRES_POOL_SIZE=20
export ETL_TIMEOUT=600
```

---

*For implementation details: [Technical Implementation](02_implementation_performance.md)*  
*For troubleshooting: [Troubleshooting Guide](04_troubleshooting_guide.md)*  
*For API reference: [API Reference](05_api_reference.md)*
