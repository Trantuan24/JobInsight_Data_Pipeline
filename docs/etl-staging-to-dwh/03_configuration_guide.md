# JobInsight ETL - Configuration Guide

## Overview

Configuration guide cho Staging to Data Warehouse ETL process, bao gồm DuckDB settings, environment variables, connection parameters, và performance tuning recommendations dựa trên production analysis.

## Configuration Overview

ETL system sử dụng environment variables cho configuration với precedence order:
1. **Environment Variables** (highest priority)
2. **Configuration Files** (fallback)
3. **Default Values** (system defaults)

Configuration được organize thành logical groups: DuckDB settings, ETL execution parameters, performance tuning, và database connections.

## Core Configuration Parameters

### 1. DuckDB Database Settings

| Parameter | Environment Variable | Default | Description | Production Recommendations |
|-----------|---------------------|---------|-------------|---------------------------|
| `duckdb_path` | `DUCKDB_PATH` | `data/jobinsight_dwh.duckdb` | DuckDB database file path | Use absolute path trong production |
| `memory_limit` | `DUCKDB_MEMORY_LIMIT` | `2GB` | DuckDB memory limit | **Optimize to 4GB** cho better performance |
| `threads` | `DUCKDB_THREADS` | 4 | Number of processing threads | **Auto-detect CPU cores** |
| `checkpoint_threshold` | `DUCKDB_CHECKPOINT_THRESHOLD` | `1GB` | Checkpoint trigger threshold | Increase to 2GB cho large datasets |

**Optimized Configuration (Based on Analysis):**
```bash
# Recommended DuckDB optimizations
export DUCKDB_PATH="/data/warehouse/jobinsight_dwh.duckdb"
export DUCKDB_MEMORY_LIMIT="4GB"        # Increased from 2GB
export DUCKDB_THREADS="8"               # Auto-detect or set based on CPU
export DUCKDB_CHECKPOINT_THRESHOLD="2GB" # Increased from 1GB
```

### 2. ETL Execution Settings

| Parameter | Environment Variable | Default | Description | Tuning Notes |
|-----------|---------------------|---------|-------------|--------------|
| `last_etl_date` | `ETL_LAST_DATE` | 7 days ago | Starting date cho incremental processing | Set based on business requirements |
| `batch_size` | `ETL_BATCH_SIZE` | None (all) | Number of staging records per batch | **Use 1000** cho memory efficiency |
| `verbose` | `ETL_VERBOSE` | false | Enable detailed logging | Enable trong development |
| `dry_run` | `ETL_DRY_RUN` | false | Validate without making changes | Use cho testing |

**Example Configuration:**
```bash
# ETL execution settings
export ETL_LAST_DATE="2025-08-01"
export ETL_BATCH_SIZE=1000
export ETL_VERBOSE=true
export ETL_DRY_RUN=false
```

### 3. Performance Optimization Settings

| Parameter | Environment Variable | Default | Description | Production Recommendations |
|-----------|---------------------|---------|-------------|---------------------------|
| `bulk_fact_generation` | `ETL_BULK_FACTS` | false | Enable bulk fact operations | **Enable for 70% improvement** |
| `parallel_dimensions` | `ETL_PARALLEL_DIMS` | false | Enable parallel dimension processing | **Enable for 30% improvement** |
| `schema_caching` | `ETL_SCHEMA_CACHE` | false | Cache schema validation | **Enable for faster startup** |
| `memory_monitoring` | `ETL_MEMORY_MONITOR` | false | Enable memory usage monitoring | Enable trong production |

**Performance-Optimized Configuration:**
```bash
# Performance optimizations (based on bottleneck analysis)
export ETL_BULK_FACTS=true           # 70% improvement trong fact processing
export ETL_PARALLEL_DIMS=true       # 30% improvement trong dimensions
export ETL_SCHEMA_CACHE=true         # Faster schema setup
export ETL_MEMORY_MONITOR=true       # Production monitoring
```

### 4. Database Connection Settings

#### PostgreSQL Staging Connection
| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `pg_host` | `POSTGRES_HOST` | localhost | PostgreSQL host |
| `pg_port` | `POSTGRES_PORT` | 5432 | PostgreSQL port |
| `pg_database` | `POSTGRES_DB` | jobinsight | Database name |
| `pg_user` | `POSTGRES_USER` | postgres | Username |
| `pg_password` | `POSTGRES_PASSWORD` | - | Password (required) |
| `pg_schema` | `POSTGRES_SCHEMA` | jobinsight_staging | Staging schema |

#### Connection Pool Settings
| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `pg_pool_size` | `POSTGRES_POOL_SIZE` | 5 | Connection pool size |
| `pg_max_overflow` | `POSTGRES_MAX_OVERFLOW` | 10 | Max overflow connections |
| `pg_pool_timeout` | `POSTGRES_POOL_TIMEOUT` | 30 | Connection timeout (seconds) |

**Database Configuration:**
```bash
# PostgreSQL staging connection
export POSTGRES_HOST="staging-db.company.com"
export POSTGRES_PORT=5432
export POSTGRES_DB="jobinsight"
export POSTGRES_USER="etl_user"
export POSTGRES_PASSWORD="secure_password"
export POSTGRES_SCHEMA="jobinsight_staging"

# Connection pooling
export POSTGRES_POOL_SIZE=10
export POSTGRES_MAX_OVERFLOW=20
export POSTGRES_POOL_TIMEOUT=60
```

## Environment-Specific Configurations

### Development Environment
```bash
# Development settings - focus on debugging
export DUCKDB_PATH="dev/jobinsight_dwh.duckdb"
export DUCKDB_MEMORY_LIMIT="1GB"
export DUCKDB_THREADS=2
export ETL_BATCH_SIZE=100
export ETL_VERBOSE=true
export ETL_DRY_RUN=false
export ETL_BULK_FACTS=false
export ETL_PARALLEL_DIMS=false
export ETL_SCHEMA_CACHE=false
```

### Staging Environment
```bash
# Staging settings - production-like với smaller scale
export DUCKDB_PATH="/staging/data/jobinsight_dwh.duckdb"
export DUCKDB_MEMORY_LIMIT="2GB"
export DUCKDB_THREADS=4
export ETL_BATCH_SIZE=500
export ETL_VERBOSE=true
export ETL_BULK_FACTS=true
export ETL_PARALLEL_DIMS=true
export ETL_SCHEMA_CACHE=true
export ETL_MEMORY_MONITOR=true
```

### Production Environment
```bash
# Production settings - optimized cho performance
export DUCKDB_PATH="/prod/data/jobinsight_dwh.duckdb"
export DUCKDB_MEMORY_LIMIT="4GB"
export DUCKDB_THREADS=8
export DUCKDB_CHECKPOINT_THRESHOLD="2GB"
export ETL_BATCH_SIZE=1000
export ETL_VERBOSE=false
export ETL_BULK_FACTS=true
export ETL_PARALLEL_DIMS=true
export ETL_SCHEMA_CACHE=true
export ETL_MEMORY_MONITOR=true

# Production database connections
export POSTGRES_HOST="prod-db-cluster.company.com"
export POSTGRES_POOL_SIZE=20
export POSTGRES_MAX_OVERFLOW=40
export POSTGRES_POOL_TIMEOUT=120
```

## Advanced Configuration

### Dimensional Modeling Settings

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `scd_type2_enabled` | `ETL_SCD_TYPE2` | true | Enable SCD Type 2 processing |
| `surrogate_key_start` | `ETL_SK_START` | 1000 | Starting value cho surrogate keys |
| `effective_date_format` | `ETL_DATE_FORMAT` | `%Y-%m-%d` | Date format cho SCD Type 2 |
| `fact_grain` | `ETL_FACT_GRAIN` | daily | Fact table grain (daily/hourly) |

### Backup and Recovery Settings

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `backup_enabled` | `ETL_BACKUP_ENABLED` | true | Enable automatic backup |
| `backup_dir` | `ETL_BACKUP_DIR` | `data/duck_db/backup/` | Backup directory |
| `keep_backups` | `ETL_KEEP_BACKUPS` | 5 | Number of backups to retain |
| `backup_compression` | `ETL_BACKUP_COMPRESS` | true | Enable backup compression |

### Export Settings

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `export_enabled` | `ETL_EXPORT_ENABLED` | false | Enable Parquet export |
| `export_dir` | `ETL_EXPORT_DIR` | `data/exports/` | Export directory |
| `export_format` | `ETL_EXPORT_FORMAT` | parquet | Export format |
| `export_partitioning` | `ETL_EXPORT_PARTITION` | load_month | Partitioning strategy |

## Configuration File Examples

### config/development.env
```bash
# Development environment configuration
DUCKDB_PATH=dev/jobinsight_dwh.duckdb
DUCKDB_MEMORY_LIMIT=1GB
DUCKDB_THREADS=2
ETL_BATCH_SIZE=100
ETL_VERBOSE=true
ETL_BULK_FACTS=false
ETL_PARALLEL_DIMS=false
POSTGRES_HOST=localhost
POSTGRES_DB=jobinsight_dev
```

### config/production.env
```bash
# Production environment configuration
DUCKDB_PATH=/prod/data/jobinsight_dwh.duckdb
DUCKDB_MEMORY_LIMIT=4GB
DUCKDB_THREADS=8
DUCKDB_CHECKPOINT_THRESHOLD=2GB
ETL_BATCH_SIZE=1000
ETL_VERBOSE=false
ETL_BULK_FACTS=true
ETL_PARALLEL_DIMS=true
ETL_SCHEMA_CACHE=true
ETL_MEMORY_MONITOR=true
POSTGRES_HOST=prod-db-cluster.company.com
POSTGRES_POOL_SIZE=20
```

## Performance Tuning Recommendations

### Based on Production Analysis

#### Critical Optimizations (54.7% bottleneck)
```bash
# Fact processing optimization
export ETL_BULK_FACTS=true              # 70% improvement
export ETL_FACT_BATCH_SIZE=1000         # Optimal batch size
export ETL_FACT_PARALLEL=true           # Parallel fact generation
```

#### High Priority Optimizations (37.9% bottleneck)
```bash
# Dimension processing optimization
export ETL_PARALLEL_DIMS=true           # 30% improvement
export ETL_DIM_BATCH_SIZE=500           # Optimal dimension batch size
export ETL_SCD_CACHE=true               # Cache SCD lookups
```

#### Medium Priority Optimizations
```bash
# Schema and connection optimization
export ETL_SCHEMA_CACHE=true            # 67% improvement trong setup
export POSTGRES_POOL_SIZE=20            # Adequate connection pool
export DUCKDB_MEMORY_LIMIT=4GB          # Sufficient memory
```

## Monitoring and Alerting Configuration

### Performance Thresholds
```bash
# Alert thresholds (based on production metrics)
export ETL_WARN_THRESHOLD=10            # Warning if execution > 10s
export ETL_CRITICAL_THRESHOLD=20        # Critical if execution > 20s
export ETL_MEMORY_THRESHOLD=500         # Memory warning at 500MB
export ETL_FACT_RATE_MIN=400            # Minimum fact generation rate (ops/sec)
```

### Logging Configuration
```bash
# Logging settings
export ETL_LOG_LEVEL=INFO               # Production log level
export ETL_LOG_FILE=/var/log/etl/staging_to_dwh.log
export ETL_LOG_ROTATION=daily           # Daily log rotation
export ETL_LOG_RETENTION=30             # Keep logs for 30 days
```

## Configuration Validation

### Validation Script
```python
def validate_configuration():
    """Validate ETL configuration settings"""
    
    # Check required environment variables
    required_vars = [
        'DUCKDB_PATH', 'POSTGRES_HOST', 'POSTGRES_DB',
        'POSTGRES_USER', 'POSTGRES_PASSWORD'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    # Validate DuckDB settings
    memory_limit = os.getenv('DUCKDB_MEMORY_LIMIT', '2GB')
    if not memory_limit.endswith(('GB', 'MB')):
        raise ValueError("DUCKDB_MEMORY_LIMIT must end with GB or MB")
    
    # Validate paths
    duckdb_path = os.getenv('DUCKDB_PATH')
    duckdb_dir = os.path.dirname(duckdb_path)
    if not os.path.exists(duckdb_dir):
        os.makedirs(duckdb_dir, exist_ok=True)
    
    print("✅ Configuration validation passed")

# Run validation
if __name__ == "__main__":
    validate_configuration()
```

## Troubleshooting Configuration Issues

### Common Issues và Solutions

#### Issue 1: DuckDB Memory Errors
```bash
# Problem: "Out of memory" errors
# Solution: Increase memory limit
export DUCKDB_MEMORY_LIMIT="4GB"  # or higher
```

#### Issue 2: Slow Performance
```bash
# Problem: ETL execution > 20 seconds
# Solution: Enable performance optimizations
export ETL_BULK_FACTS=true
export ETL_PARALLEL_DIMS=true
export DUCKDB_THREADS=8
```

#### Issue 3: Connection Pool Exhaustion
```bash
# Problem: "Connection pool exhausted" errors
# Solution: Increase pool size
export POSTGRES_POOL_SIZE=20
export POSTGRES_MAX_OVERFLOW=40
```

---

*For implementation details: [Technical Implementation](02_technical_implementation.md)*  
*For performance optimization: [Performance Guide](04_performance_optimization.md)*  
*For troubleshooting: [Troubleshooting Guide](05_troubleshooting_guide.md)*
