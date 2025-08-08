# JobInsight ETL - API Reference

## Overview

Essential API documentation cho Raw to Staging ETL functions và usage patterns.

## 1. Core ETL Function

### `run_etl(batch_size=None, only_unprocessed=False, verbose=False)`

Main ETL orchestrator function.

**Parameters:**
- `batch_size` (int): Max records to process. Default: None (all)
- `only_unprocessed` (bool): Process only unprocessed records. Default: False
- `verbose` (bool): Enable detailed logging. Default: False

**Returns:**
```python
{
    "success": bool,
    "message": str,  # Success/error message
    "stats": {
        "total_records": int,
        "processed_records": int,
        "success_count": int,
        "failure_count": int,
        "success_rate": float,          # 0-100%
        "duration_seconds": float,
        "batch_count": int
    },
    "error": str  # if success=False (optional)
}
```

**Usage:**
```python
from src.etl.raw_to_staging import run_etl

# Basic usage
result = run_etl()
if result['success']:
    print(f"✅ Processed {result['stats']['processed_records']} records")

# With batch limit
result = run_etl(batch_size=1000, only_unprocessed=True)

# Error handling
try:
    result = run_etl()
    if not result['success']:
        logger.error(f"ETL failed: {result.get('error')}")
except Exception as e:
    logger.error(f"ETL execution error: {str(e)}")
```

## 2. Supporting Functions

### `setup_database_schema()`
Ensures staging schema và tables exist.
```python
from src.etl.raw_to_staging import setup_database_schema

if setup_database_schema():
    print("✅ Database schema ready")
```

### `load_staging_data(limit=None, query_filter=None)`
Loads data from staging_jobs table với optional filtering.

**Parameters:**
- `limit` (int, optional): Giới hạn số bản ghi, mặc định None (tất cả)
- `query_filter` (str, optional): Điều kiện WHERE, ví dụ: "WHERE processed IS NULL"

```python
from src.etl.raw_to_staging import load_staging_data

# Load all data
df = load_staging_data()

# Load with limit
df = load_staging_data(limit=500)

# Load unprocessed records only
df = load_staging_data(query_filter="WHERE processed IS NULL OR processed = FALSE")
```

### `process_staging_data(df)`
Applies Python transformations to DataFrame.

**Processing Steps:**
1. Extract location_info from location_detail
2. Refine location using location_pairs
3. Clean title với clean_title
4. Standardize company_name

```python
from src.etl.raw_to_staging import process_staging_data

df = load_staging_data(limit=100)
processed_df = process_staging_data(df)
print(f"Processed {len(processed_df)} records")
```

## 3. Data Transformation Functions (src/processing/data_processing.py)

### `extract_location_info(html_content)`
Extracts location details from HTML content.

**Returns:** List of location strings

```python
from src.processing.data_processing import extract_location_info

html = '<div class="location">Hà Nội & Hồ Chí Minh</div>'
location_info = extract_location_info(html)
print(location_info)  # ['Hà Nội', 'Hồ Chí Minh']
```

### `clean_title(title)`
Cleans job titles by removing noise và excessive punctuation.
```python
from src.processing.data_processing import clean_title

original = "Senior Developer - Python/Django (Remote) - Urgent!!!"
cleaned = clean_title(original)
print(cleaned)  # "Senior Developer - Python/Django"
```

### `clean_company_name(company)`
Standardizes company names by removing recruitment keywords.

**Removes:** "tuyển dụng", "cần tuyển", "hot", "gấp"
**Preserves:** Technical terms (PHP, Java, Python, AWS, etc.)

```python
from src.processing.data_processing import clean_company_name

original = "  CÔNG TY TNHH ABC TECHNOLOGY tuyển dụng gấp  "
standardized = clean_company_name(original)
print(standardized)  # "CÔNG TY TNHH ABC TECHNOLOGY"
```

### `refine_location(row)`
Refines location based on location_pairs data.

**Handles:** Multiple locations separated by "&"
**Combines:** location và location_pairs fields

```python
from src.processing.data_processing import refine_location

# Used internally trong process_staging_data()
# Combines location="Hà Nội & Hồ Chí Minh" với location_pairs data
```

## 4. SQL Files và Database Operations

### **Required SQL Files** (sql/ directory)
- `schema_staging.sql`: Tạo staging schema và tables
- `insert_raw_to_staging.sql`: Copy data từ raw_jobs sang staging_jobs
- `stored_procedures.sql`: Salary normalization và deadline processing functions

### **Database Schema Setup**
```python
# Phase 1: Schema Setup
sql_files = [
    "schema_staging.sql",      # Create schema và tables
    "insert_raw_to_staging.sql", # Initial data copy
    "stored_procedures.sql"    # Functions for transformations
]
```

### **Stored Procedures**
- `normalize_salary(salary_text)`: Parse salary ranges
- `update_deadline()`: Calculate time remaining for applications

## 5. Validation Functions

### `verify_etl_integrity(source_count, target_count, threshold=0.98)`
Validates ETL data integrity với configurable threshold.
```python
from src.etl.raw_to_staging import verify_etl_integrity

source_records = 1000
processed_records = 995

if verify_etl_integrity(source_records, processed_records):
    print("✅ ETL integrity check passed")
else:
    print("❌ ETL integrity check failed")
```

## 6. Performance Monitoring

### `performance_monitor(phase_name)`
Context manager để monitor performance cho từng phase.

**Tracks:** Duration, Memory usage (if psutil available), CPU usage
**Logs:** Detailed performance metrics per phase

```python
from src.etl.raw_to_staging import performance_monitor

with performance_monitor("Data Processing"):
    # Your processing code here
    processed_df = process_staging_data(df)

# Example output:
# 📊 Data Processing Performance:
#   ⏱️  Duration: 55.1ms
#   🧠 Memory: 173.2MB (Δ+12.3MB)
#   ⚡ CPU: 15.4%
```

## 7. Error Handling Patterns

### **Built-in Error Handling**
```python
def run_etl(batch_size=None, only_unprocessed=False, verbose=False):
    try:
        # Phase execution với individual error handling
        with performance_monitor("Schema Setup"):
            if not setup_database_schema():
                return {"success": False, "error": "Schema setup failed"}

        # Data integrity verification
        if not verify_etl_integrity(source_count, processed_count):
            logger.warning("Data integrity issue detected")
            # Continues with warning, doesn't fail

    except Exception as e:
        logger.error(f"ETL execution failed: {e}")
        return {
            "success": False,
            "error": f"ETL execution failed: {str(e)}"
        }
```

### **Recommended Error Handling**
```python
def robust_etl_execution():
    max_retries = 3
    for attempt in range(max_retries):
        try:
            result = run_etl()
            if result['success']:
                return result
            else:
                logger.warning(f"ETL attempt {attempt + 1} failed: {result.get('error')}")
        except Exception as e:
            logger.error(f"ETL attempt {attempt + 1} exception: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(60 * (attempt + 1))  # Exponential backoff
```

## 8. Integration Examples

### Airflow DAG Integration
```python
# dags/etl_pipeline.py
from airflow.operators.python import PythonOperator
from src.etl.raw_to_staging import run_etl

def etl_task_wrapper(**context):
    result = run_etl(batch_size=5000, only_unprocessed=True)

    if not result['success']:
        raise AirflowException(f"ETL failed: {result.get('error')}")

    stats = result['stats']
    logger.info(f"ETL completed: {stats['processed_records']} records in {stats['duration_seconds']:.2f}s")

    return result

etl_task = PythonOperator(
    task_id='raw_to_staging',
    python_callable=etl_task_wrapper,
    dag=dag
)
```

### Custom ETL Pipeline
```python
from src.etl.raw_to_staging import (
    setup_database_schema,
    load_staging_data,
    process_staging_data,
    save_back_to_staging
)

def custom_etl_pipeline(batch_size=1000):
    try:
        # Setup
        if not setup_database_schema():
            raise Exception("Schema setup failed")

        # Process in batches
        offset = 0
        total_processed = 0

        while True:
            df = load_staging_data(limit=batch_size, offset=offset)
            if df.empty:
                break

            processed_df = process_staging_data(df)

            if save_back_to_staging(processed_df):
                total_processed += len(processed_df)

            offset += batch_size

        logger.info(f"Custom ETL completed: {total_processed} records")
        return True

    except Exception as e:
        logger.error(f"Custom ETL failed: {str(e)}")
        return False
```

## 6. Error Handling

### Common Error Patterns
| Error | Cause | Solution |
|-------|-------|----------|
| `DB_CONNECTION_ERROR` | Database unavailable | Check connection settings |
| `SCHEMA_SETUP_ERROR` | Permission issues | Verify database permissions |
| `PROCESSING_ERROR` | Data format issues | Check data quality |
| `INTEGRITY_ERROR` | Data loss during processing | Review transformation logic |

### Error Handling Best Practices
```python
def robust_etl_execution():
    try:
        result = run_etl()

        if not result['success']:
            error_msg = result.get('error', '')

            if 'connection' in error_msg.lower():
                logger.error("Database connection issue detected")
            elif 'schema' in error_msg.lower():
                logger.error("Schema setup issue detected")
            elif 'integrity' in error_msg.lower():
                logger.error("Data integrity issue detected")

        return result

    except Exception as e:
        logger.error(f"Unexpected ETL error: {str(e)}")
        return {'success': False, 'error': str(e)}
```

---

*For troubleshooting: [Troubleshooting Guide](04_troubleshooting_guide.md)*
*For performance optimization: [Implementation & Performance](02_implementation_performance.md)*
