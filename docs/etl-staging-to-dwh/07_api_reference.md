# JobInsight ETL - API Reference

## Overview

API documentation cho dimensional modeling functions, DuckDB operations, và usage patterns trong Staging to Data Warehouse ETL process.

## 🚀 **Main ETL Functions**

### `run_staging_to_dwh_etl()`

Primary entry point cho staging to data warehouse ETL process.

#### **Function Signature**
```python
def run_staging_to_dwh_etl(last_etl_date: Optional[datetime] = None) -> Dict[str, Any]:
```

#### **Parameters**
- **`last_etl_date`** *(Optional[datetime])*: Starting date cho incremental processing. Default: 7 days ago

#### **Returns**
```python
{
    "success": bool,                    # Overall success status
    "source_count": int,                # Staging records processed
    "fact_count": int,                  # Daily grain facts created
    "bridge_count": int,                # Bridge records created
    "dim_stats": {                      # Dimension processing results
        "DimJob": {"inserted": int, "updated": int, "unchanged": int},
        "DimCompany": {"inserted": int, "updated": int, "unchanged": int},
        "DimLocation": {"inserted": int, "updated": int, "unchanged": int},
        "DimDate": {"inserted": int, "updated": int, "unchanged": int}
    },
    "total_dim_inserted": int,          # Total dimension records inserted
    "total_dim_updated": int,           # Total dimension records updated
    "load_months": List[str],           # Processed load months (YYYY-MM format)
    "duration_seconds": float,          # Total execution time (seconds)
    "validation_success": bool,         # Data integrity validation result
    "validation_message": str,          # Validation details
    "export_success": bool,             # Parquet export success status
    "export_message": str,              # Export details
    "export_stats": dict,               # Export statistics
    "error": Optional[str]              # Error message if success=False
}
```

#### **Example**
```python
from src.etl.etl_main import run_staging_to_dwh_etl
from datetime import datetime, timedelta

# Basic usage
result = run_staging_to_dwh_etl()

# With custom date
last_week = datetime.now() - timedelta(days=7)
result = run_staging_to_dwh_etl(last_etl_date=last_week)

if result['success']:
    print(f"✅ ETL completed in {result['duration_seconds']:.2f}s")
    print(f"📊 Processed {result['source_count']} staging records")
    print(f"📈 Generated {result['fact_count']} fact records")
    print(f"🌉 Created {result['bridge_count']} bridge records")
    print(f"📅 Load months: {result['load_months']}")
else:
    print(f"❌ ETL failed: {result['error']}")
```

### `get_staging_batch()`

Load staging data từ PostgreSQL với điều kiện theo crawled_at.

#### **Function Signature**
```python
def get_staging_batch(last_etl_date: datetime) -> pd.DataFrame:
```

#### **Parameters**
- **`last_etl_date`** *(datetime)*: Mốc thời gian bắt đầu lấy dữ liệu; truyền None sẽ load tất cả bản ghi có crawled_at NOT NULL

#### **Returns**
- **`pd.DataFrame`**: Dữ liệu staging được lọc theo điều kiện crawled_at >= last_etl_date OR (crawled_at IS NOT NULL AND last_etl_date IS NULL)

#### **Example**
```python
from src.etl.staging_to_dwh import get_staging_batch
from datetime import datetime, timedelta

# Load data from last week
last_week = datetime.now() - timedelta(days=7)
staging_df = get_staging_batch(last_etl_date=last_week)

# Load all unprocessed data (default)
staging_df = get_staging_batch()

print(f"📊 Loaded {len(staging_df)} staging records")
```

## 🏗️ **Dimensional Processing Functions**

### `DimensionHandler` Class

Handles SCD Type 2 dimensional processing.

#### **Class Initialization**
```python
from src.etl.dimension_handler import DimensionHandler

duck_conn = get_duckdb_connection()
dim_handler = DimensionHandler(duck_conn)
```

#### **`process_dimension_with_scd2()`**

Process dimension table với SCD Type 2 logic.

##### **Method Signature**
```python
def process_dimension_with_scd2(
    self,
    staging_records: pd.DataFrame,
    dim_table: str,
    prepare_function: Callable,
    natural_key: str,
    surrogate_key: str,
    compare_columns: List[str]
) -> Dict[str, int]:
```

##### **Parameters**
- **`staging_records`** *(pd.DataFrame)*: Source staging data
- **`dim_table`** *(str)*: Target dimension table name
- **`prepare_function`** *(Callable)*: Function to prepare dimension records
- **`natural_key`** *(str)*: Business key column name
- **`surrogate_key`** *(str)*: Surrogate key column name
- **`compare_columns`** *(List[str])*: Columns to compare for changes

##### **Returns**
```python
{
    "inserted": int,    # New records inserted
    "updated": int,     # Records updated (SCD Type 2)
    "unchanged": int    # Records without changes
}
```

##### **Example**
```python
from src.processing.data_prepare import prepare_dim_job

# Process DimJob với SCD Type 2
result = dim_handler.process_dimension_with_scd2(
    staging_records=staging_df,
    dim_table='DimJob',
    prepare_function=prepare_dim_job,
    natural_key='job_id',
    surrogate_key='job_sk',
    compare_columns=['title_clean', 'skills']
)

print(f"📋 DimJob processing: {result}")
```

### `FactHandler` Class

Handles fact table và bridge table processing.

#### **Class Initialization**
```python
from src.etl.fact_handler import FactHandler

fact_handler = FactHandler(duck_conn)
```

#### **`generate_fact_records()`**

Generate daily grain fact records từ staging data.

##### **Method Signature**
```python
def generate_fact_records(
    self,
    staging_records: pd.DataFrame
) -> Tuple[List[Dict], List[Dict]]:
```

##### **Parameters**
- **`staging_records`** *(pd.DataFrame)*: Source staging data

##### **Returns**
- **`Tuple[List[Dict], List[Dict]]`**: (fact_records, bridge_records)

##### **Example**
```python
# Generate fact và bridge records
fact_records, bridge_records = fact_handler.generate_fact_records(staging_df)

print(f"📊 Generated {len(fact_records)} fact records")
print(f"🌉 Generated {len(bridge_records)} bridge records")
```

## 🔧 **DuckDB Operations**

### `get_duckdb_connection()`

Tạo DuckDB connection đến file DWH.

#### **Function Signature**
```python
def get_duckdb_connection(duckdb_path: str = DUCKDB_PATH) -> duckdb.DuckDBPyConnection:
```

#### **Parameters**
- **`duckdb_path`** *(str)*: Đường dẫn tới file DuckDB. Hỗ trợ relative và absolute path

#### **Returns**
- **`duckdb.DuckDBPyConnection`**: DuckDB connection

#### **Example**
```python
from src.etl.etl_utils import get_duckdb_connection

# Basic connection
conn = get_duckdb_connection()

# Custom configuration
conn = get_duckdb_connection(
    duckdb_path='custom/path/warehouse.duckdb',
    memory_limit='4GB',
    threads=8
)
```

### `setup_duckdb_schema()`

Khởi tạo schema/tables DWH trong DuckDB bằng script sql/schema_dwh.sql.

#### **Function Signature**
```python
def setup_duckdb_schema() -> bool:
```

#### **Returns**
- **`bool`**: True nếu tạo/đảm bảo bảng thành công, False nếu lỗi

#### **Example**
```python
from src.etl.etl_utils import setup_duckdb_schema

# Setup schema
success = setup_duckdb_schema(conn)

if success:
    print("✅ DuckDB schema setup completed")
```

### `batch_insert_records()`

Batch insert cho dimension/fact tables (src/etl/etl_utils.py).

#### **Function Signature**
```python
def batch_insert_records(
    duck_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    records: List[Dict],
    batch_size: int = 1000,
    on_conflict: str = None
) -> int:
```

#### **Parameters**
- **`duck_conn`** *(duckdb.DuckDBPyConnection)*: DuckDB connection
- **`table_name`** *(str)*: Tên bảng đích
- **`records`** *(List[Dict])*: Danh sách bản ghi
- **`batch_size`** *(int)*: Kích thước batch. Mặc định 1000
- **`on_conflict`** *(Optional[str])*: Cú pháp xử lý conflict, ví dụ: "ON CONFLICT (date_id) DO NOTHING" hoặc dùng INSERT OR IGNORE

#### **Returns**
- **`int`**: Số bản ghi insert thành công

#### **Example**
```python
from src.etl.etl_utils import batch_insert_records

# Batch insert
inserted_count = batch_insert_records(
    duck_conn=conn,
    table_name='FactJobPostingDaily',
    records=records,
    batch_size=500
)

print(f"✅ Inserted {inserted_count} records")
```

## 📊 **Data Preparation Functions**

### `prepare_dim_job()`

Prepare job dimension records từ staging data.

#### **Function Signature**
```python
def prepare_dim_job(staging_records: pd.DataFrame) -> pd.DataFrame:
```

#### **Parameters**
- **`staging_records`** *(pd.DataFrame)*: Raw staging data

#### **Returns**
- **`pd.DataFrame`**: Prepared dimension records với columns: job_id, title_clean, skills, effective_date, expiry_date, is_current

#### **Example**
```python
from src.processing.data_prepare import prepare_dim_job

dim_job_df = prepare_dim_job(staging_df)
print(f"📋 Prepared {len(dim_job_df)} job dimension records")
```

### `prepare_dim_company()`

Prepare company dimension records từ staging data.

#### **Function Signature**
```python
def prepare_dim_company(staging_records: pd.DataFrame) -> pd.DataFrame:
```

#### **Parameters**
- **`staging_records`** *(pd.DataFrame)*: Raw staging data

#### **Returns**
- **`pd.DataFrame`**: Prepared company dimension records

### `prepare_dim_location()`

Prepare location dimension records từ staging data.

#### **Function Signature**
```python
def prepare_dim_location(staging_records: pd.DataFrame) -> pd.DataFrame:
```

#### **Parameters**
- **`staging_records`** *(pd.DataFrame)*: Raw staging data

#### **Returns**
- **`pd.DataFrame`**: Prepared location dimension records với province, city, district hierarchy

### `generate_daily_fact_records()`

Generate daily grain fact records cho job postings.

#### **Function Signature**
```python
def generate_daily_fact_records(
    posted_date: Optional[datetime], 
    due_date: Optional[datetime],
    current_date: datetime = None
) -> List[datetime]:
```

#### **Parameters**
- **`posted_date`** *(Optional[datetime])*: Job posting date
- **`due_date`** *(Optional[datetime])*: Job application deadline
- **`current_date`** *(datetime)*: Current processing date. Default: today

#### **Returns**
- **`List[datetime]`**: List of dates for daily grain facts

#### **Example**
```python
from src.processing.data_prepare import generate_daily_fact_records
from datetime import datetime

fact_dates = generate_daily_fact_records(
    datetime(2025, 8, 1), 
    datetime(2025, 8, 5)
)
print(f"📅 Generated {len(fact_dates)} daily fact dates")
```

---

*For implementation details: [Technical Implementation](02_technical_implementation.md)*  
*For troubleshooting: [Troubleshooting Guide](06_troubleshooting_guide.md)*  
*For performance optimization: [Performance Guide](05_performance_optimization.md)*
