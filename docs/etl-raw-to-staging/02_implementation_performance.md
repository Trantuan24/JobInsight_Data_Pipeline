# JobInsight ETL - Implementation & Performance Guide

## Overview

Technical implementation details và performance optimization strategies cho Raw to Staging ETL process.

## 1. Core ETL Function

### Basic Usage
```python
from src.etl.raw_to_staging import run_etl

# Execute ETL with default settings
result = run_etl()

# Execute with batch limit và only unprocessed records
result = run_etl(batch_size=1000, only_unprocessed=True)

# Check results
if result['success']:
    print(f"✅ Processed {result['stats']['processed_records']} records in {result['stats']['duration_seconds']:.2f}s")
else:
    print(f"❌ ETL failed: {result.get('error')}")
```

### Return Structure
```python
{
    "success": bool,
    "stats": {
        "total_records": int,
        "processed_records": int,
        "success_rate": float,      # 0-100%
        "duration_seconds": float
    },
    "error": str  # if success=False
}
```

## 2. Implementation Architecture

### SQL+Python Hybrid Approach

**SQL Transformations** (Efficient for bulk operations):
- Data copying: raw_jobs → staging_jobs
- Salary parsing: "10-15 triệu VND" → min/max/type fields
- Deadline conversion: "31/12/2024" → timestamp

**Python Transformations** (Complex logic):
- Location extraction from HTML content
- Multi-location handling (split by "&")
- Text cleaning và standardization

### Database Schema
```sql
-- Key staging_jobs columns
CREATE TABLE staging_jobs (
    job_id VARCHAR(20) PRIMARY KEY,
    title VARCHAR(255),
    company_name VARCHAR(200),
    
    -- SQL-enhanced fields
    salary_min INTEGER,
    salary_max INTEGER,
    salary_type VARCHAR(10),
    due_date TIMESTAMP,
    
    -- Python-enhanced fields
    location_detail JSONB,
    location VARCHAR(100)  -- refined
);
```

## 3. Performance Optimization

### Performance Monitoring

| Phase | Purpose | Monitoring | Status |
|-------|---------|------------|--------|
| **Schema Setup** | Create schema/tables | ✅ Performance tracked | Implemented |
| **SQL Procedures** | Salary/deadline parsing | ✅ Performance tracked | Implemented |
| **Data Loading** | Load to pandas | ✅ Performance tracked | Implemented |
| **Python Processing** | Location/title cleaning | ✅ Performance tracked | Implemented |
| **Data Saving** | Upsert to database | ✅ Performance tracked | Implemented |

### Optimization Solutions

#### 1. Batch Upserts (52% bottleneck)
```python
# Current: Individual upserts (slow)
for record in records:
    cursor.execute("UPDATE staging_jobs SET ... WHERE job_id = %s", record)

# Optimized: Batch operations (fast)
def batch_upsert(records, batch_size=500):
    values = [(r['job_id'], r['location_detail'], r['title']) for r in records]
    
    cursor.executemany("""
        INSERT INTO staging_jobs (job_id, location_detail, title)
        VALUES (%s, %s, %s)
        ON CONFLICT (job_id) DO UPDATE SET
            location_detail = EXCLUDED.location_detail,
            title = EXCLUDED.title
    """, values)

# Expected improvement: 30-50% reduction in Data Saving time
```

#### 2. Schema Caching (29% overhead)
```python
# Cache schema existence checks
SCHEMA_CACHE = {}

def cached_schema_check(schema_name):
    if schema_name not in SCHEMA_CACHE:
        SCHEMA_CACHE[schema_name] = check_schema_exists(schema_name)
    return SCHEMA_CACHE[schema_name]

# Expected improvement: 70% reduction in Schema Setup time
```

#### 3. Parallel Processing (5% of time)
```python
from concurrent.futures import ThreadPoolExecutor

def parallel_data_processing(df):
    def process_locations(chunk):
        chunk['location_detail'] = chunk['location_detail'].apply(extract_location_info)
        return chunk
    
    def process_titles(chunk):
        chunk['title'] = chunk['title'].apply(clean_title)
        return chunk
    
    # Split DataFrame for parallel processing
    chunks = np.array_split(df, 3)
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(process_locations, chunks[0]),
            executor.submit(process_titles, chunks[1]),
            executor.submit(lambda x: x, chunks[2])  # No processing needed
        ]
        
        processed_chunks = [future.result() for future in futures]
    
    return pd.concat(processed_chunks, ignore_index=True)

# Expected improvement: 20-30% reduction in Processing time
```

## 4. Configuration & Deployment

For detailed configuration options, environment variables, và deployment settings, see [Configuration Guide](03_configuration_guide.md).

**Key Configuration Areas:**
- Database connection settings
- ETL execution parameters
- Performance optimization settings
- Environment-specific configurations
- Airflow integration settings

## 5. Data Processing Functions

### Location Processing
```python
def extract_location_info(html_content):
    """Extract location details from HTML"""
    if not html_content:
        return None
    
    soup = BeautifulSoup(html_content, 'html.parser')
    locations = []
    
    for element in soup.find_all(['div', 'span']):
        text = element.get_text().strip()
        if text and ':' in text:
            locations.append(text)
    
    return locations if locations else None

def refine_location(row):
    """Handle multiple locations separated by &"""
    location = row['location']
    if '&' in str(location):
        # Take first location
        return location.split('&')[0].strip()
    return location
```

### Data Cleaning
```python
def clean_title(title):
    """Clean job title"""
    if not title:
        return title
    
    # Remove extra whitespace và noise
    cleaned = re.sub(r'\s+', ' ', title).strip()
    return cleaned

def clean_company_name(company):
    """Standardize company name"""
    if not company:
        return company
    
    return company.strip()
```

## 6. Error Handling & Validation

### ETL Integrity Check
```python
def verify_etl_integrity(source_count, target_count, threshold=0.98):
    """Validate data integrity"""
    if source_count == 0:
        return True
    
    success_rate = target_count / source_count
    
    if success_rate >= threshold:
        logger.info(f"✅ ETL integrity check passed: {success_rate:.2%}")
        return True
    else:
        logger.error(f"❌ ETL integrity check failed: {success_rate:.2%} < {threshold:.2%}")
        return False
```

### Robust Error Handling
```python
def robust_etl_execution():
    """ETL with comprehensive error handling"""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            result = run_etl()
            if result['success']:
                return result
                
        except psycopg2.OperationalError as e:
            if "connection" in str(e).lower() and attempt < max_retries - 1:
                logger.warning(f"Connection error on attempt {attempt + 1}: {e}")
                time.sleep(30 * (attempt + 1))  # Progressive delay
                continue
            raise
            
        except Exception as e:
            logger.error(f"ETL error on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(10)
    
    raise Exception("ETL failed after all retry attempts")
```

## 7. Performance Monitoring

### Built-in Metrics
```python
def track_performance():
    """Monitor ETL performance"""
    metrics = {
        'start_time': time.time(),
        'phase_timings': {},
        'memory_usage': {}
    }
    
    # Track each phase
    for phase in ['schema_setup', 'stored_procedures', 'data_loading', 'data_processing', 'data_saving']:
        phase_start = time.time()
        
        # Execute phase...
        
        metrics['phase_timings'][phase] = time.time() - phase_start
    
    return metrics
```

### Alert Thresholds
```python
ALERT_THRESHOLDS = {
    'duration_seconds': {
        'warning': 3.0,      # seconds
        'critical': 10.0
    },
    'memory_usage': {
        'warning': 400,      # MB
        'critical': 800
    },
    'success_rate': {
        'warning': 0.95,     # 95%
        'critical': 0.90
    }
}
```

## 8. Memory Optimization

### DataFrame Optimization
```python
def optimize_dataframe_memory(df):
    """Optimize DataFrame memory usage"""
    
    # Convert object columns to category for repeated values
    for col in df.select_dtypes(include=['object']):
        if df[col].nunique() / len(df) < 0.5:
            df[col] = df[col].astype('category')
    
    # Downcast numeric types
    for col in df.select_dtypes(include=['int64']):
        df[col] = pd.to_numeric(df[col], downcast='integer')
    
    return df
```

### Memory Monitoring
```python
def monitor_memory():
    """Monitor memory usage"""
    import psutil
    
    process = psutil.Process()
    memory_info = process.memory_info()
    
    return {
        'rss_mb': memory_info.rss / 1024 / 1024,
        'percent': process.memory_percent()
    }
```

## Summary

### **Current Implementation**
- ✅ **5-Phase Processing**: Schema → SQL → Loading → Processing → Saving
- ✅ **Performance Monitoring**: Built-in tracking per phase
- ✅ **Data Integrity**: Verification với configurable threshold
- ✅ **Error Handling**: Comprehensive exception handling

### **Potential Optimizations** (Not yet implemented)
1. **Batch Database Operations**: Implement batch upserts
2. **Schema Validation Caching**: Cache existence checks
3. **Parallel Processing**: Concurrent transformations

### **Monitoring Capabilities**
- Per-phase execution time tracking
- Memory usage monitoring (if psutil available)
- Data integrity validation
- Comprehensive error logging

*For troubleshooting issues: [Troubleshooting Guide](04_troubleshooting_guide.md)*
