# JobInsight ETL - Troubleshooting Guide

## Overview

Solutions for operational issues và reliability concerns trong Raw to Staging ETL process.

## 🚨 **1. Intermittent Failures** (Critical Issue)

### **Problem**: Multiple Retry Attempts Required
- **Symptoms**: ETL requires 2-4 attempts before success
- **Example**: July 31st required attempt=4 before completion
- **Impact**: Delays downstream processing

### **Root Causes**
1. **Database Connection Timeouts**: Temporary connection issues
2. **Resource Contention**: Competing with other processes
3. **Memory Pressure**: System resource constraints

### **Solutions**

#### **Immediate Fix**: Enhanced Retry Logic
```python
def robust_etl_execution():
    max_retries = 5
    base_delay = 30

    for attempt in range(max_retries):
        try:
            result = run_etl()
            if result['success']:
                return result

        except psycopg2.OperationalError as e:
            if "connection" in str(e).lower():
                logger.warning(f"Connection error attempt {attempt + 1}")
                time.sleep(base_delay * (2 ** attempt))  # Exponential backoff
                continue
            raise

    raise Exception("ETL failed after all retries")
```

#### **Long-term Fix**: Connection Pool Optimization
```python
DB_POOL_CONFIG = {
    'pool_size': 3,
    'max_overflow': 5,
    'pool_timeout': 60,
    'pool_recycle': 1800,
    'pool_pre_ping': True
}
```

## ⏱️ **2. Performance Degradation**

### **Problem**: Duration > 3 seconds
- **Normal**: ~1.14 seconds
- **Warning**: > 3 seconds
- **Critical**: > 10 seconds

### **Quick Diagnosis**
```bash
# Check recent phase timings
grep "📊.*Performance:" /logs/etl_pipeline.log | tail -5

# Expected normal output:
# Schema Setup: 333ms, Data Saving: 589ms
```

### **Common Bottlenecks & Solutions**

#### **Schema Setup Slowdown** (>1 second)
```python
# Solution: Cache schema checks
SCHEMA_CACHE = {}

def cached_schema_check(schema_name):
    if schema_name not in SCHEMA_CACHE:
        SCHEMA_CACHE[schema_name] = check_schema_exists(schema_name)
    return SCHEMA_CACHE[schema_name]
```

#### **Data Saving Slowdown** (>2 seconds)
```python
# Solution: Batch upserts instead of individual updates
def batch_upsert(records, batch_size=500):
    values = [(r['job_id'], r['location_detail'], r['title']) for r in records]

    cursor.executemany("""
        INSERT INTO staging_jobs (job_id, location_detail, title)
        VALUES (%s, %s, %s)
        ON CONFLICT (job_id) DO UPDATE SET
            location_detail = EXCLUDED.location_detail,
            title = EXCLUDED.title
    """, values)
```

## 📊 **3. Data Quality Issues**

### **Problem**: ETL Integrity Check Failures
- **Symptoms**: Success rate < 98%, missing records after transformation
- **Warning**: "Phát hiện mất mát dữ liệu trong quá trình xử lý!"

### **Quick Diagnosis**
```python
def diagnose_data_loss():
    raw_count = execute_query("SELECT COUNT(*) FROM raw_jobs")[0][0]
    staging_count = execute_query("SELECT COUNT(*) FROM staging_jobs")[0][0]
    processed_count = execute_query(
        "SELECT COUNT(*) FROM staging_jobs WHERE location_detail IS NOT NULL"
    )[0][0]

    print(f"Raw: {raw_count} → Staging: {staging_count} → Processed: {processed_count}")

    if staging_count < raw_count:
        print("❌ Data loss during raw → staging copy")
    if processed_count < staging_count:
        print("❌ Data loss during Python processing")
```

### **Solution**: Enhanced Error Handling
```python
def robust_data_processing(df):
    processed_df = df.copy()
    error_count = 0

    for idx, row in df.iterrows():
        try:
            # Process each field with individual error handling
            if pd.notna(row['location_detail']):
                processed_df.at[idx, 'location_pairs'] = extract_location_info(row['location_detail'])

            if pd.notna(row['title']):
                processed_df.at[idx, 'title'] = clean_title(row['title'])

        except Exception as e:
            error_count += 1
            logger.warning(f"Failed to process record {row['job_id']}: {e}")

    logger.info(f"Processing completed: {len(df) - error_count}/{len(df)} successful")
    return processed_df
```

## 💾 **4. Memory Issues**

### **Problem**: Memory Usage > 500MB
- **Normal**: 173MB peak usage
- **Warning**: > 400MB
- **Critical**: > 800MB hoặc out of memory errors

### **Quick Check**
```python
import psutil

def check_memory():
    process = psutil.Process()
    memory_mb = process.memory_info().rss / 1024 / 1024

    if memory_mb > 400:
        logger.warning(f"High memory usage: {memory_mb:.1f}MB")

    return memory_mb
```

### **Solution**: Memory-Efficient Processing
```python
def memory_efficient_processing(df):
    # Process in smaller chunks
    chunk_size = 1000
    processed_chunks = []

    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i + chunk_size].copy()
        processed_chunk = process_data_chunk(chunk)
        processed_chunks.append(processed_chunk)

        # Force garbage collection
        import gc
        gc.collect()

    return pd.concat(processed_chunks, ignore_index=True)
```

## 🔌 **5. Database Connection Issues**

### **Problem**: Connection Timeouts
- **Symptoms**: `psycopg2.OperationalError: connection timeout`
- **Impact**: ETL fails during database operations

### **Quick Diagnosis**
```sql
-- Check active connections
SELECT count(*) as active_connections FROM pg_stat_activity WHERE state = 'active';

-- Check connection limits
SELECT setting as max_connections FROM pg_settings WHERE name = 'max_connections';
```

### **Solutions**

#### **Connection Pool Optimization**
```python
def create_robust_connection_pool():
    from sqlalchemy import create_engine
    from sqlalchemy.pool import QueuePool

    engine = create_engine(
        DATABASE_URL,
        poolclass=QueuePool,
        pool_size=3,              # Reduced pool size
        max_overflow=2,           # Limited overflow
        pool_timeout=60,          # Increased timeout
        pool_recycle=1800,        # Recycle every 30 minutes
        pool_pre_ping=True        # Validate connections
    )

    return engine
```

#### **Connection Retry Logic**
```python
def execute_with_connection_retry(operation, max_retries=3):
    for attempt in range(max_retries):
        try:
            return operation()

        except psycopg2.OperationalError as e:
            if "connection" in str(e).lower() and attempt < max_retries - 1:
                logger.warning(f"Connection error attempt {attempt + 1}")
                time.sleep(10 * (attempt + 1))  # Progressive delay
                continue
            raise
```

## 📊 **6. Monitoring & Emergency Procedures**

### **Health Check Script**
```bash
#!/bin/bash
# Quick ETL health check

echo "=== ETL Health Check ==="

# Recent execution times
echo "Recent execution times:"
grep "Quy trình ETL đã hoàn thành" /logs/etl_pipeline.log | tail -3 | \
    grep -o '[0-9.]\+ giây'

# Recent failures
echo "Recent failures:"
grep "ETL.*thất bại" /logs/etl_pipeline.log | tail -2

# Success rates
echo "Success rates:"
grep "Tỷ lệ thành công" /logs/etl_pipeline.log | tail -3
```

### **Emergency Recovery**
```python
def emergency_etl_recovery():
    """Emergency recovery for complete ETL failure"""

    logger.info("Starting emergency recovery...")

    # Check system resources
    memory_available = check_available_memory()
    if memory_available < 100:  # MB
        logger.error("Insufficient memory for recovery")
        return False

    # Run ETL with minimal batch size
    try:
        result = run_etl(limit=100, verbose=True)
        if result['success']:
            logger.info("Emergency recovery successful")
            return True
    except Exception as e:
        logger.error(f"Emergency recovery failed: {e}")

    return False
```

## 🎯 **Quick Reference**

### **Alert Thresholds**
- **Warning**: Duration > 3s, Memory > 400MB
- **Critical**: Duration > 10s, Multiple retries, Memory > 800MB

### **Common Issues Priority**
1. **Intermittent Failures** (Critical) → Enhanced retry logic
2. **Performance Degradation** (High) → Check phase timings
3. **Data Quality Issues** (Medium) → Enhanced error handling
4. **Memory Issues** (Medium) → Chunk processing
5. **Connection Issues** (Low) → Connection pool optimization

---

*For implementation details: [Implementation & Performance](02_implementation_performance.md)*
