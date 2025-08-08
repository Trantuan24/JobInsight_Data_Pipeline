# JobInsight ETL - Troubleshooting Guide

## Overview

Solutions for operational issues, dimensional modeling problems, và performance concerns trong Staging to Data Warehouse ETL process.

## 🚨 **1. Fact Processing Performance Issues** (Critical)

### **Problem**: Fact Processing Taking > 10 seconds
- **Symptoms**: Execution time > 10s, low throughput < 200 ops/sec
- **Current**: 4.1s for 1,915 operations (54.7% of execution time)
- **Impact**: Delays downstream analytics và BI refresh

### **Root Causes**
1. **Individual Fact Upserts**: One-by-one database operations
2. **Memory Pressure**: Large fact generation in memory
3. **Transaction Overhead**: Individual commits per operation

### **Solutions**

#### **Immediate Fix**: Bulk Fact Generation
```python
def diagnose_fact_processing_performance():
    """Diagnose fact processing bottlenecks"""
    
    # Check operation count vs time
    start_time = time.time()
    fact_count = 0
    
    # Monitor during fact processing
    for job in staging_jobs:
        for date in generate_date_range(job.posted_date, job.due_date):
            # Individual operation
            fact_count += 1
    
    duration = time.time() - start_time
    ops_per_second = fact_count / duration
    
    print(f"📊 Fact operations: {fact_count}")
    print(f"⏱️ Duration: {duration:.2f}s")
    print(f"🚀 Throughput: {ops_per_second:.1f} ops/sec")
    
    if ops_per_second < 400:
        print("❌ Performance below threshold - implement bulk operations")
    
    return ops_per_second
```

#### **Long-term Fix**: Optimized Bulk Processing
```python
def optimized_fact_processing(staging_df):
    """Implement bulk fact generation"""
    
    # Pre-generate all facts in memory
    fact_records = []
    
    for _, row in staging_df.iterrows():
        job_sk = get_dimension_key('DimJob', 'job_id', row['job_id'])
        company_sk = get_dimension_key('DimCompany', 'company_name', row['company_name'])
        
        # Generate daily grain facts
        for date in pd.date_range(row['posted_time'], row['due_date']):
            fact_records.append({
                'job_sk': job_sk,
                'company_sk': company_sk,
                'date_id': date.date(),
                'salary_min': row['salary_min'],
                'salary_max': row['salary_max'],
                'load_month': date.strftime('%Y-%m')
            })
    
    # Bulk insert với batch processing
    batch_size = 1000
    for i in range(0, len(fact_records), batch_size):
        batch = fact_records[i:i + batch_size]
        bulk_insert_facts(batch)
    
    return len(fact_records)
```

## 🔧 **2. Dimension Batch Insert Failures**

### **Problem**: "Binder Error: table has X columns but Y values were supplied"
- **Symptoms**: Batch inserts fail → fallback to individual inserts
- **Impact**: 30-40% performance degradation trong dimension processing
- **Example**: "table DimJob has 10 columns but 9 values were supplied"

### **Diagnostic Steps**

#### **Step 1**: Check Column Mapping
```python
def diagnose_column_mismatch(table_name, records):
    """Diagnose column count mismatch issues"""
    
    # Get table schema
    schema_query = f"PRAGMA table_info({table_name})"
    table_columns = [row[1] for row in conn.execute(schema_query).fetchall()]
    
    # Check DataFrame columns
    df = pd.DataFrame(records)
    df_columns = list(df.columns)
    
    print(f"📋 Table {table_name} columns ({len(table_columns)}): {table_columns}")
    print(f"📊 DataFrame columns ({len(df_columns)}): {df_columns}")
    
    # Find mismatches
    missing_in_df = set(table_columns) - set(df_columns)
    extra_in_df = set(df_columns) - set(table_columns)
    
    if missing_in_df:
        print(f"❌ Missing in DataFrame: {missing_in_df}")
    if extra_in_df:
        print(f"⚠️ Extra in DataFrame: {extra_in_df}")
    
    return len(table_columns) == len(df_columns)
```

#### **Step 2**: Fix Column Mapping
```python
def fix_dimension_batch_insert(table_name, records):
    """Fix column mapping for successful batch inserts"""
    
    if not records:
        return
    
    # Get exact table schema
    schema_query = f"PRAGMA table_info({table_name})"
    table_columns = [row[1] for row in conn.execute(schema_query).fetchall()]
    
    # Create DataFrame với correct column order
    df = pd.DataFrame(records)
    
    # Reindex to match table schema exactly
    df = df.reindex(columns=table_columns, fill_value=None)
    
    # Handle auto-increment columns
    if 'created_date' in table_columns and 'created_date' not in df.columns:
        df['created_date'] = datetime.now()
    
    try:
        # Batch insert với correct mapping
        df.to_sql(table_name, conn, if_exists='append', index=False)
        logger.info(f"✅ Batch inserted {len(records)} records to {table_name}")
        
    except Exception as e:
        logger.error(f"❌ Batch insert still failed: {e}")
        # Fallback to individual inserts với detailed logging
        individual_insert_with_logging(table_name, records)
```

## 📊 **3. SCD Type 2 Implementation Issues**

### **Problem**: Historical Tracking Inconsistencies
- **Symptoms**: Multiple current records, gaps trong effective dates, orphaned historical records
- **Impact**: Incorrect historical analysis, data integrity issues

### **Common SCD Type 2 Issues**

#### **Issue 1**: Multiple Current Records
```sql
-- Diagnostic query
SELECT job_id, COUNT(*) as current_count
FROM DimJob 
WHERE is_current = TRUE
GROUP BY job_id
HAVING COUNT(*) > 1;

-- Should return no rows - if it does, fix needed
```

#### **Solution**: Fix Current Record Logic
```python
def fix_multiple_current_records():
    """Fix multiple current records issue"""
    
    # Find jobs với multiple current records
    problem_jobs = conn.execute("""
        SELECT job_id, COUNT(*) as current_count
        FROM DimJob 
        WHERE is_current = TRUE
        GROUP BY job_id
        HAVING COUNT(*) > 1
    """).fetchall()
    
    for job_id, count in problem_jobs:
        logger.warning(f"⚠️ Job {job_id} has {count} current records")
        
        # Get all current records for this job
        current_records = conn.execute("""
            SELECT job_sk, effective_date 
            FROM DimJob 
            WHERE job_id = ? AND is_current = TRUE
            ORDER BY effective_date DESC
        """, [job_id]).fetchall()
        
        # Keep only the latest record as current
        latest_sk = current_records[0][0]
        
        # Update all others to not current
        for sk, _ in current_records[1:]:
            conn.execute("""
                UPDATE DimJob 
                SET is_current = FALSE, 
                    expiry_date = (SELECT effective_date FROM DimJob WHERE job_sk = ?) - 1
                WHERE job_sk = ?
            """, [latest_sk, sk])
        
        logger.info(f"✅ Fixed current records for job {job_id}")
```

#### **Issue 2**: Effective Date Gaps
```python
def validate_scd_date_integrity():
    """Validate SCD Type 2 date integrity"""
    
    # Check for gaps trong effective date ranges
    gaps_query = """
        WITH date_ranges AS (
            SELECT job_id, effective_date, expiry_date,
                   LAG(expiry_date) OVER (PARTITION BY job_id ORDER BY effective_date) as prev_expiry
            FROM DimJob
            ORDER BY job_id, effective_date
        )
        SELECT job_id, effective_date, prev_expiry
        FROM date_ranges
        WHERE prev_expiry IS NOT NULL 
          AND effective_date != prev_expiry + 1
    """
    
    gaps = conn.execute(gaps_query).fetchall()
    
    if gaps:
        logger.warning(f"⚠️ Found {len(gaps)} date gaps trong SCD Type 2 records")
        for job_id, effective_date, prev_expiry in gaps:
            logger.warning(f"  Job {job_id}: gap between {prev_expiry} and {effective_date}")
    else:
        logger.info("✅ No date gaps found trong SCD Type 2 records")
    
    return len(gaps) == 0
```

## 🔌 **4. DuckDB Limitations và Workarounds**

### **Problem**: DuckDB Sequence Management Issues
- **Symptoms**: "DuckDB không hỗ trợ reset sequence" warnings
- **Impact**: Potential sequence conflicts trong concurrent environments

### **DuckDB-Specific Issues**

#### **Issue 1**: Sequence Reset Limitations
```python
def handle_duckdb_sequence_limitations():
    """Workaround cho DuckDB sequence limitations"""
    
    # DuckDB doesn't support ALTER SEQUENCE RESTART
    # Implement manual sequence management
    
    def get_next_sequence_value(table_name, key_column):
        """Get next sequence value manually"""
        
        # Get current max value
        max_value = conn.execute(f"""
            SELECT COALESCE(MAX({key_column}), 0) FROM {table_name}
        """).fetchone()[0]
        
        return max_value + 1
    
    # Use trong dimension processing
    def insert_with_manual_sequence(table_name, records):
        key_column = f"{table_name.lower().replace('dim', '')}_sk"
        
        for record in records:
            if key_column not in record:
                record[key_column] = get_next_sequence_value(table_name, key_column)
        
        # Insert với manual sequence values
        df = pd.DataFrame(records)
        df.to_sql(table_name, conn, if_exists='append', index=False)
```

#### **Issue 2**: Concurrent Access Limitations
```python
def handle_concurrent_access():
    """Handle DuckDB single-file database limitations"""
    
    # Implement file locking for concurrent ETL runs
    import fcntl
    
    def with_database_lock(operation):
        """Execute operation với database lock"""
        
        lock_file = "data/jobinsight_dwh.lock"
        
        with open(lock_file, 'w') as f:
            try:
                # Acquire exclusive lock
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                
                # Execute operation
                result = operation()
                
                return result
                
            except IOError:
                raise Exception("Another ETL process is running - please wait")
            finally:
                # Lock automatically released when file closes
                pass
```

*For complete troubleshooting details, see full documentation file.*

*For implementation details: [Technical Implementation](02_technical_implementation.md)*  
*For performance optimization: [Performance Guide](05_performance_optimization.md)*
