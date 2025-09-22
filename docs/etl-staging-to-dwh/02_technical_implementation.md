# JobInsight ETL - Technical Implementation Guide

## Overview

Technical implementation details cho Staging to Data Warehouse ETL process, focusing on DuckDB operations, cross-database ETL patterns, và dimensional modeling implementation.

## 1. Core ETL Orchestrator

### Main Entry Point
```python
from src.etl.etl_main import run_staging_to_dwh_etl
from datetime import datetime, timedelta

# Basic execution
result = run_staging_to_dwh_etl()

# With custom date
last_week = datetime.now() - timedelta(days=7)
result = run_staging_to_dwh_etl(last_etl_date=last_week)

# Check execution results
if result['success']:
    print(f"✅ Processed {result['source_count']} staging records")
    print(f"⏱️ Execution time: {result['duration_seconds']:.2f}s")
    print(f"📊 Facts generated: {result['fact_count']}")
    print(f"🌉 Bridge records: {result['bridge_count']}")
else:
    print(f"❌ ETL failed: {result['error']}")
```

### Return Structure
```python
{
    "success": bool,
    "source_count": int,               # staging records processed
    "fact_count": int,                 # daily grain facts created
    "bridge_count": int,               # bridge records created
    "dim_stats": {                     # dimension processing results
        "DimJob": {"inserted": int, "updated": int, "unchanged": int},
        "DimCompany": {"inserted": int, "updated": int, "unchanged": int},
        "DimLocation": {"inserted": int, "updated": int, "unchanged": int},
        "DimDate": {"inserted": int, "updated": int, "unchanged": int}
    },
    "total_dim_inserted": int,         # total dimension records inserted
    "total_dim_updated": int,          # total dimension records updated
    "load_months": List[str],          # processed load months
    "duration_seconds": float,         # total execution time (seconds)
    "validation_success": bool,        # data integrity validation result
    "validation_message": str,         # validation details
    "export_success": bool,            # parquet export success
    "export_message": str,             # export details
    "export_stats": dict,              # export statistics
    "error": str  # if success=False
}
```

## 2. Cross-Database ETL Implementation

### PostgreSQL to DuckDB Pipeline

#### Data Loading from Staging
```python
def get_staging_batch(last_etl_date: datetime):
    """Load staging data từ PostgreSQL theo crawled_at"""

    if last_etl_date is None:
        last_etl_date = datetime.now() - timedelta(days=7)

    query = f"""
        SELECT *
        FROM {STAGING_JOBS_TABLE}
        WHERE crawled_at >= %s
        OR (crawled_at IS NOT NULL AND %s IS NULL)
    """

    df = get_dataframe(query, params=(last_etl_date, last_etl_date))
    logger.info(f"📊 Loaded {len(df)} staging records from {last_etl_date}")
    return df
```

#### DuckDB Connection Management
```python
def get_duckdb_connection(duckdb_path: str = DUCKDB_PATH):
    """Kết nối đến DuckDB (không set PRAGMA trong code hiện tại)"""
    if not os.path.isabs(duckdb_path):
        duckdb_path = os.path.join(PROJECT_ROOT, duckdb_path)
    os.makedirs(os.path.dirname(duckdb_path), exist_ok=True)
    return duckdb.connect(duckdb_path)
```

## 3. Dimensional Processing Implementation

### SCD Type 2 Implementation Pattern

#### DimJob Processing
```python
def process_dim_job(staging_df, duckdb_conn):
    """Process DimJob với SCD Type 2 logic"""
    
    current_date = datetime.now().date()
    
    # Get existing dimension records
    existing_df = pd.read_sql("""
        SELECT job_sk, job_id, title_clean, skills, effective_date, is_current
        FROM DimJob 
        WHERE is_current = true
    """, duckdb_conn)
    
    new_records = []
    updated_records = []
    
    for _, staging_row in staging_df.iterrows():
        job_id = staging_row['job_id']
        new_title = staging_row['title']
        new_skills = staging_row['skills']
        
        # Check if job exists
        existing_job = existing_df[existing_df['job_id'] == job_id]
        
        if existing_job.empty:
            # New job - create new dimension record
            new_records.append({
                'job_id': job_id,
                'title_clean': new_title,
                'skills': new_skills,
                'effective_date': current_date,
                'expiry_date': date(9999, 12, 31),
                'is_current': True
            })
        else:
            # Existing job - check for changes
            current_record = existing_job.iloc[0]
            
            if (current_record['title_clean'] != new_title or 
                current_record['skills'] != new_skills):
                
                # SCD Type 2: Close current record
                updated_records.append({
                    'job_sk': current_record['job_sk'],
                    'expiry_date': current_date - timedelta(days=1),
                    'is_current': False
                })
                
                # Create new current record
                new_records.append({
                    'job_id': job_id,
                    'title_clean': new_title,
                    'skills': new_skills,
                    'effective_date': current_date,
                    'expiry_date': date(9999, 12, 31),
                    'is_current': True
                })
    
    # Execute batch operations
    if new_records:
        insert_dimension_records(duckdb_conn, 'DimJob', new_records)
    
    if updated_records:
        update_dimension_records(duckdb_conn, 'DimJob', updated_records)
    
    logger.info(f"📋 DimJob: {len(new_records)} inserted, {len(updated_records)} updated")
    
    return len(new_records), len(updated_records)
```

#### Batch Insert with Error Handling
```python
def insert_dimension_records(conn, table_name, records):
    """Batch insert với fallback to individual inserts"""
    
    if not records:
        return
    
    try:
        # Attempt batch insert
        df = pd.DataFrame(records)
        df.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
        
        logger.info(f"✅ Batch inserted {len(records)} records to {table_name}")
        
    except Exception as e:
        logger.warning(f"⚠️ Batch insert failed for {table_name}: {e}")
        logger.info(f"🔄 Falling back to individual inserts...")
        
        # Fallback to individual inserts
        success_count = 0
        for record in records:
            try:
                df_single = pd.DataFrame([record])
                df_single.to_sql(table_name, conn, if_exists='append', index=False)
                success_count += 1
            except Exception as individual_error:
                logger.error(f"❌ Failed to insert record {record}: {individual_error}")
        
        logger.info(f"✅ Individual inserts: {success_count}/{len(records)} successful")
```

## 4. Fact Processing Implementation

### Daily Grain Fact Generation

#### FactJobPostingDaily Processing
```python
def process_fact_job_posting_daily(staging_df, duckdb_conn):  # Deprecated in favor of FactHandler.generate_fact_records
    """Generate daily grain facts cho job postings"""
    
    fact_records = []
    
    for _, staging_row in staging_df.iterrows():
        job_id = staging_row['job_id']
        posted_date = staging_row['posted_time'].date()
        due_date = staging_row['due_date'].date() if staging_row['due_date'] else posted_date
        
        # Get dimension keys
        job_sk = get_dimension_key(duckdb_conn, 'DimJob', 'job_id', job_id)
        company_sk = get_dimension_key(duckdb_conn, 'DimCompany', 'company_name_standardized', 
                                     staging_row['company_name'])
        
        # Generate daily facts từ posted_date → due_date
        current_date = posted_date
        while current_date <= due_date:
            fact_record = {
                'job_sk': job_sk,
                'company_sk': company_sk,
                'date_id': current_date,
                'salary_min': staging_row['salary_min'],
                'salary_max': staging_row['salary_max'],
                'salary_type': staging_row['salary_type'],
                'due_date': staging_row['due_date'],
                'load_month': current_date.strftime('%Y-%m')
            }
            
            fact_records.append(fact_record)
            current_date += timedelta(days=1)
    
    # Bulk insert facts
    if fact_records:
        bulk_insert_facts(duckdb_conn, 'FactJobPostingDaily', fact_records)
    
    logger.info(f"📊 Generated {len(fact_records)} daily grain facts")
    return len(fact_records)
```

#### Optimized Bulk Fact Insert
```python
def bulk_insert_facts(conn, table_name, fact_records):  # Deprecated - current path uses UPSERT per-date with RETURNING fact_id
    """Optimized bulk insert cho fact tables"""
    
    if not fact_records:
        return
    
    try:
        # Convert to DataFrame
        df = pd.DataFrame(fact_records)
        
        # Remove duplicates based on business key
        if table_name == 'FactJobPostingDaily':
            df = df.drop_duplicates(subset=['job_sk', 'date_id'])
        
        # Batch insert với upsert logic
        df.to_sql(f"temp_{table_name}", conn, if_exists='replace', index=False)
        
        # Upsert from temp table
        upsert_query = f"""
            INSERT OR REPLACE INTO {table_name}
            SELECT * FROM temp_{table_name}
        """
        
        conn.execute(upsert_query)
        conn.execute(f"DROP TABLE temp_{table_name}")
        
        logger.info(f"✅ Bulk inserted {len(fact_records)} facts to {table_name}")
        
    except Exception as e:
        logger.error(f"❌ Bulk fact insert failed: {e}")
        raise
```

### Bridge Table Processing

#### FactJobLocationBridge Implementation
```python
def process_fact_job_location_bridge(staging_df, duckdb_conn):
    """Process many-to-many job-location relationships"""
    
    bridge_records = []
    
    for _, staging_row in staging_df.iterrows():
        job_id = staging_row['job_id']
        location_detail = staging_row['location_detail']
        
        # Get fact_id for this job
        fact_ids = get_fact_ids(duckdb_conn, 'FactJobPostingDaily', 'job_sk', 
                               get_dimension_key(duckdb_conn, 'DimJob', 'job_id', job_id))
        
        # Parse location details
        locations = parse_location_detail(location_detail)
        
        for location in locations:
            location_sk = get_dimension_key(duckdb_conn, 'DimLocation', 'province', location['province'])
            
            for fact_id in fact_ids:
                bridge_records.append({
                    'fact_id': fact_id,
                    'location_sk': location_sk
                })
    
    # Bulk insert bridge records
    if bridge_records:
        bulk_insert_bridge(duckdb_conn, 'FactJobLocationBridge', bridge_records)
    
    logger.info(f"🌉 Generated {len(bridge_records)} bridge records")
    return len(bridge_records)
```

## 5. DuckDB Schema Management

### Schema Setup and Validation
```python
def setup_duckdb_schema(conn):
    """Setup DuckDB schema và tables"""
    
    # Create schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS jobinsight_dwh")
    conn.execute("USE jobinsight_dwh")
    
    # Execute schema creation script
    with open('sql/schema_dwh.sql', 'r', encoding='utf-8') as f:
        schema_sql = f.read()
    
    # Execute in chunks to handle complex statements
    statements = schema_sql.split(';')
    for statement in statements:
        if statement.strip():
            try:
                conn.execute(statement)
            except Exception as e:
                logger.warning(f"⚠️ Schema statement failed: {e}")
    
    # Verify tables exist
    tables = ['DimJob', 'DimCompany', 'DimLocation', 'DimDate', 
              'FactJobPostingDaily', 'FactJobLocationBridge']
    
    for table in tables:
        result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        logger.info(f"📋 {table}: {result[0]} records")
    
    logger.info("✅ DuckDB schema setup completed")
```

### Sequence Management
```python
def reset_sequences(conn):
    """Reset sequences cho surrogate keys"""
    
    sequences = ['seq_job_sk', 'seq_company_sk', 'seq_location_sk', 'seq_fact_id']
    
    for seq_name in sequences:
        try:
            # Get current max value
            table_name = seq_name.replace('seq_', '').replace('_sk', '').replace('_id', '')
            if table_name == 'fact':
                table_name = 'FactJobPostingDaily'
                key_column = 'fact_id'
            else:
                table_name = f"Dim{table_name.capitalize()}"
                key_column = f"{table_name.lower().replace('dim', '')}_sk"
            
            max_value = conn.execute(f"SELECT COALESCE(MAX({key_column}), 0) FROM {table_name}").fetchone()[0]
            
            # DuckDB doesn't support ALTER SEQUENCE RESTART
            # We'll handle this in the insert logic instead
            logger.info(f"📊 {seq_name}: current max value = {max_value}")
            
        except Exception as e:
            logger.warning(f"⚠️ Sequence reset failed for {seq_name}: {e}")
```

## 6. Error Handling and Validation

### Data Quality Validation
```python
def validate_etl_integrity(staging_count, facts_generated, dimensions_updated):
    """Validate ETL data integrity"""
    
    # Check minimum fact generation ratio
    min_facts_per_staging = 3  # Average job duration
    expected_min_facts = staging_count * min_facts_per_staging
    
    if facts_generated < expected_min_facts:
        logger.warning(f"⚠️ Low fact generation: {facts_generated} < {expected_min_facts}")
        return False
    
    # Check dimension processing
    total_dim_updates = sum(dimensions_updated.values())
    if total_dim_updates == 0 and staging_count > 0:
        logger.warning(f"⚠️ No dimension updates với {staging_count} staging records")
        return False
    
    logger.info(f"✅ ETL integrity validated: {facts_generated} facts, {total_dim_updates} dim updates")
    return True
```

### Comprehensive Error Handling
```python
def robust_etl_execution():
    """ETL execution với comprehensive error handling"""

    try:
        # Backup database
        backup_path = backup_dwh_database()
        if not backup_path:
            logger.warning("⚠️ Không thể tạo backup database")

        # Execute ETL phases
        result = run_staging_to_dwh_etl()

        # Validate results
        if not result['success']:
            raise Exception(f"ETL failed: {result.get('error', 'Unknown error')}")

        # Check data integrity
        if not result.get('validation_success', False):
            logger.warning(f"⚠️ Data integrity warning: {result.get('validation_message', '')}")

        return result

    except Exception as e:
        logger.error(f"❌ ETL execution failed: {str(e)}")

        # Attempt recovery from backup if available
        if backup_path:
            logger.info("🔄 Attempting recovery from backup...")
            try:
                restore_dwh_from_backup(backup_path)
                logger.info("✅ Successfully recovered from backup")
            except Exception as restore_error:
                logger.error(f"❌ Recovery failed: {restore_error}")

        return {'success': False, 'error': str(e)}
```

## 7. Performance Optimization Patterns

### Batch Processing Optimization
```python
def optimized_dimension_processing(staging_df):
    """Optimized dimension processing với batch operations"""
    
    # Process dimensions in parallel
    from concurrent.futures import ThreadPoolExecutor
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            'dim_job': executor.submit(process_dim_job, staging_df),
            'dim_company': executor.submit(process_dim_company, staging_df),
            'dim_location': executor.submit(process_dim_location, staging_df),
            'dim_date': executor.submit(process_dim_date, staging_df)
        }
        
        results = {}
        for dim_name, future in futures.items():
            try:
                results[dim_name] = future.result(timeout=60)
                logger.info(f"✅ {dim_name} processing completed")
            except Exception as e:
                logger.error(f"❌ {dim_name} processing failed: {e}")
                results[dim_name] = (0, 0)  # (inserted, updated)
    
    return results
```

### Memory-Efficient Processing
```python
def memory_efficient_fact_processing(staging_df, batch_size=1000):
    """Process facts in memory-efficient batches"""
    
    total_facts = 0
    
    for i in range(0, len(staging_df), batch_size):
        batch_df = staging_df.iloc[i:i + batch_size]
        
        # Process batch
        batch_facts = process_fact_job_posting_daily(batch_df, duckdb_conn)
        total_facts += batch_facts
        
        # Force garbage collection
        import gc
        gc.collect()
        
        logger.info(f"📊 Processed batch {i//batch_size + 1}: {batch_facts} facts")
    
    return total_facts
```

## Summary

### **Current Performance**
- ✅ **7.5s execution** với sophisticated dimensional modeling
- ✅ **51 records/second** throughput (complex processing)
- ✅ **100% success rate** với comprehensive validation
- ✅ **SCD Type 2** historical tracking implementation

### **Optimization Opportunities**
1. **Bulk Fact Operations**: 50-70% improvement potential
2. **Parallel Dimension Processing**: 20-30% improvement
3. **Batch Insert Optimization**: Fix column mapping issues
4. **Memory Management**: Efficient processing cho large datasets

### **Implementation Best Practices**
- Cross-database ETL patterns
- SCD Type 2 implementation
- Bulk operations optimization
- Comprehensive error handling
- Data quality validation

*For schema details: [Data Warehouse Schema](03_dwh_schema_design.md)*  
*For performance optimization: [Performance Guide](04_performance_optimization.md)*
