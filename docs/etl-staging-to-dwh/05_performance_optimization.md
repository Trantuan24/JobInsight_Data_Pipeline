# JobInsight ETL - Performance Optimization Guide

## Overview

Performance optimization strategies cho Staging to Data Warehouse ETL process, focusing on fact processing bottleneck analysis và cross-phase optimization opportunities.

## Current Performance Baseline

### Production Performance Metrics

Based on production logs analysis:

```
📊 Performance Metrics (Production Average)
┌─────────────────────────┬─────────────┬─────────────┬─────────────┐
│ Metric                  │ Current     │ Target      │ Status      │
├─────────────────────────┼─────────────┼─────────────┼─────────────┤
│ Total Execution Time    │ 7.5s        │ <10s        │ ✅ Good     │
│ Throughput              │ 51 rec/s    │ >40 rec/s   │ ✅ Good     │
│ Fact Generation Rate    │ 589 ops/s   │ >400 ops/s  │ ✅ Good     │
│ Success Rate            │ 100%        │ >98%        │ ✅ Perfect  │
│ Data Quality            │ 100%        │ >98%        │ ✅ Perfect  │
└─────────────────────────┴─────────────┴─────────────┴─────────────┘
```

### Phase-by-Phase Performance Breakdown

```
🔍 Execution Time Analysis (7.5s total)
┌─────────────────────────┬─────────────┬─────────────┬─────────────┐
│ Phase                   │ Duration    │ Percentage  │ Priority    │
├─────────────────────────┼─────────────┼─────────────┼─────────────┤
│ 1. Backup Creation      │ 0.33s       │ 4.4%        │ 🟢 Low      │
│ 2. Data Loading         │ 0.08s       │ 1.1%        │ 🟢 Low      │
│ 3. Schema Setup         │ 0.15s       │ 2.0%        │ 🟡 Medium   │
│ 4. Dimension Processing │ 2.84s       │ 37.9%       │ 🔴 High     │
│ 5. Fact Processing      │ 4.1s        │ 54.7%       │ 🔴 Critical │
│ 6. Export & Cleanup     │ minimal     │ <1%         │ 🟢 Low      │
└─────────────────────────┴─────────────┴─────────────┴─────────────┘
```

**Key Finding**: Fact Processing (54.7%) và Dimension Processing (37.9%) = **92.6%** execution time!

## Critical Optimization Opportunities

### Priority 1: Fact Processing Optimization (54.7% bottleneck)

#### Current Implementation Analysis
```python
# Current: Individual fact processing với retry logic
def _process_single_fact_record(job, job_sk, company_sk, date_id, ...):
    """Process individual fact record với transaction management"""
    # Check existing record
    # Update hoặc create new record
    # Handle conflicts và retries

# Performance: 4.1s for ~1,915 operations = 589 ops/sec (actual measurement)
```

#### Optimized Implementation
```python
# Optimized: Bulk fact generation (fast)
def bulk_generate_facts(staging_jobs):
    """Pre-generate all fact records in memory, then bulk insert"""
    
    fact_records = []
    
    # Step 1: Generate all facts in memory
    for job in staging_jobs:
        job_sk = get_dimension_key('DimJob', 'job_id', job.job_id)
        company_sk = get_dimension_key('DimCompany', 'company_name', job.company_name)
        
        # Generate daily facts
        for date in generate_date_range(job.posted_date, job.due_date):
            fact_records.append({
                'job_sk': job_sk,
                'company_sk': company_sk,
                'date_id': date,
                'salary_min': job.salary_min,
                'salary_max': job.salary_max,
                'salary_type': job.salary_type,
                'due_date': job.due_date,
                'load_month': date.strftime('%Y-%m')
            })
    
    # Step 2: Bulk insert với batch operations
    batch_size = 1000
    for i in range(0, len(fact_records), batch_size):
        batch = fact_records[i:i + batch_size]
        
        # Create temporary table
        df = pd.DataFrame(batch)
        df.to_sql('temp_facts', conn, if_exists='replace', index=False)
        
        # Bulk upsert from temp table
        conn.execute("""
            INSERT OR REPLACE INTO FactJobPostingDaily
            SELECT * FROM temp_facts
        """)
        
        conn.execute("DROP TABLE temp_facts")

# Expected improvement: 4.1s → 1.2s (70% reduction)
```

#### Performance Impact Analysis
```
Current Fact Processing: 4.1s (1,915 operations)
├── Individual processing: 589 ops/sec (actual measurement)
├── Transaction management: Per-record với retry logic
└── Conflict handling: UPSERT operations với duplicate prevention

Potential Optimized Fact Processing: ~1.2s (theoretical)
├── Bulk operations: Estimated 1,596 ops/sec (3.4x faster)
├── Memory efficiency: Pre-generate in pandas DataFrame
└── Transaction efficiency: Batch commits (not yet implemented)
```

### Priority 2: Dimension Processing Optimization (37.9%)

#### Current Dimension Processing Issues
```python
# Current: Batch insert failures → individual fallback
try:
    # Attempt batch insert
    df.to_sql('DimJob', conn, if_exists='append', index=False, method='multi')
except Exception as e:
    logger.warning(f"Batch insert failed: {e}")
    # Fallback to individual inserts (slow)
    for record in records:
        individual_insert(record)

# Issue: Column count mismatch causes batch failures
# "Binder Error: table DimJob has 10 columns but 9 values were supplied"
```

#### Optimized Dimension Processing
```python
def optimized_dimension_processing():
    """Fix batch insert issues và implement parallel processing"""
    
    # Step 1: Fix column mapping for batch inserts
    def fix_batch_insert(table_name, records):
        if not records:
            return
        
        # Get exact column mapping from table schema
        schema_query = f"PRAGMA table_info({table_name})"
        columns = [row[1] for row in conn.execute(schema_query).fetchall()]
        
        # Ensure DataFrame columns match exactly
        df = pd.DataFrame(records)
        df = df.reindex(columns=columns, fill_value=None)
        
        # Batch insert với correct column mapping
        df.to_sql(table_name, conn, if_exists='append', index=False)
    
    # Step 2: Parallel dimension processing
    from concurrent.futures import ThreadPoolExecutor
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            'dim_job': executor.submit(process_dim_job_optimized, staging_df),
            'dim_company': executor.submit(process_dim_company_optimized, staging_df),
            'dim_location': executor.submit(process_dim_location_optimized, staging_df),
            'dim_date': executor.submit(process_dim_date_optimized, staging_df)
        }
        
        results = {}
        for dim_name, future in futures.items():
            results[dim_name] = future.result(timeout=60)
    
    return results

# Expected improvement: 2.84s → 1.7s (40% reduction)
```

### Priority 3: Schema Setup Optimization (2.0%)

#### Current Schema Setup Issues
```python
# Current: Repeated schema checks every run
def setup_duckdb_schema():
    # Check if schema exists
    conn.execute("CREATE SCHEMA IF NOT EXISTS jobinsight_dwh")
    
    # Execute full schema script every time
    with open('sql/schema_dwh.sql', 'r') as f:
        schema_sql = f.read()
    
    # Execute all statements (even if tables exist)
    for statement in schema_sql.split(';'):
        conn.execute(statement)
```

#### Optimized Schema Setup
```python
# Optimized: Schema caching và incremental setup
SCHEMA_CACHE = {}

def optimized_schema_setup():
    """Cache schema state và only setup what's needed"""
    
    # Check cache first
    if 'dwh_schema_ready' in SCHEMA_CACHE:
        logger.info("📋 Schema already setup (cached)")
        return
    
    # Quick schema validation
    required_tables = ['DimJob', 'DimCompany', 'DimLocation', 'DimDate', 
                      'FactJobPostingDaily', 'FactJobLocationBridge']
    
    existing_tables = []
    for table in required_tables:
        try:
            conn.execute(f"SELECT 1 FROM {table} LIMIT 1")
            existing_tables.append(table)
        except:
            pass
    
    # Only create missing tables
    missing_tables = set(required_tables) - set(existing_tables)
    
    if missing_tables:
        logger.info(f"📋 Creating missing tables: {missing_tables}")
        create_missing_tables(missing_tables)
    
    # Cache success
    SCHEMA_CACHE['dwh_schema_ready'] = True
    logger.info("✅ Schema setup completed (optimized)")

# Expected improvement: 0.15s → 0.05s (67% reduction)
```

## Cross-Phase Performance Comparison

### System-Wide Performance Analysis

| Phase | Current Time | Optimized Time | Improvement | Main Optimization |
|-------|--------------|----------------|-------------|-------------------|
| **Phase 1: Crawler** | 111s | 55s | 50% | CAPTCHA optimization |
| **Phase 2: raw_to_staging** | 1.14s | 0.6s | 47% | Batch upserts |
| **Phase 3: staging_to_dwh** | 7.5s | 3.5s | 53% | Bulk fact operations |
| **Total Pipeline** | 119.64s | 59.1s | **51%** | System-wide optimization |

### End-to-End Throughput Impact

```
Current System Performance:
├── Total Pipeline Time: 119.64s
├── Jobs Processed: 383 jobs
├── End-to-End Throughput: 3.2 jobs/sec
└── Daily Capacity: ~277k jobs/day

Optimized System Performance:
├── Total Pipeline Time: 59.1s (51% improvement)
├── Jobs Processed: 383 jobs
├── End-to-End Throughput: 6.5 jobs/sec (100% improvement)
└── Daily Capacity: ~562k jobs/day (100% improvement)
```

*For complete optimization details, see full documentation file.*

*For troubleshooting performance issues: [Troubleshooting Guide](06_troubleshooting_guide.md)*
