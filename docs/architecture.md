# System Architecture

This document provides a deeper look at the **JobInsight Data Pipeline** architecture, highlighting the main components and how data flows through the system.

## High-Level Diagram

```mermaid
flowchart TD
    subgraph Collection
        A[Playwright Crawlers] -->|HTML/JSON| B((PostgreSQL • RAW))
    end

    subgraph Ingestion
        B --> C[Ingestion Service\n(src/ingestion)]
    end

    subgraph ETL
        C --> D[ETL • Raw → Staging\n(src/etl/raw_to_staging.py)]
        D --> E[ETL • Staging → DWH\n(src/etl/staging_to_dwh.py)]
    end

    subgraph Warehouse
        E --> F((PostgreSQL • DWH))
    end

    subgraph Analytics & Exposure
        F --> G[BI Dashboards / APIs / ML]
    end

    classDef store fill:#fef9c3,stroke:#333,stroke-width:1px;
    class B,F store;
```

## Component Breakdown

| Layer | Technology | Description |
|-------|------------|-------------|
| Collection | **Playwright**, Python | Headless browsers scrape job postings from multiple boards; implemented in `src/crawler`. |
| Raw Storage | **PostgreSQL** (table `raw_jobs`) | Stores unprocessed crawled data for traceability. |
| Ingestion | Python, SQLAlchemy | Lightweight ingestion jobs insert/update raw table; see `src/ingestion`. |
| Transformation | **pandas**, **SQL** | Modular ETL transforms raw → staging → DWH (dimension, fact) via `src/etl`. |
| Orchestration | **Apache Airflow** | Two DAGs—`crawl_jobs` & `etl_pipeline`—schedule and coordinate crawling & ETL tasks. |
| Warehouse | **PostgreSQL** schemas `jobinsight_staging`, default, and DWH tables | Optimised star-schema for analytics. |
| Monitoring | **Airflow UI**, **Grafana**, Logs | Operational visibility and alerting. |

## Airflow DAGs

1. **`crawl_jobs`**
   - Executes Playwright crawlers (parallel tasks by site or pagination).
   - Inserts raw data into `raw_jobs`.

2. **`etl_pipeline`**
   - Runs after `crawl_jobs` completes or on schedule.
   - Executes PythonOperator tasks:
     1. `raw_to_staging`: basic cleansing & type casting.
     2. `staging_to_dwh`: slowly changing dimensions & fact table loads.
   - Sends completion metrics to Grafana/Discord (future roadmap).

## Data Flow Chronology
1. **Crawl** – Raw HTML/JSON captured.
2. **Persist** – Raw rows inserted into `raw_jobs`.
3. **Cleanse** – `raw_to_staging` normalises salary, locations, skills.
4. **Load** – `staging_to_dwh` builds SCD-2 dimensions & fact tables.
5. **Visualise** – BI tools query DWH views (`vw_current_jobs`, `vw_top_companies`, ...).

## Scalability Considerations
- **Executor**: Default `LocalExecutor`; can switch to `CeleryExecutor` or `KubernetesExecutor` for scale-out.
- **Database**: Swap Postgres container with managed service (Cloud SQL, RDS).
- **Crawlers**: Run as separate Airflow DAG or external microservice for elasticity.

## Security Notes
- Secrets stored via `.env`; in production use **HashiCorp Vault** or **AWS Secrets Manager** with Airflow `Secrets Backend`.
- Playwright browsers run in a sandboxed container.

## Future Enhancements
- Add streaming ingestion (Kafka) for near real-time updates.
- Implement incremental snapshotting to reduce load on target DB.
- Auto-generate Grafana dashboards using Provisioning API.
