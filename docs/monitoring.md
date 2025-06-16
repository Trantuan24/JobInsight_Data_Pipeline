# Monitoring & Observability

This document outlines how to monitor the JobInsight Data Pipeline to ensure data quality, system reliability, and timely failure detection.

## 1. Airflow Native Monitoring

### 1.1 DAG & Task Views
- **Graph View**: Visualise task dependencies and execution order.
- **Tree View**: Inspect historical runs and quickly spot failures.
- **Gantt View**: Analyse task duration bottlenecks.

### 1.2 Alerts
Configure Airflow email/Slack/Discord alerts by editing `airflow.cfg` or environment variables:
```ini
[email]
email_backend = airflow.utils.email.send_email_smtp

[smtp]
smtp_host = smtp.gmail.com
smtp_user = airflow@example.com
smtp_password = your_app_password
smtp_mail_from = airflow@example.com
```
Set `email` and `email_on_failure=True` in your DAG default args.

## 2. Logs

| Source | Location | Retention |
|--------|----------|-----------|
| Airflow Task Logs | `./logs/` (mounted volume) | Persisted on host / Docker volume; ship to S3 or CloudWatch in prod |
| Application Logs | `stdout` of crawler / ETL containers | Captured by Docker; aggregate via Loki/Fluentd |

Best practice: forward logs to a centralised system (ELK, Loki) with labels `dag_id`, `task_id`, `run_id`.

## 3. Metrics & Dashboards

### 3.1 Grafana (Docker Compose Service `grafana`)

The default stack provisions Grafana listening on `localhost:3001`.

1. Log in (`admin` / `admin`).
2. Add **PostgreSQL** datasource (URL: `postgres:5432`, DB: `airflow`, user/pass: `airflow`).
3. Import dashboard `dashboards/airflow.json` (create if not yet).

Recommended Metrics:
- Task Success / Failure count by DAG
- Task duration P90 / P99
- Scheduler & Webserver health checks
- Database connections usage

### 3.2 Prometheus Exporter (Optional)
Add the [airflow-exporter](https://github.com/epoch8/airflow-exporter) container:
```yaml
  airflow-exporter:
    image: epoch8/airflow-exporter:latest
    environment:
      - AIRFLOW_URL=http://airflow-webserver:8080
      - AIRFLOW_USERNAME=admin
      - AIRFLOW_PASSWORD=admin
    ports:
      - "9112:9112"
    depends_on:
      - airflow-webserver
```
Scrape via Prometheus; visualise in Grafana.

## 4. Data Quality Checks
- Implement **Great Expectations** or custom assertions in ETL code.
- Fail the Airflow task if row counts deviate beyond thresholds.
- Store validation results in `logs/validation/`.

## 5. Alert Routing Strategy
| Severity | Channel |
|----------|---------|
| Critical ETL failure | PagerDuty / SMS |
| Non-critical task retry | Slack `#data-pipeline` |
| Data quality anomaly | Email to data-team |

## 6. Future Enhancements
- Integrate **OpenTelemetry** for trace propagation across DAG tasks.
- Auto-provision Grafana dashboards with Terraform.
- Add anomaly detection on daily job counts.
