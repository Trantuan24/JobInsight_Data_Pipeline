# Deployment Guide

This guide covers the recommended ways to run JobInsight Data Pipeline in local development and production environments.

## 1. Local Development (Docker Compose)

The repository ships with a pre-configured `docker-compose.yml` that spins up the entire stack: PostgreSQL, Airflow (webserver + scheduler + init), and optional Grafana.

### Prerequisites
- Docker ≥ 20.10
- Docker Compose plugin ≥ 2.x

### Steps
```bash
# 1. Clone & configure environment
cp env.example .env   # update credentials / API keys if needed

# 2. Build & run containers (detached)
docker-compose up --build -d

# 3. Access services
# Airflow UI:   http://localhost:8080  (admin/admin or airflow/airflow)
# Grafana:      http://localhost:3001  (admin/admin)
# Postgres:     localhost:5434 (user: airflow, pass: airflow)
```

### Customising Airflow
- **Executor**: Currently `LocalExecutor`. Change `AIRFLOW__CORE__EXECUTOR` to `CeleryExecutor` and add a `celery` worker service for distributed task execution.
- **DAGs**: Mount new DAG files into `./dags` or install them via a custom Docker image.
- **Python Dependencies**: Add packages to `requirements.txt` *and* modify the root `Dockerfile`.

### Persisting Data
- Postgres data is stored in the Docker volume `postgres-db-volume`.
- Airflow logs reside in `./logs`. Mount to an external volume or S3 bucket for durability.

## 2. Production Deployment

There are multiple ways to move from development to production. Two reference architectures are described below.

### 2.1 Container-Orchestrated (Kubernetes)

| Component | Recommended Service |
|-----------|--------------------|
| Airflow | MWAA (AWS) / Cloud Composer (GCP) / Astronomer / Helm-based OSS Airflow |
| Database | Managed Postgres (RDS, Cloud SQL) |
| Storage | S3 / GCS for logs & data snapshots |
| Monitoring | Prometheus + Grafana / Cloud provider stack |

Steps (Helm example):
```bash
# Add Airflow Helm repo
helm repo add apache-airflow https://airflow.apache.org
helm install jobinsight-airflow apache-airflow/airflow \
  --namespace data-pipelines \
  --values k8s/airflow-values.yaml
```

Key considerations:
- Use **KubernetesExecutor** or **CeleryKubernetesExecutor** for horizontal scaling.
- Configure a `Secrets Backend` (e.g., AWS Secrets Manager) for environment variables.
- Mount the `sql/` directory as an init-container to execute schema migrations.

### 2.2 VM-Based (Docker Compose on VM)
For smaller workloads, running `docker-compose` on a cloud VM is sufficient:
1. Provision Ubuntu 22.04 LTS VM with 2 vCPU / 4 GB RAM.
2. Install Docker & Docker Compose.
3. Copy repository code and `docker-compose up -d`.
4. Configure firewall rules for ports 8080 (Airflow) & 3001 (Grafana).

## 3. CI / CD Pipeline

A simple **GitHub Actions** workflow can automatically:
1. Run unit tests (`pytest`).
2. Lint code (Black, Flake8, SQLFluff).
3. Build Docker image and push to Docker Hub / ECR / GCR.
4. Trigger deployment via Argo CD / Helm / SSH.

```yaml
name: CI
on:
  push:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: "3.10"
      - name: Install deps
        run: |
          pip install -r requirements.txt
          pip install -r requirements-dev.txt
      - name: Run tests
        run: pytest
```

## 4. Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AIRFLOW__CORE__EXECUTOR` | Airflow executor class | `LocalExecutor` |
| `POSTGRES_JOBINSIGHT_DB` | Name of analytics DB | `jobinsight` |
| `POSTGRES_JOBINSIGHT_USER` | DB user | `jobinsight` |
| `POSTGRES_JOBINSIGHT_PASSWORD` | DB password | `jobinsight` |
| _see `.env.example`_ | Additional crawler/API secrets | _empty_ |

## 5. Backup & Restore
- **Database**: Use `pg_dump` nightly and store dumps in S3 lifecycle bucket.
- **DAGs & Code**: Source-controlled in Git; ensure regular pushes.
- **Docker Volumes**: Snapshot VM disks or use CSI drivers in Kubernetes.

## 6. Zero-Downtime Upgrade Strategy
1. Deploy new Airflow image as *blue* deployment.
2. Wait for scheduler/webserver to become healthy.
3. Switch DAG traffic via service selector change or load balancer.
4. Remove *green* (old) deployment.

## 7. Troubleshooting
| Symptom | Possible Cause | Fix |
|---------|----------------|-----|
| Airflow Webserver 502 | Container restart loop | Run `docker-compose logs airflow-webserver` & inspect env vars |
| DAG not visible | Wrong `dags_folder` path | Verify volume mount & `AIRFLOW__CORE__DAGS_FOLDER` |
| Playwright errors | Missing dependencies | Re-build Docker image to install browsers `playwright install --with-deps chromium` |
