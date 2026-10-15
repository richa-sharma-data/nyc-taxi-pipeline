# 🚕 NYC Taxi Analytics Pipeline

An end-to-end data engineering portfolio project that ingests, transforms,
and visualises NYC Yellow Taxi trip data using a modern cloud data stack.

🔗 **Live Dashboard:** [nyc-taxi-richa.streamlit.app](https://richa-nyc-taxi-dashboard.streamlit.app)

---
```
## Architecture

NYC TLC Public Data (Parquet)
↓
Amazon S3              — raw partitioned storage
↓
Redshift Serverless       — cloud data warehouse
↓
dbt Core              — staging + mart transformations
↓
Streamlit              — interactive analytics dashboard
↓
Streamlit Cloud          — free public deployment
```
---

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Python, boto3, requests |
| Storage | Amazon S3 (Hive-partitioned Parquet) |
| Warehouse | Amazon Redshift Serverless |
| Orchestration | Apache Airflow via Astro CLI |
| Transformation | dbt Core + dbt-redshift |
| Dashboard | Streamlit + Plotly |
| Deployment | Streamlit Community Cloud |

---

## Project Structure
```
nyc-taxi-pipeline/
├── dags/
│   └── nyc_taxi_pipeline.py      # Airflow DAG — extract → load
├── include/
│   ├── s3_uploader.py            # downloads parquet, uploads to S3
│   └── redshift_loader.py        # COPY from S3 into Redshift
├── models/
│   ├── staging/
│   │   ├── stg_yellow_taxi.sql   # cleans raw data, filters bad dates
│   │   └── schema.yml            # dbt tests
│   └── marts/
│       └── mart_daily_trips.sql  # daily aggregations
├── dbt_project.yml
├── app.py                        # Streamlit dashboard
└── requirements.txt
```
---

## Data Pipeline

**1. Ingestion**
- Downloads NYC TLC Yellow Taxi parquet files (monthly) from the public TLC website
- Enriches with metadata columns (`ingestion_month`, `ingested_at`, `source`)
- Uploads to S3 using Hive-style partitioning: `raw/yellow_taxi/year=YYYY/month=MM/`

**2. Loading**
- Redshift `COPY` command loads directly from S3 using IAM role authentication
- Idempotent — deletes partition before reloading, safe to re-run

**3. Transformation (dbt)**
- `stg_yellow_taxi` — cleans nulls, filters corrupt dates using `ingestion_month`
- `mart_daily_trips` — aggregates to daily level: trips, revenue, avg fare, avg distance

**4. Dashboard**
- Date range filter
- KPI tiles: total trips, total revenue, avg fare, avg distance
- Line, bar and area charts via Plotly
- Raw data table

---

## Key Design Decisions

- **In-memory parquet upload** — uses `io.BytesIO`, no temp files, works cleanly in Docker
- **IAM Role auth** — no AWS keys embedded in SQL, credentials managed via IAM
- **Idempotent loads** — DELETE + COPY pattern means safe reruns without duplicates
- **Dynamic date filtering** — dbt derives valid date range from `ingestion_month` column, no hardcoded dates
- **Snappy compression** — parquet + Snappy reduces file size ~65% vs CSV

---

## Running Locally

**Prerequisites:** Python 3.9+, Docker Desktop, AWS account

```bash
# 1. Clone
git clone https://github.com/YOUR_USERNAME/nyc-taxi-dashboard.git

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set environment variables
cp .env.example .env
# fill in your Redshift credentials

# 4. Run dashboard
streamlit run app.py
```
## About

Built by **Richa** — ETL Developer transitioning into cloud data engineering.

Connect on [Linkedin](https://www.linkedin.com/in/richa-sharma-65ba079/)

---
