# Telecom Data Ecosystem

> Dự án cá nhân tập trung vào việc xây dựng hệ sinh thái thu thập, xử lý và phân tích dữ liệu trong lĩnh vực viễn thông.

---

## Architecture

```
[Python Faker] ──batch──► [HDFS raw_zone]
                               │
[Kafka Producer] ─stream─► [Kafka: telecom_events]
                               │
                    ┌──────────┴──────────────┐
                    ▼                         ▼
            [PySpark Batch]          [PySpark Streaming]
            (CDR Aggregation)        (Fraud Detection)
                    │                         │
                    ▼                         ▼
            [HDFS processed_zone]    [PostgreSQL: fraud_alerts]
              (Parquet format)                │
                    │                         │
                    └──────────┬──────────────┘
                               ▼
                             [dbt]
                     (dim/fact modeling)
                               │
                               ▼
                    [Analytics: churn features]
                               │
                               ▼
                    [Metabase Dashboard]

Orchestration: Apache Airflow
```

---

## Tech Stack

| Component | Technology | Alternative considered | Reason chosen |
|---|---|---|---|
| Ingestion | Python + Faker | Real telco data | Faker cho phép inject bad data có chủ đích |
| Storage (raw) | HDFS (single-node) | MinIO (S3-compatible) | HDFS replication=1 tiết kiệm RAM, gần với production |
| Batch processing | PySpark 3.4 | pandas | PySpark scale horizontally — industry standard |
| Streaming broker | Kafka KRaft 7.5 | Zookeeper Kafka | KRaft bỏ Zookeeper, tiết kiệm ~512MB RAM |
| Stream processing | PySpark Structured Streaming | Flink | Cùng Spark ecosystem, không cần thêm service |
| Orchestration | Airflow 2.8 | Prefect, Dagster | Airflow phổ biến nhất, DAG-as-code |
| Data modeling | dbt-postgres 1.7 | Plain SQL scripts | Built-in testing, lineage graph, documentation |
| Sink | PostgreSQL 15 | MySQL | Native support của Airflow & Metabase |
| Dashboard | Metabase 0.47 | Grafana | No-code SQL interface, setup nhanh |
| Testing | pytest + dbt tests | unittest | pytest fixtures, PySpark local mode |

---

## Quick Start

### Prerequisites
- Docker Desktop >= 4.0
- Docker Compose >= 2.0
- 16GB RAM (minimum)

### Setup

```bash
# 1. Clone repository
git clone https://github.com/yourusername/telecom-data-ecosystem.git
cd telecom-data-ecosystem

# 2. Cấu hình environment
cp .env.example .env
# Tạo Fernet key cho Airflow:
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Paste kết quả vào .env → AIRFLOW_FERNET_KEY=...

# 3. Start infrastructure (theo thứ tự)
docker-compose up postgres -d
docker-compose up namenode datanode -d
docker-compose up airflow-init         # chờ init xong
docker-compose up airflow-webserver airflow-scheduler -d
docker-compose up kafka spark-master spark-worker -d

# 4. Verify services
open http://localhost:8080    # Airflow (admin/admin)
open http://localhost:9870    # HDFS Web UI
open http://localhost:8081    # Spark Master UI
```

### Run Batch Pipeline

```bash
# Trigger DAG thủ công qua Airflow UI hoặc CLI:
docker exec airflow-scheduler airflow dags trigger dag_01_generate_and_ingest
docker exec airflow-scheduler airflow dags trigger dag_02_batch_cdr_pipeline
```

### Run Streaming

```bash
# Terminal 1: Start Kafka producer (--rate: events/sec, --fraud-pct: fraud burst %)
docker exec -it spark-worker python /opt/airflow/streaming/kafka_producer.py --rate 20 --fraud-pct 2

# Terminal 2: Start PySpark Streaming job
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.6.0 \
  /opt/airflow/spark_jobs/job_fraud_detection.py
```

### Run Tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

### Run dbt

```bash
cd telecom_dbt
dbt deps
dbt run
dbt test
dbt docs generate && dbt docs serve
```

---

## Project Structure

```
telecom-data-ecosystem/
├── docker-compose.yml               # All services definition
├── .env.example                     # Environment variables template
├── requirements.txt                 # Python dependencies
│
├── data_generator/
│   └── generate_data.py             # Faker script: 10K customers, ~500K CDR/day
│
├── dags/
│   ├── dag_01_generate_and_ingest.py   # Daily: sinh data → HDFS
│   ├── dag_02_batch_cdr.py             # Daily: PySpark batch → DQ check → PostgreSQL
│   └── dag_03_dbt_run.py               # Daily: dbt run → test → docs
│
├── spark_jobs/
│   ├── job_batch_cdr.py             # CDR clean + aggregate (HDFS → Parquet)
│   └── job_fraud_detection.py       # Streaming fraud detection (Kafka → PostgreSQL)
│
├── streaming/
│   └── kafka_producer.py            # Telecom events producer (~20 events/sec)
│
├── data_quality/
│   └── dq_checks.py                 # DataQualityChecker (6 check types)
│
├── telecom_dbt/
│   ├── dbt_project.yml
│   ├── packages.yml
│   └── models/
│       ├── sources.yml
│       ├── staging/                 # stg_customers, stg_cdr_daily, stg_fraud_alerts
│       ├── intermediate/            # int_customer_monthly_usage
│       └── marts/                   # dim_customer, fact_*, churn_feature_store
│
├── tests/
│   ├── test_batch_cdr.py            # 8 unit tests (PySpark clean + aggregate)
│   └── test_data_quality.py         # 12 unit tests (DataQualityChecker)
│
├── sql/
│   └── init.sql                     # PostgreSQL schema init
│
└── docs/
    ├── ADR.md                       # Architecture Decision Records
    └── troubleshooting.md           # Common issues & fixes
```

---

## Data Flows

### 1. Batch (Daily)
```
generate_data.py → /tmp/raw/*.csv
→ HDFS /data/raw_zone/{entity}/{date}/
→ PySpark clean (null / negative duration / timestamp / dedup)
→ PySpark aggregate (daily metrics per subscriber)
→ HDFS /data/processed_zone/daily_cdr_agg/{date}/ (Parquet)
→ DataQualityChecker (6 checks)
→ dbt staging → intermediate → marts
→ PostgreSQL analytics schema
```

### 2. Streaming (Real-time)
```
kafka_producer.py → topic: telecom_events (~20 events/sec)
→ PySpark Structured Streaming
→ Parse JSON + validate
→ Sliding Window (1 min / 30s slide)
→ Fraud detection (call_count > 50/min)
→ PostgreSQL: fraud_alerts
→ DLQ: topic: telecom_events_dlq (malformed events)
```

### 3. Bad Data Handling
- **Batch:** 3% negative duration, 2% wrong timestamp, 1% duplicates → logged & filtered
- **Streaming:** Invalid JSON → Dead-Letter Queue topic

---

## Data Quality Rules

| Table | Rule | Check Type |
|---|---|---|
| CDR | duration_seconds > 0 | value_range |
| CDR | caller_id NOT NULL | not_null |
| CDR | no duplicate record_id | no_duplicates |
| Customers | phone_number null <= 6% | not_null (threshold) |
| Customers | plan_type IN (basic, standard, premium) | accepted_values |
| Fraud | call_count > 50 per 1-min window | streaming rule |

---

## Architecture Decisions

Chi tiết tại [docs/ADR.md](docs/ADR.md):
- **ADR-001:** Parquet vs CSV — tại sao chọn Parquet cho HDFS
- **ADR-002:** KRaft vs Zookeeper Kafka — tại sao bỏ Zookeeper
- **ADR-003:** dbt vs SQL scripts — tại sao dùng dbt
- **ADR-004:** foreachBatch vs JDBC streaming sink

---

## Hardware Requirements

| Component | RAM Allocated |
|---|---|
| HDFS (Namenode + Datanode) | 3GB |
| Spark (Master + Worker) | 4GB |
| Airflow (Webserver + Scheduler) | 2GB |
| Kafka | 1GB |
| PostgreSQL | 500MB |
| Metabase (optional) | 500MB |
| OS + overhead | ~5GB |
| **Total** | **~16GB** |

> **RAM-saving tip:** Metabase được cấu hình với Docker profile `dashboard`.  
> Start khi cần: `docker-compose --profile dashboard up metabase`
