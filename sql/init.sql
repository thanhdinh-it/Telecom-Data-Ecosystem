-- PostgreSQL Init Script — Telecom Data Ecosystem
-- Chạy tự động khi container khởi động lần đầu

SELECT 'CREATE DATABASE airflow_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec

SELECT 'CREATE DATABASE metabase_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase_db')\gexec

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Streaming: Fraud Alerts (sink từ PySpark Structured Streaming)
CREATE TABLE IF NOT EXISTS public.fraud_alerts (
    id              SERIAL PRIMARY KEY,
    caller_id       VARCHAR(20)  NOT NULL,
    window_start    TIMESTAMP    NOT NULL,
    window_end      TIMESTAMP    NOT NULL,
    call_count      INTEGER      NOT NULL,
    is_fraud        BOOLEAN      DEFAULT TRUE,
    detected_at     TIMESTAMP    DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraud_caller     ON public.fraud_alerts(caller_id);
CREATE INDEX IF NOT EXISTS idx_fraud_detected   ON public.fraud_alerts(detected_at);
CREATE INDEX IF NOT EXISTS idx_fraud_window     ON public.fraud_alerts(window_start, window_end);

-- Batch: Daily CDR Aggregation (sink từ Spark job)
CREATE TABLE IF NOT EXISTS raw.daily_cdr_agg (
    caller_id               VARCHAR(20)     NOT NULL,
    call_date               DATE            NOT NULL,
    total_calls             INTEGER,
    total_duration_seconds  BIGINT,
    avg_duration_seconds    FLOAT,
    unique_callee_count     INTEGER,
    voice_calls             INTEGER,
    video_calls             INTEGER,
    towers_used             INTEGER,
    avg_duration_minutes    FLOAT,
    loaded_at               TIMESTAMP       DEFAULT NOW(),
    PRIMARY KEY (caller_id, call_date)
);

-- Customers (sync từ data_generator)
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id         VARCHAR(20)     PRIMARY KEY,
    full_name           VARCHAR(200),
    phone_number        VARCHAR(20),
    registration_date   DATE,
    plan_type           VARCHAR(20)     CHECK (plan_type IN ('basic', 'standard', 'premium')),
    is_active           BOOLEAN,
    loaded_at           TIMESTAMP       DEFAULT NOW()
);

-- Analytics layer (populated by dbt)
CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_sk         SERIAL PRIMARY KEY,
    customer_id         VARCHAR(20)     UNIQUE NOT NULL,
    full_name           VARCHAR(200),
    phone_number        VARCHAR(20),
    registration_date   DATE,
    plan_type           VARCHAR(20),
    is_active           BOOLEAN,
    dbt_updated_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.fact_fraud_alerts (
    alert_sk            SERIAL PRIMARY KEY,
    caller_id           VARCHAR(20),
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    call_count          INTEGER,
    is_fraud            BOOLEAN,
    detected_at         TIMESTAMP,
    dbt_updated_at      TIMESTAMP
);

-- DQ Logs: kết quả data quality checks
CREATE TABLE IF NOT EXISTS public.dq_check_results (
    id              SERIAL PRIMARY KEY,
    table_name      VARCHAR(100),
    check_name      VARCHAR(100),
    column_name     VARCHAR(100),
    passed          BOOLEAN,
    details         JSONB,
    checked_at      TIMESTAMP   DEFAULT NOW()
);
