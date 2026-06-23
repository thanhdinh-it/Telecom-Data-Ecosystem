"""
DAG 02 — Batch CDR Pipeline
PySpark clean + aggregate CDR → Data Quality check → Load to PostgreSQL.
Schedule: @daily, sau DAG 01 | ID: dag_02_batch_cdr_pipeline
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner": "de_portfolio",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

SPARK_MASTER = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "telecom_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "changeme")


def run_spark_batch_fn(**context) -> None:
    """Chạy spark-submit qua Python Docker SDK."""
    import docker

    ds = context["ds"]
    client = docker.from_env()
    container = client.containers.get("spark-master")

    cmd = [
        "/spark/bin/spark-submit",
        "--master", SPARK_MASTER,
        "--deploy-mode", "client",
        "--driver-memory", "1g",
        "--executor-memory", "2g",
        "--num-executors", "1",
        "/opt/airflow/spark_jobs/job_batch_cdr.py", ds,
    ]

    print(f"[SPARK] Executing: {' '.join(cmd)}")
    exit_code, output = container.exec_run(cmd)

    print(output.decode("utf-8"))
    if exit_code != 0:
        raise Exception(f"Spark submit failed with exit code {exit_code}")


def run_dq_checks_fn(**context) -> None:
    """Verify HDFS processed_zone output không rỗng."""
    ds = context["ds"]
    import requests

    NAMENODE_HTTP = "http://namenode:9870"
    path = f"/data/processed_zone/daily_cdr_agg/{ds}"
    url = f"{NAMENODE_HTTP}/webhdfs/v1{path}?op=LISTSTATUS&user.name=root"
    resp = requests.get(url)

    if resp.status_code == 200:
        files = resp.json().get("FileStatuses", {}).get("FileStatus", [])
        total_size = sum(f.get("length", 0) for f in files)
        print(f"[DQ] Processed zone {ds}: {len(files)} file(s), {total_size:,} bytes")
        if total_size == 0:
            raise Exception(f"[DQ FAIL] Processed zone empty for {ds}")
        print(f"[DQ] All checks passed for partition {ds}")
    else:
        raise Exception(f"[DQ FAIL] Cannot access HDFS processed_zone: HTTP {resp.status_code}")


def load_agg_to_postgres_fn(**context) -> None:
    """Load daily_cdr_agg từ HDFS Parquet vào PostgreSQL raw.daily_cdr_agg."""
    import io
    import os

    import pandas as pd
    import requests
    from sqlalchemy import create_engine, text

    ds = context["ds"]
    NAMENODE_HTTP = "http://namenode:9870"
    path = f"/data/processed_zone/daily_cdr_agg/{ds}"

    url = f"{NAMENODE_HTTP}/webhdfs/v1{path}?op=LISTSTATUS&user.name=root"
    resp = requests.get(url)
    resp.raise_for_status()

    files = resp.json().get("FileStatuses", {}).get("FileStatus", [])
    parquet_files = [f["pathSuffix"] for f in files if f["pathSuffix"].endswith(".parquet")]

    if not parquet_files:
        raise Exception(f"No parquet files found in {path}")

    file_path = f"{path}/{parquet_files[0]}"
    dl_url = f"{NAMENODE_HTTP}/webhdfs/v1{file_path}?op=OPEN&user.name=root"
    resp_dl = requests.get(dl_url, allow_redirects=True)
    resp_dl.raise_for_status()

    df = pd.read_parquet(io.BytesIO(resp_dl.content))
    df.drop_duplicates(subset=["caller_id", "call_date"], inplace=True)

    POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
    POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.environ.get("POSTGRES_DB", "telecom_db")
    POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "changeme")

    engine = create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    unique_dates = df["call_date"].astype(str).unique()
    dates_str = "', '".join(unique_dates)

    with engine.begin() as conn:
        if dates_str:
            conn.execute(text(f"DELETE FROM raw.daily_cdr_agg WHERE call_date IN ('{dates_str}')"))

    df.to_sql("daily_cdr_agg", engine, schema="raw", if_exists="append", index=False)
    print(f"[POSTGRES] Loaded {len(df)} rows into raw.daily_cdr_agg for date {ds}")


def log_completion_fn(**context) -> None:
    """Ghi log hoàn thành pipeline."""
    import psycopg2

    ds = context["ds"]
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=int(POSTGRES_PORT),
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        conn.close()
        print(f"[LOG] CDR pipeline completed for {ds}")
    except Exception as e:
        print(f"[LOG WARNING] Could not connect to PostgreSQL: {e}")


with DAG(
    dag_id="dag_02_batch_cdr_pipeline",
    default_args=default_args,
    description="PySpark CDR batch processing + data quality validation",
    schedule_interval="@daily",
    start_date=datetime(2024, 6, 24),
    catchup=False,
    max_active_runs=1,
    tags=["batch", "cdr", "processing", "week2"],
) as dag:

    wait_for_ingest = ExternalTaskSensor(
        task_id="wait_for_dag_01_ingest",
        external_dag_id="dag_01_generate_and_ingest",
        external_task_id="verify_hdfs_upload",
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
    )

    run_spark_cdr = PythonOperator(
        task_id="run_pyspark_cdr_batch",
        python_callable=run_spark_batch_fn,
        provide_context=True,
    )

    validate_dq = PythonOperator(
        task_id="validate_data_quality",
        python_callable=run_dq_checks_fn,
        provide_context=True,
    )

    load_postgres = PythonOperator(
        task_id="load_agg_to_postgres",
        python_callable=load_agg_to_postgres_fn,
        provide_context=True,
    )

    log_completion = PythonOperator(
        task_id="log_pipeline_completion",
        python_callable=log_completion_fn,
        provide_context=True,
        retries=0,
    )

    wait_for_ingest >> run_spark_cdr >> validate_dq >> load_postgres >> log_completion
