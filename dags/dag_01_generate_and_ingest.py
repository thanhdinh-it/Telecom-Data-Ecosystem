"""
DAG 01 — Generate & Ingest
Sinh data CSV bằng Python script → Upload lên HDFS raw_zone qua WebHDFS REST API.
Schedule: @daily | ID: dag_01_generate_and_ingest
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

NAMENODE_HTTP = "http://namenode:9870"
HDFS_USER = "root"

default_args = {
    "owner": "de_portfolio",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def create_hdfs_dirs_fn(**context) -> None:
    """Tạo thư mục partition trên HDFS qua WebHDFS REST API."""
    import requests

    ds = context["ds"]
    dirs = [
        f"/data/raw_zone/customers/{ds}",
        f"/data/raw_zone/cdr/{ds}",
        f"/data/raw_zone/tower_status/{ds}",
    ]
    for hdfs_path in dirs:
        url = f"{NAMENODE_HTTP}/webhdfs/v1{hdfs_path}?op=MKDIRS&user.name={HDFS_USER}"
        resp = requests.put(url)
        resp.raise_for_status()
        print(f"  [MKDIR] {hdfs_path} → {resp.json()}")


def upload_to_hdfs_fn(**context) -> None:
    """Upload CSV files lên HDFS qua WebHDFS two-step redirect."""
    import requests

    ds = context["ds"]
    files = [
        ("/tmp/raw/customers.csv",    f"/data/raw_zone/customers/{ds}/customers.csv"),
        ("/tmp/raw/cdr.csv",          f"/data/raw_zone/cdr/{ds}/cdr.csv"),
        ("/tmp/raw/tower_status.csv", f"/data/raw_zone/tower_status/{ds}/tower_status.csv"),
    ]

    for local_path, hdfs_path in files:
        url = (
            f"{NAMENODE_HTTP}/webhdfs/v1{hdfs_path}"
            f"?op=CREATE&overwrite=true&user.name={HDFS_USER}"
        )
        resp = requests.put(url, allow_redirects=False)
        if resp.status_code not in (307, 200, 201):
            raise Exception(f"WebHDFS CREATE step1 failed [{resp.status_code}]: {resp.text}")

        upload_url = resp.headers.get("Location", url)
        with open(local_path, "rb") as f:
            up = requests.put(upload_url, data=f)
            up.raise_for_status()

        print(f"  [UPLOAD] {local_path} → {hdfs_path} (HTTP {up.status_code})")


def verify_hdfs_upload(**context) -> dict:
    """Verify các file đã upload thành công lên HDFS."""
    import requests

    ds = context["ds"]
    paths = [
        f"/data/raw_zone/customers/{ds}/customers.csv",
        f"/data/raw_zone/cdr/{ds}/cdr.csv",
        f"/data/raw_zone/tower_status/{ds}/tower_status.csv",
    ]

    results = {}
    print(f"[VERIFY] HDFS upload results for {ds}:")
    for hdfs_path in paths:
        url = f"{NAMENODE_HTTP}/webhdfs/v1{hdfs_path}?op=GETFILESTATUS&user.name={HDFS_USER}"
        resp = requests.get(url)
        if resp.status_code == 200:
            status = resp.json().get("FileStatus", {})
            size = status.get("length", 0)
            results[hdfs_path] = f"{size:,} bytes"
            print(f"  ✅ {hdfs_path}: {size:,} bytes")
        else:
            results[hdfs_path] = f"ERROR {resp.status_code}"
            print(f"  ❌ {hdfs_path}: HTTP {resp.status_code}")

    return results


def load_customers_to_postgres_fn(**context) -> None:
    """Load customers từ HDFS CSV vào PostgreSQL raw.customers."""
    import io
    import os

    import pandas as pd
    import requests
    from sqlalchemy import create_engine, text

    POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
    POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.environ.get("POSTGRES_DB", "telecom_db")
    POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "changeme")

    ds = context["ds"]
    hdfs_path = f"/data/raw_zone/customers/{ds}/customers.csv"

    dl_url = f"{NAMENODE_HTTP}/webhdfs/v1{hdfs_path}?op=OPEN&user.name=root"
    resp_dl = requests.get(dl_url, allow_redirects=True)
    resp_dl.raise_for_status()

    df = pd.read_csv(io.BytesIO(resp_dl.content))
    df.drop_duplicates(subset=["customer_id"], inplace=True)

    engine = create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE raw.customers CASCADE"))
    df.to_sql("customers", engine, schema="raw", if_exists="append", index=False)
    print(f"[POSTGRES] Loaded {len(df)} rows into raw.customers")


with DAG(
    dag_id="dag_01_generate_and_ingest",
    default_args=default_args,
    description="Sinh CSV data và ingest vào HDFS raw_zone",
    schedule_interval="@daily",
    start_date=datetime(2024, 6, 17),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "raw", "week1"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_raw_data",
        bash_command=(
            "pip install faker pandas && "
            "python /opt/airflow/scripts/generate_data.py "
            "--output-dir /tmp/raw "
            "--customers 10000 "
            "--days 1 "
            "--towers 500"
        ),
    )

    create_hdfs_dirs = PythonOperator(
        task_id="create_hdfs_partitions",
        python_callable=create_hdfs_dirs_fn,
        provide_context=True,
    )

    upload_to_hdfs = PythonOperator(
        task_id="upload_to_hdfs_raw_zone",
        python_callable=upload_to_hdfs_fn,
        provide_context=True,
    )

    verify_upload = PythonOperator(
        task_id="verify_hdfs_upload",
        python_callable=verify_hdfs_upload,
        provide_context=True,
    )

    load_postgres = PythonOperator(
        task_id="load_customers_to_postgres",
        python_callable=load_customers_to_postgres_fn,
        provide_context=True,
    )

    generate_data >> create_hdfs_dirs >> upload_to_hdfs >> verify_upload >> load_postgres
