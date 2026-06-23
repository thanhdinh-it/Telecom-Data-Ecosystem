"""
DAG 03 — dbt Run & Test
dbt run (staging → intermediate → marts) → dbt test → generate docs.
Schedule: @daily, sau DAG 02 | ID: dag_03_dbt_run
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner": "de_portfolio",
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": False,
}

DBT_PROJECT_DIR = "/opt/airflow/telecom_dbt"
DBT_PROFILES_DIR = "/opt/airflow/telecom_dbt"

with DAG(
    dag_id="dag_03_dbt_run",
    default_args=default_args,
    description="dbt run + test + docs cho analytics layer",
    schedule_interval="@daily",
    start_date=datetime(2024, 6, 24),
    catchup=False,
    tags=["dbt", "modeling", "analytics", "week6"],
) as dag:

    wait_for_batch = ExternalTaskSensor(
        task_id="wait_for_dag_02_batch",
        external_dag_id="dag_02_batch_cdr_pipeline",
        external_task_id="validate_data_quality",
        mode="reschedule",
        timeout=7200,
        poke_interval=60,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"pip install dbt-postgres --break-system-packages -q && "
            f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} "
            f"--select staging --vars '{{\"execution_date\": \"{{{{ ds }}}}\"}}'  "
        ),
    )

    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select intermediate"
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select marts"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --store-failures"
        ),
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt docs generate --profiles-dir {DBT_PROFILES_DIR}"
        ),
        retries=0,
    )

    (
        wait_for_batch
        >> dbt_deps
        >> dbt_run_staging
        >> dbt_run_intermediate
        >> dbt_run_marts
        >> dbt_test
        >> dbt_docs
    )
