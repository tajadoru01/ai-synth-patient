from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DBT_PROJECT_DIR = "/opt/airflow/dbt_healthcare"


def _run_load(**kwargs):
    """Lazy import so the module is only loaded at execution time, not parse time."""
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    from load_raw_data import run_load
    return run_load()


def _run_triage(**kwargs):
    """Lazy import so the module is only loaded at execution time, not parse time."""
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    from ai_triage_agent import run_triage_agent
    return run_triage_agent()


with DAG(
    dag_id="healthcare_pipeline",
    default_args=default_args,
    description="ELT + AI triage for synthetic EHR data",
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["healthcare", "elt", "ai", "dbt"],
) as dag:

    extract_load = PythonOperator(
        task_id="extract_load",
        python_callable=_run_load,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt deps"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt run --profiles-dir . --project-dir . --target prod"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt test --profiles-dir . --project-dir . --target prod"
        ),
    )

    ai_triage = PythonOperator(
        task_id="ai_triage",
        python_callable=_run_triage,
    )

    extract_load >> dbt_deps >> dbt_run >> dbt_test >> ai_triage