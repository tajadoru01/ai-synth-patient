from __future__ import annotations
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def _run_triage(**kwargs):
    """Lazy import so the module is only loaded at execution time, not parse time."""
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    from ai_triage_agent import run_triage_agent
    return run_triage_agent()

with DAG(
    dag_id="healthcare_pipeline",
    default_args=default_args,
    description="ETL + AI triage for synthetic EHR data",
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["healthcare", "etl", "ai", "spark"],
) as dag:

    spark_etl = SparkSubmitOperator(
        task_id="spark_etl",
        application="/opt/airflow/scripts/spark_etl.py",
        conn_id="spark_default",
        executor_memory="2g",
        driver_memory="1g",
        name="healthcare-etl-{{ ds }}",
        verbose=False,
        jars="/opt/airflow/jars/postgresql-42.7.3.jar",
    )

    ai_triage = PythonOperator(
        task_id="ai_triage",
        python_callable=_run_triage,
    )

    spark_etl >> ai_triage