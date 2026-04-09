from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dags.utils.ingest_data import run_ingestion

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="ingestion_online_retail",
    default_args=default_args,
    start_date=datetime(2026, 1, 4),
    schedule_interval=None,   # manuel pour l’instant
    catchup=False,
    tags=["rfm", "ingestion"],
):

    ingestion_task = PythonOperator(
        task_id="run_ingestion",
        python_callable=run_ingestion,
    )

    ingestion_task
