from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dags.utils.load import run_loading

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="loading_rfm",
    default_args=default_args,
    start_date=datetime(2026, 1, 4),
    schedule_interval=None,
    catchup=False,
    tags=["rfm", "loading"],
):

    load_task = PythonOperator(
        task_id="run_loading",
        python_callable=run_loading,
    )

    load_task
