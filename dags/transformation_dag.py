from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.transform_rfm import run_transformation

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="transformation_rfm",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["rfm", "transformation"],
):

    transform_task = PythonOperator(
        task_id="run_transformation",
        python_callable=run_transformation,
    )

    transform_task
