"""
loadshedding_dag.py
-------------------
Airflow DAG: Runs the full ETL pipeline daily at 06:00 SAST (04:00 UTC).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.extract import run as extract
from etl.transform import run as transform
from etl.load import run as load

default_args = {
    "owner": "adriaan",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="loadshedding_etl_pipeline",
    default_args=default_args,
    description="Daily ETL pipeline for SA load shedding data",
    schedule_interval="0 4 * * *",  # 04:00 UTC = 06:00 SAST
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "load-shedding", "south-africa"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    extract_task >> transform_task >> load_task
