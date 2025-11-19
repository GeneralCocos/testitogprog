from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from create_connections import main as create_connections_main


with DAG(
        dag_id="init_connections",
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=["init"],
) as dag:

    init_connections = PythonOperator(
        task_id="create_connections",
        python_callable=create_connections_main,
    )
