from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="process_test_dag",
    start_date=datetime(2025, 11, 29),
    schedule="@daily",
    catchup=False
) as dag:
    process = EmptyOperator(task_id="process")
