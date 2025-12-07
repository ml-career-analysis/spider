from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'git_pull_testi4',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    tags=['test_dag'],
    catchup=False,
) as dag:

    hello_task = BashOperator(
        task_id='hello_task',
        bash_command='echo "Hello World"',
    )
