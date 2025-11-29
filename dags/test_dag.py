from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
# test
with DAG(
    'simple_hello',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    hello_task = BashOperator(
        task_id='hello_task',
        bash_command='echo "Hello World"',
    )
