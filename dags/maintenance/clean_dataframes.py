from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from base_settings.base import default_args, FILEPATHSTART, DATAFRAMES_PATH
from pathlib import Path


with DAG(
    dag_id="airflow_dataframes_cleanup",
    default_args=default_args,
    start_date=datetime(2025,11,10),
    schedule="@once",
    tags=["airflow_maintenance"],
    catchup=False, 
) as dag:
    BORDER_DAYS_INTERVAL = 2
    border_date = (datetime.today() - timedelta(days=BORDER_DAYS_INTERVAL)).date()
    delete_old_dataframes = BashOperator(
        task_id="delete_old_files",
        bash_command="""find . -type f | wc -l && find . -type f ! -newermt {{params.border_date}} -exec rm -f {} \; && find . -type f | wc -l""",
        params={"border_date": border_date},
        cwd=DATAFRAMES_PATH,
    )
    delete_old_dataframes
