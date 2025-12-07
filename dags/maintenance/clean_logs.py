import logging
import os
import shutil
from datetime import datetime, timedelta
from airflow import DAG
from airflow import settings
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from base_settings.base import default_args, FILEPATHSTART


MAX_LOG_DAYS = 5
LOG_DIR = f"{FILEPATHSTART}/airflow/logs"


def find_old_logs():
    session = settings.Session()
    for t in session.query(TaskInstance).filter(
        TaskInstance.execution_date < days_ago(MAX_LOG_DAYS),
        (TaskInstance.execution_date > days_ago(MAX_LOG_DAYS + 3)),
    ):
        ld = f"{LOG_DIR}/dag_id={t.dag_id}/run_id={t.run_id}/"
        # print(ld)
        delete_log_dir(ld)


def delete_scheduler_logs(directory=f"{LOG_DIR}/scheduler"):
    delete_from = datetime.now() - timedelta(days=MAX_LOG_DAYS)
    logging.info(f"deleting starting from {delete_from}")
    # firstLevelDatesDirectories = os.walk(directory)
    for sub_dir in next(os.walk(directory))[1]:
        # print(sub_dir[0])
        print(sub_dir)
        try:
            if datetime.strptime(sub_dir, "%Y-%m-%d") < delete_from:
                delete_log_dir(f"{directory}/{sub_dir}")
        except ValueError as e:
            print(e)


def delete_log_dir(log_dir):
    try:
        # Recursively delete the log directory and its log contents (e.g, 1.log, 2.log, etc)
        shutil.rmtree(log_dir)
        logging.info(f"Deleted directory and log contents: {log_dir}")
    except OSError as e:
        logging.info(f"Unable to delete: {e.filename} - {e.strerror}")


with DAG(
    dag_id="airflow_log_cleanup",
    start_date=datetime(2025, 11, 10),
    schedule_interval="@once",
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    tags=["airflow_maintenance"],
    doc_md=doc_md_DAG,
) as dag:
    log_scheduler_cleanup_op = PythonOperator(
        task_id="delete_old_logs_scheduler", python_callable=delete_scheduler_logs
    )
    log_cleanup_op = PythonOperator(
        task_id="delete_old_logs", python_callable=find_old_logs
    )
    log_scheduler_cleanup_op >> log_cleanup_op
