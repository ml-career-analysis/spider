from datetime import timedelta

FILEPATHSTART = "/opt"
DATAFRAMES_PATH = f"{FILEPATHSTART}/airflow/dags/dataframes"

default_args = {
    "owner": 'admin',
    "retries": 1,
    'retry_delay': timedelta(minutes=1),
    "email": ["rkgaming248@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False
}

databaseConns = {
    "master": {
        "postgres_conn_id": "arxiv",
        "schema": "arxivdb",
    }
}
