import sys
import os

sys.path.append("/opt/airflow/dags")

import arxivscraper
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from base_settings.base import (
    DATAFRAMES_PATH,
    default_args,
    databaseConns
)
from base_settings.insert_database_func import push_df_to_db

category = "cs"
date_start = '2025-11-20'
date_end = '2025-11-30'

def fetch_metadata(ti, category, date_start, date_end):
    print("test")
    scraper = arxivscraper.Scraper(category=category, date_from=date_start, date_until=date_end)
    output = scraper.scrape()
    df = pd.DataFrame(output)
    print("test2")
    df = df[["id", "title", "abstract", "categories", "created", "authors"]]

    df = df.rename(columns={"created": "published"})

    df["pdf_url"] = df["id"].apply(lambda x: f"https://export.arxiv.org/pdf/{x}.pdf")

    filename = f"{DATAFRAMES_PATH}/test.csv"
    df.to_csv(filename, index=False)
    print("test3")
    return filename

with DAG(
    dag_id = "parser_test",
    default_args=default_args,
    start_date=datetime(2025,11,10),
    schedule="@once",
    tags=["arxiv"],
    catchup=False, 
) as dag:
    
    fetch = PythonOperator(
        task_id = "fetch_arxiv_metadata",
        python_callable = fetch_metadata,
        op_kwargs={
            "category": category,
            "date_start": date_start,
            "date_end": date_end,
        }
    )
    insert = PythonOperator(
        task_id = 'insert_arxiv_metadata',
        python_callable=push_df_to_db,
        op_kwargs={
            "task_id_xcom": "fetch_arxiv_metadata",
            "key_xcom": "return_value",
            "table_name": "articles",
            "update_cols": ["title", "abstract", "categories", "created", "authors"],
            "conn_id": databaseConns["master"]["postgres_conn_id"],
            "schema_name": databaseConns["master"]["schema"],
            "update_cols": ["id"],
        }
    )
    fetch >> insert


