import arxivscraper
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import time
import json
import arxiv
from base_settings.base import (
    DATAFRAMES_PATH,
    default_args,
    databaseConns
)
from base_settings.upgraded_postgreshook import  push_df_to_db
# workflow test
REQUEST_SLEEP = 2
BATCH = 20
category = "cs"
# date_start = '2025-11-18'
# date_end = '2025-11-19'

date_end = datetime.today().strftime('%Y-%m-%d')
date_start = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')

def fetch_metadata(ti, category, date_start, date_end):
    scraper = arxivscraper.Scraper(category=category, date_from=date_start, date_until=date_end)
    output = scraper.scrape()
    df = pd.DataFrame(output)
    df = df[["id", "title", "abstract", "categories", "created", "authors"]]

    df = df.rename(columns={"created": "published"})

    df["pdf_url"] = df["id"].apply(lambda x: f"https://export.arxiv.org/pdf/{x}.pdf")
    df['id'] = df['id'].astype(str)
    df['authors'] = df['authors'].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)
    df['published'] = pd.to_datetime(df['published']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df = df[df['id'].notna()]
    df[df['id'].notna() & (df['id'] != '')]
    filename = f"{DATAFRAMES_PATH}/test.csv"
    df.to_csv(filename, index=False)

    return filename


def fetch_arxiv_metadata(ti, task_id_xcom, key_xcom, batch_size):
    filename = ti.xcom_pull(task_ids=task_id_xcom, key=key_xcom)
    df = pd.read_csv(filename)
    comments = {}
    df['id'] = df['id'].astype(str)
    ids = df['id'].unique().tolist()
    for i in range(0, len(ids), batch_size):
        batch_ids = ids[i:i+batch_size]
        try:
            search = arxiv.Search(id_list=batch_ids)
            for article in search.results():
                id = article.get_short_id()
                comments[id] = article.comment
        except arxiv.HTTPError as e:
            time.sleep(REQUEST_SLEEP)
    df_comments = pd.DataFrame({"id": list(comments.keys()), "comment": list(comments.values())})
    df_comments['id'] = df_comments['id'].str.replace(r'v.*$', '',regex=True)
    df = df.merge(df_comments, how='left', on='id')
#    df['num_pages'] = df['comment'].apply(extract_num_pages)
    filename = f"{DATAFRAMES_PATH}/arxiv_metadata.csv"
    df.to_csv(filename, index=False)
    return filename



with DAG(
    dag_id = "parser_test",
    default_args=default_args,
    start_date=datetime(2025,11,10),
    schedule="0 9 * * *",
    tags=["arxiv"],
    catchup=False, 
) as dag:
    fetch_scraper = PythonOperator(
        task_id = "fetch_scraper_metadata",
        python_callable = fetch_metadata,
        op_kwargs={
            "category": category,
            "date_start": date_start,
            "date_end": date_end,
        }
    )
    fetch_arxiv = PythonOperator(
        task_id = "fetch_arxiv_metadata",
        python_callable=fetch_arxiv_metadata,
        op_kwargs={
            "task_id_xcom": "fetch_scraper_metadata",
            "key_xcom": "return_value",
            "batch_size": BATCH,
        }
    )
    insert = PythonOperator(
        task_id = 'insert_arxiv_metadata',
        python_callable=push_df_to_db,
        op_kwargs={
            "task_id_xcom": "fetch_arxiv_metadata",
            "key_xcom": "return_value",
            "table_name": "articles",
            "update_cols": ["title", "abstract", "categories", "published", "authors"],
            "conn_id": databaseConns["master"]["postgres_conn_id"],
            "schema_name": databaseConns["master"]["schema"],
            "index_cols": ["id"],
        }
    )
    fetch_scraper >> fetch_arxiv >> insert

