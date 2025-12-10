import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def push_df_to_db(
    ti,
    task_id_xcom,
    table_name,
    update_cols,
    conn_id,
    schema_name,
    key_xcom,
    index_cols,
):
    filename = ti.xcom_pull(task_ids=task_id_xcom, key=key_xcom)
    df = pd.read_csv(filename)
    print(df)
    pg_conn = PostgresHook(postgres_conn_id = 'arxiv')
    q = """select * from articles limit 10"""
    df = pg_conn.get_pandas_df(q)
    print(df)

