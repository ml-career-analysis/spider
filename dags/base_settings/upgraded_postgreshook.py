from airflow.hooks.postgres_hook import PostgresHook
import numpy as np
from contextlib import closing
import pandas as pd
from datetime import datetime
import ast

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
    pg = PostgresHook(postgres_conn_id=conn_id)
    q= """select distinct id from articles"""
    df_check = pg.get_pandas_df(q)
    df_check['id'] = df_check['id'].astype(str)
    df['id'] = df['id'].astype(str)
    df["references_id"] = df["references_id"].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) else []
    )
    ids = df_check['id'].unique().tolist()
    df = df[~(df['id'].isin(ids))]
    records = df.to_records(index=False).tolist()
    columns = list(df.columns)

    pg.insert_rows(
        table=table_name,
        rows=records,
        target_fields=columns,
        commit_every=1000,
        replace=False,
    )
