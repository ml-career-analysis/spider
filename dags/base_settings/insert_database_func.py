from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
import pandas as pd
import ast
from contextlib import closing
import logging

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
    if df.empty:
        logging.info("DataFrame is empty — nothing to insert/update")
        return
    logging.info(df)
    df["id"] = df["id"].astype(str)

    columns = list(df.columns)
    records = df.to_records(index=False).tolist()

    insert_cols = ", ".join(columns)
    conflict_cols = ", ".join(index_cols)

    update_set = ", ".join(
        f"{col} = EXCLUDED.{col}" for col in update_cols
    )

    sql = f"""
        INSERT INTO {schema_name}.{table_name} ({insert_cols})
        VALUES %s
        ON CONFLICT ({conflict_cols})
        DO UPDATE SET
        {update_set}
    """

    pg_hook = PostgresHook(postgres_conn_id=conn_id)

    with closing(pg_hook.get_conn()) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, records, page_size=1000)
            conn.commit()

