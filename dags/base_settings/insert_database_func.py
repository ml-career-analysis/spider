from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

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
        return

    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    insert_cols = df.columns.tolist()
    update_stmt = ", ".join([f"{col}=EXCLUDED.{col}" for col in update_cols])
    conflict_cols_sql = ", ".join(index_cols)

    with engine.begin() as conn:
        for _, row in df.iterrows():
            values_placeholders = ", ".join([f"%({c})s" for c in insert_cols])
            cols_str = ", ".join(insert_cols)

            sql = f"""
                INSERT INTO {schema_name}.{table_name} ({cols_str})
                VALUES ({values_placeholders})
                ON CONFLICT ({conflict_cols_sql}) DO UPDATE
                SET {update_stmt};
            """

            conn.execute(sql, row.to_dict())
