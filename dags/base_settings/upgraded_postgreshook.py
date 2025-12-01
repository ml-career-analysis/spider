from airflow.hooks.postgres_hook import PostgresHook
import numpy as np
from contextlib import closing
import pandas as pd
from datetime import datetime

def update_table(
    ti,
    task_id_xcom,
    table_name,
    update_cols,
    replace_index_cols,
    conn_id,
    key_xcom,
):
    import pandas as pd

    new_hook = UpgradedPostgresHook(
        postgres_conn_id=conn_id,
    )
    filename = ti.xcom_pull(task_ids=task_id_xcom, key=key_xcom)
    df = pd.read_csv(filename)
    new_hook.update_or_create_rows(
        df,
        tablename=table_name,
        update_columns=update_cols or df.columns.tolist(),
        unique_columns=replace_index_cols,
    )

class UpgradedPostgresHook(PostgresHook):
    """
    Difference from base PostgresHook is that this hook checks
    if the unique constraint for a row already exists in the table before trying to insert.
    So as a result, if primary key is bigserial, it does not increase exponentially with each try
    """

    @staticmethod
    def _join_columns_values(row, columns, separator) -> str:
        conditions = [
            (
                f"{col}='{row[col]}'"
                if isinstance(row[col], str)
                else f"{col}=null" if np.isnan(row[col]) else f"{col}={row[col]}"
            )
            for col in columns
        ]
        return separator.join(conditions)

    def _generate_update_or_create_sql(
        self, cur, row, table_name, unique_columns, update_columns
    ) -> tuple[str, bool]:
        """
        Generates SQL statement for each row we want to insert or update.
        With unique_columns checks if the row already exists and depending on the answer,
        makes either 'insert' or 'update' statement
        """
        condition = self._join_columns_values(row, unique_columns, separator=" AND ")
        query_check = f"SELECT 1 FROM {table_name} WHERE {condition}"
        cur.execute(query_check)
        is_update = bool(cur.fetchone())
        if is_update:
            values = self._join_columns_values(row, update_columns, separator=", ")
            sql = f"UPDATE {table_name} SET {values} WHERE {condition}"
        else:
            # values = [f"'{row[col]}'" if isinstance(row[col], str) else row[col] for col in unique_columns]
            values = [
                (
                    f"'{row[col]}'"
                    if isinstance(row[col], str)
                    else str(row[col]).replace("nan", "null")
                )
                for col in update_columns
            ]
            sql = f"INSERT INTO {table_name} ({', '.join(update_columns)}) SELECT {', '.join(values)}"
        return sql, is_update

    def update_or_create_rows(
        self, df, tablename, unique_columns, update_columns, commit_every=1000
    ) -> None:
        """
        Generates and executes generated SQL, updating and inserting data.
        Does not increment bigserial id.
        Parameters:
        df: pandas.DataFrame, where the necessary data is stored. May contain extra data that will not be used,
            but it's columns must have the same names as the columns in table_name
        table_name: name of the table in Database schema, where the data from df will be stored
        unique_columns: columns we check if the information we want to insert has been inserted already,
            so we will update not insert to avoid bigserial id incrementation.
            if the table has unique constraints, put them here.
        update_columns: which columns from df will be put into the table
        commit_every: how often to commit changes. default: every 1000 rows
        """
        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, False)

            conn.commit()
            is_update_actions = []
            with closing(conn.cursor()) as cur:
                for i, row in df.iterrows():
                    sql, is_update = self._generate_update_or_create_sql(
                        cur, row, tablename, unique_columns, update_columns
                    )
                    # print(sql)
                    is_update_actions.append(is_update)
                    if sql:
                        cur.execute(sql)
                        if commit_every and i % commit_every == 0:
                            conn.commit()
                            self.log.info(
                                "Loaded %s rows into %s so far, %s",
                                i,
                                tablename,
                                datetime.now().strftime("%H:%M:%S:%f"),
                            )
            conn.commit()


            updates_count = sum(is_update_actions)
            total_count = len(is_update_actions)
            inserts_count = total_count - updates_count
            if total_count > 0:
                self.log.info(
                    "INSERTS vs UPDATES in loaded rows: %s %% - %s %%",
                    round(inserts_count * 100 / total_count, 2),
                    round(updates_count * 100 / total_count, 2),
                )
            self.log.info("Done loading. Loaded a total of %s rows", total_count)

    def delete_or_update_rows_by_counting_date(
        self,
        df: pd.DataFrame,
        tablename: str,
        unique_columns: list,
        update_columns: list,
        format_string_to_datetime: str,  # =
        counting_date_column="counting_date",
    ) -> None:
        """
        Synchronizes a PostgreSQL table with the given DataFrame:
        - Deletes rows from the table that do not exist in the DataFrame,
        using unique_columns to identify records. Only rows matching any
        counting_date found in the DataFrame will be considered for deletion.
        - Inserts or updates all rows from the DataFrame using update_or_create_rows.

        Parameters:
        df: pandas.DataFrame - the input data to sync
        tablename: str - name of the table in Database schema
        unique_columns: list[str] - columns we check if the data we want to insert
            has been inserted already or needs to be deleted (must include counting_date)
        update_columns: list[str] - columns to be inserted or updated
        counting_date_column: str - column to be updated by
        """
        # counting_date_column = "counting_date"  # assumed to be included in unique_columns

        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, False)

            with closing(conn.cursor()) as cur:
                _df = df.copy()
                # get unique counting_date values from df
                _df[counting_date_column] = pd.to_datetime(
                    _df[counting_date_column], format=format_string_to_datetime
                )
                df_dates = _df[counting_date_column].dropna().unique()
                if not len(df_dates):
                    self.log.info("No counting_date values found.")
                else:
                    df_dates_sql = ",".join(f"'{d}'" for d in df_dates)
                    # load table rows matching the dated
                    cur.execute(
                        f"""
                        SELECT * FROM {tablename}
                        WHERE {counting_date_column} IN ({df_dates_sql})
                    """
                    )
                    table_rows = cur.fetchall()
                    colnames = [desc[0] for desc in cur.description]
                    table_df = pd.DataFrame(table_rows, columns=colnames)

                    # compare keys
                    df_keys = _df[unique_columns].drop_duplicates()
                    table_keys = table_df[unique_columns].drop_duplicates()

                    df_key_set = set(tuple(row) for row in df_keys.values)
                    table_key_set = set(tuple(row) for row in table_keys.values)

                    keys_to_delete = table_key_set - df_key_set
                    # delete rows by key
                    deleted_rows = []
                    for key in keys_to_delete:
                        condition = " AND ".join(
                            [f"{col} = %s" for col in unique_columns]
                        )
                        cur.execute(
                            f"DELETE FROM {tablename} WHERE {condition} RETURNING id",
                            key,
                        )
                        rows = cur.fetchall()
                        deleted_rows.extend(rows)

                    total_keys_in_table = len(table_key_set)


                    if deleted_rows:
                        self.log.info(
                            "Удалено %s из %s строк за эти же даты, хранящиеся в %s.",
                            len(deleted_rows),
                            total_keys_in_table,
                            tablename,
                        )
                    else:
                        self.log.info(
                            "Ни одной строки не удалено из %s — все %s строк за эти даты будут обновлены из входящей df.",
                            tablename,
                            total_keys_in_table,
                        )

                    conn.commit()

            # insert or update current data
            # self.update_or_create_rows(
            #     df=df,
            #     tablename=tablename,
            #     unique_columns=unique_columns,
            #     update_columns=update_columns,
            # )
