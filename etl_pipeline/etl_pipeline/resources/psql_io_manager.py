from contextlib import contextmanager
from datetime import datetime
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine, text

@contextmanager
def connect_psql(config):
    conn_info = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        raise


class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def load_input(self, context: InputContext) -> pd.DataFrame:
        pass
    
    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        tmp_tbl = f"{table}_tmp_{datetime.now().strftime('%Y_%m_%d')}"

        with connect_psql(self._config) as db_conn:

            primary_keys = (context.metadata or {}).get("primary_keys", [])
            ls_columns = (context.metadata or {}).get("columns", [])
            if not isinstance(obj, pd.DataFrame):
                obj = obj.toPandas()
            select_columns = []
            for col in ls_columns:
                if col.endswith("_at"):
                    select_columns.append(f"CAST({schema}.{tmp_tbl}.\"{col}\" AS TIMESTAMP) AS \"{col}\"")
                else:
                    select_columns.append(f"{schema}.{tmp_tbl}.\"{col}\"")

            select_columns_sql = ", ".join(select_columns)

            with db_conn.connect() as cursor:

                cursor.execute(
                    text(f"CREATE TEMP TABLE IF NOT EXISTS {tmp_tbl} (LIKE {schema}.{table})")
                )

                obj[ls_columns].to_sql(
                    name=tmp_tbl,
                    con=db_conn,
                    schema=schema,
                    if_exists="replace",
                    index=False,
                    chunksize=10000,
                    method="multi",
                )

                result = cursor.execute(text(f"SELECT COUNT(*) FROM {schema}.{tmp_tbl}"))
                for row in result:
                    print(f"Temp table records: {row}")

                if len(primary_keys) > 0:
                    update_columns = [f"{col} = EXCLUDED.{col}" for col in ls_columns if col not in primary_keys]
                    update_columns_sql = ", ".join(update_columns)
                    command = f"""
                    BEGIN TRANSACTION;
                    INSERT INTO {schema}.{table} 
                    SELECT {select_columns_sql} 
                    FROM {schema}.{tmp_tbl}
                    ON CONFLICT ({', '.join(primary_keys)}) 
                    DO NOTHING; --UPDATE SET {update_columns_sql}; 
                    END TRANSACTION;
                    """
                else:
                    command = f"""
                    BEGIN TRANSACTION;
                    TRUNCATE TABLE {schema}.{table};
                    INSERT INTO {schema}.{table}
                    SELECT {select_columns_sql}
                    FROM {schema}.{tmp_tbl};
                    END TRANSACTION;
                    """

                cursor.execute(text(command))

                # Drop the temporary table
                cursor.execute(text(f"DROP TABLE IF EXISTS {schema}.{tmp_tbl}"))