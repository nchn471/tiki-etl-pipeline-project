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
        table = table.replace(f"{schema}_", "")

        tmp_tbl = f"{table}_tmp_{datetime.now().strftime('%Y_%m_%d')}"

        with connect_psql(self._config) as db_conn:

            primary_keys = (context.metadata or {}).get("primary_keys", [])
            ls_columns = (context.metadata or {}).get("columns", [])
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

                if isinstance(obj, pd.DataFrame):
                    obj[ls_columns].to_sql(
                        name=tmp_tbl,
                        con=db_conn,
                        schema=schema,
                        if_exists="replace",
                        index=False,
                        chunksize=10000,
                        method="multi",
                    )
                else:
                    jdbc_url = (
                        f"jdbc:postgresql://{self._config['host']}:{self._config['port']}/{self._config['database']}"
                    )                    
                    obj.select(ls_columns).write.format("jdbc") \
                        .option("url",jdbc_url) \
                        .option("driver", "org.postgresql.Driver") \
                        .option("dbtable", f"{schema}.{tmp_tbl}") \
                        .option("user", self._config["user"]) \
                        .option("password", self._config["password"]) \
                        .mode("overwrite") \
                        .save()
                
                update_columns = [f"{col} = EXCLUDED.{col}" for col in ls_columns if col not in primary_keys]
                if len(update_columns) >=1:
                    update_columns_sql = ", ".join(update_columns)
                    update_command = f"UPDATE SET {update_columns_sql}"
                else:
                    update_command = f"NOTHING"

                command = f"""
                BEGIN TRANSACTION;
                INSERT INTO {schema}.{table} 
                SELECT {select_columns_sql} 
                FROM {schema}.{tmp_tbl}
                ON CONFLICT ({', '.join(primary_keys)}) 
                DO {update_command}; 
                END TRANSACTION;
                """
                
                cursor.execute(text(command))

                cursor.execute(text(f"DROP TABLE IF EXISTS {schema}.{tmp_tbl}"))