from dagster import IOManager, InputContext, OutputContext
from pyspark.sql import SparkSession, DataFrame
from typing import Union

from contextlib import contextmanager
from datetime import datetime

@contextmanager
def connect_spark(config):
    try:
        spark = SparkSession.builder.appName("task-{}".format(datetime.today()))
        for key, value in config.items():
            if key == "master_url":
                spark = spark.master(value)
            elif key != "bucket":
                spark = spark.config(key,value)
        yield spark.getOrCreate()
    except Exception:
        raise 


class SparkIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path
        base_dir = f"s3a://{self._config['bucket']}"
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        context.log.info(f"{base_dir}/{key}.parquet")
        return f"{base_dir}/{key}.parquet"


    def handle_output(self, context: OutputContext, obj: DataFrame):
        file_path = self._get_path(context)
        try:
            context.log.info("Handle Output Spark")
            obj.write.mode("overwrite").parquet(file_path)
        except Exception:
            raise 

    def load_input(self, context: InputContext):
        file_path = self._get_path(context)
        try:
            with connect_spark(self._config) as spark:
                context.log.info("Load Input Spark")

                df = spark.read.parquet(file_path)
                return df
        except Exception:
            raise