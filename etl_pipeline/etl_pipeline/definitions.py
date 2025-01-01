from dagster import Definitions, load_assets_from_modules
from etl_pipeline.resources.minio_io_manager import MinIOIOManager
from etl_pipeline.resources.psql_io_manager import PostgreSQLIOManager
from etl_pipeline.resources.spark_io_manager import SparkIOManager

from etl_pipeline.assets import bronze_layer 
from etl_pipeline.assets import silver_layer
from etl_pipeline.assets import gold_layer 
from etl_pipeline.assets import warehouse_layer
from etl_pipeline.resources.config import *
all_assets = load_assets_from_modules([bronze_layer,silver_layer, gold_layer,warehouse_layer])

defs = Definitions(
    assets= all_assets,
    resources={
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
        "spark_io_manager" : SparkIOManager(SPARK_CONFIG)
    },
)
