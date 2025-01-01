import re
import pandas as pd
from dagster import asset, Output, AssetIn
from etl_pipeline.resources.tiki_transform import TikiTransform
from etl_pipeline.resources.config import MINIO_CONFIG, SPARK_CONFIG
from etl_pipeline.resources.spark_io_manager import connect_spark

from pyspark.sql.functions import col, lit, when, from_unixtime
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType



transformer = TikiTransform(MINIO_CONFIG)

@udf(IntegerType())
def convert_warranty_period(warranty):
    if not isinstance(warranty, str) or warranty.strip() == '':
        return 0
    warranty = warranty.lower().strip()
    if 'trọn đời' in warranty:
        return 100*365 
    if re.search(r'không|no warranty|0', warranty):
        return 0
    match_year = re.search(r'(\d+)\s*(năm|year)', warranty)
    match_month = re.search(r'(\d+)\s*(tháng|month)', warranty)
    match_day = re.search(r'(\d+)\s*(ngày|day)', warranty)
    if match_year:
        years = int(match_year.group(1))
        return years * 365  
    if match_month:
        months = int(match_month.group(1))
        return months * 30  
    if match_day:
        days = int(match_day.group(1))
        return days  

    return 0

@udf(IntegerType())
def convert_to_days(joined_time):
    if joined_time is None:
        return 0
    match = re.search(r"(\d+)\s+(năm|tháng|ngày)", joined_time)
    if match:
        value, unit = int(match.group(1)), match.group(2)
        if unit == "năm":
            return value * 365  
        elif unit == "tháng":
            return value * 30  
        elif unit == "ngày":
            return value
    return 0  

@asset(
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "tiki"],
    group_name="silver_layer",
    compute_kind="PySpark"
)
def silver_sellers():
    sellers_df = transformer.transform_data(type = "sellers")
    with connect_spark(SPARK_CONFIG) as spark:
        spark_df = spark.createDataFrame(sellers_df)
        spark_df = spark_df.dropDuplicates(["seller_id"])
        spark_df = spark_df.filter(col("seller_id") != 0)

    return Output(
        spark_df,
        metadata={
            "table": "sellers_dataset",
            "records count": spark_df.count(),
        },
    )

@asset(
    ins={
        "silver_sellers": AssetIn(
            key_prefix=["silver", "tiki"]
        )
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "tiki"],
    group_name="silver_layer",
    compute_kind="PySpark"
)
def silver_products(silver_sellers):

    products_df = transformer.transform_data()
    with connect_spark(SPARK_CONFIG) as spark:
        products_spark_df = spark.createDataFrame(products_df)

        products_spark_df = products_spark_df.dropDuplicates(["product_id", "seller_id"])

        products_spark_df = products_spark_df.join(silver_sellers.select("seller_id"), on="seller_id", how="inner")


        products_spark_df = products_spark_df.withColumn("warranty_period", convert_warranty_period(col("warranty_period")))

        products_spark_df = products_spark_df.withColumn("warranty_type", when(col("warranty_type").isNull(), "Không bảo hành").otherwise(col("warranty_type")))
        products_spark_df = products_spark_df.withColumn("warranty_location", when(col("warranty_location").isNull(), "Không bảo hành").otherwise(col("warranty_location")))
        products_spark_df = products_spark_df.withColumn("return_reason", when(col("return_reason").isNull(), "no_return").otherwise(col("return_reason")))
        products_spark_df = products_spark_df.withColumn("quantity_sold", when(col("quantity_sold").isNull(), lit(0)).otherwise(col("quantity_sold")))

    return Output(
        products_spark_df,
        metadata={
            "table": "silver_products",
            "records count": products_spark_df.count(),
        },
    )



@asset(
    ins={
        "silver_products": AssetIn(
            key_prefix=["silver", "tiki"]
        )
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "tiki"],
    group_name="silver_layer",
    compute_kind="PySpark"
)
def silver_reviews(silver_products):
    reviews_df = transformer.transform_data(type = "reviews")
    with connect_spark(SPARK_CONFIG) as spark:
        reviews_spark_df = spark.createDataFrame(reviews_df)

        reviews_spark_df = reviews_spark_df.join(silver_products.select("product_id", "seller_id"), 
                             on=["product_id", "seller_id"], how="inner")

        reviews_spark_df = reviews_spark_df.dropDuplicates(["review_id"])

        reviews_spark_df = reviews_spark_df.withColumn("created_at", from_unixtime(col("created_at")).cast("timestamp"))
        reviews_spark_df = reviews_spark_df.withColumn("purchased_at", from_unixtime(col("purchased_at")).cast("timestamp"))
        reviews_spark_df = reviews_spark_df.withColumn("joined_day", convert_to_days(col("joined_time")))
    return Output(
        reviews_spark_df,
        metadata={
            "table": "silver_reviews",
            "records count": reviews_spark_df.count(),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "tiki"],
    group_name="silver_layer",
    compute_kind="MinIO"
)
def silver_categories():
    cagoteries = transformer.categories_df
    return Output(
        cagoteries,
        metadata={
            "table": "silver_categories",
            "records count": len(cagoteries),
        },
    )

















