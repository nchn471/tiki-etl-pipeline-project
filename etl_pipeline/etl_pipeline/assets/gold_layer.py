from dagster import multi_asset, Output, AssetIn, AssetOut
from pyspark.sql.functions import col, explode

@multi_asset(
    ins={
        "silver_products": AssetIn(
            key_prefix=["silver", "tiki"]
        )
    },
    outs={
        "gold_products": AssetOut(
            io_manager_key="spark_io_manager",
            key_prefix=["gold", "tiki"],
        ),
        "gold_products_authors": AssetOut(
            io_manager_key="spark_io_manager",
            key_prefix=["gold", "tiki"],
        ),
        "gold_authors": AssetOut(
            io_manager_key="spark_io_manager",
            key_prefix=["gold", "tiki"],
        ),
        "gold_brands": AssetOut(
            io_manager_key="spark_io_manager",
            key_prefix=["gold", "tiki"],
        ),
        "gold_images_url": AssetOut(
            io_manager_key="spark_io_manager",
            key_prefix=["gold", "tiki"],
        )
    },
    group_name="gold_layer",
    compute_kind="PySpark"
)
def transform_silver_products(silver_products):

    select_columns = [
        'category_id', 'product_id', 'seller_id', 'brand_id', 'product_name', 'short_description',
        'original_price', 'discount', 'price', 'discount_rate', 'quantity_sold', 'rating_average', 
        'review_count', 'day_ago_created', 'product_url', 'is_authentic', 'is_freeship_xtra',
        'is_top_deal', 'return_reason', 'inventory_type', 'warranty_period', 'warranty_type', 'warranty_location'
    ]

    gold_products = silver_products.select(*select_columns)

    gold_images_url = silver_products.select(
        col("product_id"),
        col("seller_id"),
        explode(col("images_url")).alias("image_url")
    )
    exploded_authors = silver_products.withColumn("author_id", explode(col("authors_id"))) \
                                      .withColumn("author_name", explode(col("authors_name")))

    gold_authors = exploded_authors.select("author_id", "author_name") \
                                   .drop_duplicates(["author_id"]) \
                                   .dropna()

    product_author_pairs = silver_products.withColumn("author_id", explode(col("authors_id"))) \
                                          .withColumn("author_name", explode(col("authors_name"))) \
                                          .select("product_id", "seller_id", "author_id")
    gold_products_authors = product_author_pairs.drop_duplicates()

    gold_brands = silver_products.select("brand_id", "brand_name") \
                                 .drop_duplicates(["brand_id"]) \
                                 .dropna()
    
    return (
        Output(
            gold_products, 
            metadata={
                "table": "gold_products", 
                "records count": gold_products.count()
            }
        ),
        Output(
            gold_products_authors, 
            metadata={
                "table": "gold_products_authors", 
                "records count": gold_products_authors.count()
            }
        ),
        Output(
            gold_authors, 
            metadata={
                "table": "gold_authors", 
                "records count": gold_authors.count()
            }
        ),
        Output(
            gold_brands, 
            metadata={
                "table": "gold_brands", 
                "records count": gold_brands.count()
            }
        ),
        Output(
            gold_images_url, 
            metadata={
                "table": "gold_images_url", 
                "records count": gold_images_url.count()
            }
        ),
    )



    
@multi_asset(
    ins={
        "silver_reviews": AssetIn(
            key_prefix=["silver", "tiki"]
        ),
    },
    outs={
        "gold_reviews": AssetOut(
            io_manager_key="spark_io_manager",
            key_prefix=["gold", "tiki"],
        ),
        "gold_users": AssetOut(
            io_manager_key="spark_io_manager",
            key_prefix=["gold", "tiki"],
        ),
    },
    group_name="gold_layer",
    compute_kind="PySpark"
)
def transform_silver_reviews(silver_reviews):
    user_columns = ['customer_id', 'customer_name', 'avatar_url', 'joined_day', 'joined_time', 'total_review', 'total_thank']
    gold_users = silver_reviews.select(*user_columns).drop_duplicates(['customer_id'])
    
    gold_users = gold_users.withColumnRenamed('customer_id', 'user_id') \
                 .withColumnRenamed('customer_name', 'user_name')

    gold_reviews = silver_reviews.drop(*user_columns[1:])  
    gold_reviews = gold_reviews.drop_duplicates(['review_id'])

    return (
        Output(
            gold_reviews,
            output_name="gold_reviews",
            metadata={
                "table": "gold_reviews",
                "records count": gold_reviews.count(),
            }
        ),
        Output(
            gold_users,
            output_name="gold_users",
            metadata={
                "table": "gold_users",
                "records count": gold_users.count(),
            }
        )
    )