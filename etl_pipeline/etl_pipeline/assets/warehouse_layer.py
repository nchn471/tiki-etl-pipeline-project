from dagster import multi_asset, Output, AssetIn, AssetOut

@multi_asset(
    ins={
        "silver_categories": AssetIn(key_prefix=["silver", "tiki"]),
    },
    outs={
        "categories": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["postgres", "dwh"],
            metadata={
                "primary_keys": ["category_id"],
                "columns": [
                    "category_id",
                    "title",
                    "slug"
                ],
            },
        )
    },
    group_name="warehouse_layer",
    compute_kind="PostgreSQL",
)
def dwh_categories(silver_categories) :

    return Output(
        silver_categories,
        metadata={
            "schema": "dwh",
            "records_count": len(silver_categories),
        },
    )

@multi_asset(
    ins={
        "silver_sellers": AssetIn(key_prefix=["silver", "tiki"]),
    },
    outs={
        "sellers": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["postgres", "dwh"],
            metadata={
                "primary_keys": ["seller_id"],
                "columns": [
                    "seller_id",
                    "seller_name",
                    "icon_url",
                    "store_url",
                    "avg_rating_point",
                    "review_count",
                    "total_follower",
                    "days_since_joined",
                    "is_official",
                ],
            },
        )
    },
    group_name="warehouse_layer",
    compute_kind="PostgreSQL",
)
def dwh_sellers(silver_sellers) :

    return Output(
        silver_sellers,
        metadata={
            "schema": "dwh",
            "records_count": silver_sellers.count(),
        },
    )

@multi_asset(
    ins={
        "gold_authors": AssetIn(key_prefix=["gold", "tiki"]),
    },
    outs={
        "authors": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["postgres", "dwh"],
            metadata={
                "primary_keys": ["author_id",],
                "columns": [
                    "author_id",
                    "author_name",
                ],
            },
        )
    },
    group_name="warehouse_layer",
    compute_kind="PostgreSQL",
)
def dwh_authors(gold_authors) :

    return Output(
        gold_authors,
        metadata={
            "schema": "dwh",
            "records_count": gold_authors.count(),
        },
    )

@multi_asset(
    ins={
        "gold_products_authors": AssetIn(key_prefix=["gold", "tiki"]),
    },
    outs={
        "products_authors": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["postgres", "dwh"],
            metadata={
                "primary_keys": [
                    "product_id",
                    "seller_id",
                    "author_id"     
                ],
                "columns": [
                    "product_id",
                    "seller_id",
                    "author_id"     
                ],
            },
        )
    },
    group_name="warehouse_layer",
    compute_kind="PostgreSQL",
)
def dwh_products_authors(gold_products_authors) :

    return Output(
        gold_products_authors,
        metadata={
            "schema": "dwh",
            "records_count": gold_products_authors.count(),
        },
    )

@multi_asset(
    ins={
        "gold_images_url": AssetIn(key_prefix=["gold", "tiki"]),
    },
    outs={
        "images_url": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["postgres", "dwh"],
            metadata={
                "primary_keys": [
                    "product_id",
                    "seller_id",
                    "image_url"     
                ],
                "columns": [
                    "product_id",
                    "seller_id",
                    "image_url"     
                ],
            },
        )
    },
    group_name="warehouse_layer",
    compute_kind="PostgreSQL",
)
def dwh_images_url(gold_images_url) :

    return Output(
        gold_images_url,
        metadata={
            "schema": "dwh",
            "records_count": gold_images_url.count(),
        },
    )


@multi_asset(
    ins={
        "gold_brands": AssetIn(key_prefix=["gold", "tiki"]),
    },
    outs={
        "brands": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["postgres", "dwh"],
            metadata={
                "primary_keys": ["brand_id",],
                "columns": [
                    "brand_id",
                    "brand_name",
                ],
            },
        )
    },
    group_name="warehouse_layer",
    compute_kind="PostgreSQL",
)
def dwh_brands(gold_brands) :

    return Output(
        gold_brands,
        metadata={
            "schema": "dwh",
            "records_count": gold_brands.count(),
        },
    )



@multi_asset(
    ins={
        "gold_products": AssetIn(key_prefix=["gold", "tiki"]),
    },
    outs={
        "products": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["postgres", "dwh"],
            metadata={
                "primary_keys": [
                    "product_id",
                    "seller_id",
                ],
                "columns": [
                    "product_id",
                    "seller_id",
                    "category_id",
                    "brand_id",
                    "product_name",
                    "short_description",
                    "original_price",
                    "discount",
                    "price",
                    "discount_rate",
                    "quantity_sold",
                    "rating_average",
                    "review_count",
                    "day_ago_created",
                    "product_url",
                    "is_authentic",
                    "is_freeship_xtra",
                    "is_top_deal",
                    "return_reason",
                    "inventory_type",
                    "warranty_period",
                    "warranty_type",
                    "warranty_location",
                ],
            },
        )
    },
    group_name="warehouse_layer",
    compute_kind="PostgreSQL",
)
def dwh_products(gold_products) :

    return Output(
        gold_products,
        metadata={
            "schema": "dwh",
            "records_count": gold_products.count(),
        },
    )

@multi_asset(
    ins={
        "gold_reviews": AssetIn(key_prefix=["gold", "tiki"]),
    },
    outs={
        "reviews": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["postgres", "dwh"],
            metadata={
                "primary_keys": ["review_id"],
                "columns": [
                    "review_id",
                    "product_id",
                    "seller_id",
                    "customer_id",
                    "title",
                    "content",
                    "thank_count",
                    "rating",
                    "created_at",
                    "purchased_at"
                ],
            },
        )
    },
    group_name="warehouse_layer",
    compute_kind="PostgreSQL",
)
def dwh_reviews(context, gold_reviews) :
    context.log.info("Generate reviews dataset")

    return Output(
        gold_reviews,
        metadata={
            "schema": "dwh",
            "records_count": gold_reviews.count(),
        },
    )

@multi_asset(
    ins={
        "gold_users": AssetIn(key_prefix=["gold", "tiki"]),
    },
    outs={
        "users": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["postgres", "dwh"],
            metadata={
                "primary_keys": ["user_id"],
                "columns": [
                    "user_id",
                    "user_name",
                    "avatar_url",
                    "joined_day",
                    "joined_time",
                    "total_review",
                    "total_thank"
                ],
            },
        )
    },
    group_name="warehouse_layer",
    compute_kind="PostgreSQL",
)
def dwh_users(gold_users) :

    return Output(
        gold_users,
        metadata={
            "schema": "dwh",
            "records_count": gold_users.count(),
        },
    )