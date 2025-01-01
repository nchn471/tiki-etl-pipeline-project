from dagster import asset, Output
from etl_pipeline.resources.tiki_crawler import TikiCrawler
from etl_pipeline.resources.config import MINIO_CONFIG

@asset(
    key_prefix=["bronze", "tiki"],
    group_name="bronze_layer",
    compute_kind="Crawler"
)
def bronze_raw_dataset():
    crawler = TikiCrawler(MINIO_CONFIG)

    products_each_cat, total_products = crawler.num_products()
    metadata = {"total products": total_products}
    for category_data in products_each_cat:
        for category, count in category_data.items():
            category_title = crawler.categories_df.loc[crawler.categories_df['slug'] == category, 'title'].values[0]
            metadata[f"{category_title}"] = count

    return Output(
        value=None,  
        metadata=metadata
    )



