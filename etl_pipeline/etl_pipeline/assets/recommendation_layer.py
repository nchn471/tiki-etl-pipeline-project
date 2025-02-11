from dagster import asset, Output, AssetIn
from pyspark.sql.functions import udf, concat, lit
from pyspark.sql.types import StringType
from pyspark.sql.functions import explode, col, split, concat, lit
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from underthesea import word_tokenize
from gensim import corpora, models, similarities
from etl_pipeline.resources.minio_io_manager import connect_minio
from etl_pipeline.resources.config import MINIO_CONFIG
import os
import re

@udf(StringType()) 
def process_text(document):
    # Change to lowercase
    document = document.lower()
    
    # Remove HTTP links (using regular expression)
    document = re.sub(r'((http|https)\:\/\/)?[a-zA-Z0-9\.\/\?\:@\-_=#]+\.([a-zA-Z]){2,6}([a-zA-Z0-9\.\&\/\?\:@\-_=#])*', '', document)
    
    # Remove line breaks (replace with space)
    document = re.sub(r'[\r\n]+', ' ', document)
    
    # Replace '/' and ',' with space
    document = document.replace('/', ' ').replace(',', ' ')
    
    # Remove punctuations using regular expression
    document = re.sub(r'[^\w\s]', '', document)
    
    # Remove extra spaces (replace multiple spaces with a single space)
    document = re.sub(r'[\s]{2,}', ' ', document)
    
    # Tokenize text using word_tokenize from underthesea
    document = word_tokenize(document, format="text")
    
    return document

def load_stopword(STOP_WORDS):
    with open(STOP_WORDS, 'r', encoding = 'utf-8') as file:
        stop_words = file.read()
    stop_words = stop_words.split('\n')
    return stop_words

@asset(
    ins={
        "gold_products": AssetIn(
            key_prefix=["gold", "tiki"]
        )
    },
    io_manager_key="spark_io_manager",
    key_prefix=["recommendation", "tiki"],
    group_name="reccommendation_layer",
    compute_kind="PySpark"
)
def rcm_content_based_filtering(gold_products):
    gold_products = gold_products.orderBy(["product_id", "seller_id"])

    gold_products = gold_products.withColumn(
        'content',
        concat(gold_products['product_name'], lit(' '), gold_products['description'], lit(' '), gold_products['specifications'])
    )

    gold_products = gold_products.withColumn('processed_info', process_text(gold_products['content']))

    info = gold_products.select("processed_info").rdd.map(lambda row: row[0].split()).collect()

    dictionary = corpora.Dictionary(info)

    stop_words = load_stopword("/opt/dagster/app/vietnamese-stopwords.txt")
    stop_ids = [dictionary.token2id[stopword] for stopword in stop_words if stopword in dictionary.token2id]
    once_ids = [tokenid for tokenid, docfreq in dictionary.dfs.items() if docfreq == 1]
    dictionary.filter_tokens(stop_ids + once_ids)
    dictionary.compactify()

    corpus = [dictionary.doc2bow(text) for text in info]

    tfidf = models.TfidfModel(corpus)

    feature_cnt = len(dictionary.token2id)
    index = similarities.SparseMatrixSimilarity(tfidf[corpus], num_features=feature_cnt)

    local_dict_path = "/tmp/dictionary.gensim"
    local_tfidf_path = "/tmp/tfidf_model.gensim"
    local_index_path = "/tmp/similarity_index.gensim"

    dictionary.save(local_dict_path)
    tfidf.save(local_tfidf_path)
    index.save(local_index_path)

    with connect_minio(MINIO_CONFIG) as client:
        client.fput_object("warehouse", "recommendation/tiki/content_based/dictionary.gensim", local_dict_path)
        client.fput_object("warehouse", "recommendation/tiki/content_based/tfidf_model.gensim", local_tfidf_path)
        client.fput_object("warehouse", "recommendation/tiki/content_based/similarity_index.gensim", local_index_path)

    # Xóa file local sau khi upload thành công
    os.remove(local_dict_path)
    os.remove(local_tfidf_path)
    os.remove(local_index_path)


    return Output(
        value=None,  
        metadata={"message": "Models have been successfully uploaded to MinIO"}
    )

@asset(
    ins={
        "gold_products": AssetIn(
            key_prefix=["gold", "tiki"]
        )
    },
    io_manager_key="spark_io_manager",
    key_prefix=["recommendation", "tiki"],
    group_name="reccommendation_layer",
    compute_kind="PySpark"
)
def rcm_search_query(gold_products):
    gold_products = gold_products.orderBy(["product_id", "seller_id"])

    gold_products = gold_products.withColumn(
        'content',
        concat(gold_products['product_name'], lit(' '), gold_products['breadcrumbs'])
    )

    gold_products = gold_products.withColumn('content', process_text(gold_products['content']))

    info = gold_products.select("content").rdd.map(lambda row: row[0].split()).collect()

    dictionary = corpora.Dictionary(info)

    stop_words = load_stopword("/opt/dagster/app/vietnamese-stopwords.txt")
    stop_ids = [dictionary.token2id[stopword] for stopword in stop_words if stopword in dictionary.token2id]
    once_ids = [tokenid for tokenid, docfreq in dictionary.dfs.items() if docfreq == 1]
    dictionary.filter_tokens(stop_ids + once_ids)
    dictionary.compactify()

    corpus = [dictionary.doc2bow(text) for text in info]

    tfidf = models.TfidfModel(corpus)
    feature_cnt = len(dictionary.token2id)
    index = similarities.SparseMatrixSimilarity(tfidf[corpus], num_features=feature_cnt)

    local_dict_path = "/tmp/search_dictionary.gensim"
    local_tfidf_path = "/tmp/search_tfidf_model.gensim"
    local_index_path = "/tmp/search_similarity_index.gensim"

    dictionary.save(local_dict_path)
    tfidf.save(local_tfidf_path)
    index.save(local_index_path)

    with connect_minio(MINIO_CONFIG) as client:
        client.fput_object(MINIO_CONFIG['bucket'], "recommendation/tiki/search_query/search_dictionary.gensim", local_dict_path)
        client.fput_object(MINIO_CONFIG['bucket'], "recommendation/tiki/search_query/search_tfidf_model.gensim", local_tfidf_path)
        client.fput_object(MINIO_CONFIG['bucket'], "recommendation/tiki/search_query/search_similarity_index.gensim", local_index_path)

    # Xóa file local sau khi upload thành công
    os.remove(local_dict_path)
    os.remove(local_tfidf_path)
    os.remove(local_index_path)


    return Output(
        value=None,  
        metadata={"message": "Models have been successfully uploaded to MinIO"}
    )

@asset(
    ins={
        "gold_reviews": AssetIn(
            key_prefix=["gold", "tiki"]
        )
    },
    io_manager_key="spark_io_manager",
    key_prefix=["recommendation", "tiki"],
    group_name="reccommendation_layer",
    compute_kind="PySpark"
)
def rcm_collaborative_filtering(gold_reviews):

    gold_reviews = gold_reviews.withColumn(
        'item_id',
        concat(gold_reviews['product_id'], lit('_'), gold_reviews['seller_id'])
    )

    gold_reviews = gold_reviews.withColumn('original_item_id', gold_reviews['item_id'])

    indexer = StringIndexer(inputCol="item_id", outputCol="item_index")
    gold_reviews = indexer.fit(gold_reviews).transform(gold_reviews)

    als = ALS(userCol="customer_id", itemCol="item_index", ratingCol="rating", coldStartStrategy="drop")
    model = als.fit(gold_reviews)

    user_recs = model.recommendForAllUsers(20)

    exploded_recs = user_recs.withColumn("recommendation", explode(col("recommendations")))
    exploded_recs = exploded_recs.withColumn("item_index", col("recommendation.item_index"))
    exploded_recs = exploded_recs.withColumn("rating", col("recommendation.rating"))
    
    distinct_items = gold_reviews.select("original_item_id", "item_index").dropDuplicates(["item_index"])
    processed_user_recs = exploded_recs.join(
        distinct_items,
        on="item_index"
    )

    processed_user_recs = processed_user_recs.withColumn("product_id", split(col("original_item_id"), "_")[0].cast("int"))
    processed_user_recs = processed_user_recs.withColumn("seller_id", split(col("original_item_id"), "_")[1].cast("int"))


    processed_user_recs = processed_user_recs.select(
        "customer_id",
        "product_id",
        "seller_id",
        "rating"
    )

    return Output(
        value = processed_user_recs,
        metadata={"message": "Models have been successfully uploaded to MinIO"}
    )

    






