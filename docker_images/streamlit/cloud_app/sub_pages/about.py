import streamlit as st

def show_about():
    st.markdown(
        """
        <div style='display: flex; justify-content: center; align-items: center;'>
            <h1 style='font-size: 50px; color: #0073e6; font-weight: bold; font-family: monospace;'>
                TIKI Recommender ETL Pipeline
            </h1>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    st.header("1. Abstract")
    st.markdown(
        """
        In this project, I designed and built an ETL architecture to extract raw data from [Tiki](https://tiki.vn/) (a Vietnamese e-commerce platform),  
        transform and clean it, and then store it in a data warehouse for data analytics and building a recommendation system.  

        This project applies knowldedge from course "Fundamental Data Engineering" from [AIDE](https://aisia.vn/courses).  
        With sincere thanks to Mr. Nguyen Thanh Binh and Mr. Hung Le.
        """
    )

    st.header("2. Data Pipeline Diagram")
    st.image("assets/dataflow.png")
    st.markdown("""
    ### List of Technical Tools Used in This Project  

    - **[Dagster](https://dagster.io/)**: An orchestration platform for managing and monitoring data pipelines.  
    - **[Docker](https://www.docker.com/)**: A containerization platform for packaging and deploying applications.  
    - **[MinIO](https://min.io/)**: High-performance, S3-compatible object storage used as a data lake.  
    - **[Apache Spark](https://spark.apache.org/)**: A distributed computing framework for processing big data efficiently.  
    - **[PostgreSQL](https://www.postgresql.org/)**: A powerful relational database system for managing the data warehouse.  
    - **[Metabase](https://www.metabase.com/)**: A business intelligence tool for building interactive dashboards and visualizations.  
    - **[Streamlit](https://streamlit.io/)**: A Python framework for quickly developing interactive web applications.  
    """)    
    st.markdown("### Data Lineage")  
    st.markdown("The diagram below illustrates the data flow within Dagster using asset definitions.")  
    st.image("assets/data_lineage.png")  

    
    st.markdown("### 2.1 Data Collection (Extract)")
    st.markdown(
        """
        We define a Python class [`TikiCrawler`](https://github.com/nchn471/tiki-recommender-etl-pipeline/blob/main/etl_pipeline/etl_pipeline/resources/tiki_crawler.py) to fetch data from Tiki's API and store it as JSON files in MinIO, serving as a data lake.  
        The raw data is stored in MinIO with the following structure:
        """
    )
    st.markdown("#### Raw Data Storage Structure")
    col1, col2 = st.columns(2)
    with col1:  
        st.image("assets/bronze_minio1.png")
    with col2:  
        st.image("assets/bronze_minio2.png")
    
    st.markdown(
        """
        **🟤 Bronze Layer**:

        ```plaintext
        warehouse/bronze/tiki/{category_name}/{product_id}_{seller_id}
        ```
        - **`product_{product_id}_{seller_id}.json` → Product details.**
        - **`seller_{product_id}_{seller_id}.json` → Seller details.**
        - **`reviews/reviews_{product_id}_{seller_id}_{page}.json` → Customer reviews (first 10 pages).**
        - **`categories.csv` → List of product categories.**
        """
    )

    st.markdown("**Daster Asset**")
    st.image("assets/bronze_layer.png")
    st.info(
        """
        Each product is uniquely identified by the pair **`product_id`** and **`seller_id`**.  
        This allows scheduling **cron jobs** for incremental data loading, where existing products are skipped to optimize resources and reduce execution time.
        """
    )
    st.markdown("### 2.2 Transform")
    st.markdown("#### 2.2.1 Silver Layer")
    st.markdown(
        """
        After collecting raw data, we transform it using Apache Spark.
        The bronze layer is cleaned and structured into Spark DataFrames: `categories`, `products`, `sellers`, and `reviews`.
        """
    )
    st.markdown("The cleaned dataset will be stored in MinIO as Parquet files.")
    st.image("assets/silver_minio.png")
    
    st.markdown("Example transformation for `reviews`:")
    st.code("""
    @asset(
        ins={"silver_products": AssetIn(key_prefix=["silver", "tiki"])}
        io_manager_key="spark_io_manager",
        key_prefix=["silver", "tiki"],
        group_name="silver_layer",
        compute_kind="PySpark"
    )
    def silver_reviews(silver_products):
        transformer = TikiTransform(MINIO_CONFIG)
        reviews_df = transformer.transform_data(type="reviews")
        
        with connect_spark(SPARK_CONFIG) as spark:
            reviews_spark_df = spark.createDataFrame(reviews_df)
            reviews_spark_df = reviews_spark_df.join(
                silver_products.select("product_id", "seller_id"),
                on=["product_id", "seller_id"], how="inner"
            )
            reviews_spark_df = reviews_spark_df.dropDuplicates(["review_id"])
        
        return Output(
            reviews_spark_df,
            metadata={"table": "silver_reviews", "records count": reviews_spark_df.count()},
        )
    """, language="python")
    st.markdown("**Detail asset in silver layer**")
    st.image("assets/silver_layer.png")
    
    st.markdown("#### 2.2.2 Gold Layer")
    st.markdown(
        """
        In the Gold Layer, data is restructured to optimize storage and query performance. \
        Attributes such as `brand`, `author`, and others are extracted into separate tables.  
        See details below.
        """
    )

    
    st.image("assets/gold_layer.png")
    st.markdown("Data is stored in the **gold layer** in MinIO.")
    st.image("assets/gold_minio.png")
    
    st.markdown("#### 2.2.3 Recommendation Layer")
    
    st.write("Recommender systems play a crucial role in e-commerce by suggesting relevant items.")
    st.image("assets/filtering.png",use_container_width=True)
    
    col1, col2 = st.columns(2)
    with col1:
        st.write("##### [Collaborative-Based Filtering](https://developers.google.com/machine-learning/recommendation/collaborative/basics)")
        st.markdown(
            """
            - Generates recommendations based on user interactions.
            - Uses the 'wisdom of the crowd' to suggest items.
            - More effective and self-learning compared to content-based filtering.
            - Requires Big Data processing and optimization techniques for efficiency.
            """
        )
    
    with col2:
        st.write("##### [Content-Based Filtering](https://developers.google.com/machine-learning/recommendation/content-based/basics)")
        st.markdown(
            """
            - Focuses on item attributes to suggest similar products.
            - Assumes that if a user liked an item before, they will like similar ones.
            - Recommendations update only when new data is available.
            """
        )
    
    st.markdown("Data from the gold layer is leveraged for building recommendation models and search queries by Apache Spark.")
    st.image("assets/rcm_layer.png")
    st.markdown("#### Content-Based Filtering")

    st.markdown("""
    Content-based filtering works by analyzing textual information such as **title** and **description**, removing stopwords, and tokenizing text.  
    Then, **TF-IDF (Term Frequency-Inverse Document Frequency)** and **Gensim similarity** are used to compute similarity scores between products.  
    The **higher the similarity score**, the more relevant two products are to each other.
    """)
    st.markdown("**TF-IDF Formula**")

    st.latex(r"TF(t, d) = \frac{f_{t,d}}{ \sum_{t' \in d} f_{t',d} }")

    st.latex(r"IDF(t) = \log \left(\frac{N}{1 + DF(t)} \right)")

    st.latex(r"TF-IDF(t, d) = TF(t, d) \times IDF(t)")

    st.info("The search query mechanism follows the same approach.")
    st.markdown("#### Collaborative Filtering")
    st.markdown("""
    We used the **ALS (Alternating Least Squares)** model in Apache Spark to build this recommendation system.  
    ALS is particularly useful for implicit feedback datasets and large-scale recommendation problems.
    """)

    st.code("""
    from pyspark.ml.recommendation import ALS

    # Define ALS model
    als = ALS(userCol="customer_id", itemCol="item_index", ratingCol="rating", coldStartStrategy="drop")

    # Train the model
    model = als.fit(gold_reviews)
    """, language="python")
    st.markdown("After training,these models are saved in MinIO")
    st.image("assets/recommendation_minio.png")
    st.image("assets/rcm_layer.png")
    st.markdown("### 2.3 Data Warehouse (Load)")
    st.markdown("""
    The processed data is loaded into **PostgreSQL** for further analysis.
    Below is the schema used in the data warehouse.
    """)

    # Display the database schema and DWH asset structure
    st.image("assets/schema.png", caption="PostgreSQL Data Warehouse Schema")
    st.image("assets/dwh_layer.png", caption="Data Warehouse Layer in Dagster")
    st.markdown("## 3. Serving")
    st.markdown("""
    Now that the data has been processed and stored, we can utilize it to build:  
    - **Metabase Dashboard** for visualization and insights.  
    - **Streamlit-based Recommendation System** for interactive product recommendations.  

    Check them out in the sidebar! 🚀
    """)
    st.markdown("## 4. Detail Code")
    st.markdown("Check out the project on [GitHub](https://github.com/nchn471/tiki-recommender-etl-pipeline)")
