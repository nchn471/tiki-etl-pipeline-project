import streamlit as st
import pandas as pd
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from minio import Minio
from gensim import corpora, models, similarities
from underthesea import word_tokenize
import os 
import shutil 
import random
import re
MINIO_CONFIG = {
    "endpoint_url": "minio:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}

PSQL_CONFIG = {
    "host": "de_psql",
    "port": 5432,
    "database": "postgres",
    "user": "admin",
    "password": "admin123",
}

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

@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("aws_access_key_id"),
        secret_key=config.get("aws_secret_access_key"),
        secure=False,
    )
    try:
        yield client
    except Exception:
        raise

def load_stopword(STOP_WORDS):
    with open(STOP_WORDS, 'r', encoding = 'utf-8') as file:
        stop_words = file.read()
    stop_words = stop_words.split('\n')
    return stop_words

def process_text(document):
    # Change to lower text
    document = document.lower()
    # Remove HTTP links
    document = document.replace(
        r'((http|https)\:\/\/)?[a-zA-Z0-9\.\/\?\:@\-_=#]+\.([a-zA-Z]){2,6}([a-zA-Z0-9\.\&\/\?\:@\-_=#])*', '')
    # Remove line break
    document = document.replace(r'[\r\n]+', ' ')
    # Change / by white space
    document = document.replace('/', ' ') 
    # Change , by white space
    document = document.replace(',', ' ') 
    # Remove punctuations
    document = document.replace('[^\w\s]', '')
    punctuation = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
    for char in punctuation:
        document = document.replace(char, '')
    # Replace mutiple spaces by single space
    document = document.replace('[\s]{2,}', ' ')
    # Word_tokenize
    document = word_tokenize(document, format="text")   
    
    return document

def gensim_recommendation(n, product_id, seller_id, dictionary, tfidf,index, df):

    product_result = df[(df['product_id'] == product_id) & (df['seller_id'] == seller_id)]

    content = (
        product_result["product_name"] + " " + product_result["description"] + " " + product_result["specifications"]
    ).values[0]

    view_product = process_text(content).split()
    stop_words = load_stopword("vietnamese-stopwords.txt")

    view_product = [text for text in view_product if text not in stop_words]

    bow_vector = dictionary.doc2bow(view_product)
    sim = index[tfidf[bow_vector]]
    
    df_result = pd.DataFrame({'id': range(len(sim)), 'score': sim})
    
    top_scores = df_result.sort_values(by='score', ascending=False).head(n + 1)
    
    product_find = df[df.index.isin(top_scores['id'])]
    
    result = pd.concat([product_find[['product_id', 'seller_id']], top_scores], axis=1)
    
    result = result[(result['product_id'] != product_id) | (result['seller_id'] != seller_id)]
    result = result.sort_values(by='score', ascending=False)

    return result[['product_id','seller_id']] 

def search_query(text, dictionary, tfidf,index, df):

    view_product = process_text(text).split()
    stop_words = load_stopword("vietnamese-stopwords.txt")

    view_product = [text for text in view_product if text not in stop_words]

    bow_vector = dictionary.doc2bow(view_product)
    sim = index[tfidf[bow_vector]]
    
    df_result = pd.DataFrame({'id': range(len(sim)), 'score': sim})
    
    match_products = df_result.sort_values(by='score', ascending=False).head(10)
    
    product_find = df[df.index.isin(match_products['id'])]
    
    result = pd.concat([product_find[['product_id', 'seller_id']], match_products], axis=1)
    result = result.sort_values(by='score', ascending=False)
    
    return result[['product_id','seller_id']] 

def als_recommendation(customer_id, als_data):
    als_result = als_data[als_data["customer_id"] == customer_id]
    return als_result[["product_id","seller_id"]]

def download_minio(minio_path):

    with connect_minio(MINIO_CONFIG) as client:
        objects = list(client.list_objects(MINIO_CONFIG['bucket'], prefix=minio_path, recursive=True))
        os.makedirs("tmp", exist_ok=True)
        local_path = "tmp/" + minio_path.split('/')[-1]
        if len(objects) == 1 and objects[0].object_name == minio_path:
            client.fget_object(MINIO_CONFIG['bucket'], minio_path, local_path)
        else:
            os.makedirs(local_path, exist_ok=True)
            for obj in objects:
                file_name = os.path.basename(obj.object_name)
                local_file_path = os.path.join(local_path, file_name)

                client.fget_object(MINIO_CONFIG['bucket'], obj.object_name, local_file_path)

        return local_path



def next_image():
    st.session_state.counter += 1

def prev_image():
    st.session_state.counter -= 1

def fetch_images(product_id, seller_id, size=None):
    with connect_psql(PSQL_CONFIG) as db_conn:
        sql_stm = f"""
            SELECT image_url
            FROM dwh.images_url 
            WHERE product_id = '{product_id}' and seller_id = '{seller_id}'
        """
        image_urls = pd.read_sql(sql_stm, db_conn).iloc[:, 0].tolist()

    if size is None:
        return image_urls
    
    sizes = {
        "large": "w1200",
        "medium": "w300",
        "small": "200x280",
        "thumbnail": "200x280"
    }

    if size not in sizes:
        raise ValueError(f"Invalid size: {size}. Choose from 'large', 'medium', 'small', or 'thumbnail'.")
    
    size_value = sizes[size]
    image_urls_with_size = [
        base_url.replace("ts/product", f"cache/{size_value}/ts/product") for base_url in image_urls
    ]

    return image_urls_with_size

def fetch_brand_or_author(product_id,seller_id):
    author_stm = f"""
        select a.author_name
        from dwh.authors a
        join dwh.products_authors pa on a.author_id = pa.author_id 
        where product_id = '{product_id}' and seller_id = '{seller_id}'
    """
    brand_stm = f"""
        SELECT b.brand_name
        FROM dwh.brands b
        JOIN dwh.products p on b.brand_id = p.brand_id 
        WHERE product_id = '{product_id}' and seller_id = '{seller_id}'
    """
    with connect_psql(PSQL_CONFIG) as db_conn:

        name = pd.read_sql(author_stm, db_conn).iloc[:, 0].tolist()
        if len(name) > 0:
            author_name = ",".join(name)
            return ("Tác giả:",author_name)
        
        name = pd.read_sql(brand_stm, db_conn).iloc[:, 0].tolist()
        brand_name = ",".join(name)

        return ("Thương hiệu:", brand_name)
    
def generate_star_rating(rating, max_stars=5):
    full_stars = int(rating)  # Full stars
    empty_stars = max_stars - full_stars 
    
    # HTML for full, half, and empty stars
    stars_html = (
        f"<span style='color:#FFC400; font-size: 20px;'>{'★' * full_stars}</span>"  # Full stars
        f"<span style='color:#C7C7C7; font-size: 20px;'>{'☆' * empty_stars}</span>"  # Empty stars
    )
    return stars_html

def show_product(product_id, seller_id, df):
    if 'counter' not in st.session_state:
        st.session_state.counter = 0
    product = df[(df.product_id == product_id) & (df.seller_id == seller_id)].to_dict(orient='records')[0]

    cover, info = st.columns([3,8])

    with cover:

        image_urls = fetch_images(product_id, seller_id)
        num_images = len(image_urls)
        current_image_url = image_urls[st.session_state.counter % num_images]  
        st.image(current_image_url, use_container_width=True)
        col1, col2 = st.columns(2)
        with col1:
            st.button("⬅️", on_click=prev_image, use_container_width=True)
        with col2:
            st.button("➡️", on_click=next_image, use_container_width=True)

    with info:


        breadcrumbs = product['breadcrumbs'].split(' / ')
        brand_author_name = fetch_brand_or_author(product_id, seller_id)


        # Display breadcrumbs in light gray color
        st.markdown(f"""
            <p style="font-size: 16px; color: #A9A9A9; margin-bottom: 0px;">
                {' &nbsp;&nbsp;&gt;&nbsp;&nbsp;'.join(breadcrumbs)}
            </p>
    
            <span style="font-size: 16px; color: #333">
                {brand_author_name[0]} {brand_author_name[1]}
            </span>

            <h1 style="font-size: 28px; font-weight: bold; color: #333">
                {product['product_name']}
            </h1>
            
            <p style="font-size: 18px;">  
                <b style="color: #333; font-size: 20px;">{product['rating_average']}</b>
                {generate_star_rating(product['rating_average'])} 
                <span style="color: grey; font-size: 16px;">({product['review_count']})</span>
                <span style="color: grey; font-size: 16px;">| Đã bán {product['quantity_sold']}</span>
            </p>

            <div style="display: flex; align-items: center; gap: 12px;">
                <span style="color: rgb(255, 66, 78); font-size: 26px; font-weight: bold;">
                    {product["price"]}₫
                </span>
                <!-- Only show discount if discount_rate > 0 -->
                {f'<span style="color: rgb(120, 120, 120); background-color: rgb(240, 240, 240); padding: 3px 6px; border-radius: 4px; font-size: 16px;">-{product["discount_rate"]}%</span>' if product["discount_rate"] > 0 else ''}
                {f'<span style="color: grey; text-decoration: line-through; font-size: 18px; font-weight: normal;">{product["original_price"]}₫</span>' if product["discount_rate"] > 0 else ''}
            </div>
        """, unsafe_allow_html=True)

    with st.expander("Mô tả sản phẩm"):
        # Use empty for scrollable content
        description = product['description']

        description = re.sub(r'<img[^>]*>', '', description)
        scrollable_content = st.empty()
            #     <h1 style="font-size: 24px; font-weight: bold; color: #333"> 
            #     Mô tả sản phẩm
            # </h1>
        scrollable_content.markdown(f"""

            <div style="max-height: 500px; overflow-y: scroll; font-size: 14px;">
                <style>
                    img {{
                        max-width: 100%;
                        height: auto;
                    }}
                </style>
                {description}
            </div>
        """, unsafe_allow_html=True)

def show_thumbnail_product(product_id, seller_id, df):
    # Fetch image URL for the product
    product = df[(df.product_id == product_id) & (df.seller_id == seller_id)].to_dict(orient='records')[0]

    images_url = fetch_images(product_id, seller_id, size = "thumbnail")
    
    st.button('📖', key=random.random(), on_click=select_product, args=(product_id,seller_id))
    st.image(images_url[0], use_container_width=True)
             
    product_name = product['product_name']
    if len(product_name) > 40:
        product_name = product_name[:37] + "..."  # Rút gọn tên nếu quá dài
    
    st.markdown(f"""
        <p style="font-size: 14px; font-weight: bold; color: #333; margin-bottom: 4px;">
            {product_name}
        </p>
        <div style="display: flex; align-items: center; gap: 6px;">
            <span style="color: rgb(255, 66, 78); font-size: 14px; font-weight: bold;">
                {product["price"]}₫
            </span>
            <p style="font-size: 12px; color: grey; margin: 2px 0;">⭐ {product['rating_average']} ({product['review_count']} đánh giá)</p>

        </div>
    """, unsafe_allow_html=True)
    


def show_thumbnail_products(ids, df):

    items_per_row = 5
    num_rows = len(ids) // items_per_row + (len(ids) % items_per_row > 0)
    for i in range(num_rows):
        cols = st.columns(items_per_row)
        start_idx = i * items_per_row
        end_idx = (i + 1) * items_per_row if i < num_rows - 1 else len(ids)
        for c in range(start_idx, end_idx):
            with cols[c % items_per_row]:
                product = ids.iloc[c]
                product_id = product['product_id']
                seller_id = product['seller_id']
                show_thumbnail_product(product_id,seller_id, df)

def select_product(product_id, seller_id):
    st.session_state['product_id'] = product_id
    st.session_state['seller_id'] = seller_id
def select_user(user_id):
    st.session_state['user_id'] = user_id
def show_recommendation():

    # ============== Prepare Data ========================== #
    content_based_dir = "recommendation/tiki/content_based/"
    tfidf_path = content_based_dir + "tfidf_model.gensim"
    dict_path = content_based_dir + "dictionary.gensim"
    index_path = content_based_dir + "similarity_index.gensim"

    tfidf = models.TfidfModel.load(download_minio(tfidf_path))
    index = similarities.SparseMatrixSimilarity.load(download_minio(index_path))
    dictionary = corpora.Dictionary.load(download_minio(dict_path))

    search_query_dir = "recommendation/tiki/search_query/"
    search_tfidf_path = search_query_dir + "search_tfidf_model.gensim"
    search_dict_path = search_query_dir + "search_dictionary.gensim"
    search_index_path = search_query_dir + "search_similarity_index.gensim"

    search_tfidf = models.TfidfModel.load(download_minio(search_tfidf_path))
    search_index = similarities.SparseMatrixSimilarity.load(download_minio(search_index_path))
    search_dictionary = corpora.Dictionary.load(download_minio(search_dict_path))

    als_dir = download_minio("recommendation/tiki/rcm_collaborative_filtering.parquet")
    als_data = pd.read_parquet(als_dir, engine="pyarrow")
    # shutil.rmtree("tmp")

    with connect_psql(PSQL_CONFIG) as db_conn:
        sql_stm = "SELECT * FROM dwh.products"
        products_df = pd.read_sql(sql_stm,db_conn)
        products_df = products_df.astype({'product_id': 'int', 'seller_id': 'int'})  
        products_df = products_df.sort_values(by=['product_id', 'seller_id']).reset_index()

    # ============== End  ========================== #

    option = st.radio("Select an option:", ["Search", "Enter Product ID and Seller ID"])
    if option == "Search":
        text = st.text_input("Search Engine: ")

        if text:

            ids = search_query(text, search_dictionary, search_tfidf, search_index, products_df)

            with st.expander("Search Results", expanded=True):
                show_thumbnail_products(ids, products_df)

    else:
        product_id = st.text_input("pid:")
        seller_id = st.text_input("sid:")    
        if product_id and seller_id:
            select_product(int(product_id), int(seller_id))
        
    show_product(st.session_state['product_id'],st.session_state['seller_id'], products_df)

    with st.expander("Content Based Filtering"):
        if 'product_id' in st.session_state and 'seller_id' in st.session_state:

            gensim_ids = gensim_recommendation(10, st.session_state['product_id'], st.session_state['seller_id'], dictionary, tfidf, index, products_df)
            st.header("Có thể bạn sẽ thích")
            show_thumbnail_products(gensim_ids, products_df)

    with st.expander("Collaborative Filtering"):
        customer_id = st.sidebar.text_input("User-ID", placeholder="Input your user id")
        log_in_clicked = st.sidebar.button("Log In")
        if log_in_clicked:
            select_user(int(customer_id))
            als_ids = als_recommendation(st.session_state['user_id'],als_data)
            st.header("Có thể bạn sẽ thích")
            show_thumbnail_products(als_ids, products_df)


