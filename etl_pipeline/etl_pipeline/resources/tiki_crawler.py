import requests
import time
import random
import json
import pandas as pd
import urllib.request
import os

from bs4 import BeautifulSoup
from tqdm import tqdm
from etl_pipeline.resources.minio_io_manager import MinIOHandler, connect_minio

# Headers để fake User-Agent
headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'vi-VN,vi;q=0.8,en-US;q=0.5,en;q=0.3',
    'x-guest-token': 'yj5i8HfLhplN6ckw471WVBG2QAzbTr3a',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}

params_page = {
    'limit': '40',
    'aggregations': '2',
    'trackity_id': 'dd393ec3-561d-b612-c009-277495b0b207',
    'page': '1',
    'category': '1883',
    'urlKey':  'nha-cua-doi-song',
}

params = {
    "params_product": {
        "platform": "web",
        "version": 3
    },
    "params_reviews": {
        'sort': 'score|desc,id|desc,stars|all',
        'page': 1,
        'limit': 5,
        'include': 'comments,contribute_info,attribute_vote_summary'
    },
    "params_seller": {
        'trackity_id': 'dd393ec3-561d-b612-c009-277495b0b207',
        "platform": "desktop",
        "version": 3
    }
}


base_urls = {
    "base_page_url" : "https://tiki.vn/api/personalish/v1/blocks/listings",
    "base_product_url" : "https://tiki.vn/api/v2/products/{}",
    "base_reviews_url" : "https://tiki.vn/api/v2/reviews",
    "base_seller_url" : "https://api.tiki.vn/product-detail/v2/widgets/seller"
}

class TikiCrawler(MinIOHandler):

    def __init__(self, minio_config, root_dir = "bronze/tiki/", tmp_dir = "./tmp"):
        super().__init__(minio_config, tmp_dir, root_dir)

        self.tmp_dir = tmp_dir
        self.root_dir = root_dir

        self.base_page_url = base_urls["base_page_url"]
        self.base_product_url = base_urls["base_product_url"]
        self.base_reviews_url = base_urls["base_reviews_url"]
        self.base_seller_url = base_urls["base_seller_url"]

        self.headers = headers
        self.params_page = params_page
        self.params_product = params["params_product"]
        self.params_reviews = params["params_reviews"]
        self.params_seller = params["params_seller"]


        self.categories_path = "categories.csv"
        self.tracking_ids_path = "tracking_ids.csv"

        self.categories_df = self.fetch_categories(self.categories_path)
        self.tracking_ids_df = self.init_track_ids(self.tracking_ids_path)

    def init_track_ids(self, path):
        with connect_minio(self.minio_config) as client:
            minio_path = os.path.join(self.root_dir, path)
            objects = list(client.list_objects(self.minio_config["bucket"], prefix=minio_path, recursive=True))
            if not objects:
                tracking_df = pd.DataFrame(columns=['pid', 'spid', 'seller_id', 'category_id', 'slug'])
                self.put_file_to_minio(tracking_df, path, file_type="csv")
            else:
                tracking_df = self.get_file_from_minio(path, file_type="csv")
            return tracking_df

    def tracking_ids(self, id):
        new_id_df = pd.DataFrame([id], columns=self.tracking_ids_df.columns)
        self.tracking_ids_df = pd.concat([self.tracking_ids_df, new_id_df], ignore_index=True)
        self.put_file_to_minio(self.tracking_ids_df,self.tracking_ids_path,file_type= "csv")

    def download_html(self, url):
        with urllib.request.urlopen(url) as response:
            html = response.read().decode('utf-8')
        return html
    
    def fetch_categories(self, path):
        source = self.download_html("https://tiki.vn")
        soup = BeautifulSoup(source, 'html.parser')
        cat = soup.find('div', {'class': 'styles__StyledListItem-sc-w7gnxl-0 cjqkgR'})
        sub_cats = cat.find_all('a', {'title': True})
        result = [{'title': sub_cat['title'], 'href': sub_cat['href']} for sub_cat in sub_cats]
        df = pd.DataFrame(result)
        df[['slug', 'category_id']] = df['href'].str.extract(r'/([^/]+)/c(\d+)')
        df.drop(columns = ['href'], inplace=True)
        self.put_file_to_minio(df,path,file_type="csv")

        return df
    
    def fetch_ids(self, urlKey, category, page=10):
        self.params_page['urlKey'] = urlKey
        self.params_page['category'] = category
        name = self.categories_df.loc[self.categories_df['category_id'] == str(category), 'title'].values[0]
    
        ids = []

        print(f"Fetching products id from category: {name}")
        for i in tqdm(range(1, page + 1), desc="Pages Scraped", unit="page"):
            self.params_page['page'] = i
            response = requests.get(
                self.base_page_url, headers=self.headers, params=self.params_page)

            if response.status_code == 200:
                data = response.json().get('data', [])
                for record in data:
                    ids.append({'pid': record.get('id'),
                                'spid': record.get('seller_product_id'),
                                'seller_id': record.get('seller_id'),
                                'category_id': category,
                                'slug': urlKey})   
                time.sleep(random.randint(3, 10))

        return ids

    def fetch_product(self, id):
        pid = id['pid']
        spid = id['spid']
        seller_id = id['seller_id']
        slug = id['slug']

        url = self.base_product_url.format(pid)
        self.params_product['spid'] = spid
        response = requests.get(url, headers=self.headers, params=self.params_product)
        if response.status_code == 200:
            path = f"{slug}/{pid}_{seller_id}/product_{pid}_{seller_id}.json"
            self.put_file_to_minio(response.json(), path, file_type="json")
            return True
        return False

    def fetch_reviews(self, id, page=10):
        pid = id['pid']
        spid = id['spid']
        seller_id = id['seller_id']
        slug = id['slug']

        self.params_reviews['product_id'] = pid
        self.params_reviews['spid'] = spid
        self.params_reviews['seller_id'] = seller_id

        for i in range(1, page+1):
            self.params_reviews['page'] = i
            response = requests.get(self.base_reviews_url, headers=self.headers, params=self.params_reviews)
            if response.status_code == 200:
                try:
                    file = response.json()
                    path = f"{slug}/{pid}_{seller_id}/reviews/reviews_{pid}_{seller_id}_{i}.json"
                    self.put_file_to_minio(file, path, file_type="json")
                except Exception as e:
                    print(f"Error fetching reviews: {e}")
                    pass
            else:
                return False
        return True

    def fetch_seller(self, id):
        pid = id['pid']
        spid = id['spid']
        seller_id = id['seller_id']
        slug = id['slug']

        self.params_seller['mpid'] = pid
        self.params_seller['spid'] = spid
        self.params_seller['seller_id'] = seller_id

        response = requests.get(self.base_seller_url, headers=self.headers, params=self.params_seller)
        if response.status_code == 200:
            path = f"{slug}/{pid}_{seller_id}/seller_{pid}_{seller_id}.json"
            self.put_file_to_minio(response.json(), path, file_type="json")
            return True
        return False
    
    def scrape_all_category(self, urlKey, category, page=10):
        ids = self.fetch_ids(urlKey, category, page)
        products = []
        for id in tqdm(ids, desc="Processing", unit="product"):
            is_existing = (
                (self.tracking_ids_df['pid'] == id['pid']) &  
                (self.tracking_ids_df['seller_id'] == id['seller_id'])
            ).any()

            if not is_existing:
                f1 = self.fetch_product(id)
                f2 = self.fetch_reviews(id)
                f3 = self.fetch_seller(id)
                if f1 and f2 and f3:
                    self.tracking_ids(id)
                    products.append(id)
        
        print(f"{len(products)} products added")
        return products
    
    def scrape_all(self, page = 10):
        for _, category in self.categories_df.iterrows():
            urlKey = category['slug']
            cat_id = category['category_id']
            self.scrape_all_category(urlKey, cat_id, page)

    def num_products(self):
        with connect_minio(self.minio_config) as client:
            objects = client.list_objects(self.minio_config["bucket"], prefix=self.root_dir, recursive=True)
        
        level1_folders = set()
        level2_subfolders = {}

        for obj in objects:
            parts = obj.object_name[len(self.root_dir):].split('/')
            if len(parts) > 1: 
                level1_folder = parts[0]
                level1_folders.add(level1_folder)
                
                if len(parts) > 2 and parts[1]: 
                    level2_folder = f"{level1_folder}/{parts[1]}"
                    if level1_folder not in level2_subfolders:
                        level2_subfolders[level1_folder] = set()
                    level2_subfolders[level1_folder].add(level2_folder)

        total_subfolders = 0
        products_num = []
        for level1_folder, subfolders in level2_subfolders.items():
            products_num.append({level1_folder:len(subfolders)})
            total_subfolders += len(subfolders)

        return products_num, total_subfolders
