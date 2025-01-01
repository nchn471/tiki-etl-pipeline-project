import re
import pandas as pd
from etl_pipeline.resources.minio_io_manager import MinIOHandler, connect_minio

class TikiTransform(MinIOHandler):
    def __init__(self,minio_config,root_dir = "bronze/tiki/", tmp_dir = "./tmp"):
        super().__init__(minio_config, tmp_dir, root_dir)

        self.root_dir = root_dir
        self.minio_config = minio_config

        categories_path = "categories.csv"
        self.categories_df = self.get_file_from_minio(categories_path,file_type="csv")

    @staticmethod
    def product_parser(json):
        d = {}
        d['product_id'] = json.get('id')
        d['product_sku'] = json.get('sku')
        d['product_name'] = json.get('name')
        d['product_url'] = json.get('short_url')
        # d['image_urls'] = [image.get('base_url') for image in json.get('images', [])]
        d['image_urls'] = ', '.join([image.get('base_url', '') for image in json.get('images', [])])
        d['short_description'] = json.get('short_description')


        # Price field
        d['original_price'] = json.get('list_price')
        d['discount'] = json.get('discount')
        d['price'] = json.get('price')
        d['discount_rate'] = json.get('discount_rate')


        tracking_info = json.get('tracking_info', {}).get('amplitude', {})
        d['is_authentic'] = tracking_info.get('is_authentic')
        d['is_freeship_xtra'] = tracking_info.get('is_freeship_xtra')
        d['is_top_deal'] = tracking_info.get('is_hero')
        d['is_top_brand'] = tracking_info.get('is_top_brand')
        d['return_reason'] = tracking_info.get('return_reason')

        d['inventory_status'] = json.get('inventory_status')
        d['inventory_type'] = json.get('inventory_type')
        d['quantity_sold'] = json.get('all_time_quantity_sold')
        d['day_ago_created'] = json.get('day_ago_created')

        brand = json.get('brand', {})
        d['brand_id'] = brand.get('id')
        d['brand_name'] = brand.get('name')
        # if book
        authors = json.get('authors', {})
        d['authors_id'] = [author['id'] for author in authors] #if len(authors) >= 1 else None
        d['authors_name'] = [author['name'] for author in authors] #if len(authors) >= 1 else None


        d['rating_average'] = json.get('rating_average')
        d['review_count'] = json.get('review_count')

        current_seller = json.get('current_seller', {})
        d['store_id'] = current_seller.get('store_id')
        d['seller_id'] = current_seller.get('id')
        d['seller_sku'] = current_seller.get('sku')
        d['seller_name'] = current_seller.get('name')
        d['seller_url'] = current_seller.get('link')
        d['seller_logo'] = current_seller.get('logo')

        for item in json.get('warranty_info', []):
            name = item.get('name', '').lower().replace(" ", "_")
            value = item.get('value')
            
            if name == 'thời_gian_bảo_hành':  
                d['warranty_period'] = value
            elif name == 'hình_thức_bảo_hành':  
                d['warranty_type'] = value
            elif name == 'nơi_bảo_hành':  
                d['warranty_location'] = value

        return d

    @staticmethod
    def seller_parser(json):
        d = {}
        seller = json.get('data', {}).get('seller', {})
        # d['store_id'] = seller.get('store_id')
        d['seller_id'] = seller.get('id')
        d['seller_name'] = seller.get('name')
        d['icon_url'] = seller.get('icon')
        d['store_url'] = seller.get('url')
        d['avg_rating_point'] = seller.get('avg_rating_point')
        d['review_count'] = seller.get('review_count')
        d['total_follower'] = seller.get('total_follower')
        d['days_since_joined'] = seller.get('days_since_joined')
        d['is_official'] = seller.get('is_official')
        d['store_level'] = seller.get('store_level')

        return d

    @staticmethod
    def reviews_parser(json):
        all_comments = []
        comments = json.get('data', [])
        for comment in comments:
            d = {}
            try:
                d['review_id'] = comment.get('id')
                d['product_id'] = comment.get('product_id')
                d['seller_id'] = comment.get('seller').get("id")
                d['title'] = comment.get('title')
                d['content'] = comment.get('content')
                d['status'] = comment.get('status')
                d['thank_count'] = comment.get('thank_count')
                d['rating'] = comment.get('rating')
                d['created_at'] = comment.get('created_at')
                d['customer_id'] = comment.get('customer_id')

                created_by = comment.get('created_by', {})
                if created_by:
                    # d['customer_id'] = created_by.get('id')
                    d['customer_name'] = created_by.get('full_name')
                    d['purchased_at'] = created_by.get('purchased_at')
                    d['avatar_url'] = created_by.get('avatar_url')
                    summary = created_by.get('contribute_info', {}).get('summary', {})
                    d['joined_time'] = summary.get('joined_time')
                    d['total_review'] = summary.get('total_review')
                    d['total_thank'] = summary.get('total_thank')
                else:
                    d['customer_name'] = None
                    d['purchased_at'] = None
                    d['avatar_url'] = None
                    d['joined_time'] = None
                    d['total_review'] = 0
                    d['total_thank'] = 0
                all_comments.append(d)
            except:
                pass
        return all_comments
        
    def parse_json(self,file_path, parser_func):
        try:
                data = self.get_file_from_minio(file_path, file_type="json")
                return parser_func(data)
        except Exception as e:
            print(f"Error in JSON: {file_path}: {e}")
            return None
        
    def transform_data(self, type="products"):
        type_map = {
            "products": (r'product.*\.json', self.product_parser),
            "sellers": (r'seller.*\.json', self.seller_parser),
            "reviews": (r'reviews.*\.json', self.reviews_parser)
        }

        if type not in type_map:
            raise ValueError(f"Invalid type: {type}. Accepted values are: {list(type_map.keys())}")

        pattern, parser_func = type_map[type]
        all_data = []

        with connect_minio(self.minio_config) as client:

            objects = client.list_objects(self.minio_config["bucket"], prefix=self.root_dir, recursive=True)

            for obj in objects:
                obj_path = obj.object_name[len(self.root_dir):]
                path_parts = obj_path.split('/')

                if type == "reviews" and "reviews" in obj_path:

                    if re.match(pattern, path_parts[-1]):
                        parsed_data = self.parse_json(obj.object_name,parser_func)
                        if parsed_data:
                            all_data.extend(parsed_data)

                elif re.match(pattern, path_parts[-1]):
                    parsed_data = self.parse_json(obj.object_name,parser_func)
                    if parsed_data:
                        if type == "products":
                            category_map = dict(zip(self.categories_df['slug'], self.categories_df['category_id']))
                            category_name = path_parts[0] if len(path_parts) > 0 else None
                            parsed_data['category_id'] = category_map.get(category_name, None)
                        all_data.append(parsed_data)

        return pd.DataFrame(all_data)
