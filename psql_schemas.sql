CREATE SCHEMA IF NOT EXISTS dwh;

DROP TABLE IF EXISTS dwh.reviews CASCADE;
DROP TABLE IF EXISTS dwh.products_authors CASCADE;
DROP TABLE IF EXISTS dwh.images_url CASCADE;
DROP TABLE IF EXISTS dwh.authors CASCADE;
DROP TABLE IF EXISTS dwh.products CASCADE;
DROP TABLE IF EXISTS dwh.brands CASCADE;
DROP TABLE IF EXISTS dwh.sellers CASCADE;
DROP TABLE IF EXISTS dwh.categories CASCADE;
DROP TABLE IF EXISTS dwh.users CASCADE;

-- Categories table
CREATE TABLE dwh.categories (
    category_id VARCHAR(64) PRIMARY KEY,
    title VARCHAR(64),
    slug VARCHAR(64)
);

-- Sellers table
CREATE TABLE dwh.sellers (
    seller_id VARCHAR(64) PRIMARY KEY,
    seller_name VARCHAR(255),
    icon_url TEXT,
    store_url TEXT,
    avg_rating_point DECIMAL(4, 3) DEFAULT 0,
    review_count INT DEFAULT 0,
    total_follower INT DEFAULT 0,
    days_since_joined INT DEFAULT 0,
    is_official BOOLEAN DEFAULT FALSE,
    store_level VARCHAR(32) DEFAULT 'NONE'
);

-- Authors table
CREATE TABLE dwh.authors (
    author_id VARCHAR(64) PRIMARY KEY,
    author_name VARCHAR(255)
);

-- Brands table
CREATE TABLE dwh.brands (
    brand_id VARCHAR(64) PRIMARY KEY,
    brand_name VARCHAR(255)
);

-- Products table
CREATE TABLE dwh.products (
    product_id VARCHAR(64) NOT NULL,
    seller_id VARCHAR(64) NOT NULL,
    category_id VARCHAR(64),
    brand_id VARCHAR(64),
    product_name TEXT,
    description TEXT,
    specifications TEXT,
    breadcrumbs TEXT,
    original_price DECIMAL(15, 2),
    discount DECIMAL(15, 2) DEFAULT 0,
    price DECIMAL(15, 2),
    discount_rate INT DEFAULT 0,
    quantity_sold INT DEFAULT 0,
    rating_average DECIMAL(2, 1) DEFAULT 0.0,
    review_count INT DEFAULT 0,
    day_ago_created INT,
    product_url TEXT,
    is_authentic BOOLEAN DEFAULT FALSE,
    is_freeship_xtra BOOLEAN DEFAULT FALSE,
    is_top_deal BOOLEAN DEFAULT FALSE,
    return_reason VARCHAR(32) DEFAULT 'no_return',
    inventory_type VARCHAR(32),
    warranty_period INT DEFAULT 0,
    warranty_type VARCHAR(100) DEFAULT 'Không bảo hành',
    warranty_location VARCHAR(100) DEFAULT 'Không bảo hành',
    PRIMARY KEY (product_id, seller_id),
    FOREIGN KEY (category_id) REFERENCES dwh.categories (category_id),
    FOREIGN KEY (seller_id) REFERENCES dwh.sellers (seller_id),
    FOREIGN KEY (brand_id) REFERENCES dwh.brands (brand_id)

);


-- Products-Authors junction table
CREATE TABLE dwh.products_authors (
    product_id VARCHAR(64) NOT NULL,
    seller_id VARCHAR(64) NOT NULL,
    author_id VARCHAR(64) NOT NULL,
    PRIMARY KEY (product_id, seller_id, author_id),
    FOREIGN KEY (product_id, seller_id) REFERENCES dwh.products (product_id, seller_id),
    FOREIGN KEY (author_id) REFERENCES dwh.authors (author_id)
);

-- Images URL table
CREATE TABLE dwh.images_url (
    product_id VARCHAR(64) NOT NULL,
    seller_id VARCHAR(64) NOT NULL,
    image_url TEXT NOT NULL,
    PRIMARY KEY (product_id, seller_id, image_url),
    FOREIGN KEY (product_id, seller_id) REFERENCES dwh.products (product_id, seller_id)
);

-- Users table
CREATE TABLE dwh.users (
    user_id VARCHAR(64) PRIMARY KEY,
    user_name TEXT,
    avatar_url TEXT,
    joined_day INT,
    joined_time TEXT,
    total_review INT,
    total_thank INT
);

-- Reviews table
CREATE TABLE dwh.reviews (
    review_id VARCHAR(64) PRIMARY KEY,
    product_id VARCHAR(64) NOT NULL,
    seller_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    title TEXT,
    content TEXT,
    thank_count INT DEFAULT 0,
    rating DECIMAL(2, 1),
    created_at TIMESTAMP,
    purchased_at TIMESTAMP,
    FOREIGN KEY (product_id, seller_id) REFERENCES dwh.products (product_id, seller_id),
    FOREIGN KEY (customer_id) REFERENCES dwh.users (user_id) 
);



