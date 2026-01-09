-- MySQL Sink initialization script for E2E testing
-- This will be automatically executed when MySQL container starts

CREATE DATABASE IF NOT EXISTS inventory;
USE inventory;

-- Products sink table (mirrors GaussDB source)
CREATE TABLE IF NOT EXISTS products_sink (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10, 2),
    stock INT,
    sync_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Grant permissions
GRANT ALL PRIVILEGES ON inventory.* TO 'flinkuser'@'%';
FLUSH PRIVILEGES;
