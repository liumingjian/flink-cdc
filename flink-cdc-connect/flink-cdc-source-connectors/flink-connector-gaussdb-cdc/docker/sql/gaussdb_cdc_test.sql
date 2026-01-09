-- GaussDB CDC to MySQL Sink - Flink 1.16 E2E Test
-- Simplified test for validating GaussDB CDC connector on Flink 1.16

-- =====================================================
-- Performance Settings
-- =====================================================
SET 'parallelism.default' = '2';
SET 'table.exec.state.ttl' = '1h';
SET 'execution.checkpointing.interval' = '10s';

-- =====================================================
-- Source Table: GaussDB CDC (Single DN for simplified testing)
-- =====================================================
CREATE TABLE gaussdb_products (
    product_id INT NOT NULL,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '10.250.0.30',
    'port' = '40000',
    'username' = 'tom',
    'password' = 'Gauss_235',
    'database-name' = 'db1',
    'schema-name' = 'public',
    'table-name' = 'products',
    'slot.name' = 'flink_cdc_test_slot',
    'decoding.plugin.name' = 'pgoutput',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.snapshot.fetch.size' = '1000',
    'scan.incremental.snapshot.chunk.size' = '10000'
);

-- =====================================================
-- Sink Table: MySQL
-- =====================================================
CREATE TABLE mysql_products (
    product_id INT NOT NULL,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql-sink:3306/inventory',
    'table-name' = 'products_sink',
    'username' = 'flinkuser',
    'password' = 'flinkpw',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

-- =====================================================
-- Start CDC Sync: GaussDB -> MySQL
-- =====================================================
INSERT INTO mysql_products
SELECT product_id, product_name, category, price, stock
FROM gaussdb_products;
