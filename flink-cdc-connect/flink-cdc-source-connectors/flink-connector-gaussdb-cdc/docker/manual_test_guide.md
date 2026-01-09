# Manual Verification Guide for GaussDB CDC

This document provides step-by-step instructions to verify the fix for the GaussDB CDC connector.

## 📋 Prerequisites

1.  **Flink Job is Running**: Ensure the Flink SQL job (Job ID: `d1daaf3ad770c102403af1a4f132a819` or similar) is in `RUNNING` state.
2.  **Access to GaussDB Source**: You need a client (e.g., DBeaver, psql, or Data Studio) connected to:
    *   **Host**: `10.250.0.30`
    *   **Port**: `40000`
    *   **User**: `tom`
    *   **Password**: `Gauss_235`
    *   **Database**: `db1`
    *   **Schema**: `public`
3.  **Access to MySQL Sink**: You can access the MySQL sink via Docker:
    ```bash
    docker exec -it mysql-sink mysql -u flinkuser -pflinkpw -D inventory
    ```

---

## 🧪 Test Case 1: Verify Snapshot Synchronization

**Objective**: Confirm that existing data has been synchronized.

1.  **Run in MySQL Sink**:
    ```sql
    SELECT count(*) FROM products_sink;
    ```

2.  **Expected Result**:
    *   The count should match the source table count (~33,581 rows).
    *   If the count is non-zero and stable, snapshot sync is complete.

---

## 🧪 Test Case 2: Verify Real-time INSERT

**Objective**: Confirm that new records are captured and synced.

1.  **Run in GaussDB Source**:
    ```sql
    INSERT INTO public.products (product_id, product_name, category, price, stock)
    VALUES (100001, 'Test Product A', 'Electronics', 99.99, 50);
    ```

2.  **Run in MySQL Sink**:
    ```sql
    SELECT * FROM products_sink WHERE product_id = 100001;
    ```

3.  **Expected Result**:
    *   A new row with `product_id = 100001` should appear in MySQL immediately.

---

## 🧪 Test Case 3: Verify Real-time UPDATE

**Objective**: Confirm that updates to existing records are synced.

1.  **Run in GaussDB Source** (Update the record inserted in Test 2):
    ```sql
    UPDATE public.products
    SET price = 129.99, stock = 45
    WHERE product_id = 100001;
    ```

2.  **Run in MySQL Sink**:
    ```sql
    SELECT * FROM products_sink WHERE product_id = 100001;
    ```

3.  **Expected Result**:
    *   The `price` should update to `129.99`.
    *   The `stock` should update to `45`.

---

## 🧪 Test Case 4: Verify Real-time DELETE

**Objective**: Confirm that deleted records are removed from the sink.

1.  **Run in GaussDB Source**:
    ```sql
    DELETE FROM public.products WHERE product_id = 100001;
    ```

2.  **Run in MySQL Sink**:
    ```sql
    SELECT * FROM products_sink WHERE product_id = 100001;
    ```

3.  **Expected Result**:
    *   The query should return **Empty Set** (no rows).

---

## 🧹 Cleanup (Optional)

If you need to restart the test from scratch:

1.  **Cancel Flink Job**:
    ```bash
    docker exec flink-jobmanager ./bin/flink cancel <JOB_ID>
    ```
2.  **Truncate MySQL Sink** (Optional, to verify clean snapshot):
    ```sql
    TRUNCATE TABLE products_sink;
    ```
3.  **Resubmit Job**:
    ```bash
    ./run_e2e_test.sh deploy
    ```
