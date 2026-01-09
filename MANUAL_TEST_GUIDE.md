# GaussDB CDC Distributed Manual Verification Guide

This guide provides steps to manually verify the GaussDB CDC connector fixes in a distributed environment (CN + 3 DNs).

## 🌍 Environment Overview

| Role | Host | Port | Purpose |
| :--- | :--- | :--- | :--- |
| **CN (Coordinator)** | `10.250.0.30` | `8000` | DML (INSERT/UPDATE/DELETE) |
| **DN1 (Data Node)** | `10.250.0.30` | `40000` | Source Segment 1 |
| **DN2 (Data Node)** | `10.250.0.30` | `40020` | Source Segment 2 |
| **DN3 (Data Node)** | `10.250.0.157` | `40040` | Source Segment 3 |
| **MySQL Sink** | `localhost` | `3306` | Destination Table (`inventory.products_sink`) |

---

## 🚀 Quick Verification (Automated)

You can run the full verification suite using the provided script:

```bash
./test_gaussdb_distributed_cdc.sh
```

This script will:
1. Rebuild and deploy the connector JAR.
2. Restart Flink with the updated JAR.
3. Perform distributed `INSERT`, `UPDATE`, and `DELETE` operations.
4. Verify data consistency between all DNs and the MySQL sink.

---

## 🛠️ Step-by-Step Manual Verification

### 1. Initialize Environment
Reset the test data on both source and sink:
```bash
./run_gaussdb_distributed_test.sh init
```

### 2. Verify INSERT
Connect to the **Coordinator Node (CN)** and insert records:
```sql
-- Run on CN (10.250.0.30:8000)
INSERT INTO products (product_id, product_name, category, price, stock) VALUES (2001, 'Manual Test 1', 'Test', 10.99, 100);
INSERT INTO products (product_id, product_name, category, price, stock) VALUES (2002, 'Manual Test 2', 'Test', 20.99, 200);
```

Verify in **MySQL**:
```sql
-- Run on MySQL
SELECT * FROM products_sink WHERE product_id IN (2001, 2002);
```

### 3. Verify UPDATE
Update records via CN:
```sql
-- Run on CN
UPDATE products SET price = 99.99 WHERE product_id = 2001;
```

Verify in **MySQL**:
```sql
-- Run on MySQL
SELECT price FROM products_sink WHERE product_id = 2001; -- Should be 99.99
```

### 4. Verify DELETE
Delete records via CN:
```sql
-- Run on CN
DELETE FROM products WHERE product_id = 2002;
```

Verify in **MySQL**:
```sql
-- Run on MySQL
SELECT count(*) FROM products_sink WHERE product_id = 2002; -- Should be 0
```

---

## 📊 Data Consistency Check

To check if all data across distributed nodes matches the sink:
```bash
./check_distributed_sync_result.sh
```

## 🔍 Troubleshooting

### Check TaskManager Logs
If data is not syncing, check for `TableId` or `LSN` errors:
```bash
docker logs flink-taskmanager --tail 200
```

### Check Replication Slots
Verify that slots are active on DNs:
```sql
-- Run on each DN
SELECT * FROM pg_replication_slots WHERE slot_name LIKE 'flink_cdc%';
```

---

## 🧹 Cleanup
To clean up all test data:
```bash
./run_gaussdb_distributed_test.sh cleanup
```
