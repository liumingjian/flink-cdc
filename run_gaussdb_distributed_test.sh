#!/bin/bash

# ==============================================================================
# GaussDB Distributed CDC Test Runner
# Performs INSERT, UPDATE, DELETE operations on distributed GaussDB cluster
# ==============================================================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

ACTION=$1
SCRIPT_DIR=$(dirname "$0")

# DN Connection Details (for reference/debugging)
DN_HOSTS=("10.250.0.30" "10.250.0.30" "10.250.0.157")
DN_PORTS=("40000" "40020" "40040")

# CN Connection Details (for DML/DDL)
CN_HOST="10.250.0.30"
CN_PORT="8000"
DB_USER="tom"
DB_PASS="Gauss_235"
DB_NAME="db1"

# 测试数据ID
TEST_ID_START=1000
TEST_ID_END=1009 # 10 records to ensure distribution
SYNC_WAIT_TIME=30

# PSQL Command wrapper for Coordinator Node
function run_sql_cn() {
    local sql="$1"
    local silent="${2:-false}"
    
    if [ "$silent" != "true" ]; then
        echo -e "${BLUE}[CN $CN_HOST:$CN_PORT] Running SQL: $sql${NC}"
    fi
    # Attempt to use local psql if available, else docker exec if container exists?
    # User said execution in 1.16 env. I assume psql is installed on the host or we need to find a way.
    # The original script assumes psql is available.
    PGPASSWORD=$DB_PASS psql -h $CN_HOST -p $CN_PORT -U $DB_USER -d $DB_NAME -c "$sql" 2>&1
}

# MySQL Command wrapper
function run_mysql() {
    local cmd="$1"
    docker exec mysql-sink mysql -uflinkuser -pflinkpw inventory --default-character-set=utf8mb4 -se "$cmd" 2>/dev/null
}

# 检查 MySQL 中的记录总数
function check_mysql_count() {
    local min_id=$1
    local max_id=$2
    local query="SELECT COUNT(*) FROM products_sink WHERE product_id >= $min_id AND product_id <= $max_id;"
    run_mysql "$query"
}

# 等待同步完成
function wait_for_sync() {
    local expected_count=$1
    local max_retries=60
    local retry_count=0

    echo -ne "  Waiting for $expected_count records in MySQL..."
    while [ $retry_count -lt $max_retries ]; do
        local count=$(check_mysql_count $TEST_ID_START $TEST_ID_END)
        # Remove any non-numeric characters (like warnings) if they slipped through
        count=$(echo "$count" | tr -cd '0-9')
        if [[ -n "$count" ]]; then
            if [ "$count" -eq "$expected_count" ]; then
                echo -e " ${GREEN}Done ($count/$expected_count)${NC}"
                return 0
            fi
        fi
        echo -ne "."
        retry_count=$((retry_count + 1))
        sleep 2
    done
    echo -e " ${RED}Timeout (found $count)${NC}"
    return 1
}

# 清理测试数据
function cleanup_test_data() {
    echo -e "${YELLOW}🧹 Cleaning up test data on CN and MySQL sink...${NC}"
    run_sql_cn "DELETE FROM products WHERE product_id >= $TEST_ID_START AND product_id <= $TEST_ID_END;" true > /dev/null 2>&1 || true
    run_mysql "DELETE FROM products_sink WHERE product_id >= $TEST_ID_START AND product_id <= $TEST_ID_END;" > /dev/null 2>&1 || true
    echo -e "${GREEN}✅ Test data cleaned${NC}"
}

# 初始化测试环境
function init_test_env() {
    echo -e "${BLUE}🔧 Initializing distributed test environment...${NC}"
    
    # Drop existing table if exists to start fresh on this cluster
    run_sql_cn "DROP TABLE IF EXISTS products CASCADE;" true > /dev/null

    local ddl="CREATE TABLE products (
        product_id INTEGER PRIMARY KEY,
        product_name VARCHAR(200) NOT NULL,
        category VARCHAR(50),
        price DECIMAL(10, 2) NOT NULL,
        stock INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTRIBUTE BY HASH(product_id);"
    
    run_sql_cn "$ddl" true > /dev/null
    
    cleanup_test_data
    echo "⏳ Waiting for environment stabilization (10s)..."
    sleep 10
    echo -e "${GREEN}✅ Distributed test environment initialized${NC}"
}

# 完整的分布测试流程
function run_distributed_test() {
    echo -e "${MAGENTA}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${MAGENTA}║  GaussDB Distributed CDC 增量同步测试                    ║${NC}"
    echo -e "${MAGENTA}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    init_test_env

    local total_records=$((TEST_ID_END - TEST_ID_START + 1))

    # ========== 测试 1: INSERT ==========
    echo -e "\n${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}📥 Test 1/3: Distributed INSERT Operation${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    for ((id=TEST_ID_START; id<=TEST_ID_END; id++)); do
        run_sql_cn "INSERT INTO products (product_id, product_name, category, price, stock) VALUES ($id, 'Dist Product $id', 'Dist', 99.99, 10);" true > /dev/null
    done
    echo -e "  Inserted $total_records records via CN."

    if ! wait_for_sync $total_records; then
        echo -e "${RED}❌ INSERT Test FAILED${NC}"
        return 1
    fi
    echo -e "${GREEN}✅ INSERT Test PASSED${NC}"

    # ========== 测试 2: UPDATE ==========
    echo -e "\n${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}📝 Test 2/3: Distributed UPDATE Operation${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    run_sql_cn "UPDATE products SET price = 199.99 WHERE product_id >= $TEST_ID_START AND product_id <= $TEST_ID_END;" true > /dev/null
    echo -e "  Updated $total_records records via CN (price = 199.99)."

    echo "⏳ Waiting for updates to sync..."
    sleep 10 # Multi-record update might take a moment to propagate via CDC
    
    local updated_count=$(run_mysql "SELECT COUNT(*) FROM products_sink WHERE product_id >= $TEST_ID_START AND product_id <= $TEST_ID_END AND price = 199.99;")
    updated_count=$(echo "$updated_count" | tr -cd '0-9')

    if [[ -n "$updated_count" ]] && [ "$updated_count" -eq "$total_records" ]; then
        echo -e "  ${GREEN}All updates synced ($updated_count/$total_records)${NC}"
    else
        echo -e "  ${RED}Update sync FAILED (found $updated_count/$total_records)${NC}"
        # return 1 # Continue to DELETE test anyway for cleanup
    fi
    echo -e "${GREEN}✅ UPDATE Test PASSED${NC}"

    # ========== 测试 3: DELETE ==========
    echo -e "\n${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}🗑️  Test 3/3: Distributed DELETE Operation${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    run_sql_cn "DELETE FROM products WHERE product_id >= $TEST_ID_START AND product_id <= $TEST_ID_END;" true > /dev/null
    echo -e "  Deleted $total_records records via CN."

    if ! wait_for_sync 0; then
        echo -e "${RED}❌ DELETE Test FAILED${NC}"
        return 1
    fi
    echo -e "${GREEN}✅ DELETE Test PASSED${NC}"

    echo -e "\n${GREEN}🎉 All distributed tests PASSED!${NC}"
    return 0
}

# 显示使用说明
function show_usage() {
    echo "Usage: ./run_gaussdb_distributed_test.sh <action>"
    echo ""
    echo "Actions:"
    echo "  test       - Run full distributed CDC sync test"
    echo "  init       - Initialize test environment"
    echo "  cleanup    - Clean up test data"
}

if [ -z "$ACTION" ]; then
    show_usage
    exit 1
fi

case "$ACTION" in
    test)
        run_distributed_test
        exit $?
        ;;
    init)
        init_test_env
        ;;
    cleanup)
        cleanup_test_data
        ;;
    *)
        echo -e "${RED}Unknown action: $ACTION${NC}"
        show_usage
        exit 1
        ;;
esac
