#!/bin/bash

# ==============================================================================
# GaussDB CDC E2E Test Script for Flink 1.16
# Performs end-to-end testing: deploy job -> insert data -> verify sync
# ==============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

# Script paths
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
SQL_FILE="$SCRIPT_DIR/sql/gaussdb_cdc_test.sql"

# GaussDB connection details
GAUSSDB_HOST="10.250.0.30"
GAUSSDB_PORT="40000"
GAUSSDB_USER="tom"
GAUSSDB_PASS="Gauss_235"
GAUSSDB_DB="db1"

# MySQL Sink connection details
MYSQL_HOST="localhost"
MYSQL_PORT="3308"
MYSQL_USER="flinkuser"
MYSQL_PASS="flinkpw"
MYSQL_DB="inventory"

# Test parameters
TEST_ID_START=5000
TEST_ID_END=5009
SOURCE_TABLE="products"
SINK_TABLE="products_sink"

ACTION="${1:-help}"

# ==============================================================================
# Helper Functions
# ==============================================================================

function run_gaussdb_sql() {
    local sql="$1"
    PGPASSWORD=$GAUSSDB_PASS psql -h $GAUSSDB_HOST -p $GAUSSDB_PORT -U $GAUSSDB_USER -d $GAUSSDB_DB -c "$sql" 2>&1
}

function run_mysql_sql() {
    local sql="$1"
    docker exec mysql-sink mysql -u$MYSQL_USER -p$MYSQL_PASS -D$MYSQL_DB -e "$sql" 2>&1
}

function check_mysql_count() {
    local result=$(docker exec mysql-sink mysql -u$MYSQL_USER -p$MYSQL_PASS -D$MYSQL_DB -N -e \
        "SELECT COUNT(*) FROM $SINK_TABLE WHERE product_id >= $TEST_ID_START AND product_id <= $TEST_ID_END;" 2>/dev/null)
    echo "$result" | tr -cd '0-9'
}

function wait_for_sync() {
    local expected=$1
    local max_wait=120
    local elapsed=0
    
    echo -ne "  Waiting for $expected records in MySQL sink"
    while [ $elapsed -lt $max_wait ]; do
        local count=$(check_mysql_count)
        if [ "$count" = "$expected" ]; then
            echo -e " ${GREEN}✅ Done ($count/$expected)${NC}"
            return 0
        fi
        echo -ne "."
        sleep 2
        elapsed=$((elapsed + 2))
    done
    echo -e " ${RED}❌ Timeout (found: $count, expected: $expected)${NC}"
    return 1
}

function cleanup_test_data() {
    echo -e "${YELLOW}🧹 Cleaning up test data...${NC}"
    run_gaussdb_sql "DELETE FROM $SOURCE_TABLE WHERE product_id >= $TEST_ID_START AND product_id <= $TEST_ID_END;" > /dev/null 2>&1 || true
    run_mysql_sql "DELETE FROM $SINK_TABLE WHERE product_id >= $TEST_ID_START AND product_id <= $TEST_ID_END;" > /dev/null 2>&1 || true
    echo -e "${GREEN}  ✅ Test data cleaned${NC}"
}

# ==============================================================================
# Deploy: Submit Flink SQL Job
# ==============================================================================
function do_deploy() {
    echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${MAGENTA}║  Deploying Flink SQL Job for GaussDB CDC                         ║${NC}"
    echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    if [ ! -f "$SQL_FILE" ]; then
        echo -e "${RED}❌ SQL file not found: $SQL_FILE${NC}"
        exit 1
    fi
    
    echo -e "${BLUE}📄 Submitting SQL job from: $(basename $SQL_FILE)${NC}"
    
    # Copy SQL file to container
    docker cp "$SQL_FILE" flink-jobmanager:/tmp/flink_job.sql
    
    # Execute SQL via Flink SQL Client
    docker exec -i flink-jobmanager /opt/flink/bin/sql-client.sh -f /tmp/flink_job.sql
    
    echo ""
    echo -e "${GREEN}✅ Flink SQL job submitted successfully${NC}"
    echo ""
    echo -e "${YELLOW}⏳ Waiting 30 seconds for job to initialize...${NC}"
    sleep 30
    
    # Check job status
    echo -e "${BLUE}📊 Checking job status...${NC}"
    curl -s http://localhost:8081/jobs/overview | python3 -m json.tool 2>/dev/null || echo "Unable to fetch job status"
    echo ""
}

# ==============================================================================
# Test: INSERT/UPDATE/DELETE verification
# ==============================================================================
function do_test() {
    echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${MAGENTA}║  Running GaussDB CDC E2E Tests                                   ║${NC}"
    echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    local tests_passed=0
    local tests_failed=0
    
    # Cleanup first
    cleanup_test_data
    echo ""
    
    # ---------- TEST 1: INSERT ----------
    echo -e "${CYAN}━━━ TEST 1: INSERT Operations ━━━${NC}"
    echo -e "${BLUE}  Inserting 10 records into GaussDB source...${NC}"
    
    for i in $(seq $TEST_ID_START $TEST_ID_END); do
        run_gaussdb_sql "INSERT INTO $SOURCE_TABLE (product_id, product_name, category, price, stock) VALUES ($i, 'Product_$i', 'Category_$((i % 3))', $((i * 10)).99, $((i * 5)));" > /dev/null 2>&1
    done
    echo -e "${GREEN}  ✅ Inserted 10 records${NC}"
    
    if wait_for_sync 10; then
        echo -e "${GREEN}  ✅ TEST 1 PASSED: INSERT sync verified${NC}"
        tests_passed=$((tests_passed + 1))
    else
        echo -e "${RED}  ❌ TEST 1 FAILED: INSERT sync failed${NC}"
        tests_failed=$((tests_failed + 1))
    fi
    echo ""
    
    # ---------- TEST 2: UPDATE ----------
    echo -e "${CYAN}━━━ TEST 2: UPDATE Operations ━━━${NC}"
    echo -e "${BLUE}  Updating 5 records in GaussDB source...${NC}"
    
    for i in $(seq $TEST_ID_START $((TEST_ID_START + 4))); do
        run_gaussdb_sql "UPDATE $SOURCE_TABLE SET product_name = 'Updated_Product_$i', price = price + 100 WHERE product_id = $i;" > /dev/null 2>&1
    done
    echo -e "${GREEN}  ✅ Updated 5 records${NC}"
    
    echo -e "${YELLOW}  ⏳ Waiting 30 seconds for UPDATE sync...${NC}"
    sleep 30
    
    # Verify update was synced (check if price increased)
    local updated_count=$(docker exec mysql-sink mysql -u$MYSQL_USER -p$MYSQL_PASS -D$MYSQL_DB -N -e \
        "SELECT COUNT(*) FROM $SINK_TABLE WHERE product_name LIKE 'Updated_%' AND product_id >= $TEST_ID_START AND product_id <= $((TEST_ID_START + 4));" 2>/dev/null | tr -cd '0-9')
    
    if [ "$updated_count" = "5" ]; then
        echo -e "${GREEN}  ✅ TEST 2 PASSED: UPDATE sync verified ($updated_count/5 updated)${NC}"
        tests_passed=$((tests_passed + 1))
    else
        echo -e "${RED}  ❌ TEST 2 FAILED: UPDATE sync failed ($updated_count/5 updated)${NC}"
        tests_failed=$((tests_failed + 1))
    fi
    echo ""
    
    # ---------- TEST 3: DELETE ----------
    echo -e "${CYAN}━━━ TEST 3: DELETE Operations ━━━${NC}"
    echo -e "${BLUE}  Deleting 3 records from GaussDB source...${NC}"
    
    for i in $(seq $((TEST_ID_END - 2)) $TEST_ID_END); do
        run_gaussdb_sql "DELETE FROM $SOURCE_TABLE WHERE product_id = $i;" > /dev/null 2>&1
    done
    echo -e "${GREEN}  ✅ Deleted 3 records${NC}"
    
    if wait_for_sync 7; then
        echo -e "${GREEN}  ✅ TEST 3 PASSED: DELETE sync verified${NC}"
        tests_passed=$((tests_passed + 1))
    else
        echo -e "${RED}  ❌ TEST 3 FAILED: DELETE sync failed${NC}"
        tests_failed=$((tests_failed + 1))
    fi
    echo ""
    
    # ---------- SUMMARY ----------
    echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${MAGENTA}║  Test Summary                                                    ║${NC}"
    echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "  Tests Passed: ${GREEN}$tests_passed${NC}"
    echo -e "  Tests Failed: ${RED}$tests_failed${NC}"
    echo ""
    
    if [ $tests_failed -eq 0 ]; then
        echo -e "${GREEN}🎉 ALL TESTS PASSED!${NC}"
        return 0
    else
        echo -e "${RED}❌ SOME TESTS FAILED${NC}"
        return 1
    fi
}

# ==============================================================================
# Verify: Check current sync status
# ==============================================================================
function do_verify() {
    echo -e "${BLUE}━━━ Current Sync Status ━━━${NC}"
    echo ""
    
    echo -e "${CYAN}GaussDB Source (products table):${NC}"
    run_gaussdb_sql "SELECT COUNT(*) as count FROM $SOURCE_TABLE;" 2>/dev/null || echo "  Unable to connect to GaussDB"
    echo ""
    
    echo -e "${CYAN}MySQL Sink (products_sink table):${NC}"
    run_mysql_sql "SELECT COUNT(*) as count FROM $SINK_TABLE;" 2>/dev/null || echo "  Unable to connect to MySQL"
    echo ""
    
    echo -e "${CYAN}Flink Jobs:${NC}"
    curl -s http://localhost:8081/jobs/overview 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for job in data.get('jobs', []):
        print(f\"  - {job.get('name', 'Unknown')}: {job.get('state', 'Unknown')}\")
except:
    print('  Unable to fetch job status')
" || echo "  Unable to connect to Flink"
    echo ""
}

# ==============================================================================
# Cleanup: Stop all containers and clean data
# ==============================================================================
function do_cleanup() {
    echo -e "${YELLOW}🧹 Cleaning up...${NC}"
    cleanup_test_data
    
    echo -e "${BLUE}🛑 Stopping Flink cluster...${NC}"
    cd "$SCRIPT_DIR"
    docker-compose down --remove-orphans
    
    echo -e "${GREEN}✅ Cleanup complete${NC}"
}

# ==============================================================================
# Help
# ==============================================================================
function show_help() {
    echo ""
    echo -e "${BLUE}GaussDB CDC E2E Test Script for Flink 1.16${NC}"
    echo ""
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  deploy   - Submit Flink SQL job for GaussDB CDC"
    echo "  test     - Run INSERT/UPDATE/DELETE tests"
    echo "  verify   - Check current sync status"
    echo "  cleanup  - Stop containers and clean test data"
    echo "  help     - Show this help message"
    echo ""
    echo "Example:"
    echo "  ./run_e2e_test.sh deploy  # First: submit the job"
    echo "  ./run_e2e_test.sh test    # Then: run tests"
    echo ""
}

# ==============================================================================
# Main
# ==============================================================================
case "$ACTION" in
    deploy)
        do_deploy
        ;;
    test)
        do_test
        ;;
    verify)
        do_verify
        ;;
    cleanup)
        do_cleanup
        ;;
    help|*)
        show_help
        ;;
esac
