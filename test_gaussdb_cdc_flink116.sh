#!/bin/bash

# ==============================================================================
# GaussDB CDC 一键端到端测试脚本 - Flink 1.16 版本
# One-Click E2E Test for GaussDB CDC Connector on Flink 1.16
# 
# 执行顺序: 部署集群 -> 提交作业 -> 运行测试 -> 显示结果
# ==============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m'

# Paths
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
DOCKER_DIR="$SCRIPT_DIR/flink-cdc-connect/flink-cdc-source-connectors/flink-connector-gaussdb-cdc/docker"

# Start time
START_TIME=$(date +%s)

# Header
clear
echo -e "${MAGENTA}"
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                                                                  ║"
echo "║    GaussDB CDC 一键端到端测试 - Flink 1.16                       ║"
echo "║    One-Click E2E Test for GaussDB CDC - Flink 1.16               ║"
echo "║                                                                  ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo ""
echo -e "${CYAN}测试环境 / Test Environment:${NC}"
echo "  Flink Version: 1.16.3"
echo "  CDC Version:   3.1-SNAPSHOT"
echo "  GaussDB:       10.250.0.30:40000 (DN1)"
echo "  MySQL Sink:    localhost:3307"
echo ""
echo -e "${CYAN}测试流程 / Test Flow:${NC}"
echo "  1️⃣  部署 Flink 集群 / Deploy Flink Cluster"
echo "  2️⃣  提交 CDC 作业 / Submit CDC Job"
echo "  3️⃣  运行同步测试 / Run Sync Tests (INSERT/UPDATE/DELETE)"
echo ""
echo -e "${YELLOW}⏱️  开始时间 / Start Time: $(date '+%Y-%m-%d %H:%M:%S')${NC}"
echo ""

# Check if docker directory exists
if [ ! -d "$DOCKER_DIR" ]; then
    echo -e "${RED}❌ Error: Docker directory not found at $DOCKER_DIR${NC}"
    exit 1
fi

cd "$DOCKER_DIR"

# ========== Step 1: Deploy Flink Cluster ==========
echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║  步骤 1/3: 部署 Flink 1.16 集群                                  ║${NC}"
echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

bash ./deploy.sh

echo ""
echo -e "${GREEN}✅ 步骤 1 完成: Flink 集群已部署${NC}"
echo ""
sleep 5

# ========== Step 2: Submit CDC Job ==========
echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║  步骤 2/3: 提交 GaussDB CDC 作业                                 ║${NC}"
echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

bash ./run_e2e_test.sh deploy

echo ""
echo -e "${GREEN}✅ 步骤 2 完成: CDC 作业已提交${NC}"
echo ""
sleep 10

# ========== Step 3: Run Tests ==========
echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║  步骤 3/3: 运行同步测试 (INSERT/UPDATE/DELETE)                   ║${NC}"
echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

TEST_RESULT=0
bash ./run_e2e_test.sh test || TEST_RESULT=$?

echo ""

# ========== Final Report ==========
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║  测试报告 / Test Report                                          ║${NC}"
echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  Flink Version: ${CYAN}1.16.3${NC}"
echo -e "  CDC Version:   ${CYAN}3.1-SNAPSHOT${NC}"
echo -e "  总耗时 / Duration: ${YELLOW}${DURATION}s${NC}"
echo ""

if [ $TEST_RESULT -eq 0 ]; then
    echo -e "${GREEN}╔══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║  🎉 所有测试通过! ALL TESTS PASSED!                              ║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════════════════════════╝${NC}"
else
    echo -e "${RED}╔══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║  ❌ 部分测试失败 SOME TESTS FAILED                               ║${NC}"
    echo -e "${RED}╚══════════════════════════════════════════════════════════════════╝${NC}"
fi

echo ""
echo -e "${BLUE}相关命令 / Useful Commands:${NC}"
echo "  查看 Flink UI:     open http://localhost:8081"
echo "  查看同步状态:      cd $DOCKER_DIR && ./run_e2e_test.sh verify"
echo "  清理测试环境:      cd $DOCKER_DIR && ./run_e2e_test.sh cleanup"
echo ""

exit $TEST_RESULT
