#!/bin/bash

# ==============================================================================
# GaussDB CDC Connector Deployment Script for Flink 1.16
# Deploys the connector JAR and starts the Flink cluster
# ==============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Project paths
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../../../.." && pwd)
CONNECTOR_MODULE="flink-cdc-connect/flink-cdc-source-connectors/flink-connector-gaussdb-cdc"
SQL_CONNECTOR_MODULE="flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-gaussdb-cdc"
DOCKER_DIR="$SCRIPT_DIR"
JARS_DIR="$DOCKER_DIR/jars"

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║       GaussDB CDC Connector Deployment - Flink 1.16              ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# 1. Build the connector if needed
CONNECTOR_JAR="$PROJECT_ROOT/$SQL_CONNECTOR_MODULE/target/flink-sql-connector-gaussdb-cdc-3.1-SNAPSHOT.jar"

if [ ! -f "$CONNECTOR_JAR" ]; then
    echo -e "${YELLOW}📦 Building GaussDB CDC connector...${NC}"
    cd "$PROJECT_ROOT"
    mvn package -pl $SQL_CONNECTOR_MODULE -am -Dmaven.test.skip=true -Dfast 
    cd "$SCRIPT_DIR"
fi

if [ ! -f "$CONNECTOR_JAR" ]; then
    echo -e "${RED}❌ Error: Connector JAR not found at $CONNECTOR_JAR${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Connector JAR found: $(basename $CONNECTOR_JAR)${NC}"

# 2. Prepare JARs directory
echo -e "${BLUE}📁 Preparing JARs directory...${NC}"
mkdir -p "$JARS_DIR"

# Copy connector JAR
cp "$CONNECTOR_JAR" "$JARS_DIR/"
echo -e "${GREEN}  ✅ Copied connector JAR${NC}"

# Copy GaussDB JDBC driver
GAUSSDB_JDBC="$PROJECT_ROOT/$CONNECTOR_MODULE/lib/gaussdbjdbc.jar"
if [ -f "$GAUSSDB_JDBC" ]; then
    cp "$GAUSSDB_JDBC" "$JARS_DIR/"
    echo -e "${GREEN}  ✅ Copied GaussDB JDBC driver${NC}"
else
    echo -e "${YELLOW}  ⚠️ GaussDB JDBC driver not found at $GAUSSDB_JDBC${NC}"
fi

# Download MySQL JDBC driver if not present
MYSQL_JDBC="$JARS_DIR/mysql-connector-java-8.0.28.jar"
if [ ! -f "$MYSQL_JDBC" ]; then
    echo -e "${BLUE}  📥 Downloading MySQL JDBC driver...${NC}"
    curl -sL -o "$MYSQL_JDBC" \
        "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar"
    echo -e "${GREEN}  ✅ Downloaded MySQL JDBC driver${NC}"
fi

# Download Flink JDBC connector if not present
FLINK_JDBC="$JARS_DIR/flink-connector-jdbc-1.16.0.jar"
if [ ! -f "$FLINK_JDBC" ]; then
    echo -e "${BLUE}  📥 Downloading Flink JDBC connector...${NC}"
    curl -sL -o "$FLINK_JDBC" \
        "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/1.16.0/flink-connector-jdbc-1.16.0.jar"
    echo -e "${GREEN}  ✅ Downloaded Flink JDBC connector${NC}"
fi

# 3. Start Flink cluster
echo ""
echo -e "${BLUE}🚀 Starting Flink 1.16 cluster...${NC}"
cd "$DOCKER_DIR"

# Stop existing containers
docker-compose down --remove-orphans 2>/dev/null || true

# Clean up old checkpoints
rm -rf ./flink-checkpoints/* ./flink-savepoints/* ./log/* 2>/dev/null || true

# Start fresh cluster
docker-compose up -d

echo -e "${YELLOW}⏳ Waiting for Flink cluster to be ready (30s)...${NC}"
sleep 30

# Check if JobManager is running
if docker ps | grep -q "flink-jobmanager"; then
    echo -e "${GREEN}✅ Flink JobManager is running${NC}"
else
    echo -e "${RED}❌ Flink JobManager failed to start${NC}"
    exit 1
fi

# Check if TaskManager is running
if docker ps | grep -q "flink-taskmanager"; then
    echo -e "${GREEN}✅ Flink TaskManager is running${NC}"
else
    echo -e "${RED}❌ Flink TaskManager failed to start${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  ✅ Deployment Complete!                                          ║${NC}"
echo -e "${GREEN}║                                                                  ║${NC}"
echo -e "${GREEN}║  Flink Web UI: http://localhost:8081                             ║${NC}"
echo -e "${GREEN}║  MySQL Sink:   localhost:3307 (user: flinkuser, pass: flinkpw)   ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "  1. Run: ./run_e2e_test.sh deploy   # Submit Flink SQL job"
echo "  2. Run: ./run_e2e_test.sh test     # Run data verification tests"
echo ""
