#!/bin/bash
# =============================================================================
# Wait for services to be ready
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to wait for HTTP endpoint
wait_for_http() {
    local name=$1
    local url=$2
    local max_attempts=${3:-30}
    local attempt=1

    echo -e "${YELLOW}Waiting for $name at $url...${NC}"

    while [ $attempt -le $max_attempts ]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            echo -e "${GREEN}$name is ready!${NC}"
            return 0
        fi
        echo "  Attempt $attempt/$max_attempts..."
        sleep 5
        attempt=$((attempt + 1))
    done

    echo -e "${RED}$name failed to start after $max_attempts attempts${NC}"
    return 1
}

# Function to wait for TCP port
wait_for_port() {
    local name=$1
    local host=$2
    local port=$3
    local max_attempts=${4:-30}
    local attempt=1

    echo -e "${YELLOW}Waiting for $name at $host:$port...${NC}"

    while [ $attempt -le $max_attempts ]; do
        if nc -z "$host" "$port" 2>/dev/null; then
            echo -e "${GREEN}$name is ready!${NC}"
            return 0
        fi
        echo "  Attempt $attempt/$max_attempts..."
        sleep 5
        attempt=$((attempt + 1))
    done

    echo -e "${RED}$name failed to start after $max_attempts attempts${NC}"
    return 1
}

echo "============================================="
echo "Waiting for Data Platform services..."
echo "============================================="
echo ""

# Wait for core services
wait_for_port "PostgreSQL" "localhost" 5432 60
wait_for_port "Redis" "localhost" 6379 30
wait_for_http "HDFS NameNode" "http://localhost:9870" 60
wait_for_port "Hive Metastore" "localhost" 9083 60
wait_for_http "Spark Master" "http://localhost:8080" 60
wait_for_http "Airflow" "http://localhost:8081/health" 90
wait_for_http "Trino" "http://localhost:8082/v1/info" 60

echo ""
echo "============================================="
echo -e "${GREEN}All services are ready!${NC}"
echo "============================================="
echo ""
echo "Web UI URLs:"
echo "  HDFS NameNode:  http://localhost:9870"
echo "  Spark Master:   http://localhost:8080"
echo "  Airflow:        http://localhost:8081"
echo "  Trino:          http://localhost:8082"
