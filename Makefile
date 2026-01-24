# =============================================================================
# Data Platform - Makefile
# =============================================================================

.PHONY: help build up down restart logs ps clean health test-spark test-trino init-hdfs shell-spark shell-airflow shell-trino

# Default target
help:
	@echo "Data Platform Commands:"
	@echo ""
	@echo "  make build        - Build all Docker images"
	@echo "  make up           - Start all services"
	@echo "  make down         - Stop all services"
	@echo "  make restart      - Restart all services"
	@echo "  make logs         - Show logs (use SERVICE=name for specific)"
	@echo "  make ps           - Show service status"
	@echo "  make clean        - Remove all containers and volumes"
	@echo "  make health       - Check health of all services"
	@echo ""
	@echo "  make init-hdfs    - Initialize HDFS Medallion directories"
	@echo "  make test-spark   - Run Spark integration test"
	@echo "  make test-trino   - Run Trino integration test"
	@echo ""
	@echo "  make shell-spark  - Open shell in Spark master"
	@echo "  make shell-airflow - Open shell in Airflow webserver"
	@echo "  make shell-trino  - Open shell in Trino"

# Build images
build:
	docker compose build

# Start services
up:
	docker compose up -d
	@echo ""
	@echo "Waiting for services to start..."
	@sleep 10
	@$(MAKE) ps

# Start services with build
up-build:
	docker compose up -d --build
	@echo ""
	@echo "Waiting for services to start..."
	@sleep 10
	@$(MAKE) ps

# Stop services
down:
	docker compose down

# Restart services
restart:
	docker compose restart

# Show logs
logs:
ifdef SERVICE
	docker compose logs -f $(SERVICE)
else
	docker compose logs -f
endif

# Show status
ps:
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

# Clean everything
clean:
	docker compose down -v --remove-orphans
	docker system prune -f

# Health check
health:
	@echo "Checking service health..."
	@echo ""
	@echo "=== HDFS NameNode ==="
	@curl -sf http://localhost:9870/ > /dev/null && echo "OK: http://localhost:9870" || echo "FAIL: NameNode not responding"
	@echo ""
	@echo "=== Spark Master ==="
	@curl -sf http://localhost:8080/ > /dev/null && echo "OK: http://localhost:8080" || echo "FAIL: Spark Master not responding"
	@echo ""
	@echo "=== Airflow ==="
	@curl -sf http://localhost:8081/health > /dev/null && echo "OK: http://localhost:8081" || echo "FAIL: Airflow not responding"
	@echo ""
	@echo "=== Trino ==="
	@curl -sf http://localhost:8082/v1/info > /dev/null && echo "OK: http://localhost:8082" || echo "FAIL: Trino not responding"
	@echo ""
	@echo "=== PostgreSQL ==="
	@docker compose exec -T postgres pg_isready -U postgres > /dev/null && echo "OK: PostgreSQL ready" || echo "FAIL: PostgreSQL not ready"
	@echo ""
	@echo "=== Redis ==="
	@docker compose exec -T redis redis-cli -a redis123 ping 2>/dev/null | grep -q PONG && echo "OK: Redis ready" || echo "FAIL: Redis not ready"

# Initialize HDFS directories
init-hdfs:
	@echo "Initializing HDFS Medallion directories..."
	docker compose exec namenode /usr/local/bin/init-medallion.sh

# Test Spark integration
test-spark:
	@echo "Running Spark integration test..."
	docker compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark_jobs/test/hello_world.py
	@echo ""
	@echo "Spark integration test completed!"

# Test Trino
test-trino:
	@echo "Testing Trino connectivity..."
	@docker compose exec trino trino --execute "SHOW CATALOGS"
	@echo ""
	@echo "Testing Hive catalog..."
	@docker compose exec trino trino --execute "SHOW SCHEMAS FROM hive"
	@echo ""
	@echo "Trino integration test completed!"

# Shell access
shell-spark:
	docker compose exec spark-master /bin/bash

shell-airflow:
	docker compose exec airflow-webserver /bin/bash

shell-trino:
	docker compose exec trino /bin/bash

shell-namenode:
	docker compose exec namenode /bin/bash

# Show Web UI URLs
urls:
	@echo "Web UI URLs:"
	@echo "  HDFS NameNode:  http://localhost:9870"
	@echo "  Spark Master:   http://localhost:8080"
	@echo "  Airflow:        http://localhost:8081 (admin/admin)"
	@echo "  Trino:          http://localhost:8082"
