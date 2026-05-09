# =============================================================================
# Data Platform - Makefile
# =============================================================================

.PHONY: help build up down restart logs ps clean health test-spark test-trino init-hdfs init-storage shell-spark shell-airflow shell-trino test-fbref-curl test-fbref-nodriver test-fbref-full test-proxy-stats superset-import superset-dashboards

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
	@echo "  make init-hdfs    - Initialize HDFS Medallion directories (legacy)"
	@echo "  make init-storage - Initialize HDFS + Hive schemas (recommended)"
	@echo "  make test-spark   - Run Spark integration test"
	@echo "  make test-trino   - Run Trino integration test"
	@echo ""
	@echo "  make shell-spark  - Open shell in Spark master"
	@echo "  make shell-airflow - Open shell in Airflow webserver"
	@echo "  make shell-trino  - Open shell in Trino"
	@echo ""
	@echo "FBref Scraping Tests:"
	@echo "  make test-fbref-curl      - Test curl_cffi with residential proxy"
	@echo "  make test-fbref-nodriver  - Test nodriver (browser) fallback"
	@echo "  make test-fbref-full      - Run full test pipeline"
	@echo "  make test-proxy-stats     - Show proxy pool statistics"

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

# Initialize HDFS directories (legacy - uses shell script)
init-hdfs:
	@echo "Initializing HDFS Medallion directories..."
	docker compose exec namenode /usr/local/bin/init-medallion.sh

# Initialize storage (HDFS + Hive schemas via Python)
init-storage:
	@echo "Initializing storage (HDFS directories + Hive schemas)..."
	docker compose exec airflow-scheduler python /opt/airflow/scripts/init_storage.py

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

# =============================================================================
# FBref Scraping Tests
# =============================================================================

# Test curl_cffi with residential proxy (basic test)
test-fbref-curl:
	@echo "Testing FBref with curl_cffi + residential proxy..."
	@docker compose exec airflow-webserver python -c "\
from curl_cffi.requests import Session; \
import random; \
proxies = open('/opt/airflow/proxys.txt').readlines(); \
proxy = random.choice(proxies).strip(); \
host, port, user, pwd = proxy.split(':'); \
proxy_url = f'http://{user}:{pwd}@{host}:{port}'; \
s = Session(impersonate='chrome120'); \
s.proxies = {'http': proxy_url, 'https': proxy_url}; \
print(f'Using proxy: {host}:{port}'); \
r = s.get('https://api.ipify.org?format=json'); \
print(f'Proxy IP: {r.json()}'); \
r = s.get('https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures'); \
print(f'FBref Status: {r.status_code}'); \
print(f'Has tables: {\"<table\" in r.text}'); \
print(f'Cloudflare blocked: {\"challenge\" in r.text.lower() or \"just a moment\" in r.text.lower()}'); \
print(f'Content length: {len(r.text)} chars'); \
"

# Test nodriver fallback (browser-based)
test-fbref-nodriver:
	@echo "Testing FBref with nodriver fallback..."
	docker compose exec airflow-webserver python dags/scripts/run_fbref_scraper.py \
		--scraper-type selenium \
		--use-nodriver \
		--proxy-file /opt/airflow/proxys.txt \
		--mode match_data \
		--match-data-type schedule \
		--leagues "ENG-Premier League" \
		--season 2025 \
		--output /tmp/test_fbref_nodriver.json \
		--verbose

# Full FBref test pipeline (curl_cffi -> nodriver fallback)
test-fbref-full:
	@echo "Running full FBref test pipeline..."
	@echo ""
	@echo "=== Step 1: Test curl_cffi ==="
	@$(MAKE) test-fbref-curl || true
	@echo ""
	@echo "=== Step 2: Test nodriver fallback ==="
	@$(MAKE) test-fbref-nodriver || true
	@echo ""
	@echo "Full FBref test pipeline completed!"

# Test proxy pool statistics
test-proxy-stats:
	@echo "Testing proxy pool statistics..."
	@docker compose exec airflow-webserver python -c "\
from scrapers.utils.proxy_manager import ProxyManager, ProxyType; \
import json; \
pm = ProxyManager(cooldown_seconds=60.0); \
pm.load_from_file_custom_format('/opt/airflow/proxys.txt'); \
print(f'Loaded {pm.total_count} proxies'); \
stats = pm.get_stats(); \
print(json.dumps({k: v for k, v in stats.items() if k != 'proxies'}, indent=2)); \
print(f'\\nFirst 5 proxies:'); \
for p in stats['proxies'][:5]: \
    print(f'  {p[\"host\"]}:{p[\"port\"]} - success_rate={p[\"success_rate\"]}'); \
"


# Re-import Superset datasets from datasources.yaml
superset-import:
	docker compose exec superset python /app/configs/superset/import_datasources.py

# Re-import Superset dashboards from dashboards/*.py
superset-dashboards:
	docker compose exec superset python /app/configs/superset/import_dashboards.py
