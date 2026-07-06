# =============================================================================
# Data Platform - Makefile
# =============================================================================

.PHONY: help build up up-lite up-full up-build down restart logs ps clean health test-trino init-storage shell-airflow shell-trino test-fbref-curl test-fbref-nodriver test-fbref-full test-proxy-stats up-bi up-catalog down-bi down-catalog superset-init superset-import superset-dashboards om-ingest-trino om-lineage-trino om-apply-descriptions om-cleanup-lineage logs-superset logs-om shell-superset shell-om

# Default target
help:
	@echo "Data Platform Commands:"
	@echo ""
	@echo "  make build        - Build all Docker images"
	@echo "  make up           - Start ALL services (core + heavy: OM, ES, superset-worker/beat, tor)"
	@echo "  make up-lite      - Start CORE only (saves ~4GB RAM; recommended on 11GB VM)"
	@echo "  make up-full      - Alias for 'make up' (all services)"
	@echo "  make down         - Stop all services"
	@echo "  make restart      - Restart all services"
	@echo "  make logs         - Show logs (use SERVICE=name for specific)"
	@echo "  make ps           - Show service status"
	@echo "  make clean        - Remove all containers and volumes"
	@echo "  make health       - Check health of all services"
	@echo ""
	@echo "  make init-storage - Initialize HDFS + Hive schemas (recommended)"
	@echo "  make test-trino   - Run Trino integration test"
	@echo ""
	@echo "  make shell-airflow - Open shell in Airflow webserver"
	@echo "  make shell-trino  - Open shell in Trino"
	@echo ""
	@echo "FBref Scraping Tests:"
	@echo "  make test-fbref-curl      - Test curl_cffi with residential proxy"
	@echo "  make test-fbref-nodriver  - Test nodriver (browser) fallback"
	@echo "  make test-fbref-full      - Run full test pipeline"
	@echo "  make test-proxy-stats     - Show proxy pool statistics"
	@echo ""
	@echo "BI / Catalog:"
	@echo "  make up-bi                - Start Superset stack (web + worker + beat)"
	@echo "  make up-catalog           - Start OpenMetadata stack (server + ingestion + ES)"
	@echo "  make down-bi              - Stop Superset stack"
	@echo "  make down-catalog         - Stop OpenMetadata stack"
	@echo "  make superset-init        - Bootstrap Superset (admin + datasources + dashboards)"
	@echo "  make superset-import      - Import only datasources (no admin/db upgrade)"
	@echo "  make om-ingest-trino      - Run OpenMetadata Trino schema ingestion"
	@echo "  make om-lineage-trino     - Run OpenMetadata Trino lineage workflow"
	@echo "  make om-apply-descriptions- Apply YAML table descriptions to OpenMetadata"
	@echo "  make logs-superset        - Tail Superset web logs"
	@echo "  make logs-om              - Tail OpenMetadata server logs"
	@echo "  make shell-superset       - Open shell in Superset container"
	@echo "  make shell-om             - Open shell in OpenMetadata server container"

# Build images
build:
	docker compose build

# Start services (FULL: core + heavy profile = OM, ES, superset-worker/beat, tor)
up:
	docker compose --profile heavy up -d
	@echo ""
	@echo "Waiting for services to start..."
	@sleep 10
	@$(MAKE) ps

# Start services with build (FULL)
up-build:
	docker compose --profile heavy up -d --build
	@echo ""
	@echo "Waiting for services to start..."
	@sleep 10
	@$(MAKE) ps

# Start LITE stack (core only: SeaweedFS + Lakekeeper + Postgres + Redis + Airflow + Trino + Superset + FlareSolverr).
# Skips: superset-worker/beat, elasticsearch, openmetadata-*, tor. Use when RAM-constrained.
up-lite:
	docker compose up -d
	@echo ""
	@echo "Waiting for services to start..."
	@sleep 10
	@$(MAKE) ps

# Alias for clarity
up-full: up

# Stop services (includes heavy profile so nothing is left orphaned)
down:
	docker compose --profile heavy down

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

# Initialize storage (HDFS + Hive schemas via Python)
init-storage:
	@echo "Initializing storage (HDFS directories + Hive schemas)..."
	docker compose exec airflow-scheduler python /opt/airflow/scripts/init_storage.py

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
	@echo "  Superset:       http://localhost:8088"
	@echo "  OpenMetadata:   http://localhost:8585"

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

# =============================================================================
# BI / Catalog (Superset + OpenMetadata)
# =============================================================================

# Start Superset stack (web + worker + beat)
up-bi:
	@docker compose up -d superset superset-worker superset-beat

# Start OpenMetadata stack (server + ingestion + Elasticsearch)
up-catalog:
	@docker compose up -d elasticsearch openmetadata-server openmetadata-ingestion

# Stop Superset stack
down-bi:
	@docker compose stop superset superset-worker superset-beat

# Stop OpenMetadata stack
down-catalog:
	@docker compose stop openmetadata-server openmetadata-ingestion elasticsearch

# Bootstrap Superset (creates admin, runs db upgrade, imports datasources/dashboards)
superset-init:
	@docker compose exec superset bash /app/pythonpath/bootstrap.sh

# Import only datasources (skip admin/db upgrade)
superset-import:
	@docker compose exec superset python /app/pythonpath/import_datasources.py

# E7: Import declarative dashboards from configs/superset/dashboards/*.py
# (#496: оркестратор живёт в dashboards/ — top-level копия не монтировалась)
superset-dashboards:
	@docker compose exec superset python /app/pythonpath/dashboards/import_dashboards.py

# Run OpenMetadata Trino schema ingestion workflow
om-ingest-trino:
	@docker compose exec openmetadata-ingestion metadata ingest -c /opt/configs/trino_ingestion.yaml

# Run OpenMetadata Trino lineage workflow
om-lineage-trino:
	@docker compose exec openmetadata-ingestion metadata ingest -c /opt/configs/trino_lineage.yaml

# Apply YAML-based table descriptions to OpenMetadata
om-apply-descriptions:
	@docker compose exec openmetadata-ingestion python /opt/configs/apply_descriptions.py

# Bootstrap OpenMetadata classifications (Tier / Domain / PII / UseCase) — idempotent.
# Reads JWT from OPENMETADATA_JWT_TOKEN, falling back to OM_JWT_TOKEN (already in compose env).
om-bootstrap:
	@docker compose exec openmetadata-ingestion python /opt/configs/bootstrap_classifications.py

# Hard-delete OM entities of tables dropped in epic #478 → cascades to remove their stale lineage edges (#529).
# Default = dry-run preview; real delete needs --apply (see configs/openmetadata/README.md). Reads OM_JWT_TOKEN.
om-cleanup-lineage:
	@docker compose exec openmetadata-ingestion python /opt/configs/cleanup_lineage.py

# Tail Superset web logs
logs-superset:
	@docker compose logs -f --tail=100 superset

# Tail OpenMetadata server logs
logs-om:
	@docker compose logs -f --tail=100 openmetadata-server

# Open shell in Superset container
shell-superset:
	@docker compose exec superset bash

# Open shell in OpenMetadata server container
shell-om:
	@docker compose exec openmetadata-server bash


# --- Analyst access (docs/design/analyst-access.md) ---

# Рендер realm-импорта Keycloak из шаблона (секреты из .env)
render-keycloak-realm:
	@python3 scripts/render_keycloak_realm.py

# Создать базу keycloak в работающем postgres (идемпотентно)
keycloak-db:
	@bash scripts/create_keycloak_db.sh

# Tail Keycloak logs
logs-keycloak:
	@docker compose logs -f --tail=100 keycloak

# Собрать образы JupyterHub и ноутбука аналитика
build-jupyter:
	@docker build -t data-platform/jupyterhub:5.3 docker/images/jupyterhub
	@docker build -t data-platform/jupyter-singleuser:latest docker/images/jupyter-singleuser

# Tail JupyterHub logs
logs-jupyterhub:
	@docker compose logs -f --tail=100 jupyterhub

# Рендер конфига Headscale из шаблона (PLATFORM_DOMAIN и секрет из .env)
render-headscale-config:
	@PLATFORM_DOMAIN=$$(grep '^PLATFORM_DOMAIN=' .env | cut -d= -f2-); \
	SECRET=$$(grep '^HEADSCALE_OIDC_CLIENT_SECRET=' .env | cut -d= -f2-); \
	if [ -z "$$PLATFORM_DOMAIN" ] || [ -z "$$SECRET" ]; then \
		echo "ERROR: PLATFORM_DOMAIN / HEADSCALE_OIDC_CLIENT_SECRET не заданы в .env" >&2; exit 1; \
	fi; \
	sed -e "s|__PLATFORM_DOMAIN__|$$PLATFORM_DOMAIN|g" \
	    -e "s|__HEADSCALE_OIDC_CLIENT_SECRET__|$$SECRET|g" \
	    configs/headscale/config.yaml.example > configs/headscale/config.yaml && \
	echo "OK: configs/headscale/config.yaml"

# Tail Headscale logs
logs-headscale:
	@docker compose logs -f --tail=100 headscale
