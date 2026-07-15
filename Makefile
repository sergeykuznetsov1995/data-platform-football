# =============================================================================
# Data Platform - Makefile
# =============================================================================

.PHONY: help build up up-lite up-full up-build down restart logs ps clean health test-trino init-storage shell-airflow shell-trino sofascore-discovery sofascore-discovery-check test-fbref-offline test-proxy-stats up-bi up-catalog down-bi down-catalog superset-init superset-import superset-dashboards om-ingest-trino om-lineage-trino om-apply-descriptions om-cleanup-lineage logs-superset logs-om shell-superset shell-om

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
	@echo "  make sofascore-discovery       - Refresh source metadata via direct JSON"
	@echo "  make sofascore-discovery-check - Check registry drift without writing"
	@echo ""
	@echo "  make shell-airflow - Open shell in Airflow webserver"
	@echo "  make shell-trino  - Open shell in Trino"
	@echo ""
	@echo "FBref Scraping Tests:"
	@echo "  make test-fbref-offline   - Run raw/parser/control/DAG tests without network"
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

# Source metadata discovery is deliberately outside the ingestion DAG. The
# normal Airflow mount stays read-only; this one-shot overrides only the
# SofaScore registry directory and runs as the checkout owner. New records are
# still enabled=false, so refreshing source metadata never activates scraping.
sofascore-discovery:
	docker compose run --rm --no-deps \
		--user "$$(id -u):$$(id -g)" \
		--volume "$(CURDIR)/configs/sofascore:/work/sofascore:rw" \
		airflow-scheduler \
		python /opt/airflow/dags/scripts/run_sofascore_discovery.py \
		--registry /work/sofascore/tournaments.json

# Metered variant of the same one-shot: the catalog fan-out that SofaScore's
# edge refuses from a datacentre egress. Residential bytes are billable, so the
# byte ceiling is mandatory and there is no default:
#   make sofascore-discovery-lease BUDGET_CAP_BYTES=6291456 \
#        DISCOVERY_ARGS="--scope full --dry-run"
# The proxy filter refuses every discovery lease until it is started with
# PROXY_FILTER_SOFASCORE_DISCOVERY_DAGRUN_BUDGET_BYTES set.
sofascore-discovery-lease:
	@test -n "$(BUDGET_CAP_BYTES)" || { \
		echo "BUDGET_CAP_BYTES=<bytes> is required: metered discovery never runs on an implicit budget"; \
		exit 1; \
	}
	docker compose run --rm --no-deps \
		--user "$$(id -u):$$(id -g)" \
		--volume "$(CURDIR)/configs/sofascore:/work/sofascore:rw" \
		airflow-scheduler \
		python /opt/airflow/dags/scripts/run_sofascore_discovery.py \
		--registry /work/sofascore/tournaments.json \
		--transport lease-proxy \
		--budget-cap-bytes $(BUDGET_CAP_BYTES) \
		$(DISCOVERY_ARGS)

sofascore-discovery-check:
	docker compose run --rm --no-deps \
		--user "$$(id -u):$$(id -g)" \
		--volume "$(CURDIR)/configs/sofascore:/work/sofascore:ro" \
		airflow-scheduler \
		python /opt/airflow/dags/scripts/run_sofascore_discovery.py \
		--registry /work/sofascore/tournaments.json \
		--check

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

# Deterministic FBref verification. Live traffic requires an explicit bounded
# canary; this target never opens the production transport.
test-fbref-offline:
	@docker compose exec airflow-scheduler bash -ec '\
		tests="$$(find tests/unit -type f -name "*fbref*.py" -print | sort)"; \
		test -n "$$tests"; \
		python -m pytest -q $$tests \
			tests/unit/dags/test_dag_iceberg_maintenance_daily.py \
			tests/unit/dags/test_maintenance_tasks.py \
			tests/unit/scrapers/test_proxy_manager.py \
			tests/unit/scrapers/test_scrapers_lazy_import.py'

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
    print(f'  {p[\"proxy\"]} - success_rate={p[\"success_rate\"]}'); \
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

# RSA-ключи подписи бот-JWT OpenMetadata (вместо публично известного дефолта
# образа) — обязательны до публикации meta (#866). См. configs/openmetadata/README.md.
gen-om-jwt-keys:
	@bash scripts/gen_om_jwt_keys.sh

# Применить auth-конфиг OM на ЖИВОМ сервере (#866): env недостаточно — OM 1.13
# хранит конфиг в БД. Значения (OM_AUTH_*, OM_ADMIN_PASSWORD) скрипт читает из
# .env сам. Сначала прогони с --dry-run (см. configs/openmetadata/README.md).
om-apply-security-config:
	@python3 scripts/om_apply_security_config.py

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
