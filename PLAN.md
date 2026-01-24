# План реализации: Data-платформа для футбольных данных

## Обзор проекта

Data-платформа на базе Medallion Architecture для сбора, обработки и анализа футбольной статистики из 9 источников (FBref, Understat, WhoScored, FotMob, Sofascore, SoFIFA, ClubElo, ESPN, MatchHistory).

**Технологический стек:**
- HDFS — распределённое хранилище
- Spark — обработка и трансформация данных
- Airflow — оркестрация пайплайнов
- Trino — SQL-движок для аналитики
- Hive Metastore — каталог метаданных
- Docker — контейнеризация (современный `compose.yaml` формат)

---

## Доступные агенты

| Агент | Специализация | Цвет |
|-------|---------------|------|
| **docker-expert** | Docker, compose.yaml, контейнеризация | pink |
| **airflow-expert** | Airflow DAGs, операторы, оркестрация | orange |
| **web-scraping-expert** | Парсинг данных, обход Cloudflare | red |
| **spark-data-engineer** | PySpark jobs, DataFrame, оптимизация | green |
| **trino-specialist** | Trino SQL, коннекторы, оптимизация запросов | yellow |
| **data-platform-architect** | Архитектура, проектирование данных | cyan |
| **data-quality-agent** | Валидация данных, Great Expectations | blue |
| **testing-agent** | Unit/Integration/E2E тесты, pytest | red |
| **code-simplifier** | Рефакторинг, улучшение кода | - |
| **platform-planner** | Планирование, декомпозиция задач | purple |

---

## Фазы реализации

---

### Фаза 1: Инфраструктура (MVP)

**Основной агент:** docker-expert

| ID | Задача | Агент | Сложность | Зависимости |
|----|--------|-------|-----------|-------------|
| TASK-001 | Создать структуру проекта и `compose.yaml` базу | docker-expert | S | - |
| TASK-002 | Сервис HDFS (Namenode + Datanodes) | docker-expert | M | TASK-001 |
| TASK-003 | Сервис PostgreSQL (Airflow + Hive Metastore) | docker-expert | S | TASK-001 |
| TASK-004 | Сервис Hive Metastore | docker-expert | M | TASK-002, TASK-003 |
| TASK-005 | Сервис Spark (Master + Workers) | docker-expert | M | TASK-002, TASK-004 |
| TASK-006 | Сервис Airflow (Webserver, Scheduler, Workers) | docker-expert | L | TASK-003 |
| TASK-007 | Сервис Trino (Coordinator + Worker) | docker-expert + trino-specialist | M | TASK-004 |
| TASK-008 | Интеграция: единый `compose.yaml` + Makefile | docker-expert | L | TASK-002..007 |

**Важно:** Используется современный синтаксис Docker Compose:
- Файл: `compose.yaml` (не `docker-compose.yml`)
- Команда: `docker compose` (не `docker-compose`)
- Healthcheck с `depends_on: condition: service_healthy`

**Критерии проверки Фазы 1:**
```bash
# Запуск платформы
docker compose up -d

# Проверка healthcheck всех сервисов
docker compose ps --format "table {{.Name}}\t{{.Status}}"
# Все сервисы должны быть "healthy"

# Проверка Web UI
curl -s http://localhost:9870  # HDFS NameNode
curl -s http://localhost:8080  # Spark Master
curl -s http://localhost:8081  # Airflow
curl -s http://localhost:8082  # Trino

# Тестовый Spark job пишет в HDFS
docker compose exec spark-master spark-submit --master spark://spark-master:7077 /test/write_test.py

# Данные видны в Trino
docker compose exec trino trino --execute "SELECT * FROM hive.default.test_table"
```

---

### Фаза 2: Scrapers (Ingestion Layer)

**Основной агент:** web-scraping-expert

| ID | Задача | Агент | Сложность | Зависимости |
|----|--------|-------|-----------|-------------|
| TASK-009 | Базовый wrapper: soccerdata + Cloudflare bypass | web-scraping-expert | L | TASK-008 |
| TASK-010 | Scraper для FBref (10 методов) | web-scraping-expert | M | TASK-009 |
| TASK-011 | Scraper для Understat (7 методов) | web-scraping-expert | M | TASK-009 |
| TASK-012 | Scraper для WhoScored (SPADL events) | web-scraping-expert | L | TASK-009 |
| TASK-013 | Scraper для FotMob | web-scraping-expert | M | TASK-009 |
| TASK-014 | Scraper для Sofascore | web-scraping-expert | M | TASK-009 |
| TASK-015 | Scraper для SoFIFA (FIFA атрибуты) | web-scraping-expert | M | TASK-009 |
| TASK-016 | Scraper для ClubElo | web-scraping-expert | S | TASK-009 |
| TASK-017 | Scraper для ESPN | web-scraping-expert | M | TASK-009 |
| TASK-018 | Scraper для MatchHistory | web-scraping-expert | S | TASK-009 |
| TASK-019 | Rate limiter + retry + circuit breaker | web-scraping-expert | M | TASK-009 |
| TASK-020 | Unit тесты для scrapers | testing-agent | M | TASK-010..018 |

**Критерии проверки Фазы 2:**
```bash
# Cloudflare bypass работает
python -c "from scrapers.base_scraper import CloudflareBypass; print(CloudflareBypass().test())"
# Output: "Cloudflare bypassed successfully"

# FBref scraper
python -c "from scrapers.fbref import FBrefScraper; print(FBrefScraper().scrape_schedule('ENG-Premier League', '2024-2025').shape)"
# Output: (380, 15)  # 380 матчей, 15 колонок

# Данные в HDFS Bronze layer
hdfs dfs -ls /data/bronze/fbref/schedule/2024/01/
# Found 1 items: schedule_2024-01-24.parquet

# Rate limiter работает
python -c "from scrapers.utils import RateLimiter; rl = RateLimiter(5); [rl.acquire() for _ in range(10)]"
# 5 запросов сразу, остальные с задержкой

# Тесты проходят
pytest tests/unit/scrapers/ -v
# 50+ тестов passed
```

---

### Фаза 3: Airflow DAGs (Orchestration)

**Основной агент:** airflow-expert

| ID | Задача | Агент | Сложность | Зависимости |
|----|--------|-------|-----------|-------------|
| TASK-021 | Airflow utilities: connections, notifications, config | airflow-expert | M | TASK-006 |
| TASK-022 | DAG: dag_ingest_fbref (daily) | airflow-expert | M | TASK-010, TASK-021 |
| TASK-023 | DAG: dag_ingest_understat (daily) | airflow-expert | M | TASK-011, TASK-021 |
| TASK-024 | DAG: dag_ingest_whoscored (daily) | airflow-expert | M | TASK-012, TASK-021 |
| TASK-025 | DAG: dag_ingest_fotmob (daily) | airflow-expert | M | TASK-013, TASK-021 |
| TASK-026 | DAG: dag_ingest_sofascore (daily) | airflow-expert | S | TASK-014, TASK-021 |
| TASK-027 | DAG: dag_ingest_sofifa (weekly) | airflow-expert | S | TASK-015, TASK-021 |
| TASK-028 | DAG: dag_ingest_clubelo (daily) | airflow-expert | S | TASK-016, TASK-021 |
| TASK-029 | DAG: dag_ingest_espn (daily) | airflow-expert | S | TASK-017, TASK-021 |
| TASK-030 | DAG: dag_ingest_matchhistory (daily) | airflow-expert | S | TASK-018, TASK-021 |
| TASK-031 | DAG: dag_master_pipeline (координатор) | airflow-expert | L | TASK-022..030 |
| TASK-032 | Integration тесты DAGs | testing-agent | M | TASK-022..031 |

**Критерии проверки Фазы 3:**
```bash
# DAG парсится без ошибок
docker compose exec airflow-scheduler airflow dags list
# dag_ingest_fbref, dag_ingest_understat, ... dag_master_pipeline

# Нет import errors
docker compose exec airflow-scheduler airflow dags list-import-errors
# No data found

# Ручной запуск DAG
docker compose exec airflow-scheduler airflow dags trigger dag_ingest_fbref
# Triggered dag_ingest_fbref

# Проверка статуса
docker compose exec airflow-scheduler airflow dags state dag_ingest_fbref $(date +%Y-%m-%d)
# success

# Тесты DAGs
pytest tests/integration/dags/ -v
# All passed
```

---

### Фаза 4: Silver Layer (Transformations)

**Основной агент:** spark-data-engineer

| ID | Задача | Агент | Сложность | Зависимости |
|----|--------|-------|-----------|-------------|
| TASK-033 | Spark job: dim_teams (Entity Resolution) | spark-data-engineer | L | TASK-008 |
| TASK-034 | Spark job: dim_players (Entity Resolution) | spark-data-engineer | L | TASK-033 |
| TASK-035 | Spark job: dim_matches | spark-data-engineer | M | TASK-033 |
| TASK-036 | Spark job: dim_leagues | spark-data-engineer | S | TASK-008 |
| TASK-037 | Spark job: fact_match_stats | spark-data-engineer | L | TASK-033, TASK-035 |
| TASK-038 | Spark job: fact_player_season_stats | spark-data-engineer | M | TASK-034 |
| TASK-039 | Spark job: fact_player_match_stats | spark-data-engineer | M | TASK-034, TASK-035 |
| TASK-040 | Spark job: fact_shot_events | spark-data-engineer | M | TASK-034, TASK-035 |
| TASK-041 | Spark job: fact_team_elo | spark-data-engineer | S | TASK-033 |
| TASK-042 | DAG: dag_bronze_to_silver | airflow-expert | L | TASK-033..041 |
| TASK-043 | Unit тесты Spark jobs (Silver) | testing-agent | M | TASK-033..041 |

**Критерии проверки Фазы 4:**
```bash
# Entity Resolution: один ID для команды из всех источников
docker compose exec trino trino --execute "
  SELECT team_id, team_name, name_fbref, name_understat, name_whoscored
  FROM hive.silver.dim_teams
  WHERE team_name = 'Manchester United'
"
# t001 | Manchester United | Manchester Utd | Manchester United | Man United

# Нет дублей в dimensions
docker compose exec trino trino --execute "
  SELECT team_id, COUNT(*)
  FROM hive.silver.dim_teams
  GROUP BY team_id
  HAVING COUNT(*) > 1
"
# No rows returned

# Foreign keys валидны
docker compose exec trino trino --execute "
  SELECT COUNT(*) FROM hive.silver.fact_match_stats f
  LEFT JOIN hive.silver.dim_teams t ON f.home_team_id = t.team_id
  WHERE t.team_id IS NULL
"
# 0

# Spark job выполняется без ошибок
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /spark_jobs/silver/dim_teams.py
# Job completed successfully
```

---

### Фаза 5: Gold Layer (Analytics)

**Основной агент:** spark-data-engineer

| ID | Задача | Агент | Сложность | Зависимости |
|----|--------|-------|-----------|-------------|
| TASK-044 | Spark job: team_performance_summary | spark-data-engineer | M | TASK-042 |
| TASK-045 | Spark job: player_performance_summary | spark-data-engineer | M | TASK-042 |
| TASK-046 | Spark job: league_standings | spark-data-engineer | S | TASK-042 |
| TASK-047 | Spark job: head_to_head | spark-data-engineer | M | TASK-042 |
| TASK-048 | Spark job: team_xg_trends | spark-data-engineer | M | TASK-042 |
| TASK-049 | Spark job: player_xg_performance | spark-data-engineer | M | TASK-042 |
| TASK-050 | Spark job: shot_quality_analysis | spark-data-engineer | M | TASK-042 |
| TASK-051 | Spark job: match_features (ML) | spark-data-engineer | L | TASK-044, TASK-047, TASK-048 |
| TASK-052 | DAG: dag_silver_to_gold | airflow-expert | M | TASK-044..051 |
| TASK-053 | Unit тесты Spark jobs (Gold) | testing-agent | M | TASK-044..051 |

**Критерии проверки Фазы 5:**
```bash
# Метрики корректны
docker compose exec trino trino --execute "
  SELECT team_name, wins + draws + losses AS calculated, matches
  FROM hive.gold.team_performance_summary
  WHERE calculated != matches
"
# No rows returned (все совпадает)

# Form рассчитывается правильно (последние 5 матчей)
docker compose exec trino trino --execute "
  SELECT team_name, form
  FROM hive.gold.team_performance_summary
  WHERE league = 'ENG-Premier League'
  LIMIT 5
"
# WDWLW, WWWDL, ...

# ML features без data leakage
docker compose exec trino trino --execute "
  SELECT COUNT(*) FROM hive.gold.match_features
  WHERE match_date <= feature_calculation_date
"
# 0 (все features из прошлых данных)
```

---

### Фаза 6: Data Quality & Monitoring

**Основные агенты:** data-quality-agent, airflow-expert

| ID | Задача | Агент | Сложность | Зависимости |
|----|--------|-------|-----------|-------------|
| TASK-054 | Great Expectations: Bronze suites | data-quality-agent | M | TASK-022..030 |
| TASK-055 | Great Expectations: Silver suites | data-quality-agent | M | TASK-042 |
| TASK-056 | Cross-source consistency checks | data-quality-agent | M | TASK-055 |
| TASK-057 | DAG: dag_data_quality | airflow-expert | M | TASK-054..056 |
| TASK-058 | Alerting: Slack/Email notifications | airflow-expert | S | TASK-057 |
| TASK-059 | DAG: dag_compaction (weekly) | airflow-expert | M | TASK-008 |

**Критерии проверки Фазы 6:**
```bash
# Great Expectations validation
python -c "
from great_expectations import get_context
context = get_context()
result = context.run_checkpoint('bronze_fbref_checkpoint')
print(result.success)
"
# True

# Cross-source consistency: xG diff <= 0.5
docker compose exec trino trino --execute "
  SELECT AVG(ABS(fbref_xg - understat_xg)) as avg_diff
  FROM hive.silver.fact_match_stats
  WHERE fbref_xg IS NOT NULL AND understat_xg IS NOT NULL
"
# 0.23 (меньше 0.5 - OK)

# Alert при ошибке
docker compose exec airflow-scheduler airflow dags trigger dag_data_quality --conf '{"test_mode": true}'
# Alert sent to Slack
```

---

### Фаза 7: Trino Optimization & Views

**Основной агент:** trino-specialist

| ID | Задача | Агент | Сложность | Зависимости |
|----|--------|-------|-----------|-------------|
| TASK-060 | Trino catalogs: hive.bronze, hive.silver, hive.gold | trino-specialist | M | TASK-007 |
| TASK-061 | Аналитические views для удобного доступа | trino-specialist | M | TASK-052 |
| TASK-062 | Resource groups и query limits | trino-specialist | S | TASK-060 |
| TASK-063 | Оптимизация: статистика таблиц | trino-specialist | M | TASK-060 |

**Критерии проверки Фазы 7:**
```bash
# Catalogs настроены
docker compose exec trino trino --execute "SHOW CATALOGS"
# hive, system

# Schemas доступны
docker compose exec trino trino --execute "SHOW SCHEMAS FROM hive"
# bronze, silver, gold

# Views работают
docker compose exec trino trino --execute "
  SELECT * FROM hive.gold.v_premier_league_standings
  WHERE season = '2024-2025'
"
# Турнирная таблица

# Query < 30 секунд
time docker compose exec trino trino --execute "
  SELECT * FROM hive.gold.team_performance_summary
  WHERE league = 'ENG-Premier League'
"
# real 0m2.5s
```

---

### Фаза 8: Документация и финализация

**Агенты:** все

| ID | Задача | Агент | Сложность | Зависимости |
|----|--------|-------|-----------|-------------|
| TASK-064 | README: инструкция развёртывания | docker-expert | S | TASK-063 |
| TASK-065 | Документация API и схем данных | data-platform-architect | M | TASK-052 |
| TASK-066 | Runbook: операционные процедуры | data-platform-architect | S | TASK-059 |
| TASK-067 | Рефакторинг и code review | code-simplifier | M | TASK-063 |
| TASK-068 | End-to-end тестирование | testing-agent | L | TASK-063 |

**Критерии проверки Фазы 8:**
```bash
# README позволяет развернуть с нуля
git clone ... && cd data_platform
make up
# Платформа запущена за < 10 минут

# E2E тест проходит
pytest tests/e2e/ -v --tb=short
# All passed

# Code quality
ruff check . && mypy . && black --check .
# All passed
```

---

## Сводная таблица: 68 задач, 10 агентов

| Фаза | Задачи | Основные агенты | Кол-во |
|------|--------|-----------------|--------|
| 1. Инфраструктура | TASK-001..008 | docker-expert, trino-specialist | 8 |
| 2. Scrapers | TASK-009..020 | web-scraping-expert, testing-agent | 12 |
| 3. Airflow DAGs | TASK-021..032 | airflow-expert, testing-agent | 12 |
| 4. Silver Layer | TASK-033..043 | spark-data-engineer, airflow-expert, testing-agent | 11 |
| 5. Gold Layer | TASK-044..053 | spark-data-engineer, airflow-expert, testing-agent | 10 |
| 6. Data Quality | TASK-054..059 | data-quality-agent, airflow-expert | 6 |
| 7. Trino | TASK-060..063 | trino-specialist | 4 |
| 8. Документация | TASK-064..068 | все | 5 |

**Итого:** 68 задач

---

## Назначение агентов по задачам

| Агент | Задачи | Кол-во |
|-------|--------|--------|
| docker-expert | TASK-001..008, TASK-064 | 9 |
| web-scraping-expert | TASK-009..019 | 11 |
| airflow-expert | TASK-021..032, TASK-042, TASK-052, TASK-057..059 | 17 |
| spark-data-engineer | TASK-033..041, TASK-044..051 | 17 |
| trino-specialist | TASK-007, TASK-060..063 | 5 |
| data-quality-agent | TASK-054..056 | 3 |
| testing-agent | TASK-020, TASK-032, TASK-043, TASK-053, TASK-068 | 5 |
| data-platform-architect | TASK-065, TASK-066 | 2 |
| code-simplifier | TASK-067 | 1 |

---

## Риски и митигации

| Риск | Вероятность | Митигация |
|------|-------------|-----------|
| Cloudflare блокировки | Высокая | undetected-chromedriver, proxy rotation, cloudscraper |
| Entity Resolution ошибки | Средняя | Fuzzy matching + manual overrides в config |
| Изменение структуры источников | Высокая | Schema evolution, версионирование scrapers |
| Spark OOM | Средняя | Adaptive Query Execution, partitioning |
| Rate limiting | Высокая | Token bucket, exponential backoff |

---

## Критерии успеха

- [ ] Данные из всех 9 источников загружаются ежедневно
- [ ] Bronze → Silver → Gold работает автоматически
- [ ] SQL-запросы через Trino < 30 секунд
- [ ] Data freshness < 24 часов
- [ ] Data quality checks > 95%
- [ ] Entity Resolution accuracy > 95%
- [ ] `docker compose up` поднимает платформу за < 10 минут
- [ ] Все тесты проходят: unit, integration, e2e

---

## E2E Verification

```bash
# 1. Запуск платформы
make up
# Ожидание: все контейнеры healthy

# 2. Проверка веб-интерфейсов
curl -f http://localhost:9870  # HDFS
curl -f http://localhost:8080  # Spark
curl -f http://localhost:8081  # Airflow
curl -f http://localhost:8082  # Trino

# 3. Запуск полного pipeline
docker compose exec airflow-scheduler \
  airflow dags trigger dag_master_pipeline

# 4. Ожидание завершения (мониторинг в Airflow UI)

# 5. Проверка данных в Gold layer
docker compose exec trino trino --execute "
  SELECT COUNT(*) FROM hive.gold.team_performance_summary
"
# > 0

# 6. Проверка качества данных
docker compose exec airflow-scheduler \
  airflow dags trigger dag_data_quality

# 7. Остановка
make down
```

---

## Следующий шаг

**Начать Фазу 1:** TASK-001 с агентом docker-expert
- Создать структуру проекта
- Настроить `compose.yaml` с современным синтаксисом
