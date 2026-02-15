# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Обзор проекта

Data-платформа для сбора, обработки и анализа футбольной статистики на базе Medallion Architecture (Bronze → Silver → Gold).

**Стек технологий:**
- HDFS — распределённое хранилище
- Spark 3.5 — обработка данных
- Airflow 2.10 — оркестрация пайплайнов
- Trino — SQL-движок для аналитики
- Hive Metastore — каталог метаданных
- Apache Iceberg — формат таблиц

## Команды

```bash
# Запуск платформы
make up                  # запуск всех сервисов
make up-build           # сборка и запуск
make down               # остановка
make ps                 # статус сервисов
make health             # проверка health всех сервисов

# Инициализация
make init-hdfs          # создание директорий Medallion в HDFS

# Тестирование инфраструктуры
make test-spark         # тест Spark job
make test-trino         # тест Trino connectivity

# Тесты Python
pytest tests/unit/                              # unit тесты
pytest tests/unit/scrapers/test_fbref_scraper.py  # один файл
pytest tests/integration/ -m integration        # интеграционные
pytest -m "not slow and not cloudflare"         # без медленных/selenium тестов
pytest --cov=scrapers tests/unit/               # с coverage

# Shell доступ
make shell-spark        # spark-master
make shell-airflow      # airflow-webserver
make shell-trino        # trino
make shell-namenode     # HDFS namenode
```

## Web UI (после `make up`)

| Сервис | URL | Credentials |
|--------|-----|-------------|
| HDFS NameNode | http://localhost:9870 | — |
| Spark Master | http://localhost:8080 | — |
| Airflow | http://localhost:8081 | admin/admin |
| Trino | http://localhost:8082 | — |

## Архитектура

### Структура проекта

```
data_platform/
├── compose.yaml           # Docker Compose (современный синтаксис)
├── Makefile               # команды управления
├── scrapers/              # парсеры данных
│   ├── base/              # BaseScraper, CloudflareBypass, IcebergWriter
│   ├── utils/             # RateLimiter, RetryPolicy, CircuitBreaker
│   ├── schemas/           # PyArrow схемы таблиц
│   ├── sources/           # конфиги источников (sources.yaml, leagues.yaml)
│   └── *_scraper.py       # конкретные скраперы
├── dags/                  # Airflow DAGs
├── spark_jobs/            # PySpark трансформации
├── tests/
│   ├── unit/              # моки, без сети
│   └── integration/       # реальные запросы
├── configs/               # конфигурации сервисов
│   ├── hdfs/              # core-site.xml, hdfs-site.xml
│   ├── hive/              # hive-site.xml
│   ├── spark/             # spark-defaults.conf
│   └── trino/             # catalogs, node.properties
└── docker/images/         # Dockerfile для каждого сервиса
```

### Medallion Architecture

- **Bronze** — сырые данные из источников (append-only, immutable)
- **Silver** — нормализованные данные, Entity Resolution, dimensions/facts
- **Gold** — агрегированные данные для аналитики и ML

Структура HDFS:
```
/data/bronze/{источник}/{сущность}/{year}/{month}/{day}/
/data/silver/{домен}/{сущность}/league={лига}/season={сезон}/
/data/gold/{use_case}/league={лига}/
```

### Scrapers

Иерархия классов:
- `BaseScraper` — rate limiting, retry, circuit breaker, proxy rotation, Iceberg writer
- `SoccerdataScraper(BaseScraper)` — обёртка над soccerdata library
- `SeleniumScraper(BaseScraper)` — Cloudflare bypass (два режима: undetected-chromedriver или FlareSolverr)

9 источников данных: FBref, Understat, WhoScored, FotMob, Sofascore, SoFIFA, ClubElo, ESPN, MatchHistory

Конфиги источников: `scrapers/sources/sources.yaml`
Конфиг лиг и сезонов для DAG: `dags/utils/config.py`

### pytest markers

```python
@pytest.mark.unit         # быстрые изолированные тесты без внешних зависимостей
@pytest.mark.integration  # реальные HTTP запросы к внешним источникам
@pytest.mark.slow         # тесты >10 секунд
@pytest.mark.cloudflare   # требуют Selenium для Cloudflare bypass
@pytest.mark.flaky        # могут падать из-за внешних сервисов
@pytest.mark.tor          # требуют Tor proxy на порту 9050
@pytest.mark.soccerdata   # требуют библиотеку soccerdata
```

### DAGs

DAG'и используют `BashOperator` для запуска скраперов в изолированном subprocess (избегая проблем с памятью LocalExecutor). Результаты сохраняются в JSON-файл и валидируются последующим `PythonOperator`.

Расписание DAG'ов настраивается в `dags/utils/config.py` (SCHEDULES dict).

## Важные соглашения

- Используется современный Docker Compose: файл `compose.yaml`, команда `docker compose`
- Все скраперы наследуются от `BaseScraper` и используют общие утилиты (rate limiter, circuit breaker, proxy manager)
- Данные записываются в Iceberg через `IcebergWriter` (с Parquet fallback)
- DAG'и используют `BashOperator` для изоляции процессов скраперов
- Переменные окружения в `.env` файле (не в репозитории)
- Прокси загружаются из `proxys.txt` (формат: `host:port:user:pass`)

## Cloudflare Bypass (FBref)

**ВАЖНО:** 2captcha и другие платные CAPTCHA-сервисы **НЕ используются** в этом проекте.

Бесплатное решение для обхода Cloudflare на FBref:

1. **curl_cffi** с Chrome 120 TLS fingerprint (патч для soccerdata)
2. **Резидентские прокси** из файла `proxys.txt`
3. **Sticky sessions** + human-like delays между запросами
4. **nodriver fallback** для сложных случаев

Конфигурация в `dags/dag_ingest_fbref.py`:
```python
DEFAULT_SCRAPER_TYPE = 'soccerdata'  # HTTP-based (не Selenium)
HEADLESS = True
USE_NODRIVER = True  # nodriver fallback
NODRIVER_CLOUDFLARE_WAIT = 90.0
```

Документация: `docs/fbref_cloudflare_bypass.md`

## Основные зависимости

- Python: pandas, pyarrow, pyspark, soccerdata, selenium, undetected-chromedriver
- Resilience: tenacity, pybreaker, PySocks
- Iceberg: pyiceberg[hive]
- Cloudflare bypass: curl_cffi, nodriver
