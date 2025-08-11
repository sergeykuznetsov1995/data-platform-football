# Архитектура платформы данных (футбол)

## 1) Цели и требования
- Use‑cases: аналитика (SQL), ML (Spark), дашборды (BI через Trino).
- Источники: FBref, Transfermarkt, SoFIFA, Understat, WhoScored, StatsBomb Open Data (+ расширяемо).
- Обновления: разная периодичность (неделя/месяц).
- Развёртывание: одна VM, Docker/Compose; масштабируемость по мере роста.

---

## 2) Высокоуровневая схема
**Data Lake/Lakehouse на HDFS** + **Hive Metastore** + **Spark** (ETL/ML) + **Trino** (SQL/BI) + **Airflow** (оркестрация).

```
[Parsers/Loaders] -> [HDFS: raw] -> [Spark ETL] -> [HDFS: silver] -> [Spark/SQL] -> [HDFS: gold]
                                     |                                   |
                                  [Hive Metastore] <------------------ [Trino SQL/BI]
                                     ^
                                  [Airflow DAGs]
```

---

## 3) Развёртывание (минимум)
- Docker‑контейнеры: `hdfs-namenode`, `hdfs-datanode`, `hive-metastore`(+DB), `trino`, `spark-master`/`spark-worker` (или `spark-submit`), `airflow-web`/`airflow-scheduler`(LocalExecutor).
- Сетевые тома: `/data/hdfs`, `/data/metastore-db`, `/opt/airflow/dags`, `/opt/airflow/logs`.
- Резервные копии: дампы БД метаданных + снапшоты каталогов `raw/silver/gold`.

---

## 4) Слои данных и директории HDFS
```
/data/raw/<source>/<entity>/ingest_date=YYYY-MM-DD/*.json|html|csv
/data/silver/<domain>/<table>/partition_keys.../*.parquet
/data/gold/<mart>/<table>/partition_keys.../*.parquet
```
- **raw**: неизменённые выгрузки (idempotent append).
- **silver**: очищено/нормализовано, единая схема, типы, единицы измерения, timezone.
- **gold**: витрины/агрегации под BI/ML.

**Партиционирование (рекоменд.):**
- `matches`: `league`, `season`, `match_date` (by day).
- `events` (StatsBomb): `season`, `competition`, `match_id` (bucketing/сегментация по `match_id`).
- `players_stats`: `season`, `league`, `team_id`.
- Использовать **Parquet** (+ ZSTD/Snappy), **ORC** по необходимости; `sorted by` для частых фильтров.

---

## 5) Модель данных (ключевые таблицы)

### Справочники (единственная точка истины)
```sql
-- Игроки
CREATE TABLE dim_player (
  player_id        BIGINT,                -- внутренний стабильный ID
  full_name_canon  STRING,                -- канонизированное имя
  birth_date       DATE,
  country_code     STRING,
  height_cm        INT,
  foot             STRING,                -- L/R/Both, если доступно
  -- исходные IDs
  fbref_id         STRING,
  transfermarkt_id STRING,
  sofifa_id        STRING,
  understat_id     STRING,
  whoscored_id     STRING,
  statsbomb_id     STRING,
  -- технические
  valid_from       TIMESTAMP,
  valid_to         TIMESTAMP
)
PARTITIONED BY (snapshot_date DATE);

-- Команды
CREATE TABLE dim_team (
  team_id          BIGINT,
  team_name_canon  STRING,
  country_code     STRING,
  fbref_id         STRING,
  transfermarkt_id STRING,
  whoscored_id     STRING,
  understat_id     STRING,
  statsbomb_id     STRING,
  valid_from       TIMESTAMP,
  valid_to         TIMESTAMP
)
PARTITIONED BY (snapshot_date DATE);
```

### Факт‑таблицы (примеры)
```sql
-- Матчи (консолидировано)
CREATE TABLE fact_match (
  match_id         BIGINT,         -- внутренний
  ext_match_key    STRING,         -- ключ источника (для трассируемости)
  season           STRING,
  league           STRING,
  stage            STRING,
  match_date       DATE,
  home_team_id     BIGINT,
  away_team_id     BIGINT,
  home_goals       INT,
  away_goals       INT,
  venue            STRING,
  source           STRING,         -- fbref|whoscored|statsbomb|...
  ingest_ts        TIMESTAMP
)
PARTITIONED BY (season STRING, league STRING);

-- События (StatsBomb, нормализованные координаты)
CREATE TABLE fact_event (
  match_id       BIGINT,
  period         INT,
  minute         INT,
  second         INT,
  team_id        BIGINT,
  player_id      BIGINT,
  event_type     STRING,
  outcome        STRING,
  x              DOUBLE,
  y              DOUBLE,
  xg             DOUBLE,
  qualifiers     MAP<STRING,STRING>,
  source         STRING,
  ingest_ts      TIMESTAMP
)
PARTITIONED BY (season STRING, league STRING, match_id BIGINT);
```

---

## 6) Унификация сущностей (ID игроков/команд)

**Принципы:**
1. Внутренний **стабильный ID** (`player_id`, `team_id`) + **таблица соответствий** `xref_entity(source, source_id, internal_id, confidence, first_seen_ts, last_seen_ts)`.
2. Канонизация: нормализуем имена (регистры, диакритика, частые псевдонимы), даты рождения, гражданства.
3. Алгоритм сопоставления (порядок применения):
   - Жёсткие ключи (уникальные внешние IDs) → прямое соответствие.
   - Правила: `(full_name_canon, birth_date)`; `(full_name_canon, team, season)`; Levenshtein для имени + толерантность к транслитерации.
   - Доп‑сигналы: рост, позиция, фут, номер, страна, исторический трансфер‑путь.
   - Скоринг `confidence` (0..1); при конфликте — ручная валидация.
4. Генерация внутреннего ID: монотонный счётчик или детерминированный хеш от `(name_canon|birth_date|country)`; **не** использовать внешний ID как внутренний.
5. Историзация (`valid_from/to`) для смены имени/клуба; soft deletes не допускаются — только закрытие периода валидности.

---

## 7) Интеграция источников (ingestion)
- **FBref / WhoScored / Understat**: HTTP‑парсеры (requests + rate‑limit + retries + backoff). Сохранять **raw**: HTML/JSON, gzip.
- **Transfermarkt**: осторожно с антибот‑мерами; кэширование; хранить HTML + извлечённый JSON.
- **SoFIFA**: CSV/JSON dumps → сразу в raw.
- **StatsBomb OD**: читать официальные JSON; нормализовать события, координаты в единую систему.
- Все загрузчики: **идемпотентность**, `ingest_date` как часть пути, контроль дубликатов по `(source, source_id, version)`.

---

## 8) ETL (Spark)
- Чтение `raw/*` → парсинг/валидация → маппинг на `dim_*` → запись в `silver` (Parquet).
- Стандартизация единиц (м/ярды, минуты/секунды, координаты поля), таймзона UTC.
- Обогащение (xG, модели позиций, агрегаты по окнам).
- Запись `gold`: витрины под BI/ML (пример: сезонные сводки игрока/команды, форма, рейтинги).
- **Схемы**: schema registry (JSON Schema/Avro внутри репо), `spark.sql.shuffle.partitions` под объём.

---

## 9) Доступ и BI
- **Trino** (Hive connector) поверх Hive Metastore.
- Каталоги: `raw` (read‑only, ограничить), `silver`, `gold` (для BI).
- Индексы/ускорение: партиционирование, predicate pushdown, сортировка; при необходимости — агрегированные материализации в `gold`.

---

## 10) Горячее/холодное хранение
- **Hot**: последние 2–3 сезона в `silver/gold`, Parquet + Snappy, частые запросы.
- **Cold**: архивные партиции (старше N сезонов) — более агрессивное сжатие (ZSTD), отдельный префикс `/archive/...`.
- Политика ретенции (Airflow DAG): ежеквартальный перевод партиций из hot→cold; каталогизация в метасторе.

---

## 11) Оркестрация (Airflow)
- Отдельный DAG на источник: `ingest_<source>` (schedule по источнику).
- Downstream: `etl_<domain>` → `gold_build_<mart>`.
- SLA/алерты: email/Slack; ретраи с экспоненциальной задержкой; идемпотентность задач.
- Datasets/trigger rules для каскадов при успешной загрузке.

---

## 12) Качество и версионирование
- **Data Quality**: Great Expectations (ключевые проверки: not null, диапазоны, уникальность ключей, согласованность размерностей).
- **Версионирование**: путь содержит `ingest_date`; в `silver/gold` — `data_version` (метка построения). Хранить raw навсегда (или по ретенции).
- **Линеаж**: простая трассировка через столбцы `source`, `source_id`, `ext_match_key`, `ingest_ts`.

---

## 13) Безопасность и доступ
- Read‑only роль для BI (Trino); write для Spark ETL.
- HDFS/Hive разрешения по каталогам/таблицам.
- Секреты (пароли, токены) — в переменных окружения Airflow/ Docker secrets.

---

## 14) Мониторинг и операционка
- Airflow UI + алерты (SLA, failure).
- Логи Spark/Trino в томах; метрики через Prometheus/JMX (по возможности) + Grafana.
- Бэкапы: ежедневно БД Metastore; еженедельно снапшоты `silver/gold` критичных витрин.
- Тесты: unit для парсеров, интеграционные для ETL (малые выборки).

---

## 15) Конвенции имен и каталоги
- Таблицы: `<layer>_<domain>_<entity>` (напр., `silver_core_players`, `gold_agg_player_season`).
- Поля ключей: `*_id` (internal), `src_*` (исходные атрибуты), `ingest_ts`, `data_version`.
- Директории: только `snake_case`; партиции — `key=value`.

---

## 16) Масштабирование
- По мере роста: вынести компоненты на отдельные узлы; добавить Spark/Trino воркеры; HDFS datanodes.
- Отделить метастор (БД) на выделенный сервис/диск.
- Рассмотреть вынос cold‑слоя в объектное хранилище (S3‑совместимое) с тем же Metastore.

---

## 17) Минимальные артефакты для старта
- Репозиторий:
```
/dags/ingest_fbref.py
/dags/ingest_whoscored.py
/dags/etl_core.py
/dags/build_gold_marts.py
/spark_jobs/parse_fbref.py
/spark_jobs/normalize_events.py
/schemas/*.json
/sql/gold/*.sql
```
- CI: линтеры + тесты парсеров/ETL.
- Makefile/Invoke для локального запуска задач.

---

## 18) Пример: генерация внутреннего ID
```python
# детерминированный ID (если нет прямого маппинга)
key = f"{name_canon}|{birth_date}|{country_code}"
player_id = hash_fn64(key)   # стабильный 64‑битный хеш (SipHash/xxHash)
```
- При появлении внешних IDs дополняем `dim_player` и `xref_entity` без смены `player_id`.

---

## 19) Витрины (gold) — минимальный набор
- `gold_player_season`: агрегаты по игроку/сезону (минуты, xG/xA, голы/ассисты, рейтинги).
- `gold_team_season`: командные метрики (PPDA, xG for/against, серия форм).
- `gold_market_player`: стоимость/трансферы (Transfermarkt) + спортивные метрики (фичи для ML).

---

## 20) SLA и ретенции
- Инкрементальные DAGи по источникам: `D+1` после доступности данных.
- Ретенция логов: 30 дней; `raw` — ≥ 12 мес. (или согласно бюджету); `gold` — актуальные N сезонов в hot.

--- 
