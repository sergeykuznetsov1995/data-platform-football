# Архитектура Data-платформы для футбольных данных

## 1. Обзор

Data-платформа на базе Medallion Architecture для сбора, обработки и анализа футбольной статистики.

**Технологический стек:**
- **HDFS** — распределённое хранилище
- **Spark** — обработка и трансформация данных
- **Airflow** — оркестрация пайплайнов
- **Trino** — SQL-движок для аналитики
- **Hive Metastore** — каталог метаданных

---

## 2. Высокоуровневая архитектура

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES (soccerdata)                             │
├────────┬──────────┬──────────┬────────┬──────────┬────────┬────────┬─────────┤
│ FBref  │Understat │WhoScored │ FotMob │Sofascore │ SoFIFA │ClubElo │  ESPN   │
├────────┴──────────┴──────────┴────────┴──────────┴────────┴────────┴─────────┤
│                         MatchHistory (Football-Data.co.uk)                   │
└──────────────────────────────────┬───────────────────────────────────────────┘
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                  INGESTION (Airflow + Python)                   │
│         • Scraper DAGs • Rate Limiting • Data Quality           │
└────────────────────────────┬────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                         HDFS STORAGE                            │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  BRONZE — сырые данные (immutable, append-only)           │  │
│  └───────────────────────────────────────────────────────────┘  │
│                             │ Spark                             │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  SILVER — очищенные, нормализованные данные               │  │
│  └───────────────────────────────────────────────────────────┘  │
│                             │ Spark                             │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  GOLD — агрегированные данные для аналитики               │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────┬───────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                       SERVING (Trino)                           │
│              SQL-доступ к Bronze / Silver / Gold                │
│                             │                                   │
│         ┌───────────────────┼───────────────────┐               │
│         ▼                   ▼                   ▼               │
│    BI (Superset)      Jupyter/Python       REST API             │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Источники данных (soccerdata)

| Источник | Данные | Частота |
|----------|--------|---------|
| **FBref** | Статистика команд/игроков, xG, расписание, удары, составы, события матчей | Ежедневно |
| **Understat** | xG метрики, shot events с координатами, статистика по матчам | Ежедневно |
| **WhoScored** | Event data матчей (SPADL формат), травмы/дисквалификации | Ежедневно |
| **FotMob** | Турнирные таблицы, статистика матчей (xG, passes, defence, duels) | Ежедневно |
| **Sofascore** | Турнирные таблицы, расписание | Ежедневно |
| **SoFIFA** | FIFA атрибуты игроков, рейтинги команд, версии FIFA | Еженедельно |
| **ClubElo** | ELO рейтинги по дате, история ELO команд | Ежедневно |
| **ESPN** | Расписание, составы, matchsheet статистика | Ежедневно |
| **MatchHistory** | Исторические результаты, букмекерские коэффициенты (Football-Data.co.uk) | Ежедневно |

**Поддерживаемые лиги:** Premier League, La Liga, Bundesliga, Serie A, Ligue 1 и другие

---

## 4. Medallion Architecture

### 4.1 Bronze Layer (Сырые данные)

**Назначение:** Хранение данных "как есть" из источников

**Структура HDFS:**
```
/data/bronze/{источник}/{сущность}/{year}/{month}/{day}/
```

**Принципы:**
- Immutable (только append)
- Партиционирование по дате загрузки
- Метаданные: `_source`, `_ingested_at`, `_batch_id`
- Оригинальная структура из источника

**Сущности по источникам (методы read_*):**

| Источник | Методы | Описание |
|----------|--------|----------|
| **FBref** | `read_leagues`, `read_seasons`, `read_schedule`, `read_team_season_stats`, `read_team_match_stats`, `read_player_season_stats`, `read_player_match_stats`, `read_lineup`, `read_events`, `read_shot_events` | Полная статистика команд и игроков |
| **Understat** | `read_leagues`, `read_seasons`, `read_schedule`, `read_team_match_stats`, `read_player_season_stats`, `read_player_match_stats`, `read_shot_events` | xG метрики и shot events |
| **WhoScored** | `read_schedule`, `read_missing_players`, `read_events` | Event data (SPADL), травмы |
| **FotMob** | `read_leagues`, `read_seasons`, `read_league_table`, `read_schedule`, `read_team_match_stats` | Таблицы и статистика матчей |
| **Sofascore** | `read_leagues`, `read_seasons`, `read_league_table`, `read_schedule` | Таблицы и расписание |
| **SoFIFA** | `read_leagues`, `read_versions`, `read_teams`, `read_players`, `read_team_ratings`, `read_player_ratings` | FIFA атрибуты |
| **ClubElo** | `read_by_date`, `read_team_history` | ELO рейтинги |
| **ESPN** | `read_schedule`, `read_matchsheet`, `read_lineup` | Расписание и составы |
| **MatchHistory** | `read_games` | Исторические результаты и коэффициенты |

---

### 4.2 Silver Layer (Очищенные данные)

**Назначение:** Нормализованные, дедуплицированные данные с единой моделью

**Структура HDFS:**
```
/data/silver/{домен}/{сущность}/league={лига}/season={сезон}/
```

**Принципы:**
- Партиционирование по бизнес-ключам (league, season)
- Entity Resolution — единые ID для команд/игроков из разных источников
- SCD Type 2 для dimension tables
- Дедупликация записей

**Dimensions (справочники):**

| Таблица | Описание |
|---------|----------|
| dim_teams | Унифицированный справочник команд с маппингом имён из всех источников |
| dim_players | Унифицированный справочник игроков |
| dim_matches | Справочник матчей с ID из разных источников |
| dim_leagues | Справочник лиг |

**Facts (факты):**

| Таблица | Описание |
|---------|----------|
| fact_match_stats | Статистика команд в матчах (голы, xG, владение, удары) |
| fact_player_season_stats | Сезонная статистика игроков |
| fact_player_match_stats | Матчевая статистика игроков |
| fact_shot_events | Детали каждого удара (xG, координаты, результат) |
| fact_team_elo | Исторические ELO рейтинги команд |

---

### 4.3 Gold Layer (Агрегированные данные)

**Назначение:** Готовые датасеты для аналитики и ML

**Структура HDFS:**
```
/data/gold/{use_case}/league={лига}/
```

**Принципы:**
- Денормализованные wide tables
- Предрассчитанные метрики
- Оптимизация под конкретные сценарии

**Аналитические таблицы:**

| Таблица | Описание |
|---------|----------|
| team_performance_summary | Позиция, очки, голы, xG, форма, ELO за сезон |
| player_performance_summary | Голы, ассисты, xG, per90 метрики, FIFA рейтинги |
| league_standings | Турнирные таблицы |
| head_to_head | Статистика личных встреч |

**xG Аналитика:**

| Таблица | Описание |
|---------|----------|
| team_xg_trends | Тренды xG команд по времени |
| player_xg_performance | xG перформанс игроков |
| shot_quality_analysis | Анализ качества ударов |

**ML Features:**

| Таблица | Описание |
|---------|----------|
| match_features | Фичи для предсказания результатов (ELO, форма, xG, H2H) |

---

## 5. Формат хранения

**Формат:** Apache Parquet

| Параметр | Bronze | Silver | Gold |
|----------|--------|--------|------|
| Compression | Snappy | Snappy | ZSTD |
| Row group size | 64MB | 128MB | 256MB |
| Target file size | 64-128MB | 128-256MB | 256-512MB |

---

## 6. ETL-пайплайны (Airflow)

### Ingestion DAGs

**Ежедневно:**
- `dag_ingest_fbref` — статистика команд/игроков, xG, события
- `dag_ingest_understat` — xG метрики, shot events
- `dag_ingest_whoscored` — event data (SPADL)
- `dag_ingest_fotmob` — таблицы, статистика матчей
- `dag_ingest_sofascore` — таблицы, расписание
- `dag_ingest_clubelo` — ELO рейтинги
- `dag_ingest_espn` — расписание, составы, matchsheet
- `dag_ingest_matchhistory` — результаты, коэффициенты

**Еженедельно:**
- `dag_ingest_sofifa` — FIFA атрибуты игроков

### Transformation DAGs

- `dag_bronze_to_silver` — очистка, entity resolution, нормализация
- `dag_silver_to_gold` — агрегация, расчёт метрик

### Maintenance DAGs

- `dag_data_quality` — проверки качества данных
- `dag_compaction` — компактификация мелких файлов

### Orchestration

- `dag_master_pipeline` — координация всех пайплайнов

**Расписание:**
```
05:00 — master_pipeline запускает ingestion
06:00-09:00 — ingestion DAGs (с rate limiting)
10:00 — bronze_to_silver
12:00 — silver_to_gold
14:00 — data_quality
03:00 (воскресенье) — compaction
```

---

## 7. Entity Resolution

**Проблема:** Одна команда/игрок имеет разные названия в разных источниках

**Решение:**
- Таблица маппинга имён (ручная + fuzzy matching)
- Единый `team_id` / `player_id` в Silver layer
- Хранение оригинальных названий для трассировки

**Пример dim_teams:**

| team_id | team_name | name_fbref | name_understat | name_sofifa |
|---------|-----------|------------|----------------|-------------|
| t001 | Manchester United | Manchester Utd | Manchester United | Manchester United |
| t002 | Tottenham Hotspur | Tottenham | Tottenham | Tottenham Hotspur |

---

## 8. Trino Configuration

**Catalogs:**
- `hive.bronze` — доступ к Bronze layer
- `hive.silver` — доступ к Silver layer
- `hive.gold` — доступ к Gold layer

**Connector:** Hive + HDFS

---

## 9. Data Quality

**Проверки Bronze:**
- Наличие обязательных метаданных (_ingested_at, _batch_id)
- Freshness — данные не старше 24 часов

**Проверки Silver:**
- Primary keys not null
- Referential integrity (team_id существует в dim_teams)
- Valid ranges (xG: 0-10, possession: 0-100)

**Cross-source consistency:**
- Сравнение xG между FBref и Understat (допустимая разница ±0.5)

---

## 10. Операционные аспекты

### Retention Policy

| Слой | Срок хранения |
|------|---------------|
| Bronze | 90 дней |
| Silver | 2 года |
| Gold | Без ограничений |

### Мониторинг

- Статус DAG-ов в Airflow
- Freshness данных
- Размер файлов и партиций
- Ошибки quality checks

### Backup

- Еженедельный backup Silver и Gold в отдельную директорию HDFS
- Хранение 4 последних backup-ов

---

## 11. Структура проекта

```
data_platform/
├── airflow/
│   └── dags/
│       ├── ingestion/          # DAGs для парсинга
│       ├── transformation/     # Bronze→Silver→Gold
│       └── maintenance/        # Quality, compaction
├── spark_jobs/
│   ├── silver/                 # Трансформации в Silver
│   └── gold/                   # Агрегации в Gold
├── scrapers/
│   └── soccerdata_wrapper.py   # Обёртки над soccerdata
├── data_quality/
│   └── expectations/           # Great Expectations suites
└── config/
    ├── trino/                  # Конфигурация Trino
    └── reference/              # Маппинги команд/игроков
```

---

## 12. Дальнейшее развитие

**Потенциальные улучшения:**
- Apache Iceberg — ACID транзакции, time travel
- Kafka + Spark Streaming — real-time обновления
- Feature Store (Feast) — для ML пайплайнов
- Data Catalog (DataHub) — lineage и discovery
