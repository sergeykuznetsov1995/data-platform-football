# OpenMetadata — Trino ingestion и lineage

Конфиги для OpenMetadata 1.5.x ingestion-контейнера. Смонтированы в
`openmetadata-ingestion` как `/opt/configs:ro` (см. `compose.yaml`).

## Файлы

| Файл | Что делает |
|------|-----------|
| `trino_ingestion.yaml` | Schema discovery: каталог `iceberg`, схемы `bronze` / `silver` / `gold` → таблицы и колонки попадают в OpenMetadata |
| `trino_lineage.yaml`   | Парсит `system.runtime.queries` Trino, строит lineage Bronze → Silver → Gold по CTAS/INSERT |

## Запуск

```bash
# Schema/metadata ingest (выполнять после изменений DDL — создания/удаления таблиц)
docker compose exec openmetadata-ingestion \
    metadata ingest -c /opt/configs/trino_ingestion.yaml

# Lineage (запускать ПОСЛЕ ingest — lineage матчит имена таблиц с уже залитыми сущностями)
docker compose exec openmetadata-ingestion \
    metadata ingest -c /opt/configs/trino_lineage.yaml
```

## Получение JWT-токена для ingestion-bot

1. Открыть OpenMetadata UI → `http://localhost:8585`
2. **Settings → Bots → ingestion-bot**
3. Скопировать `JWT Token` (поле появляется при первом раскрытии бота)
4. Положить в корневой `.env` как `OM_JWT_TOKEN=<token>` рядом с
   `OPENMETADATA_DB_PASSWORD`. Контейнер `openmetadata-ingestion` должен
   получить эту переменную через `env_file: .env` или `environment:` в
   `compose.yaml`.

## Переменные окружения, используемые в YAML

| Переменная | Где взять |
|------------|-----------|
| `OM_JWT_TOKEN` | UI → Bots → ingestion-bot |
| `TRINO_OPENMETADATA_PASSWORD` | `.env` (создаётся в Wave 1, бутстрап `password.db` Trino) |

CLI `metadata ingest` подставляет `${VAR}` из переменных окружения процесса
ingestion-контейнера. Никаких секретов в YAML коммитить не нужно.

## Cadence (рекомендуемая)

| Workflow | Частота | Способ запуска |
|----------|---------|----------------|
| `trino_ingestion.yaml` | 1 раз / сутки | вручную или через Airflow `BashOperator` (Wave 3) |
| `trino_lineage.yaml`   | 1 раз / сутки, ПОСЛЕ ingestion | то же |

Запуск чаще раза в сутки имеет смысл только если в bronze/silver/gold
часто меняется DDL (для ingestion) или вы упираетесь в окно
`query.max-history` Trino (для lineage — см. ниже).

## Замечание про Trino query history

Lineage читает `system.runtime.queries` — **in-memory** таблицу
координатора. Размер ограничен:

- `query.max-history` — default **100** запросов
- `query.min-expire-age` — default **10 минут**

Текущий `configs/trino/config.properties` **не задаёт** эти параметры
(используются дефолты). Silver+Gold DAG генерируют ~17 CTAS-запросов в
день — этого пока хватает, но при росте пайплайна (или при включении
дополнительных сорсов) запросы будут вытесняться раньше, чем сработает
lineage workflow.

**Рекомендация (если lineage начнёт пропускать таблицы):**
добавить в `configs/trino/config.properties` строку:

```properties
query.max-history=1000
```

После — `docker compose restart trino`. Память это почти не съедает
(каждая запись — несколько КБ).

Альтернатива на будущее — `event-listener` Trino, пишущий запросы в
Postgres/Kafka, и `queryLogFilePath` в lineage workflow. Для MVP не нужно.

## Self-signed TLS

Trino поднят с self-signed сертификатом (`configs/trino/certs/keystore.jks`).
В обоих YAML установлено:

```yaml
verifySSL: no-ssl
connectionArguments:
  http_scheme: https
  verify: false
```

Для production — смонтировать публичную часть сертификата в ingestion-контейнер
и переключить на `verifySSL: validate-ssl` + `sslConfig.caCertificate: <path>`.
