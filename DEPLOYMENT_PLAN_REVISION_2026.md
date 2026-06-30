# Ревизия плана развёртывания — сверка с рынком 2026

> Дополнение к `DEPLOYMENT_PLAN.md`. Оригинал верен **как инструкция «как запустить то, что лежит в репозитории»**.
> Этот документ — про то, что из развёрнутого **устарело на середину 2026** и что с этим делать.
> Составлен 2026-06-30 на основе веб-research (версии и даты проверены) + полной сверки репозитория.

---

## ✅ Принятые решения владельца

- **Оркестратор: остаёмся на Apache Airflow 2.x** — на 3.x / Dagster / Kestra **не** мигрируем. Минимизация риска: поднять `2.7.3 → 2.11.2` (последний патч ветки 2.x, 14.03.2026), чтобы закрыть известные CVE; EOL 2.x (22.04.2026) принят осознанно.
- **OpenMetadata: возвращаем в стек** (нужен для просмотра описаний таблиц через витрину). Обновить `1.5.0 → 1.13.1`, поиск перевести с Elasticsearch 8.11 на **OpenSearch**. +3 контейнера (server + ingestion + opensearch).

---

## 0. Объясняю как для пятилетнего 🧒

Помнишь нашу **кухню-фабрику**, которая готовит футбольную аналитику? Грузчики возят продукты, склад-холодильник их хранит, повар готовит, официант показывает блюда на витрине. Так вот, мы проверили — **часть техники на кухне старая или сломанная**, и кое-что чинить нельзя ставить «как есть»:

- ⏰ **Будильник-расписание** (Airflow 2.7.3) — мастер, который его чинил, **уволился навсегда** (поддержку закрыли 22 апреля 2026). Если будильник сломается — починить будет некому. Нужен **новый будильник**.
- 🍽️ **Официант с витриной** (Superset 4.1.1) — у него **дырявый карман**, и чужой человек может незаметно стащить блюда (это дырки в защите, CVE). Надо **зашить карман заплаткой** (обновить).
- 🧊 **Склад-холодильник** (HDFS) — это на самом деле **три холодильника, втиснутые в один**, которые делают вид, что еда в безопасности. Но стоят они на одной полке: если полка упадёт — пропадёт всё сразу. Дорого, шумно, сложно. Современный склад — **один простой холодильник** (SeaweedFS).
- 📖 **Меню-каталог** (Hive Metastore) — **толстая старая тетрадь**, которую тяжело таскать. Есть новая **тонкая лёгкая тетрадь** (Lakekeeper).
- 🔥 **Вторая большая печь** (Spark) — стоит на кухне, греется, ест электричество, а в ней **ничего не готовят**. Можно просто **выключить**.
- 📝 **Рецепты** (голый SQL) — записаны на **разбросанных бумажках без подписей**, и никто не проверяет, правильно ли вышло блюдо. Лучше сложить в **книгу рецептов** (dbt) с галочками-проверками.

**Главная хитрость:** холодильник и тетрадь-меню проще всего поменять **сейчас, пока в них ещё нет еды**. Потом придётся перетаскивать все продукты — а это долго.

То, что **хорошее и трогать не надо:** повар (Trino) и его способ готовки (Iceberg), сами три стола (Bronze→Silver→Gold) и наши умные грузчики (скрейперы) — это лучшее, что есть на кухне.

---

## 1. 🔴 Две «просрочки» — нельзя разворачивать как есть

| Что | Проблема | Минимальная починка |
|---|---|---|
| **Airflow 2.7.3** | Вся ветка 2.x — **EOL 22.04.2026**. На 2.7.3 висят известные CVE. | ✅ **Решено: остаёмся на 2.x.** Поднять 2.7.3 → **2.11.2** (последний патч, 14.03.2026). EOL-риск принят. |
| **Superset 4.1.1** | Незакрытые **CVE-2025-27696** (захват дашбордов read-юзером) и **CVE-2025-48912** (обход RLS через SQL-инъекцию). Фикс в **4.1.2**. | Минимум → **4.1.2**; цель → **5.x / 6.1.0**. Драйвер — только пакет `trino`. |

---

## 2. ✅ Что современно и менять НЕ нужно

- **Trino + Apache Iceberg** — правильное ядро 2026 (только версии подтянуть).
- **PostgreSQL 16** — ок.
- **Superset как BI** — самый нативный к Trino (вопрос только в версии/CVE).
- **Скрейпинг-слой** (nodriver / Camoufox / FlareSolverr / curl_cffi, пиннинг `soccerdata==1.8.8`) — уникальная ценность, не трогать.
- **Медальон Bronze→Silver→Gold + xref-логика** — концептуально верно.

---

## 3. Послойный разбор (сейчас → рынок 2026 → решение)

| Слой | Сейчас в репо | Рынок 2026 | Решение |
|---|---|---|---|
| Оркестрация | Airflow **2.7.3** | 2.x EOL; текущая **3.2.2**; для малой команды — Dagster / Kestra | ✅ Остаёмся на 2.x → поднять до **2.11.2** |
| BI | Superset **4.1.1** | CVE; текущая **6.1.0**; драйвер `trino` (а не `sqlalchemy-trino`) | 🔴 ≥4.1.2, цель 5.x/6.1.0 |
| Хранилище | **HDFS 3.4.1** (namenode+2 datanode) | Объектное хранилище вытеснило HDFS; MinIO **заархивирован 12.02.2026** (AGPL) | 🟠 **SeaweedFS** (Apache-2.0) / Garage |
| Каталог таблиц | **Hive Metastore 4.0.0** | Сдвиг к **Iceberg REST Catalog** | 🟠 **Lakekeeper** (Rust, 1 бинарь) / Polaris |
| Вычисления | **Spark 3.5** (master+2 worker) | В трансформациях не используется → «мёртвый вес» | 🟠 **Убрать**; лёгкое — DuckDB/Polars |
| Трансформации | Голый **Trino SQL** (CTAS в DAG) | Антипаттерн → **dbt-trino** / SQLMesh (тесты, lineage, `ref()`) | 🟠 Перенести в dbt; Airflow↔dbt через Cosmos |
| Каталог/lineage | **OpenMetadata 1.5.0** + ES 8.11 | Текущая 1.13.1; ES залочен → OpenSearch; тяжёлый | 🟡 Отложить или обновить (1.13.x + OpenSearch) |
| Качество данных | **нет** | dbt-tests + Elementary, или Soda Core 4.x | 🟡 Минимум: SQL-ассерты + Iceberg `NOT NULL` |
| Секреты | plain **`.env`** | виден в `docker inspect`/логах | 🟡 **SOPS+age** + Compose `secrets:` |
| TLS | скрипт самоподписанных | reverse-proxy с авто-TLS | 🟡 **Caddy** `tls internal` |
| Версии образов | теги (`trino:479`, ...) | digest-пиннинг + healthchecks + mem-лимиты | 🟢 Гигиена compose |

---

## 4. Дорожная карта по приоритетам

### Tier 0 — в рамках текущего деплоя (дёшево, безопасность; ложится в план)
1. Superset → **≥4.1.2** (закрыть CVE); драйвер — только `pip install "trino[sqlalchemy]"`, **убрать** `sqlalchemy-trino`.
2. Trino **479 → 481/482**.
3. Секреты: **SOPS+age + Compose `secrets:`** вместо паролей в `.env`.
4. Гигиена compose: убрать `version:`, добавить `healthcheck` + `depends_on: service_healthy`, **memory-лимит каждому** сервису (поправка на JVM — см. §6), лимиты логов.
5. **Не поднимать Spark и OpenMetadata** на старте (оба `heavy`, ценность сейчас низкая).

### Tier 1 — высокая отдача, правки кода репозитория (не только плана)
6. **Убрать Spark** из архитектуры.
7. **HDFS → SeaweedFS** + **Hive Metastore → Lakekeeper** (REST-каталог). Снимает **4 JVM-контейнера** (namenode + 2×datanode + hive-metastore). Правки: `compose.yaml`, `configs/trino/iceberg.properties`, `scripts/init_storage.py`, удаление Hive/HDFS-образов.
8. **Качество данных** — минимум SQL-ассерты + `NOT NULL`, либо Soda Core.

### Tier 2 — стратегические решения (отдельный проект)
9. ✅ **Решено остаться на Airflow 2.x** — поднять 2.7.3 → 2.11.2 (последний патч). Миграцию на 3.x / Dagster / Kestra не делаем.
10. **dbt-trino / SQLMesh** — перенос SQL-трансформаций в версионируемые модели с тестами и lineage.
11. ✅ **Решено вернуть OpenMetadata** — обновить `1.5.0 → 1.13.1`, поиск Elasticsearch 8.11 → **OpenSearch**.

> ⏳ **Тайминг:** п.7 (хранилище+каталог) дешевле всего сделать **до первой загрузки данных** — сейчас в Iceberg пусто, миграции таблиц нет. «Потом» = переезд с данными.

---

## 5. Конкретные правки (Tier 0)

### 5.1. Superset — Dockerfile (уточняет Блокер A оригинала)
```dockerfile
FROM apache/superset:4.1.2          # было 4.1.1 — закрывает CVE-2025-27696 / 48912
USER root
RUN pip install --no-cache-dir \
    "trino[sqlalchemy]" \            # правильный пакет; sqlalchemy-trino НЕ ставить
    psycopg2-binary \
    redis
USER superset
```
> URI датасорса: `trino://{user}:{pass}@trino:8443/iceberg/{schema}` (HTTPS/JWT — в `connect_args`).

### 5.2. Trino — версия
```yaml
# compose.yaml
trino:
  image: trinodb/trino:482          # было 479 (дек 2025)
```

### 5.3. Секреты — SOPS+age (вместо паролей в .env)
```bash
# один раз
age-keygen -o ~/.config/sops/age/keys.txt        # приватный ключ остаётся на VM
# .sops.yaml — правило шифрования, в git коммитится только зашифрованный secrets.enc.yaml
sops --encrypt --age <public-key> secrets.yaml > secrets.enc.yaml
```
В `compose.yaml` — доставка файлом, не через `environment:`:
```yaml
secrets:
  trino_keystore_password:
    file: ./secrets/trino_keystore_password
services:
  trino:
    secrets: [trino_keystore_password]    # появится в /run/secrets/...
```

### 5.4. Гигиена compose
```yaml
name: data-platform-football          # вместо устаревшего version:
services:
  postgres:
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $$POSTGRES_USER -d $$POSTGRES_DB"]
      interval: 10s; timeout: 5s; retries: 5; start_period: 30s
    logging: { driver: json-file, options: { max-size: "10m", max-file: "3" } }
```

---

## 6. ⚠️ JVM-готча для memory-лимитов (Trino критично)

RSS контейнера = heap + metaspace + code cache + direct buffers + GC → **всё считается в cgroup-лимит**, и контейнер ловит **OOMKilled молча, без Java-ошибки**.
- Лимит контейнера ≠ `-Xmx`. Держать heap ≲ **70–75%** лимита (`-XX:MaxRAMPercentage=70`).
- Для Trino учесть `memory.heap-headroom-per-node` (~30% heap).

---

## 7. Целевая «лёгкая» архитектура 2026 (после Tier 1)

```
Скрейперы ──▶ [Bronze/Silver/Gold = Iceberg-таблицы]
                       │
   данные ───▶ SeaweedFS (S3-хранилище, Apache-2.0)   ← вместо HDFS (3 JVM)
   каталог ──▶ Lakekeeper (REST, 1 Rust-бинарь + Postgres)  ← вместо Hive Metastore (JVM)
   движок ───▶ Trino 482 (iceberg.catalog.type=rest)
   BI ───────▶ Superset (≥4.1.2) за Caddy (авто-TLS)
   расписание ▶ Airflow 2.11.2 (остаёмся на 2.x по решению владельца)
   (Spark и OpenMetadata убраны / отложены)
```
**Выигрыш:** минус namenode + 2 datanode + hive-metastore (4 JVM-контейнера) и вся эксплуатация namenode; высвобождённые RAM/CPU → Trino.

### Trino → REST-каталог (пример `iceberg.properties`)
```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://lakekeeper:8181/catalog
iceberg.rest-catalog.warehouse=football
fs.native-s3.enabled=true
s3.endpoint=http://seaweedfs:8333
s3.path-style-access=true
s3.aws-access-key=<static>          # на старте — статические ключи, НЕ vended-credentials
s3.aws-secret-key=<static>
```
> Vended-credentials в Trino исторически с багами — включать позже, когда понадобится изоляция доступа по таблицам.

---

## 8. Открытые решения (выбрать с владельцем)

1. ✅ ~~Оркестратор~~ — **решено: Airflow 2.x** (поднять до 2.11.2).
2. **Каталог:** Lakekeeper (легче) vs Polaris (стандарт «как у больших»)?
3. **Хранилище:** SeaweedFS (есть встроенный REST-каталог) vs Garage?
4. **Трансформации:** dbt-trino (мейнстрим) vs SQLMesh (virtual environments)?
5. ✅ ~~OpenMetadata~~ — **решено: возвращаем** (обновить до 1.13.1 + OpenSearch).
6. **Глубина сейчас:** только Tier 0 (быстрый безопасный деплой) или сразу Tier 1 (пока данных нет)?

---

## 9. Источники (проверено вебом, июнь 2026)

- Iceberg-каталоги: [State of Apache Iceberg Catalogs, June 2026](https://dev.to/alexmercedcoder/the-state-of-apache-iceberg-catalogs-in-june-2026-265e)
- Хранилище: [Self-Hosted S3 2026: MinIO vs SeaweedFS vs Garage](https://www.pistack.xyz/posts/2026-05-03-self-hosted-s3-object-storage-minio-seaweedfs-garage-guide/) · [MinIO CE feature removal](https://blocksandfiles.com/2025/06/19/minio-removes-management-features-from-basic-community-edition-object-storage-code/)
- Каталоги: [Lakekeeper](https://github.com/lakekeeper/lakekeeper) · [Apache Polaris](https://polaris.apache.org/)
- Airflow: [Airflow 3 GA](https://airflow.apache.org/blog/airflow-three-point-oh-is-here/) · [endoflife.date/apache-airflow](https://endoflife.date/apache-airflow)
- Оркестраторы: [Dagster asset checks](https://dagster.io/blog/dagster-asset-checks) · [Kestra self-hosted](https://kestra.io/resources/infrastructure/self-hosted-workflow-orchestration)
- Трансформации: [dbt Trino configs](https://docs.getdbt.com/reference/resource-configs/trino-configs) · [SQLMesh comparisons](https://sqlmesh.readthedocs.io/en/stable/comparisons/)
- BI/CVE: [Superset CVEs](https://superset.apache.org/docs/security/cves) · [trino PyPI](https://pypi.org/project/trino)
- DQ: [Soda Core](https://github.com/sodadata/soda-core) · [Elementary](https://github.com/elementary-data/elementary)
- Секреты/TLS: [Docker Compose secrets](https://docs.docker.com/compose/how-tos/use-secrets) · [SOPS](https://github.com/getsops/sops) · [Caddy auto-HTTPS](https://caddyserver.com/docs/automatic-https)
- Trino: [Iceberg connector](https://trino.io/docs/current/connector/iceberg.html) · [release 479](https://trino.io/docs/current/release/release-479.html)
