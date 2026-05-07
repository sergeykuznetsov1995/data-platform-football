# E0 — Baseline & Tooling postmortem

**Этап завершён**: 2026-05-07
**Бюджет план/факт**: 1-2 / ~1 день
**Ветка**: `feature/medallion-e0-baseline` (от `feature/phase-2-bi-catalog`)

## Что сделано

- **Iceberg snapshot retention 7d → 30d** для weekly maintenance — `dags/utils/maintenance_tasks.py:32` (`DEFAULT_RETENTION`); поясняющий docstring в `dags/dag_iceberg_maintenance.py:15-21`. Daily DAG (3d retention для `HIGH_CHURN_BRONZE`) намеренно оставлен без изменений.
- **Stand-alone утилита `scripts/audit_storage_baseline.py`** — собирает per-table метрики (`row_count`, `partition_count`, `iceberg_files_count`, `iceberg_snapshot_count`, `latest_snapshot`, HDFS sizes) + summary (per-schema rollup, `hdfs_overall`, `host_disk`). HDFS-локации резолвятся через `SHOW CREATE TABLE` (UUID-суффиксы тейблов). Output → JSON в `data/audit/`.
- **Baseline snapshot собран** — `data/audit/storage_baseline_2026-05-07.json` (71 таблица, все non-null HDFS sizes).
- **Существующий gold-usage baseline** (R0.5, `data/audit/gold_usage_2026-05-06.json`, 11 active / 6 unused) переиспользован без перезапуска.
- **Maintenance tests** — 3/3 passing, hardcoded `"7d"` ассертов не найдено.

## Baseline numbers (E0 entry-point)

| Metric | Value |
|---|---|
| Total tables | 71 (40 bronze + 14 silver + 17 gold) |
| Total rows | 2 155 764 |
| Total HDFS bytes | 2.85 GB |
| Total Iceberg metadata bytes | 2.72 GB (**95.3 %** от total — bloat) |
| HDFS pool used | 3.0 % (5.7 GB / 199 GB) |
| Host disk used | 53.0 % (52 GB / 99 GB) |

## Anomalies discovered (handoff важно!)

- **46 / 71 таблиц с metadata-ratio > 50 %** — epidemic-scale bloat после HDFS overflow 2026-05-04.

| Top by snapshot count | Snapshots |
|---|---|
| `clubelo_team_history` | 530 |
| `espn_matchsheet` | 155 |
| `espn_lineup` | 141 |

| Top by HDFS size | Size | Metadata % |
|---|---|---|
| `clubelo_team_history` | 1.57 GB | 99.4 % |

→ Аномалии будут подметены **следующим weekly run** `dag_iceberg_maintenance` (Sunday 2026-05-10 05:00 UTC) с новым 30d retention. Manual trigger не делаем — roadmap не требует немедленного применения.

## Что отложено

- **Storage guard DAG** (auto Telegram alert ≥75 % HDFS, auto-pause ingestion ≥85 %) → **E8a**. Threshold-модель уже зафиксирована в R0.3.
- **Storage tiering** (rolling 3 сезона vs forever-retain) → **R10 Tier 2 spike**.
- **Production player resolver** → **R2-followup**, post-Phase-1.
- **Manual trigger expire-snapshots с 30d retention** → плановый cron-run Sunday 05:00 UTC (не блокирует E2).

## Что узнали (input для E1-E2)

1. **Bronze metadata bloat сильнее ожиданий** — 95 % HDFS bytes это metadata, не data. После применения 30d retention следующим weekly run **нужна повторная сборка baseline** (suggest: scheduled-DAG-обёртка над `audit_storage_baseline.py` в фазе E8) для количественной оценки эффекта retention-bump.
2. **HDFS-локации Iceberg содержат UUID-суффиксы** (`<table>-<uuid>`) — любой downstream скрипт работающий с HDFS обязан резолвить location через `SHOW CREATE TABLE` (не угадывать `<schema>.db/<table>`). Pattern зафиксирован в `scripts/audit_storage_baseline.py`.
3. **HDFS pool на 3 % при APL-only scope** — буфер ~22 percentage points до ALERT@75 % threshold для T1 expansion. Per R0.3 projected T1 size = 12-17 GB → +10 pp. Margin healthy.
4. **gold-usage baseline 6 unused tables** — кандидаты на decommission в **E9** после T1 stable ≥30 дней.

## DQ-baseline (ссылки)

- `data/audit/storage_baseline_2026-05-07.json` — entry-point snapshot E0.
- `data/audit/gold_usage_2026-05-06.json` — R0.5 usage tracker output.
- `docs/research/R0.3_storage_projection.md` — projection model.
- `docs/research/R0.5_usage_tracker.md` — usage methodology.
- Airflow DAG `dag_iceberg_maintenance` — next run **2026-05-10 05:00 UTC** применит 30d retention.

---

## Handoff to E2 (architectural input)

E2 (Master-data dims) запускается следующим. Зависимости и precondition'ы:

- **R0.2a (FBref managers parser)** — должен быть verified до старта E2.
- **R8 (Referee fuzzy match cross-locale)** — Tier 2; может идти параллельно с E2 или предварять его.
- **R7 (Standings point deductions)** — Tier 2; необходим для `dim_standings`.
- E2 **не зависит** от storage-guard DAG (это E8).
- E2 **не зависит** от backfill metadata cleanup — bronze bloat подметается отдельно через `dag_iceberg_maintenance`.

**HDFS-headroom check перед стартом E2**:

```bash
jq '.hdfs_overall.used_pct' data/audit/storage_baseline_*.json   # must be < 75
```
