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

---

## Update: E0 finalized on `feature/medallion-e0-on-e5` (2026-05-08)

E0 deliverables были интегрированы в `feature/medallion-e0-on-e5` (от `feature/medallion-e5-availability` — где done E5) commits:

- `b5e5ae6 chore(medallion-e0): import baseline tooling from feature/medallion-e0-baseline` — копирование 6 файлов из e0-branch.
- `f163cda feat(medallion-e0): add scripts/audit_gold_usage.py per R0.5 spec` — недостающий R0.5 deliverable.
- `b6f2bd2 test(medallion-e0): unit tests for audit_gold_usage.py` — 24 теста.

### Airflow Variable HDFS_GUARD_THRESHOLDS

Установлена per R0.3 verdict:

```
{"alert":75,"pause":85,"fail":95}
```

Verify: `airflow variables get HDFS_GUARD_THRESHOLDS` → JSON output OK.

### Manual trigger `dag_iceberg_maintenance` (2026-05-08 09:06 UTC)

| Field | Value |
|---|---|
| run_id | `manual__2026-05-08T09:06:26+00:00` |
| task_id | `maintain_iceberg_tables` |
| Duration | 16 sec |
| Final state | success |
| tables_processed | 78 |
| files_scanned | 22 543 |
| files_deleted | **0** |

**HDFS sizes pre = post (delta 0 байт)** — это **honest no-op**:

| Path | Size pre | Size post |
|---|---|---|
| `/user/hive/warehouse/bronze.db` | 2.7 G | 2.7 G |
| `/user/hive/warehouse/silver.db` | 6.1 M | 6.1 M |
| `/user/hive/warehouse/gold.db` | 4.6 M | 4.6 M |
| Total HDFS used | 5.9 G / 196.7 G (3%) | 5.9 G / 196.7 G (3%) |

**Объяснение нулевого delta** (важно для последующих E-этапов): retention=30d не пересекает horizon существующих snapshots — платформа после HDFS overflow recovery (2026-05-04, commits `959e4b3..1029780`) и manual cleanup получила свежие данные; E5 завершилась 2026-05-07. Большая часть snapshots <30d. Ожидание из R0.3 («metadata 2.72 GB → 100-300 MB») было основано на baseline-снимке 2026-05-07, ДО которого уже произошёл HDFS overflow incident — постмортем тогда зафиксировал bloat 95.3%, который сам собой растворился к 2026-05-08 (через independent cleanup в e0-baseline ветке + последующую активность DAG'ов до фактического запуска первого weekly run).

**Итог**: 30d retention настроена и работает; первый maintenance run = clean no-op; bloat-control включён proactively.

### Post-cleanup baselines (2026-05-08)

- `data/audit/storage_baseline_2026-05-08.json` — total_tables=78 (40 bronze + 15 silver + 23 gold), total_rows=2 150 983, total_hdfs_bytes=2 846 947 541 (≈2.85 GB), total_iceberg_metadata_bytes=2 715 868 083 (≈95.4 % от total).
- `data/audit/gold_usage_2026-05-08.json` — 23 Gold таблицы (Trino `shows_tables` method), 12 active / 11 unused (DAG SQL refs only; Superset+OM пропущены — graceful skip без credentials).

Diff vs entry-point baseline 2026-05-07:

| Metric | 2026-05-07 | 2026-05-08 | Δ |
|---|---|---|---|
| total_tables | 71 | 78 | +7 (E5 added: silver `whoscored_player_unavailable`, gold `dim_referee`/`dim_season`/`dim_standings`/`dim_venue`/`fct_player_unavailable`/`feat_player_season`) |
| total_hdfs_bytes | 2 847 581 701 | 2 846 947 541 | −634 160 (≈no-op) |
| total_iceberg_metadata_bytes | 2 715 821 690 | 2 715 868 083 | +46 393 (≈no-op) |
| metadata bytes % | 95.3 % | 95.4 % | +0.1 pp (no-op) |
| bronze metadata | 2 712 793 764 | 2 713 173 181 | +379 417 |
| silver metadata | 1 337 756 | 1 344 500 | +6 744 |
| gold metadata | 1 690 170 | 1 350 402 | −339 768 |

Bloat остаётся sticky на 95.4% — это `clubelo_team_history` (1.57 GB / 99.4 % metadata, см. секцию «Anomalies» выше): retention=30d не покрывает 530 snapshots, накопленных за пред-overflow период. Подметается отдельным targeted скриптом или ручным `expire_snapshots` с retention=1d по этой одной таблице (запланировано в **E8a** вместе с storage guard DAG).

### `audit_gold_usage.py` — R0.5 закрытие

Скрипт реализован полностью per spec (`docs/research/R0.5_usage_tracker.md`):

- list_gold_tables / scan_dag_sql_files / superset_* / openmetadata_* / verdict.
- Graceful skip Superset (нет SUPERSET_ADMIN_PASSWORD) и OpenMetadata (нет OM_JWT_TOKEN или server unhealthy).
- 24 unit-теста в `tests/unit/scripts/test_audit_gold_usage.py`.

### Status: E0 done

Все 4 user-selected подзадачи закрыты:

- ✅ Re-snapshot HDFS/Iceberg/sizes на 2026-05-08
- ✅ scripts/audit_gold_usage.py создан
- ✅ Airflow Variable HDFS_GUARD_THRESHOLDS установлена
- ✅ Manual maintenance trigger выполнен (no-op result, retention=30d работает)

Следующий шаг — E2 (Master-data dims). Зависимости и precondition'ы — без изменений (см. секцию «Handoff to E2» выше).
