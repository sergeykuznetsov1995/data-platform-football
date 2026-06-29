## E7 — BI + Catalog postmortem

**Этап завершён**: 2026-05-09 (shipped scope; **≥3 дня green DQ-gate** перед E8 — pending до 2026-05-12+)
**Бюджет план/факт**: 3-5 / **~1 день** (parallel-agent execution: T2/T4/T5 одновременно, T6/T7 после готовности скелетов)
**Ветка**: `feature/medallion-e7-bi-catalog` (от `feature/medallion-e6-features-ml-v2`, HEAD `7fd4b4e`)
**Reference**: `docs/decisions/E7-scope.md`, `docs/MEDALLION_REDESIGN_ROADMAP.md` L384-392

---

## Что сделано

| # | Задача | Owner-agent | Артефакт |
|---|---|---|---|
| T1 | Bootstrap ветки + scope-doc | `github-data-platform-manager` | `docs/decisions/E7-scope.md`, commit `9c19f3d` |
| T2 | 3 mart SQL CTAS + empty-stubs | `trino-specialist` | `dags/sql/gold/{mart_scouting_radar,mart_referee_dashboard,mart_event_heatmap}.sql` (+ `_empty.sql` fallback'и). Commits `1ac4e1b`, `5b13c04` (smoke fixes на live Trino) |
| T3 | Wire mart-таски в `dag_transform_fbref_gold` + `dag_master_pipeline` | `airflow-expert` | Commit `aac7c50` — 3 task'а в STAGE_4_FEATS / соответствующих TG, sequential after fct/dim TGs |
| T4 | Superset infra | `superset-dashboard-builder` | `configs/superset/{import_dashboards.py, dashboards/{scouting_radar,referee_dashboard,event_heatmap}.py, datasources.yaml, README.md}`, Makefile цели `superset-import`/`superset-dashboards`. Commits `5ea310f`, `7dad52a` (app-context fix) |
| T5 | OpenMetadata infra | `data-platform-architect` | `configs/openmetadata/{apply_descriptions.py, descriptions/*.yaml (31 файл — 7 dim + 12 fct + 5 feat + 3 mart + entity_xref + predictions_input_v2 + 2 fct_team/player_match), README.md}`, Makefile цели `om-apply-descriptions`/`om-ingest-trino`/`om-lineage-trino`. Commit `288f493` |
| T6 | Unit + integration тесты | `testing-agent` | 3 mart unit-тесты (`tests/unit/sql/test_mart_*.py`), apply-script тесты (`tests/unit/configs/test_{import_dashboards,apply_descriptions}.py`), DAG-imports (`tests/integration/test_e7_dag_imports.py`). Commit `087f2b4` |
| T7 | DQ extension в `gold_tasks.validate_gold_quality()` для 3 mart'ов | `data-quality-agent` | `no_duplicates` + `freshness` + `value_range` + `point_in_time` checks. Commit `876f246` |
| T8 | Postmortem + roadmap update + memory | manual | этот файл (для shipped scope; ≥3d green-gate отдельно) |
| Misc | Pre-existing infra cleanup | self | `b751f0f` (Superset+OM compose services), `1ab4e4a` (BI-catalog tests), `cbd485c` (WhoScored migrate to FlareSolverr), `959e4b3` (FotMob env propagation), `92a10dc` (scrapers exit codes), `3fe16a6` + `1029780` (clubelo metadata bloat fix + HIGH_CHURN_BRONZE), `43b0a14` (merge phase-2-bi-catalog) |

**Materialized counts (live Trino, 2026-05-09)**:

| Таблица | Rows | DoD threshold | Status |
|---|---|---|---|
| `iceberg.gold.mart_scouting_radar` | **100,793** | ≥800 | ✅ ×125 over |
| `iceberg.gold.mart_referee_dashboard` | **218** | ≥40 | ✅ ×5 over |
| `iceberg.gold.mart_event_heatmap` | **25,895** | ≥80 | ✅ ×323 over |

**Tests** (host pytest на ветке): 483/488 passed (3 mart SQL test'а DuckDB-bridge + apply scripts + integration DAG-imports).

---

## Архитектурные решения (подтверждены кодом)

- **D1 — Денормализованные mart'ы для прямого Superset usage**. `mart_scouting_radar` JOIN'ит `dim_player.name`, `dim_team.name`, `dim_match.date` в строки → Superset не делает cross-dataset JOIN'ы (быстрее charts, меньше Trino-load). Аналогично `mart_referee_dashboard` (referee_name, league/season). Альтернатива (Superset с virtual-table на runtime JOIN) отвергнута: SQLLab latency на dim-fct join hot-path > рекомендуемого 2s SLO.
- **D2 — Empty-stub fallback'и для каждого mart'а**. `mart_*_empty.sql` зарегистрированы в `STAGE_4_FALLBACKS` (по аналогии с feat_team_xg_form / feat_team_event_style из E5/E6). Если upstream Silver/Gold отсутствует — runner вместо crash материализует schema-valid пустую таблицу. Гарантирует idempotency `dag_transform_fbref_gold` без race conditions при cold-start.
- **D3 — Idempotent REST clients для Superset/OpenMetadata** (`import_dashboards.py`, `apply_descriptions.py`). CREATE-IF-NOT-EXISTS pattern + `--dry-run` mode + JWT auth (OpenMetadata) / Bearer (Superset). Re-run на shipped state = no-op (existing dashboards/descriptions detected by slug). Без этого re-import каждый раз дублировал бы dashboards / валил FQN-conflict в OM.
- **D4 — OpenMetadata relationships декларативно в YAML** (поле `relationships:` per таблица). `apply_descriptions.py` парсит список FK → создаёт edges через `/v1/relationships/entityRelationships` REST API. Альтернатива (UI-clicks) отвергнута: 31 таблица × ~2-3 FK каждая = ~70 manual edges, не воспроизводимо.
- **D5 — Superset datasets в YAML, dashboards в Python**. `datasources.yaml` — declarative source-of-truth (table FQN, column types, metrics). `dashboards/*.py` — programmatic Dashboard SDK (chart specs, filters, layout). Гибрид: data-engineers редактируют YAML; BI-инженеры — Python. Меньше boilerplate в YAML, больше типобезопасности в Python.
- **D6 — `mart_event_heatmap` zone grid 12×8 = 96 buckets**. SPADL-compliant (FIFA Connect compatible) — match WyScout/StatsBomb spatial conventions. Per-(team, season, league, zone, action) aggregates → готов под heatmap viz без дальнейшего pivot'а в Superset.
- **D7 — DQ для mart'ов лёгкий** (no_duplicates + freshness + 1 value_range + 1 point_in_time per mart). Mart'ы — derived от уже валидированных Gold facts/features; повторять весь чейн ref_integrity / value_range избыточно. Heavy-validation остаётся на Gold-уровне.

---

## DQ baseline (post-smoke)

DQ ERROR=0 на shipped state. WARNINGs ≤8 (наследие E4 ref_integrity bug в `data_quality.py` — не E7 issue, отслеживается отдельно).

`pytest tests/unit/sql/test_mart_*.py tests/unit/configs/ tests/integration/test_e7_dag_imports.py`: GREEN.

**≥3 дня green DQ-gate** (стандартный roadmap-gate перед E8 T1 expansion) — **открыт до 2026-05-12+**. Trigger gate: 3 successive `dag_master_pipeline` runs без DQ-ERRORs на E7 marts + downstream features.

---

## Что отложено

| Item | Куда | Причина |
|---|---|---|
| **`mart_manager_profile`** | Phase 1.5 | `dim_manager` STUB пуст (R0.2c FALLBACK после R0.2 recon 2026-05-08); mart без dim бессмысленен |
| **E8 T1 expansion (UCL/Top-5/EL)** | После E7 ≥3d green-gate | Roadmap-gate; HDFS / DQ stability ground |
| **Superset Telegram alerts через `dag_superset_alerts.py`** | Уже shipped в `dag_superset_alerts.py` (pre-existing) | Конфиг через Airflow Variable `superset_alerts_config` (JSON) — не тронут E7 |
| **OpenMetadata Trino lineage ingestion** | Daily cron | `make om-lineage-trino` — UI-trigger stub; ручной запуск каждый день для now |
| **R8 referee fuzzy match cross-locale** | Tier 2 spike | APL referee dataset не имеет non-ASCII names → откладывается до E8a (UCL/Top-5 принесёт Çakir/García/Müller) |
| **`mart_match_outcomes` rolling refresh** | Pre-existing | Уже materialized через `dag_transform_fbref_gold` STAGE_3; не E7 scope |

---

## Что узнали (input для следующих этапов)

1. **Empty-stub pattern всё-ещё нужен на mart-уровне**, даже когда upstream Gold/Silver shipped. Stage-4 fallbacks `feat_team_xg_form_empty` / `feat_team_event_style_empty` применили к mart'ам идентичный template — runner cleanly падает в empty-CTAS если соответствующая Silver-таблица пуста, вместо обвала dependency chain.
2. **Superset SDK Python dashboards >> JSON exports**. Pre-existing dashboard JSONs (`team_form_overview`, `match_outcomes`) deprecated в пользу Python-spec'ов. Diff-friendly, type-safe, программируемый layout. Future dashboards следуют Python-only convention.
3. **OpenMetadata `relationships:` field в YAML — критичен для ER-diagram lineage**. Без явных FK-edges OpenMetadata показывает таблицы изолированно, без визуализации связей dim←fct←mart. 70+ декларативных edges в 31 YAML — minimal effort vs UI clicks.
4. **`scrapers/__init__.py` heavy import — все ещё избегаем**. `import_dashboards.py` / `apply_descriptions.py` standalone (только requests/yaml), не зависят от scrapers/. Запускаются Makefile-ом или прямо `python configs/...` без Airflow runtime.
5. **Mart row counts разнятся на 3 порядка**: scouting (100K) vs heatmap (26K) vs referee (218). DoD-thresholds должны быть relativе к ожидаемой cardinality (player×match >> referee×season). Roadmap E7 thresholds (≥800 / ≥40 / ≥80) — намеренно conservative; реальные numbers выше в 5-300×.

---

## Followups (отложено в bookkeeping, не блокирует merge)

1. **≥3 дня green DQ-gate** (target 2026-05-12+) перед E8a UCL expansion. Manual gate: 3 successive successful master_pipeline runs без DQ-ERRORs.
2. **E1.5 cutover** (target 2026-05-12+) — `gold.entity_xref` → `silver.xref_*` references в `dim_match`/`dim_player`/`dim_team`/`feat_*`. Зависит от ≥3 дней green-parity на FBref-only subset (E1 D5 dual-run policy).
3. **`predictions_input_v2` cutover** (target 2026-05-23+) — `airflow variables set predictions_serving_active_version v2` после ≥2 недель dual-run (E6 F2).
4. **R2-followup resolver fix** — multi-season spine team-bucket bug (см. `E1-postmortem.md` § "Followup #5"); orphan rate Understat 16.2% → ~4% expected. ~3-4 часа кода + multi-season transfer unit-тесты.
5. **`data_quality.CHECK.ref_integrity` Trino alias bug** — 6+ WARN'ов в E4/E7 DQ из-за parent-table alias 'p' resolution; общесистемный fix в `dags/utils/data_quality.py`.
6. **Storage cleanup baseline** (Task 2 этой итерации) — re-snapshot `data/audit/storage_baseline_2026-05-09.json` после maintenance DAG run; сравнение с `2026-05-08.json`.

---

## Files touched (E7 net diff vs `dae7014`)

**Created** — 3 mart SQL + 3 stub'ы, 3 dashboards, `import_dashboards.py`, 31 OpenMetadata YAML, `apply_descriptions.py`, README.md в обоих configs/, 6 test файлов, `E7-scope.md`, `E7-postmortem.md` (this file).

**Modified** — `dag_transform_fbref_gold.py` (+3 mart task'а), `dag_master_pipeline.py` (mart trigger), `gold_tasks.py` (+mart DQ), `data_quality.py` (mart DQ checks), `Makefile` (+5 superset/OM целей).

**NOT touched** — pre-existing E1-E6 объекты (dashboards, descriptions, dims/facts/features). E7 строго аддитивный.

---

## Pre-deploy checklist

1. ✅ 3 mart-таблицы материализованы row counts >> threshold (100K/218/26K).
2. ✅ Pytest 483/488 PASSED (host) — 5 skipped — Airflow-only.
3. ✅ 31 OpenMetadata YAML descriptions в репо.
4. ✅ 3 Superset dashboards декларативно в Python.
5. ✅ Makefile цели `superset-{import,dashboards}` / `om-{apply-descriptions,ingest-trino,lineage-trino}` готовы.
6. ⚠️ ≥3 дня green DQ-gate — **opened**, target close 2026-05-12+ (3 successful master_pipeline runs).
7. ⚠️ Superset / OpenMetadata UI smoke — manual verify через `make up-bi` / `make up-catalog` после merge.
8. ⚠️ Image rebuild не требуется (T7 не добавлял pip deps).
