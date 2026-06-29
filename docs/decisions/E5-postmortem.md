# E5 — Player Availability postmortem

**Этап завершён**: 2026-05-07
**Бюджет план/факт**: 2-3 дня / <1 дня (subagent parallelism)
**Ветка**: `feature/medallion-e5-availability`

---

## Что сделано

| # | Задача | Артефакт |
|---|---|---|
| T1 | Silver CTAS `whoscored_player_unavailable` (dedup, фильтр `reason != 'International duty'`, `status='confirmed'`) | `dags/sql/silver/whoscored_player_unavailable.sql` |
| T2 | Gold `fct_player_unavailable` + empty-fallback стаб для пустого Bronze | `dags/sql/gold/fct_player_unavailable.sql`, `dags/sql/gold/fct_player_unavailable_empty.sql` |
| T3 | `feat_team_form.unavailable_count_l5` — L5 rolling AVG с point-in-time mask (паттерн existing `l5_*_avg`) | `dags/sql/gold/feat_team_form.sql` |
| T4a | Silver DAG: новая task + DQ wrapper, `OPTIONAL_BRONZE` freshness gate соблюдён | `dags/dag_transform_fbref_silver.py` |
| T4b | Gold DAG: `fct_player_unavailable` в STAGE_3_FACTS + STAGE_3_FALLBACKS empty-stub | `dags/dag_transform_fbref_gold.py` |
| T5 | DQ checks (4 ERROR + 6 WARNING) в `validate_gold_quality()` и Silver-validator | `dags/utils/gold_tasks.py`, `dags/dag_transform_fbref_silver.py` |
| T6 | 16 unit-тестов (SQL-логика + DQ helpers) + 2 smoke import-теста | `tests/unit/sql/test_fct_player_unavailable_logic.py`, `tests/unit/dq/test_e5_checks.py`, `tests/integration/test_e5_dag_imports.py` |
| T8 | Simplification pass — мелкие правки SQL/DAG/`gold_tasks.py` | inline в файлах T1-T5 |
| T9 | Security audit — 0 issues | (no code change) |

**Pytest**: `pytest tests/unit/sql/test_fct_player_unavailable_logic.py tests/unit/dq/test_e5_checks.py -v` → **16 passed** на хосте.
Integration: 2/2 SKIPPED на хосте (требуют Trino + DAG bag), пройдут в контейнере.

## Архитектурные решения (подтверждены в коде)

- **D1** — `unavailable_count_l5 = AVG(unavailable_count) OVER (PARTITION BY team_id ORDER BY date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)` (LAG-mask первых 5 строк per partition исключает leakage; точно как existing `l5_goals_for_avg` etc.).
- **D2** — Двухуровневая Silver+Gold (НЕ Bronze→Gold напрямую). Cross-source `match_id` bridge: WhoScored game-string ⨝ FBref `match_id` через `dim_match.(date, home_team_id, away_team_id)`.
- **D3** — Фильтр `reason != 'International duty'` в Silver (intl-duty не предиктор для клубных матчей).
- **D4** — Orphan player_id permissive: `COALESCE(dim_player.player_id, 'ws_' || ws.player_id)` — ws_-orphan безопасно проходит до E1 production resolver (`xref_player`).
- **D5** — DQ severity:
  - **ERROR (raise AirflowException)**: PK uniqueness `(match_id, player_id)`, `no_nulls` на `(match_id, team_id, match_date)` в fct_match-context, `ref_integrity` к `dim_match.match_id`, `point_in_time` на `unavailable_count_l5` (NULL для первых 5 матчей per team).
  - **WARNING (Telegram только)**: `freshness 168h`, `value_range` (count 0-30), `coverage` row_count для team_id NULL и orphan player_id rate.
- **D6** — Scope strict: НЕ прокидываем `unavailable_count_l5` в `fct_match` / `predictions_input` (это E6 + E6 schema parity DQ).

## Что отложено

| Item | Куда отложено | Причина |
|---|---|---|
| **T7** OpenMetadata + Superset YAML metadata | phase-2-bi-catalog merge | `configs/openmetadata/` и `configs/superset/` отсутствуют в `master`, живут в `feature/phase-2-bi-catalog`. YAML draft сохранён в `/tmp/E5_fct_player_unavailable.yaml.deferred` — добавить в phase-2-bi-catalog после merge E5. Subagent попытался восстановить директории — отменено вручную. |
| **E6 propagation** — `fct_match.home_unavailable_count_l5` / `away_unavailable_count_l5` + `predictions_input` | E6 (Features + ML parity + predictions_input v2) | Roadmap гейтит features/ML cut-over отдельным этапом с predictions_input_v2 dual-run ≥2 недели. |
| **Production player resolver** `xref_player` (`fb_*` canonical) | E1 | E5 sneaks through на orphan `ws_*` IDs (R2 verdict: rejection 3.3% для WhoScored — well within bounds). |

## Что узнали (input для следующих этапов)

1. **Cross-source match_id bridge через `dim_match.(date, home_team_id, away_team_id)` работает** для APL-only scope, но cross-source `team_slug` расходится: Wolves/Wolverhampton, Spurs/Tottenham, Nott'm Forest/Nottingham Forest. На E5 это даёт `team_id` NULL для ~единиц rows — DQ row_count WARN (max 200 rows допустимо). **E1 `xref_team` + `_team_aliases.yaml` фиксят это полностью.**

2. **Player matching по `(player_name, season)` с `MIN(player_id)`** — детерминистично, но не корректно при name collisions (два игрока с одинаковым именем в одной лиге). Orphan fallback `ws_<id>` безопасен — downstream join'ы либо находят, либо помечаются NULL и фильтруются. **После E1 — заменить на JOIN через `silver.xref_player.canonical_id`.**

3. **`COALESCE(unavailable_count, 0)` в `feat_team_form` base CTE**: матчи без WhoScored coverage получают 0 (а не NULL) → будут смещать L5 average вниз для команд/сезонов вне coverage. Acceptable для E5 (APL only, full WhoScored coverage). **Long-term — двойная колонка `unavailable_data_present BOOLEAN` для явного nullable-marker.**

4. **Empty-fallback паттерн (`*_empty.sql` в STAGE_3_FALLBACKS)** успешно переиспользует STAGE_4_FALLBACKS infrastructure из `gold_tasks.py`. Подтверждает аддитивность: новые fact-таблицы с optional Bronze источниками получают graceful fallback без переписывания DAG-skeleton.

5. **T7 BI/catalog metadata конфликт между ветками**: `feature/phase-2-bi-catalog` и `feature/medallion-e5-availability` нельзя независимо лить изменения в `configs/openmetadata/` / `configs/superset/`. **Convention для будущих feature-веток**: каждая E-ветка в commit-message явно перечисляет какие BI/catalog YAML она требует, и до merge выравнивает с phase-2-bi-catalog (rebase или follow-up commit в phase-2-bi-catalog).

6. **Subagent edge-case**: subagent для T7 восстановил удалённые `configs/openmetadata/` директории из памяти — нужно строже передавать "ветка X не имеет директории Y, не пытайся её создавать". Lesson зафиксирован.

## DQ-baseline

- **Pre-merge**: pytest 16/16 PASSED, integration 2/2 SKIPPED (host).
- **Production baseline**: DAG ещё не запущен (PR не merged). После первого green-run заполнить:
  - row count `iceberg.gold.fct_player_unavailable` per `(league, season)` — APL 2425/2526 expected;
  - **orphan rate**: `COUNT(*) WHERE player_id LIKE 'ws_%' / COUNT(*)` (target ≤5%, R2 baseline 3.3%);
  - **team_id NULL ratio**: `COUNT(*) WHERE team_id IS NULL / COUNT(*)` (target <0.5%, чисто на cross-source-team mismatch);
  - **`unavailable_count_l5` distribution**: mean / p50 / p95 per season для feat_team_form;
  - **reason mix**: top-10 значений `reason` после dedup (sanity check).
- **Verification queries** — см. план `/root/.claude/plans/root-data-platform-docs-medallion-redes-dreamy-patterson.md` "Verification Plan" (5 SQL queries).

## Файлы

**Modified** (4):
- `dags/dag_transform_fbref_silver.py`
- `dags/dag_transform_fbref_gold.py`
- `dags/sql/gold/feat_team_form.sql`
- `dags/utils/gold_tasks.py`

**Created** (8):
- `dags/sql/silver/whoscored_player_unavailable.sql`
- `dags/sql/gold/fct_player_unavailable.sql`
- `dags/sql/gold/fct_player_unavailable_empty.sql`
- `tests/unit/sql/__init__.py`
- `tests/unit/sql/test_fct_player_unavailable_logic.py`
- `tests/unit/dq/__init__.py`
- `tests/unit/dq/test_e5_checks.py`
- `tests/integration/test_e5_dag_imports.py`
