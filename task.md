# Backlog: cross-source player unification

Дата создания: 2026-05-15.
Принципы решения конфликтов атрибутов: per-source колонки без winning-value логики.
Зерно профилей: `(player_id, league, season)`.

## Status

| ID  | Subject                                                          | Status              |
|-----|------------------------------------------------------------------|---------------------|
| T1  | Silver FotMob per-season profiles (player + keeper)              | ✅ Done 2026-05-15  |
| T2  | Research-spike: архитектура multi-source player unification (RX) | ✅ Done 2026-05-15  |
| T3  | FotMob → `silver.xref_player` (resolver extension)               | ✅ Done 2026-05-15  |
| T4  | `silver.fotmob_player_profile` + `gold.dim_player_attributes`    | ✅ Done 2026-05-15  |
| T5  | `gold.fct_player_season_stats` (общая stats-витрина)             | ✅ Done 2026-05-15  |

Легенда: ✅ done · 🔄 in progress · ⏳ open · ⏸ blocked.

---

## T1 ✅ — Silver FotMob per-season profiles (две таблицы)

**Что**: материализовать в Silver per-season профили FotMob, симметрично с FBref (где есть `fbref_player_season_profile` + `fbref_keeper_profile`). Полевые игроки и вратари — в разных таблицах.

**Источники Bronze**:
- `iceberg.bronze.fotmob_player_stats` (10 162 row, long-формат — `participant_id × stat_name`)
- `iceberg.bronze.fotmob_player_details` (572 row)

**Зерно**: `(player_id, league, season)`. Bronze coverage = только сезон 2025, одна лига `ENG-Premier League`.

**Принцип схемы**: в этих таблицах — ТОЛЬКО per-season данные. Time-invariant атрибуты игрока (birth_date, height, foot, country) и snapshot-like (market_value, contract_end) НЕ хранятся — уйдут в `silver.fotmob_player_profile` (T4). Pass-through JSON удалены — raw json остаётся в Bronze.

**Фильтры на уровне SQL**:
- `NOT is_coach` — выкидывает 20 тренеров (FotMob `/api/playerData` возвращает их в той же таблице).
- `LOWER(primary_position) = 'keeper'` → `silver.fotmob_keeper_profile`.
- `LOWER(primary_position) <> 'keeper'` → `silver.fotmob_player_season_profile`.

**Нормализация position**: `LOWER(json_extract_scalar(position_description, '$.primaryPosition.label'))` — FotMob отдаёт inconsistent case (`Keeper` vs `keeper`). Trino не имеет `INITCAP`, capitalize делается на BI-слое.

### `silver.fotmob_player_season_profile` (полевые, 487 row)

**Архитектура SQL** (2 CTE): dedup `details` → PIVOT `stats` из long в wide → LEFT JOIN. JOIN-ключ: `CAST(participant_id AS VARCHAR) = player_id`.

**Колонки** (~42):
- Identity (5): `player_id`, `player_name`, `primary_position`, `primary_team_id`, `primary_team_name`
- Volume + Top Stat (6): `matches_played`, `minutes_played`, `goals`, `assists`, `goals_assists`, `fotmob_rating`
- Attacking (16): `goals_per_90`, `expected_goals(_per_90)`, `expected_goals_on_target`, `expected_assists(_per_90)`, `xg_xa_per_90`, `shots_per_90`, `shots_on_target_per_90`, `chances_created`, `big_chances_created/missed`, `accurate_passes_per_90`, `accurate_long_balls_per_90`, `successful_dribbles_per_90`, `penalties_won`
- Defending (8): `defensive_actions_per_90`, `tackles_per_90`, `interceptions_per_90`, `clearances_per_90`, `recoveries_per_90`, `blocks_per_90`, `poss_won_final_third_per_90`, `penalties_conceded`
- Discipline (3): `yellow_cards`, `red_cards`, `fouls_per_90`
- Lineage/partition (4): `_bronze_ingested_at`, `_silver_created_at`, `league`, `season`

### `silver.fotmob_keeper_profile` (вратари, 65 row)

**Колонки** (~21):
- Identity (4): `player_id`, `player_name`, `primary_team_id`, `primary_team_name` (без `primary_position` — все `keeper`)
- Volume (3): `matches_played`, `minutes_played`, `fotmob_rating`
- Goalkeeping (5): `clean_sheets`, `goals_conceded_per_90`, `save_percentage`, `saves_per_90`, `goals_prevented`
- Distribution (2): `accurate_passes_per_90`, `accurate_long_balls_per_90`
- Discipline (3): `yellow_cards`, `red_cards`, `fouls_per_90`
- Lineage/partition (4)

### Артефакты

- `dags/sql/silver/fotmob_player_season_profile.sql`
- `dags/sql/silver/fotmob_keeper_profile.sql`
- `dags/dag_transform_fotmob_silver.py` — два sequential CTAS + row_count + DQ
- DQ-чеки (ERROR): PK uniqueness `(player_id, league, season)`, `no_nulls(player_id, league, season)`, `row_count` (≥450 / ≥40); (WARNING): freshness 48h, value_range для key метрик.

### Acceptance — выполнен

- 487 row outfield + 65 row keeper (Bronze 572 − 20 coach = 552, разделены на две таблицы).
- 100% DQ pass для ERROR-severity чеков.
- Identity-блок заполнен на 100%.
- Inconsistent position case устранён (`Keeper`/`keeper` → `keeper`).

**Зависимости**: нет.

**Статус**: DONE 2026-05-15.

---

## T2 ✅ — research-spike: архитектура multi-source player unification (RX)

**Что**: спроектировать решение для трёх связанных задач, которые пользователь хочет получить дальше:
1. Профили игроков со статистикой по каждому источнику (fbref, fotmob, потом sofascore/transfermarkt/...).
2. Словарь игроков с базовой информацией (height, weight, гражданство и т.п.) и общим `player_id_canonical`, единым для fbref и fotmob (и масштабируемым на новые источники).
3. Общая stats-витрина fbref+fotmob, где характеристики игрока **не дублируются**, а статистика остаётся per-source.

**Зачем research, а не сразу писать SQL**:
- xref_player сейчас FBref-spine, не рассчитан на N источников без переписывания. R2-followup улучшил resolver для understat/whoscored, но fotmob явно отложен (`docs/decisions/R2-followup-implementation-plan.md`).
- Per-attribute стратегия (где fotmob — primary для height, fbref — для season-stats) не зафиксирована.
- Архитектурный выбор «словарь в Silver vs Gold», зерно словаря, формат per-source колонок (длинный vs широкий) не очевиден.
- Каскад: что ломается при подключении нового источника?

**Вопросы для исследования**:
1. **Resolver масштабируемость**. Как должна выглядеть процедура подключения N+1 источника к `silver.xref_player`? Что общего, что специфично? Хватит ли текущего паттерна `_fetch_<source>_players()` или нужна обобщённая регистрация sources через декларативный yaml?
2. **Словарь игроков**. Где жить — Silver `dim_player_attributes` или Gold `dim_player` расширить? Какое зерно — `(player_id_canonical)` snapshot или `(player_id_canonical, source)` long? Какие атрибуты обязательные (height/weight/nation/foot/dob) и какие источники их отдают?
3. **Общая stats-витрина**. Формат хранения per-source колонок: `goals_fbref`/`goals_fotmob` (wide) vs `(player, season, source, metric, value)` (long). Trade-off по объёму, BI-удобству, добавлению новых источников.
4. **Каскад**. При подключении нового источника — какие таблицы/DAG'и должны перематериализоваться? Документировать explicit dependency graph.

**Артефакты T2**:
- `docs/research/RX_cross_source_player_profile.md` — findings, options, рекомендация (формат как `docs/research/R2-followup_resolver_v2.md`).
- `docs/decisions/RX-implementation-plan.md` — step-by-step roadmap для T3/T4/T5 (формат как `docs/decisions/R2-followup-implementation-plan.md`).
- Добавить T3/T4/T5 в этот же `task.md` после ревью research.

**Acceptance**:
- Two markdown artifacts в `docs/`.
- В `task.md` появились T3/T4/T5 с конкретными SQL-файлами, зерном, acceptance criteria.
- Решение по каждому из 4 вопросов выше зафиксировано (даже если answer = "deferred").

**Зависимости**: нет. Параллельно с T1.

**Артефакты T2 — shipped 2026-05-15**:
- `docs/research/RX_cross_source_player_profile.md` — 7-секционный research с attribute matrix (10 атрибутов × 7 источников) и metric overlap inventory (HARD_FACT/MODELED/UNIQUE classification).
- `docs/decisions/RX-implementation-plan.md` — 16-step roadmap для T3/T4/T5 с временами, файлами, acceptance.

**Ключевые решения (D1-D4 из research)**:
1. **Two-grain split**: словарь per-`(player_id_canonical)` snapshot, stats per-`(player_id_canonical, league, season)`.
2. **Silver per-source → Gold merged**: новая `silver.fotmob_player_profile` (T4) + `gold.dim_player_attributes` через `silver.xref_player`.
3. **Wide single-column stats** с FBref primary для HARD_FACT + audit-колонки `<metric>_diff_fotmob`; lazy suffix policy (суффиксы `_fbref/_fotmob` только при MODELED conflict).
4. **FotMob к v1 resolver** (token_sort ≥90) без ожидания R2-followup.

**Статус**: DONE 2026-05-15.

---

## T3 ✅ — FotMob → `silver.xref_player` (resolver extension)

**Что**: подключить FotMob к cascade resolver в `silver.xref_player` через существующий tier-2 (`token_sort_ratio ≥ 90`). FBref-spine без изменений.

**Артефакты**:
- `dags/utils/xref_player_resolver.py` — новая функция `_fetch_fotmob_players()` (по образцу `_fetch_whoscored_players()`), расширение `SOURCES` tuple, orphan prefix dict (`'fotmob': 'fm'`).
- `dags/utils/xref_dq.py` — расширение `source` enum allow-list на `'fotmob'`.
- `tests/unit/utils/test_xref_player_resolver.py` — расширение `KNOWN_PAIRS` 10+ FotMob-парами (Haaland, Saka, etc.).

**Зерно**: `(source, source_id, season)` — уже зафиксировано в `silver.xref_player`.

**Acceptance**:
- FotMob rows в `silver.xref_player` ≥500 (APL 2526).
- Orphan rate ≤10% (WARNING threshold; <3% — bonus после R2-followup).
- 10/10 known-pair regression test GREEN (включая FotMob пары).
- DQ source enum allow-list updated, `pytest tests/unit/dq/test_xref_dq.py` PASS.
- `airflow tasks test dag_transform_xref xref_player_resolver 2026-05-15` GREEN.

**Зависимости**: T1 ✅ (FotMob player_id pool из Bronze ready).

**Source plan**: `docs/decisions/RX-implementation-plan.md` Steps 1-5.

### Результаты (shipped 2026-05-15)

- `silver.xref_player`: **551 FotMob rows** (close to plan ~552, разница 1 — dedupe на (player_id, league, season)).
- Confidence distribution: `name_team=447`, `name_team_surname=13`, `name_team_subset=7`, `orphan=84`.
- **Orphan rate: 15.25%** (выше планового WARNING ≤10%, но **0 ERROR**). 43/84 orphans = U21-команды (Arsenal U21, Aston Villa U21 и т.д.), которых FBref не покрывает для APL. Остальные — резервные вратари / ушедшие игроки. Дальнейшее снижение — задача R2-followup resolver v2.
- **Known-pair regression: 10/10 GREEN.**
- DQ: 35/37 checks passed (0 ERROR, 2 WARNING). WARNINGs: `bridge_coverage[xref_match.fotmob/sofascore]` (не T3-related) + `orphan_rate[xref_player.fotmob]` (15.25% > 10%).
- Duration: 12.26s end-to-end.

### Артефакты — shipped

- `dags/utils/xref_player_resolver.py`: новая функция `_fetch_fotmob_players()` (read from `bronze.fotmob_player_details`, filter `NOT is_coach`, slug conversion via `_fbref_year_to_slug`); SOURCES += 'fotmob'; `_orphan_prefix` + 'fm'; `_resolve_all` signature += `fm_rows`; `_verify_known_pairs` обобщён до «'fbref' in sources and len ≥3»; `run_resolver` вызывает FotMob reader.
- `dags/utils/xref_dq.py`: `allowed_sources` для `xref_player` и `xref_player_review` extended на 'fotmob'; `canonical_id_format` regex extended на `fm_` prefix.
- `tests/unit/utils/test_xref_player_resolver.py`: добавлен `test_fotmob_cascade_match`, signature-обновления в `test_end_to_end_minimal` / `test_orphan_prefix_per_source`; `test_unknown_source_raises_keyerror` теперь использует 'transfermarkt' вместо 'fotmob' (последний теперь валидный).
- 227/227 unit-тестов GREEN на хосте; container smoke `airflow tasks test dag_transform_xref xref_player 2026-05-15` SUCCESS.

---

## T4 ✅ — `silver.fotmob_player_profile` + `gold.dim_player_attributes`

**Что**: материализовать FotMob time-invariant атрибуты (dob, foot, height, nationality) в Silver; собрать общий per-player словарь в Gold с per-source колонками.

**Зерно**:
- Silver: `(player_id, league, season)` — для symmetry с другими FotMob Silver таблицами (хотя атрибуты сами time-invariant).
- Gold: `(player_id_canonical)` snapshot — атрибуты не меняются со временем, сезонные строки = шум.

**Артефакты**:
- `dags/sql/silver/fotmob_player_profile.sql` — CTAS из `bronze.fotmob_player_details` (JSON extraction для foot/nation/dob) + LEFT JOIN на `bronze.fotmob_team_squad.height_cm`.
- `dags/dag_transform_fotmob_silver.py` — третий CTAS task (после `build_player_season_profile` и `build_keeper_profile`).
- `dags/sql/gold/dim_player_attributes.sql` — CTAS из `silver.xref_player` + `silver.fbref_player_season_profile` + `silver.fotmob_player_profile`.
- `dags/dag_transform_fbref_gold.py` — task `build_dim_player_attributes` после существующего `build_dim_player` (НЕ заменяя его).

**Колонки `gold.dim_player_attributes`** (per-source, без winning logic):
- Identity: `player_id_canonical`, `player_name_canonical` (COALESCE FBref → FotMob).
- Attributes: `born_year_fbref`, `dob_fotmob`, `nationality_fbref`, `nationality_fotmob`, `height_cm_fotmob`, `foot_fotmob`.
- Lineage: `_gold_created_at`.

**Acceptance**:
- Silver: ~552 outfield rows (572 Bronze − 20 coaches), `height_cm` filled ≥85% (после squad JOIN).
- Gold: ≥562 rows (FBref spine), `height_cm_fotmob` filled ≥80%, `foot_fotmob` ≥95%, `dob_fotmob` ≥95%.
- DQ ERROR: PK uniqueness, `no_nulls(player_id_canonical)`, ref_integrity к `silver.xref_player`.
- DQ WARNING: `value_range(height_cm, 140, 220)`, `coverage(height_cm, 80, 95)`.

**Зависимости**: T3 (FotMob в xref_player нужен для JOIN в Gold).

**Source plan**: `docs/decisions/RX-implementation-plan.md` Steps 6-9. Локальный план (после T3 done): `/root/.claude/plans/recursive-herding-stream.md`.

### Результаты (shipped 2026-05-15)

- `silver.fotmob_player_profile`: **552 rows** (572 Bronze − 20 coaches). Coverage: `height_cm` 94.4% (521/552), `date_of_birth` 100%, `foot` 97.3%, `nationality` 100%, `country_code` 100%. Источники: `bronze.fotmob_team_squad` (4 поля структурно) + `bronze.fotmob_player_details.player_information_json` (только `foot`).
- `gold.dim_player_attributes`: **1244 rows** (FBref-spine, все сезоны). Coverage FotMob 39-40% (Bronze покрывает только APL 2025; FBref-spine — 9 сезонов). 0 orphans vs `silver.xref_player`. 7/7 T4 DQ checks GREEN.
- `gold.dim_player` (existing, 5338 rows) НЕ изменён — additive deploy.
- Render-smoke tests: 15/15 PASS (`tests/unit/sql/test_fotmob_player_profile_render.py` + `test_dim_player_attributes_render.py`).

### Артефакты — shipped

- `dags/sql/silver/fotmob_player_profile.sql` (new). JSON-extract pattern для `foot`: `element_at(map_from_entries(transform(...)), 'Preferred foot')` — JSONPath filter `[?(...)]` и correlated UNNEST в Trino не поддерживаются.
- `dags/sql/gold/dim_player_attributes.sql` (new). Snapshot per `canonical_id`, FBref-spine; FotMob bridge через xref_player (latest season per `canonical_id` через `ROW_NUMBER`).
- `dags/dag_transform_fotmob_silver.py`: добавлен 3-й transform `player_profile` + 7 DQ checks.
- `dags/dag_transform_fbref_gold.py`: добавлен task `dim_player_attributes` в `STAGE_2_DIMS`.
- `dags/utils/gold_tasks.py`: 7 DQ checks в `validate_gold_quality()` + row_count floor в `validate_gold_row_counts()`.

### Lessons learned (для T5)

- `xref_player.season` — `varchar '2526'`; Silver таблицы — `bigint 2025`. Snapshot-зерно через `MAX_BY(... ORDER BY season DESC)` per source избавляет от season-type mapping.
- FBref не имеет `confidence='orphan'` (все 'exact' по построению); фильтр оставлен для symmetry.
- FotMob coverage в Gold ограничена Bronze покрытием — для повышения нужно расширить FotMob Bronze на исторические сезоны (R3 scope).

---

## T5 ✅ — `gold.fct_player_season_stats` (общая stats-витрина)

**Что**: единая Gold-таблица per-(player, season) с метриками FBref+FotMob. Wide single-column с audit-колонками для HARD_FACT расхождений.

**Зерно**: `(player_id_canonical, league, season)`.

**Артефакты**:
- `dags/sql/gold/fct_player_season_stats.sql` — CTAS из `silver.xref_player` + `silver.fbref_player_season_profile` + `silver.fotmob_player_season_profile`.
- `dags/sql/gold/fct_keeper_season_stats.sql` — аналог для голкиперов (~65 rows).
- `dags/dag_transform_fbref_gold.py` — два новых task'а после `build_dim_player_attributes`.

**Принцип колонок** (из research D3):
- **HARD_FACT overlap** (21 метрика — goals, assists, minutes, cards, penalties): **single column**, `COALESCE(fbref, fotmob)` с FBref primary; audit `<metric>_diff_fotmob` для WARNING (abs > 1 для >5% rows).
- **UNIQUE_FBREF** (15 метрик — complete_matches, plus_minus, shots, crosses, etc.): single column.
- **UNIQUE_FOTMOB** (16 метрик — xG, xA, big_chances, fotmob_rating, defensive_actions, etc.): single column.
- **Lazy suffix policy**: суффиксы `_fbref/_fotmob` только когда появится MODELED conflict (Sofascore xG, future FBref shot revival).

**Acceptance**:
- ~487 outfield rows для APL 2526 (FBref spine).
- ~65 keeper rows.
- `goals_diff_fotmob` ABS > 1 для <5% rows.
- `minutes_diff_fotmob` ABS > 90 для <5% rows.
- DQ ERROR: PK uniqueness, ref_integrity к `gold.dim_player_attributes`.
- DQ WARNING: audit diff distribution (p95 thresholds — измерить и закрепить в R1 после shipping).

**Зависимости**: T3 (canonical_id) + T4 (dim_player_attributes для ref_integrity).

**Source plan**: `docs/decisions/RX-implementation-plan.md` Steps 10-16. Локальный план: `/root/.claude/plans/root-data-platform-task-md-recursive-dawn.md`.

### Результаты (shipped 2026-05-15)

- `gold.fct_player_season_stats`: **2551 rows** (FBref-spine across 9 сезонов APL × ~280 outfield). Acceptance план был ~487 для APL 2526 — фактически витрина шире, потому что spine = весь FBref Bronze, а FotMob LEFT JOIN покрывает только 2526 (NULL для остальных).
- `gold.fct_keeper_season_stats`: **204 rows** (~25 keepers × 9 сезонов).
- DQ: **23/23 T5 checks PASS** (0 ERROR, 0 T5 WARNING). Audit diff distribution `<5% beyond threshold` план — фактически:
  - matches/minutes/goals/assists: 99.5-100% within
  - yellow_cards: 99.3% within (17 outliers — известное расхождение в трактовке second yellow между источниками)
  - red_cards/penalties_won/penalties_conceded: 100% within
  - keeper matches/minutes/clean_sheets: 100% within
- Container smoke: `airflow tasks test ... fct_player_season_stats` → 2551 rows за <1s; `... fct_keeper_season_stats` → 204 rows; `validate_gold_row_counts` 15/15 pass; `validate_gold_quality` 76/77 pass (единственный WARN — давний `dim_standings.team_id`, не T5).
- Render-smoke tests: **22/22 PASS** на хосте (`tests/unit/sql/test_fct_player_season_stats_render.py` + `test_fct_keeper_season_stats_render.py`); 377/377 SQL unit-тестов GREEN — нет регрессий.

### Артефакты — shipped (4 таблицы Gold)

**Business-витрины** (без audit-метаданных):
- `dags/sql/gold/fct_player_season_stats.sql` — FBref-spine + FotMob bridge через `silver.xref_player`, season slug '2526' → bigint 2025 идиомой `2000 + CAST(SUBSTR(season, 1, 2) AS BIGINT)` (см. `fct_card.sql:51`). Outfield filter `pos NOT LIKE '%GK%'`. **8 HARD_FACT COALESCE + 12 UNIQUE_FBREF + 14 UNIQUE_FOTMOB** (без diff-колонок).
- `dags/sql/gold/fct_keeper_season_stats.sql` — `INNER JOIN silver.fbref_keeper_profile` (он уже фильтрует pos LIKE '%GK%'). 5 HARD_FACT + 13 UNIQUE_FBREF + 5 UNIQUE_FOTMOB. `save_pct_fbref` / `save_percentage_fotmob` отдельные колонки (шкалы могут различаться).

**DQ-audit таблицы** (только для observability, не для BI):
- `dags/sql/gold/fct_player_season_stats_audit.sql` — INNER JOIN на оба источника → row только где обе стороны имеют запись. ТОЛЬКО PK + 8 diff-колонок + lineage. Никаких business-метрик.
- `dags/sql/gold/fct_keeper_season_stats_audit.sql` — аналог для GK, 3 diff-колонки.

**Wiring + DQ:**
- `dags/dag_transform_fbref_gold.py` STAGE_2_DIMS: 4 новых task'а (main fct >> audit) после `dim_player_attributes`.
- `dags/utils/gold_tasks.py`:
  - `validate_gold_row_counts()`: floors `fct_player_season_stats ≥400` / `fct_keeper_season_stats ≥50` / `fct_player_season_stats_audit ≥100` / `fct_keeper_season_stats_audit ≥10`.
  - `validate_gold_quality()`: business fct — 6 ERROR (PK + ref к dim_player_attributes); audit — 6 ERROR (PK + ref к main fct) + **11 WARNING coverage** (`error_threshold=0.0` — audit это observability, не gate; ERROR ломал бы DAG при нормальных NULL расхождениях).

**Тесты:**
- `tests/unit/sql/test_fct_player_season_stats_render.py` (11 tests): spine, outfield filter, 8 HARD_FACT COALESCE, **отсутствие** diff-колонок, UNIQUE_*.
- `tests/unit/sql/test_fct_keeper_season_stats_render.py` (12 tests): аналог + INNER JOIN check + save_pct separate.
- `tests/unit/sql/test_fct_player_season_stats_audit_render.py` (8 tests): INNER JOIN обоих источников, 8 diff-колонок, **отсутствие** business-метрик.
- `tests/unit/sql/test_fct_keeper_season_stats_audit_render.py` (7 tests): аналог.
- `tests/integration/test_t5_dag_imports.py`: 4 task'а присутствуют, audit идёт ПОСЛЕ main fct (ref_integrity ordering).

### Результаты после рефакторинга (2026-05-15 v2)

| Таблица | Rows | Природа |
|---|---:|---|
| `fct_player_season_stats` | 2551 | business, FBref-spine 5 сезонов |
| `fct_keeper_season_stats` | 204 | business, GK FBref-spine |
| `fct_player_season_stats_audit` | 439 | DQ, INNER JOIN пересечение |
| `fct_keeper_season_stats_audit` | 39 | DQ, INNER JOIN пересечение |

DQ: **82/83 passed, 0 ERROR, 1 WARN** (тот WARN — давний `dim_standings.team_id`, не T5).

**Audit-coverage по реальной выборке** (где обе стороны имеют значение):
- matches/minutes: 97.3-97.5% within ±1/±90
- goals/assists: 99.5-99.8%
- yellow_cards: 96.1%
- red_cards/penalties: 100% (NULL diff = "не сравнивали", не ошибка)
- keeper matches/minutes/clean_sheets: 100%

### Lessons learned

- **Audit-метаданные в отдельной таблице.** Изначальный дизайн смешивал business-метрики (goals, assists) и DQ-сигнал (`goals_diff_fotmob`) в одной таблице — пользователь правильно отметил что это загромождает Gold-витрину. Рефакторинг: 8/3 diff-колонок переехали в `fct_*_audit`. Main fct → чистая для BI; audit → DQ + engineer-debug.
- **Audit DQ должен быть observability, не gate.** Первая попытка с `error_threshold=0.80` упала в ERROR (penalties_won 0% within because most rows have NULL diff из-за sparse FotMob stats). Правильно: `error_threshold=0.0` (только WARNING) + `OR diff IS NULL` в condition. Audit показывает «вот насколько источники согласны», не «остановите DAG».
- **INNER JOIN не гарантирует non-NULL колонок.** Только non-NULL JOIN keys. `silver.fotmob_player_stats.penalty_won` часто NULL (FotMob не отдаёт нулевые stats), даже когда сам игрок есть в обоих источниках. Поэтому audit-coverage должен учитывать NULL diff как «не измеряли», а не «ошибка».
- **Spine = FBref-only filter** даёт ~2551 rows (5 сезонов × ~500), не ~487 — план оценивал per-season, реально фактур получилось multi-season.
- **`save_pct` vs `save_percentage` шкалы**: FBref % (75.5) vs FotMob (формат отличается). COALESCE замаскировал бы расхождение → отдельные колонки.
- **xref season type drift**: silver.xref_player хранит slug '2526' (varchar), Silver profile — bigint 2025. Идиома `2000 + CAST(SUBSTR(season, 1, 2) AS BIGINT)` повторно используется во всех T5/T6+ JOIN'ах.
- **STAGE_2_DIMS подходит для T5**: 4 task'а (2 main + 2 audit) лежат рядом с `dim_player_attributes` — natural ordering без нового TaskGroup.

### Out of scope (followup)

- **OpenMetadata YAML** descriptions для `fct_player_season_stats` / `fct_keeper_season_stats` — отделено в follow-up issue.
- **Superset datasources entry** — отделено в follow-up issue.
- **Postmortem `docs/decisions/RX-postmortem.md`** — после ≥3 дней green DQ.
- **R1 калибровка**: измерить p95/p99 audit-diff на полном проде (когда FotMob расширит исторические сезоны), занести thresholds в `R1_cross_source_thresholds.md`.
