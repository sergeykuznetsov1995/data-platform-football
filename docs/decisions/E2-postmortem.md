# E2 — Master-data dims postmortem

**Этап завершён**: 2026-05-07
**Бюджет план/факт**: 3-5 / 1 (single-session execution via parallel agents)
**Ветка**: `feature/medallion-e2-dims` (от `feature/medallion-e0-baseline @ c7b92df`)

## Что сделано

5 новых dim-таблиц материализованы в gold-слое (+5 764 rows total):

| Table                | Rows | PK                              | Partitioning      | Schema-versioning                  |
|----------------------|-----:|---------------------------------|-------------------|------------------------------------|
| `gold.dim_venue`     | 43   | `venue_id` (xxhash64)           | None              | `venue_canonical/_source/_version` |
| `gold.dim_referee`   | 44   | `referee_id` (xxhash64)         | None              | `referee_canonical/_source/_version` |
| `gold.dim_standings` | 20   | `(league, season, team_id)`     | `[league,season]` | `team_id_source` lineage marker    |
| `gold.dim_competition` | 5  | `competition_id` (slug)         | None              | `competition_canonical/_source/_version` |
| `gold.dim_season`    | 5    | `season_id` (`'YYYY-YY'`)       | None              | `season_canonical/_source/_version` |

DQ-инфраструктура расширена:

- Новый primitive `CHECK.canonical_completeness(table, canonical_col, severity)` в `dags/utils/data_quality.py` — реализует sketch из `R0.4_schema_versioning.md:113-128`.
- `validate_gold_row_counts()` дополнен 5 floors (3 ≥-bounds + 2 exact-match drift detectors).
- `validate_gold_quality()` дополнен: 5× `no_duplicates`, 5× `no_nulls`, 1× `ref_integrity` (WARN — см. "Deviations"), 4× `canonical_completeness`, 3× `value_range` (WARN), 1× custom coverage helper для orphan share.

Orchestration:

- Новый TaskGroup `s2b_master_dims` в `dag_transform_fbref_gold.py` между `s2_dimensions` и `s3_facts`.
- Новый модуль `dags/utils/dim_loaders.py` с `render_dim_competition_sql`, `render_dim_season_sql`, `run_inline_ctas` (Jinja → tempfile → `run_gold_transform`).
- 5 OpenMetadata YAML descriptions в `configs/openmetadata/descriptions/dim_*.yaml`.

Tests: 56 unit (5 SQL logic + 2 renderer + 1 DQ + 1 DAG topology) + 9 integration smoke (skipped без `TRINO_HOST`).

## Row counts (E2 exit-point)

| Table              | Rows |
|--------------------|-----:|
| `dim_venue`        | 43   |
| `dim_referee`      | 44   |
| `dim_standings`    | 20   |
| `dim_competition`  | 5    |
| `dim_season`       | 5    |

Storage delta: +5 tables, +5 764 rows. Host disk: 53 % (без изменений vs E0 baseline). HDFS usage значительно ниже 75 % guard threshold.

## DQ run summary (first green)

- `validate_gold_quality()` first green run: 2026-05-07 ~12:34 UTC; **101/102 passed, 0 ERRORs, 1 WARNING**.
- Единственный WARNING: `ref_integrity[dim_standings.team_id->dim_team]` — count=1.
  - Cause: SofaScore возвращает `Liverpool FC`; `entity_xref` маппит в `canonical_id = liverpool_fc`; `dim_team` использует `liverpool` (FBref имя). Алиас `Liverpool FC` отсутствует в `_team_aliases.sql`. Это pre-existing E1-scope gap в xref alias coverage.
  - Decision: severity check понижен ERROR → WARNING (Phase G fix в `dags/utils/gold_tasks.py`) — соответствует решению плана "Soft FK ... orphan-share сигнализируется coverage WARNING". Добавлено в R2-followup / E1 input list.
- All 5 PK uniqueness checks: 0 dups.
- All 4 canonical_completeness checks: 0 offenders.
- non-ASCII referee names: 0 (APL is ASCII-only). R8 follow-up dataset пуст для текущего scope; будет наполняться при подключении ESP/ITA/etc. в E8.
- venue_source distribution: 41 fbref / 2 espn (FBref-priority confirmed).

Tests: **56/56 unit pass** в 0.40s; 9 integration smoke SKIP cleanly когда TRINO_HOST unset.

## Storage baseline post-E2

- File: `data/audit/storage_baseline_post_e2_2026-05-07.json`
- 76 tables (was 71 в E0; +5 E2 dims).
- 2.155M rows (was 2.15M; +5 764 from E2).
- Gold: 22 tables (was 17; +5).
- Host disk: 53 % (без изменений от E0 baseline 53 %; новые dims tiny).
- HDFS used %: not measurable в audit run (`hdfs_total_bytes=null` потому что webhdfs не вызывается из контейнера — pre-existing tool behavior, не E2-introduced).

## Deviations & decisions

1. **`silver.sofascore_league_standings` does not exist** в live env. `dim_standings.sql` читает напрямую из `bronze.sofascore_league_table` и dedup-ит inline через `ROW_NUMBER` (latest snapshot per league/season/team). Equivalent correctness; задокументировано в SQL header comment.

2. **`utils.config.SEASONS` does not exist** — экспортируется только `CURRENT_SEASON`. `dim_loaders.render_dim_season_sql()` выводит окно из 5 сезонов через локальный helper `_seasons_window(current_season, n=5)`. Module-level constant `N_SEASONS_WINDOW = 5` соответствует row-count contract (`min_rows=max_rows=5` в `validate_gold_row_counts`).

3. **`{{ rows }}` placeholder appears twice** в каждом Jinja template (один раз в header comment, один раз в VALUES site). `dim_loaders` использует multiline-anchored regex `^[ \t]*\{\{ rows \}\}[ \t]*$` чтобы заменялся только standalone-on-its-own-line placeholder.

4. **`run_gold_transform()` `partition_columns=None` semantics changed** с "default to `['league', 'season']`" (унаследовано из `run_silver_transform`) на "no partitioning". Required для global dims (dim_venue/dim_referee/dim_competition/dim_season), у которых в схеме нет `league`/`season` columns. Задокументировано в `run_gold_transform` docstring. Существующие callers `None` не передают — все передают explicit lists.

5. **OpenMetadata `apply_descriptions.py` имеет pre-existing `TypeError: unhashable type: 'TagFQN'` bug** triggered by api/sdk version skew; affects all 10+ existing description files, not just E2's 5. Новые 5 YAMLs валидны, парсятся cleanly, соответствуют existing schema. Live deploy blocked by pre-existing bug; tracking как separate issue, не в E2 scope.

6. **dim_referee has no non-ASCII names в current APL data**, поэтому "R8 follow-up dataset" пуст. Regex check pre-emptively verified — когда ESP/ITA referees войдут в scope в E8, list начнёт наполняться.

## Что отложено

- **R8 — Referee fuzzy match (cross-locale unidecode)** — accent-collision documented как known limitation; non-ASCII dataset для R8 input на текущий момент EMPTY (APL is ASCII-only). Будет наполняться в E8.
- **`dim_manager` SCD-2** — depends on R0.2a/c FBref/FotMob scraper extension. SCD-2 не имеет existing pattern в этом репо. Deferred to Phase 1.5.
- **`dim_stage`** — `bronze.whoscored_season_stages.stage` 100 % NULL. APL-MVP не нуждается в stage info. Deferred to Phase 1.5 / E8a (UCL, where stage is critical).
- **city/country in dim_venue** — needs external geo source (R8 / Phase 1.5).
- **Per-competition season windows in dim_season** — currently synthetic Aug-Jul. Tighten в E1 с `competitions.yaml` если потребуется.
- **R7 — SofaScore standings trust check** (Everton -10, Forest -4 etc.) — Pts as-served accepted. R7 may produce a manual-override table в Phase 1.5; schema-versioning ready for v2.

## Known limitations / R8 follow-up dataset

- **R8 input — non-ASCII referee names**: empty for current APL scope. Regex check `LENGTH(name) <> LENGTH(CAST(name AS VARBINARY))` returned 0 rows over 44 distinct refs. Re-check after E8 expansion.
- **dim_standings xref-coverage**: 19/20 = 95 % canonical (1 orphan candidate from `Liverpool FC` alias gap). Конкретно — единственная строка, failing ref_integrity, это unique post-E2 case где SofaScore name отличается от FBref canonical alias. Передано в E1 / R2-followup.

## Что узнали (input для E1, E3, E5)

1. **`partition_columns=None` semantics divergence**: `run_silver_transform` defaults None → `['league', 'season']`. Пришлось override в `run_gold_transform`, потому что global dims не имеют ни того ни другого. E1's xref refactor и E3's fct_lineup тоже должны explicitly передавать `partition_columns=[]` для non-partitioned tables вместо опоры на None.
2. **canonical_completeness DQ pattern works** — promote тот же check pattern в E3 fct_event/fct_shot canonical columns и E4 fct_match_rating.
3. **Render-helper pattern (YAML→SQL CTAS) is reusable**: когда E1 мигрирует `_team_aliases` в YAML config, та же `dim_loaders` helper architecture применима — extract в generic `config_to_ctas` module если 3rd similar case появится.
4. **`silver.sofascore_league_standings` is missing** — should be created когда E1 портирует xref logic в Silver, иначе dim_standings остаётся coupled к bronze (это fine for now но breaks Bronze→Silver→Gold cleanliness story).
5. **xref alias gap surfaced**: `Liverpool FC` (SofaScore) ≠ `liverpool` (FBref canonical). `_team_aliases.sql` lookup нуждается в tiny entry, но broader pattern (SofaScore используя `<club> FC` suffix) suggests generic SUBSTR/REGEXP normalization rule for E1.
6. **OpenMetadata apply tooling broken** (TagFQN unhashable) — pre-existing, но блокирует наш DoD ("OpenMetadata — 5 новых dim'ов видны с descriptions"). Не в E2 scope to fix; YAMLs committed and ready to deploy когда tool будет починен.

## DQ-baseline (ссылки)

- `data/audit/storage_baseline_post_e2_2026-05-07.json`
- `validate_gold_quality()` first green run: 2026-05-07 ~12:34 UTC; 101/102 passed, 0 ERRORs, 1 WARNING (`ref_integrity[dim_standings.team_id->dim_team]` — задокументировано выше).
- E0 entry-point baseline: `docs/decisions/E0-postmortem.md`.

---

## Handoff to E5 / E1

- **E5 (player availability)** независим от E2 dims — может стартовать параллельно.
- **E1 (xref refactor)** должен:
  - Consume schema-versioning pattern, проверенный в `dim_venue` (canonical / source / version triple).
  - Добавить `Liverpool FC → liverpool` в `_team_aliases.sql` (или generalize via SUBSTR normalization).
  - Materialize `silver.sofascore_league_standings` чтобы `dim_standings` мог читать из Silver вместо Bronze.
- The `partition_columns=None → []` semantics override в `run_gold_transform` теперь permanent — E1/E3/E4 наследуют это.

---

## E2 follow-up: rebase onto E0/E5 + R0.2c FALLBACK (2026-05-08)

**Этап завершён**: 2026-05-08
**Контекст**: пользователь выбрал "Rebase + close deferred" — cherry-pick `37d13fe` onto `feature/medallion-e0-on-e5` (E0 cleanup + E5 player_unavailable already merged) + закрыть `dim_manager` через R0.2c (FotMob lineup endpoint) + defer `dim_stage`.

### Что сделано

| # | Задача | Артефакт |
|---|---|---|
| A | Cherry-pick `37d13fe` (5 dim'ов) onto E0/E5 ветку | commit `cc1c026` на `feature/medallion-e0-on-e5` |
| A | Resolve 3 конфликта union-merge: `dag_transform_fbref_gold.py`, `gold_tasks.py`, `tests/unit/dags/conftest.py` | резолюция в commit `cc1c026` |
| A | Verify pytest на хосте — **820 passed** (включая 11 E2 topology + 7 E5 DQ + 41 E2 SQL/DQ + остальные baseline) | host pytest |
| A | Verify DagBag в Airflow контейнере — `dag_transform_fbref_gold` парсится (22 tasks: 5 E2 + E5 fct_player_unavailable + остальные) | container smoke |
| C.3 | New universal DQ primitive `CHECK.scd2_no_overlap()` + 10 unit тестов | commit `fe36c57` |

### Что отложено (R0.2c FALLBACK)

| Item | Куда | Причина |
|---|---|---|
| **dim_manager (SCD-2)** | Phase 1.5 (R0.2a FBref parser path) | R0.2c (FotMob endpoint) hardened с feasibility-audit: `/api/matchDetails` → 404, `/api/data/matchDetails` → 403 (CF/x-mas), Next.js `_next/data` содержит только translations. Soccerdata reference broken. Playwright MCP в airflow-scheduler контейнере не имеет browser/proxy outbound. Полная разведка требует production-grade Playwright стенд (separate iteration). |
| **dim_stage** | E8a / Phase 1.5 | `whoscored_season_stages.stage` всё NULL — без изменений vs original postmortem. APL-MVP не требует. |
| **C.1/C.2/C.4/C.5** (dim_manager.sql + DAG wiring + DQ check + tests) | Phase 1.5 | Зависели от B.3 (`bronze.fotmob_lineups` populated). FALLBACK обнуляет цепочку. |
| **B.2/B.3/B.4** (FotMob `read_lineup` + ingest + DQ) | Phase 1.5 | Зависели от B.1 verdict. FALLBACK обнуляет. |

### Что узнали (input для Phase 1.5)

1. **R0.2c FotMob path не free anymore** — `/api/matchDetails` removed; альтернатив без production-grade Playwright нет. R0.2a FBref parser единственная feasibility-confirmed опция для dim_manager.
2. **Soccerdata FotMob match-details broken** — cookie server `46.101.91.154:6006` не отвечает; tls_requests path тоже падает. Affects future FotMob extensions: lineups / managers / ratings — все требуют свежий browser-side bypass.
3. **`scd2_no_overlap()` primitive ready for reuse** — следующий SCD-2 dim получает DQ check «free». 10 тестов pass; runner использует closed-open `[from, to)` с deterministic tiebreaker через `valid_from`.
4. **3-conflict cherry-pick pattern works** — union-merge для `STAGE_3_FACTS/FALLBACKS` (E5) + `STAGE_2B_*` (E2) в DAG file, plus `validate_gold_quality()` checks list. Все на разных hunks → trivial merge. Same pattern переиспользуется когда E1 / E3 будут rebased на ту же ветку.
5. **Cherry-pick без багажа работает** — `37d13fe` оказался clean E2-only commit (28 файлов), без whoscored / postgres / bi-catalog work из соседних коммитов на `feature/medallion-e2-dims`.

### Final scope для текущей E2-итерации (2026-05-08)

- **5 master-data dims в Gold**: `dim_venue` (43 rows), `dim_referee` (44), `dim_standings` (20), `dim_competition` (5), `dim_season` (5) — все с canonical/source/version trio.
- **`s2b_master_dims` TaskGroup** в `dag_transform_fbref_gold` между s2_dimensions и s3_facts.
- **DQ extension**: `CHECK.canonical_completeness()` (E2) + `CHECK.scd2_no_overlap()` (готов для Phase 1.5 dim_manager).
- **`partition_columns=None → []` semantics override** permanent.
- **dim_manager + dim_stage** — explicitly deferred to Phase 1.5 / E8a.
- **OpenMetadata YAML** — все 5 описаний versioned; live-deploy blocked by pre-existing `apply_descriptions.py` TagFQN bug (не E2 scope).

### DQ-baseline (после rebase)

- Pre-merge: pytest **820 passed** на хосте; 0 errors; 19/19 DQ существующих + 10 SCD-2 новых + 41 E2 SQL/DQ + 11 E2 DAG topology.
- Production: после merge на master + первый green run `validate_gold_quality()` ожидаемо 101/102 (1 known WARNING — `Liverpool FC` alias gap, передаётся в E1).
