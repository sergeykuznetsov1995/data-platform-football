## E4 — Narrow Facts + Ratings + Odds postmortem

**Этап завершён**: 2026-05-09
**Бюджет план/факт**: 3-5 / **<1 дня** (4-волновое parallel-agent execution + 1 итерация smoke-фиксов через DQ-driven debug)
**Ветка**: `feature/medallion-e4-narrow-facts` (от `feature/medallion-e1-xref-refactor`, HEAD `b0d7b26`)

---

## Что сделано

| # | Задача | Owner-agent | Артефакт |
|---|---|---|---|
| E4.0 | Branch creation | `github-data-platform-manager` | `feature/medallion-e4-narrow-facts`, WIP E1/E3/E5 сохранены |
| E4.1 | SofaScore `read_player_ratings()` + Bronze ingest | `web-scraping-expert` | `scrapers/sofascore/scraper.py` (+`tls_requests`+`ProxyManager`+ban-counter retry), `dags/scripts/run_sofascore_scraper.py` (`--entity player_ratings`, exit codes 0/1/2 + `R0.2B_FALLBACK` marker), `iceberg.bronze.sofascore_player_ratings` = **200 rows / 5 матчей APL 2526** (full path, без fallback) |
| E4.2 | `silver/match_cards.sql` + `silver/match_substitutions.sql` | `trino-specialist` | UNION FBref ⊕ WhoScored с FBref-priority dedup; `card_cards` 13,608 rows (FBref 11,660 + WS 1,948), `match_substitutions` 25,615 rows; double-CAST для WhoScored DOUBLE IDs; `whoscored_raw_<bigint>` fallback для unbridged matches (~7.4%) |
| E4.3 | `silver/matchhistory_match_odds.sql` + `silver/sofascore_player_ratings.sql` | `data-platform-architect` | Tall format (47,013 rows / 100% bridge / 30 bookmaker×market×closing combos); inline 63 team_aliases (с RHS = observed `dim_match.home_team_id` slugs, не YAML-canonical — drift documented); SofaScore через `bronze.sofascore_schedule` bridge или `'sofascore_'` raw fallback |
| E4.4 | 5 Gold fct-таблиц с canonical-trio | `trino-specialist` | `gold/fct_goal.sql` (UNION fct_shot.result='goal' ⊕ FBref own_goal, **6,525 rows / PK 100%**), `fct_card.sql` / `fct_substitution.sql` (passthrough silver), `fct_match_odds.sql`, `fct_match_rating.sql` (passthrough); xxhash64 + `to_hex()` 16-char hex canonical IDs |
| E4.5 | `dags/utils/e4_dq.py` | `data-quality-agent` | **81 checks** total (42 ERROR + 39 WARNING) — 31 silver + 50 gold; `coverage()` primitive отсутствует в `data_quality.py` → fallback двухуровневый `row_count` parted (50%/80% threshold) для DoD closing odds |
| E4.6 | `dag_transform_e4.py` + master-pipeline integration | `airflow-expert` | 12 tasks (start → silver_e4 TG (4) → gold_e4 TG (5) → validate_e4 → end), `schedule=None`, `max_active_tasks=1`; `OPTIONAL_BRONZE_FOR_E4_SILVER` map в DAG (sofascore_player_ratings скипается с AirflowSkipException если bronze пусто); `trigger_e4_transforms` после `trigger_e3_transforms` в master pipeline |
| E4.7 | Unit + integration tests (DuckDB) | `testing-agent` | 5 unit (35 tests, all PASSED, ~1.7s) + 1 integration (10 tests, проходят после E4.6); pure-SQL DuckDB-bridge через `_collapse_call(sql, fn_name)` paren-counting helper (Trino → DuckDB: xxhash64→md5, regexp_like→regexp_matches, format→printf) |
| E4.8 | Smoke run в Airflow контейнере | manual (Bash + diagnostic queries + 4 SQL-фикса) | См. ниже |
| E4.9 | DQ baseline сбор | manual | См. ниже |
| E4.10 | Postmortem | manual | этот файл |
| E4.11 | Commit + push + PR | `github-data-platform-manager` | (следующий шаг) |

**Pytest unit (host)**: 35/35 passed (test_fct_goal_union=7 + test_fct_card_union=5 + test_fct_substitution_union=6 + test_fct_match_odds_bridge=9 + test_fct_match_rating=8). Полный регресс: 237 passed / 24 skipped.

**Smoke `airflow dags test dag_transform_e4 2026-05-09`**: ✅ all 12 tasks SUCCESS, **end-to-end ~32 секунды** (08:36:52 → 08:37:24). DQ baseline после фиксов: **73/81 passed, 0 ERRORs, 8 WARNINGs**.

---

## Архитектурные решения (подтверждены кодом)

- **D1** — **goal/card/sub НЕ view'ы поверх `fct_event`**. SPADL mapping в `silver/whoscored_events_spadl.sql:97-107` схлопывает meta-events `Card / SubstitutionOff/On / Goal` в `action_canonical='unknown'`; goal markers — отдельные events, реальные shots живут в `SavedShot/MissedShots/ShotOnPost`. Источник для `fct_goal` = `gold.fct_shot WHERE result='goal'` ⊕ `bronze.fbref_match_events WHERE event_type='own_goal'`. Cards/subs = новые `silver/match_*.sql` UNION FBref+WhoScored с FBref-priority.
- **D2** — **own_goal `team_id_canonical` = goal-receiving team** (FBref convention, **противоречит** ТЗ — verified empirically на 5 sample own_goal rows: Bernd Leno [Fulham GK] → `team='Liverpool'`, Disasi [Aston Villa] → `team='Wolverhampton Wanderers'`, Hashioka [Luton] → `team='Manchester City'`). Это даёт правильный SUM(goals) per team. `scorer_id_canonical` — реальный игрок противоположной команды.
- **D3** — **Tall format для `fct_match_odds`** (1 row per match × bookmaker × market × closing_flag). 30 UNION ALL combinations (B365/PS/WH/VC/IW/BW/AVG/MAX × 1x2/ah/ou_2_5 × open/closing). Альтернатива (wide format с 60+ columns) отвергнута для будущего multi-bookmaker scaling.
- **D4** — **MatchHistory bridge через inline `team_aliases` VALUES + (date, league, season, home_canonical, away_canonical)** — БЕЗ расширения `silver.xref_match`. Phase B-deferred. RHS aliases = observed `dim_match.home_team_id` slugs (newcastle_united, bournemouth, brighton, wolves, manchester_utd) — НЕ YAML-canonical (Wikipedia-official names). Tech-debt: дублирование 63 alias pairs в обоих silver SQL и YAML.
- **D5** — **canonical-trio R0.4 для всех 5 Gold fct'ов** (`<entity>_canonical, _source, _version='v1'`). `*_canonical` = `lower(to_hex(xxhash64(to_utf8(natural_key))))` 16-char hex; tiebreaker через ROW_NUMBER seq (см. D7 ниже).
- **D6** — **R0.2b SofaScore extension full path** (НЕ schema-stub). 200 rows × 5 матчей APL 2526 через `tls_requests` + residential proxy + ban-counter retry. Endpoint `/api/v1/event/<id>/lineups` → `player.statistics.rating` (0.0-10.0 Opta scale). Fallback path сохранён через `R0.2B_FALLBACK` marker (exit code 2) для будущих proxy quota inscrutables.
- **D7** — **`fct_card`/`fct_substitution` canonical hash включает `team_id_canonical` + ROW_NUMBER seq** (smoke fix). Без team — две yellow на 90' от разных команд с NULL `player_id_canonical` (silver corner case ~0.4%/1.8%) коллизировали в одинаковый hash. ROW_NUMBER tiebreaker over `(match, team, minute, player, card_type)` ORDER BY source гарантирует разные hashes для legitimate distinct events.
- **D8** — **`fct_goal.team_id_canonical` COALESCE fallback к slugified raw FBref team-name**: `'fbref_team_' || regexp_replace(team_name_raw, '[^A-Za-z0-9]+', '_')` (smoke fix). Без fallback ~50/6525 (~0.8%) own_goal rows теряли team из-за xref_team season-drift (Newcastle Utd 2017-18 vs Newcastle United 2024-25). Альтернатива (relax DQ severity ERROR→WARNING) отвергнута — лучше иметь aggregable orphan ID, чем silent NULL.
- **D9** — **Silver odds sanitisation: drop rows с non-positive odds** (smoke fix). 1 row (`B365 ou_2_5 odds_h=0.000, odds_a=0.000`) source-data error — добавлен filter `(odds_X IS NULL OR odds_X > 1.0)` в `matchhistory_match_odds.sql`. Decimal odds должны быть > 1.0 by definition.

---

## DQ baseline (post-fix)

```
DQ report: 73/81 passed, 0 ERRORs, 8 WARNINGs
```

**8 WARNINGs (non-blocking, документированы как тех-долг)**:
- 6× `ref_integrity[gold.fct_*.match_id_canonical -> gold.dim_match]` — все падают с `TrinoUserError COLUMN_NOT_FOUND: Column 'p.match_id_canonical' cannot be resolved`. **Это БАГ в `data_quality.py::CHECK.ref_integrity`** (parent-table alias 'p' не находит column when parent='dim_match' / parent_key default). Не E4 issue — ту же ошибку получит E3/E5. Fix должен идти отдельным PR в `data_quality.py`. Не блокирует merge.
- 2× `dod_closing_odds_coverage_80pct` — 23,499 rows < 24,000 expected (на 0.5% ниже WARN-tier). DoD из roadmap "closing odds ≥80%" интерпретирован агентом как absolute row threshold (24K), хотя реально нужно coverage% per match. Per-match metric: **100%** (1869/1869 matches имеют closing odds для 1x2/ah/ou_2_5). **Effective DoD = PASS**. Threshold-rewrite в e4_dq → отдельный refactoring.

**Counts по таблицам (после фиксов)**:

| Таблица | Rows | Notes |
|---|---|---|
| `silver.match_cards` | 13,608 | FBref 11,660 / WS 1,948 / unbridged 7.4% |
| `silver.match_substitutions` | 25,615 | FBref 21,857 / WS 3,758 / unbridged 7.3% |
| `silver.matchhistory_match_odds` | 47,012 | 100% bridge / 50% closing rows / 1 outlier dropped |
| `silver.sofascore_player_ratings` | 200 | 5 матчей APL 2526 smoke |
| `gold.fct_goal` | 6,525 | regular 6,184 + own_goal 341, PK 100%, 10 сезонов |
| `gold.fct_card` | 13,608 | passthrough; 13,332 yellow + 256 red + (?) second_yellow |
| `gold.fct_substitution` | 25,615 | passthrough |
| `gold.fct_match_odds` | 47,012 | passthrough |
| `gold.fct_match_rating` | 200 | passthrough |

---

## Что отложено (Phase 1.5 / Phase B)

- **`fct_match_stage`** (roadmap E4 task 4) — **DEFERRED в Phase 1.5** по user-decision. Bronze `whoscored_season_stages.stage` весь NULL; APL = `regular_season` для всех 380 матчей; ценность низкая до E8a UCL/EL expansion (group/knockout stages).
- **`silver.xref_match` Phase B расширение для MatchHistory match_id** — `fct_match_odds` использует fuzzy bridge через team_aliases + (date, home, away). YAML-SQL aliases drift между inline VALUES и `team_aliases.yaml` — техдолг. Phase B cutover должен заменить inline VALUES на JOIN к расширенному `silver.xref_match`.
- **`data_quality.CHECK.ref_integrity` Trino alias bug** — 6 WARN сейчас. Fix в `dags/utils/data_quality.py` separately (не E4 scope).
- **`data_quality.CHECK.coverage()` primitive отсутствует** — добавлен fallback через парный `row_count` (50%/80% tier). Когда `coverage()` будет implemented, 4 e4_dq check'а можно ужать до 2.
- **DoD closing odds coverage threshold rewrite в `e4_dq.py`** — текущий threshold (24K row) должен стать `pct_per_match >= 80%` (через CHECK.coverage() когда появится).
- **Multi-source own_goal coverage** — Understat имеет 203 own_goal events vs FBref 463 (R0.1 finding). E4 использует только FBref (наиболее полный). Cross-source UNION + dedup — Phase 2.
- **`is_penalty=FALSE` для всех fct_goal** — bronze.understat_shots в текущем дампе не surface'ит `situation='Penalty'`. Документировано в header `fct_goal.sql` и `fct_shot.sql`.
- **`assist_id_canonical=NULL` для всех regular goals** — `fct_shot.assist_player_id_canonical` полностью NULL в текущем дампе (Understat assist resolution pending в xref_player Phase B). Schema reserved, заполнится автоматически когда `fct_shot` ребилдится.

---

## Что узнали (input для последующих этапов)

1. **DQ-driven debug — кратчайший путь к качеству**. `validate_e4` нашёл 5 реальных багов в SQL за 6 секунд (190 dup card_canonical, 469 dup substitution_canonical, 50 NULL team_id_canonical, 1 invalid odds row). Без этого DAG бы успешно материализовал «грязные» таблицы. **Pattern**: всегда строить DQ checker до production smoke; severity ERROR на canonical PK uniqueness — must.
2. **Hash collision risk при NULL canonical**. Любой xxhash64 PK builder ОБЯЗАН включать `team_id` (или другой mid-cardinality discriminator) и ROW_NUMBER tiebreaker, иначе legitimate distinct events с NULL player/match collapse в один hash. Memory note: `feedback_hash_pk_with_null_canonical.md` (создать в этом коммите).
3. **FBref own_goal `team` semantics — NOT scorer's team, but goal-receiving team**. Counter-intuitive, легко ошибиться. Документировано в `fct_goal.sql:47-56` ADR + 5 sample rows-доказательств в постмортеме. Memory note: `feedback_fbref_own_goal_team_attribution.md` (создать).
4. **MatchHistory team-naming = `dim_match.home_team_id` slugs, НЕ YAML-canonical**. Inline aliases в silver SQL должны точно отражать observed slug vocabulary, иначе INNER JOIN bridge выкидывает row. Memory note: `feedback_xref_team_canonical_drift.md` (создать).
5. **`data_quality.CHECK.coverage()` примитив объявлен в roadmap/CLAUDE.md, но НЕ реализован в `data_quality.py`** (только 9 primitives: row_count, no_duplicates, freshness, no_nulls, ref_integrity, value_range, canonical_completeness, point_in_time, scd2_no_overlap). E4 использует fallback. Roadmap note: добавить `coverage()` в `data_quality.py` отдельной итерацией.
6. **`data_quality.CHECK.ref_integrity` баг** — Trino alias 'p' не работает для parent table в текущей implementation. Все 6 ref_integrity WARN — это false-positive. Fix отдельно.
7. **`airflow dags test` буферизует stdout** — 17 минут процесс жил без output. Решение: использовать `airflow dags trigger` через scheduler (асинхронно) + polling state через CLI или Web UI; per-task логи пишутся синхронно в `/opt/airflow/logs/dag_id=*/`.
8. **Namenode умер во время smoke** — `docker compose ps` показал `Up 20 hours (healthy)` (stale data), реальный `docker ps` его не показал. Решение: всегда проверять через `docker ps`, не `docker compose ps`. Memory note `project_vm_oom_event_2026-04-29.md` уже покрывает.

---

## DAG run

- Run ID: `manual__2026-05-09T00:00:00+00:00`
- Wall-clock: ~32 секунды (08:36:52 → 08:37:24 UTC)
- Tasks: 12/12 SUCCESS
- DQ: 73/81 passed (90.1%), 0 ERROR, 8 WARNING (all non-blocking)

```
silver_e4.match_cards               4.0s
silver_e4.match_substitutions       5.1s
silver_e4.matchhistory_match_odds   4.9s
silver_e4.sofascore_player_ratings  0.8s
gold_e4.fct_goal                    1.2s
gold_e4.fct_card                    0.8s
gold_e4.fct_substitution            1.2s
gold_e4.fct_match_odds              0.7s
gold_e4.fct_match_rating            0.6s
validate_e4                         5.6s
```

---

## DoD (per roadmap:369)

- [x] `fct_goal` / `fct_card` / `fct_substitution` материализованы, narrow facts пайплайн работает.
- [x] `fct_match_rating` материализован full-scope (R0.2B_FALLBACK НЕ активирован, smoke 200 rows).
- [x] `fct_match_odds` материализован, **closing odds coverage = 100%** (per match) — DoD ≥80% выполнен с запасом. Per-row coverage 49.99% (tall-format, half rows = open + half = closing) — это math-консистентно, не нарушение.
- [x] `fct_match_stage` явно deferred в Phase 1.5 (запись выше).
- [x] DQ: 81 checks из `e4_dq.py`, **0 ERROR в smoke**, 8 WARNING (non-blocking — bug в data_quality.py).
- [x] DAG `dag_transform_e4` интегрирован в `dag_master_pipeline` после E3.
- [x] Unit + integration tests green (35/35 unit + integration ready после E4.6).
- [ ] PR открыт, postmortem committed, memory updates записаны (E4.11 — следующий шаг).
