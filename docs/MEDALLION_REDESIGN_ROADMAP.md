# Medallion Redesign — Bronze / Silver / Gold

Полный roadmap редизайна слоёв data-платформы с research-фазой, итеративным workflow и этапами реализации.

> **Статус**: v2 (May 2026). Переработан после фактической сверки с кодом — раздел [Контекст](#контекст) содержит честную картину того, что уже есть в Bronze/Silver/Gold, и какие сущности **требуют расширения скраперов** перед использованием.

---

## Workflow / Iterative Process

Roadmap — **живой документ**. Каждый этап выполняется по циклу:

```
Research spike → docs/research/<R-id>.md
       ↓
Update этого roadmap'а с принятым решением
       ↓
Execute этап (код + тесты + DQ + DAG-runs)
       ↓
Postmortem → docs/decisions/<E-id>-postmortem.md
       ↓
Следующий research-spike (если требуется для next stage)
```

### Структура артефактов

| Директория | Что лежит | Когда обновляется |
|---|---|---|
| `docs/research/R0.*` | Source/feasibility audits (новая Phase 0) | До любой реализации |
| `docs/research/R1-R5` | Tier 1 spikes (blocking) | После R0, до E1+ |
| `docs/research/R6-R11` | Tier 2 spikes (parallel/deferred) | По мере необходимости |
| `docs/decisions/E*` | Postmortem каждого E-этапа: что сделано, что отложено, что узнали | После каждого E-этапа |
| `docs/MEDALLION_REDESIGN_ROADMAP.md` | Этот файл — мастер-план | После каждого spike/этапа |

### Шаблон research-документа

```markdown
# R<N> — <Название>

**Статус**: planned | in-progress | done
**Бюджет**: N дней
**Разблокирует**: E<N>, E<N>...
**Дата завершения**: YYYY-MM-DD

## Question
<точная формулировка вопроса, на который отвечаем>

## Method
<benchmark / SQL coverage analysis / prototype / known-cases verification>

## Findings
<данные / графики / coverage % / выбранный алгоритм>

## Decision
<что приняли + почему>

## "Good enough to unblock" rule
<минимальный output, при котором следующий E-этап можно начинать>

## Open questions / тех. долг
<что отложено в Tier 2 или на postmortem>

## Inputs to roadmap
<какие правки нужно внести в этот файл после spike'а>
```

### Шаблон postmortem-документа

```markdown
# E<N> — <Название> postmortem

**Этап завершён**: YYYY-MM-DD
**Бюджет план/факт**: N / M дней
**Что сделано**: <bullets>
**Что отложено**: <bullets с указанием в какой Tier 2 spike или будущий E-этап>
**Что узнали (input для последующих этапов)**: <bullets>
**DQ-baseline**: <ссылка на Airflow DAG runs / Telegram alerts>
```

### Принципы

1. **"Good enough to unblock"** — каждый research-spike должен иметь явное minimal-output правило, при котором следующий этап стартует. Идеальный ответ откладывается в Tier 2.
2. **Аддитивность first** — новые таблицы/колонки добавляются раньше, чем рефакторятся старые. Refactor только после стабильного DQ-baseline на новом.
3. **Postmortem обязателен** — даже если этап прошёл «как ожидалось», финальный документ фиксирует факты для будущих сверок.

---

## Контекст

### Что есть на 2026-05 (verified)

**Bronze** (через scrapers + Iceberg) — verified by R0.1 audit (40 таблиц, APL only):
- ✅ FBref: schedule, lineups, match_events (narrow: substitution/yellow_card/goal/penalty/own_goal/red_card), match_player_stats, match_team_stats, keeper_keeper, keeper_keeper_adv (psxg всё NULL), player_* (×4), team_* (×4) — **без xG/xA в любой таблице**, **без manager-полей**, **shot_events НЕТ** (R0.1 gap)
- ✅ Understat: schedule (home_xg/away_xg full), shots (shot-grained xG, x/y, body_part, situation), player_match_stats (xg/xa full), team_match_stats, players
- ✅ WhoScored: events (528K rows 2526; 48 distinct types incl. SavedShot/MissedShots/Card; qualifiers 93%), missing_players (full), season_stages (stage column ВСЯ NULL → бесполезна), schedule (stage column тоже NULL)
- ✅ SofaScore: schedule (380×2526), league_table (20×2526 — primary для dim_standings, snapshot only) — **player_ratings отсутствует** (R0.2b extension)
- ✅ MatchHistory: games (169 колонок 2021-2025 × 380, full B365/PS/Pinnacle odds + referee)
- ✅ ESPN: schedule, lineup (player-row), **matchsheet** (venue/attendance — не "standings" как было заявлено)
- ✅ ClubElo: ratings (snapshot per team×league), team_history (105K rows multi-season; APPEND-mode см. memory)
- ✅ FotMob: schedule (1140×2526), team_stats, player_stats — **lineups + league_table ОТСУТСТВУЮТ** (R0.2c extension)
- ✅ SoFIFA: player_ratings, players, team_ratings, teams, versions (top-5 leagues)

**Silver** (15 файлов в `dags/sql/silver/`): source-specific enrichment, **xref не существует** на этом уровне.

**Gold** (20 файлов в `dags/sql/gold/`):
- 3 dims: dim_team, dim_player, dim_match
- 3 facts: fct_team_match, fct_player_match, fct_match
- 6 features: feat_team_form, feat_team_xg_form, feat_team_elo, feat_team_h2h, feat_player_form, feat_player_season
- 2 ML splits: fct_match_train, fct_match_test
- 3 supporting: entity_xref, match_outcomes, predictions_input
- 2 fallback stubs

**DQ-инфраструктура** (`dags/utils/data_quality.py`) — все 8 проверок реализованы: `row_count`, `no_duplicates`, `freshness`, `no_nulls`, `ref_integrity`, `value_range`, `point_in_time`, `coverage` (two-tier).

### Что вызывает редизайн

Текущий Gold покрывает **3 группы сущностей** (team / player / match) с базовыми features. Industry research (StatsBomb / Opta / Wyscout / FIFA Connect / CDF) показал, что **в Bronze частично есть данные ещё для 9 сущностей**, и **3 сущности требуют расширения существующих скраперов**.

**Цель v2**: довести Gold до Phase 1 модели (12 новых сущностей), с расширением scope с APL до **поэтапного T1** (UCL → Top-5 → EL).

**Use cases**: ML predictions (1X2, BTTS, totals), BI/scouting dashboards, tactical/event analytics, data catalog.

### Корректировка: 12 сущностей по статусу источника

| Группа | Сущность | Источник (R0.1 verdict) | Статус Bronze |
|---|---|---|---|
| **Dims** | venue | FBref schedule.venue (full) + ESPN matchsheet.venue (fallback) | ✅ есть |
| | referee | FBref schedule.referee (full) + MatchHistory.referee (fallback) | ✅ есть |
| | manager (SCD-2) | **FotMob lineup-endpoint coach.name** (R0.2c primary) → FBref match-page parser (R0.2a fallback) | ⚠️ **требует scraper extension** (R0.2a/c) |
| | standings (snapshot) | SofaScore league_table | ✅ есть (1 сезон 2526; pre-2526 — нет) |
| **Match-level facts** | lineup | FBref lineups + ESPN lineup + **FotMob (R0.2c)** | ⚠️ FotMob нужен extension; **WhoScored не имеет lineup-таблицы** |
| | event (raw or SPADL) | WhoScored events (528K rows 2526, 48 types, qualifiers 93%) | ✅ есть; SPADL — R3 decision |
| | shot | Understat shots (primary, shot-grained xG/x/y) | ✅ есть |
| | goal/card/sub | view'ы over fct_event | ✅ derivable |
| | match_rating | SofaScore /api/v1/event/<id>/lineups (R0.2b extension) | ❌ **player_ratings endpoint требует extension** |
| | match_odds | MatchHistory (380×5 сезонов full) | ✅ есть |
| **Player-level** | player_unavailable | WhoScored missing_players (full 2021-2526) | ✅ есть |
| **Tournament** | match_stage | ⚠️ **whoscored_season_stages.stage ВСЯ NULL** — bronze backfill required ИЛИ derive from competition rules | ❌ **Bronze present но column empty** |

**xG cascade** (R0.1 finding): **FBref выпадает полностью** — все FBref-таблицы НЕ имеют xG/xA/npxg/psxg колонок (verified empirically на 10 сезонах).

| Granularity | Primary | Fallback |
|---|---|---|
| Shot-level xG | Understat shots | WhoScored events (qualifier-flag) |
| Per-match team xG | Understat schedule.home_xg/away_xg | Understat team_match_stats |
| Per-player match xG | Understat player_match_stats | — |

**Сущности по статусу источника после R0**:
- **9 сущностей** из existing Bronze: venue, referee, standings (snapshot only), lineup (FBref+ESPN), event, shot, goal/card/sub, match_odds, player_unavailable.
- **3 сущности через scraper extensions**: manager (R0.2a/c), lineup 3-й источник FotMob (R0.2c), match_rating (R0.2b).
- **1 сущность blocked**: match_stage (whoscored_season_stages.stage всё NULL — требует backfill scraper, либо derive).

---

## Архитектурные решения (industry-grounded)

1. **Formation — атрибут**, не dim. Хранится на `team_match.formation_code` + `lineup.position` (CDF/Opta/SB-стандарт).
2. **MatchEvent = SPADL-нормализованный** в Gold (R3 verdict: **89.97% coverage** на WhoScored — bucket 85-95%). Path = **SPADL primary + open-action proprietary supplement** (22 SPADL actions + `ball_recovery` proprietary + `'unknown'` для meta-events). Mapping logic — own SQL CTAS в `dags/sql/silver/whoscored_events_spadl.sql` (E3); socceraction explicitly deferred. Schema-version: `action_canonical / action_source='whoscored_spadl_proprietary_v1' / action_version='v1'`. **Execution path** (R4 verdict): Trino partition-by-partition INSERT (528K rows за 2.7s, peak 47MB) — **Spark исключён** из roadmap'а.
3. **Standings = stored snapshot** от SofaScore/ESPN, не derived из результатов. Иначе теряются point deductions (Everton, Forest 2023-24, Juventus 2022-23).
4. **Narrow facts (goal/card/sub) поверх fct_event** — view'ы для BI/Superset.
5. **Cross-source метрики**: store-all + canonical-with-priority + DQ-alert при расхождениях. Не усреднять (биас моделей не компенсируется). **Threshold — data-driven** (p95/p99 на исторической выборке, не интуиция).
6. **Schema versioning** — у canonical-колонок (`xg_canonical`, `score_canonical`, etc.) обязательна **колонка `<col>_source`** + `_version` в metadata. Re-materialization runbook фиксируется в R1.

---

## Phase 0 — Source & feasibility research (NEW, blocking)

Бюджет план/факт: **5-7 / 1.3 дня**, идёт **до Tier 1 spike'ов**. Цель — закрыть фактологические пробелы, обнаруженные в ревью v1.

> **Status (2026-05-06)**: **R0.1–R0.5 done** + **Tier 1 (R1–R4) done same day**. Все Findings/Decision в `docs/research/R*.md` заполнены empirical numerics. Baseline gold-usage snapshot в `data/audit/gold_usage_2026-05-06.json`. Sprint 4 R0.2 gate — **GREEN** (HDFS 3% / host 53%). Следующий шаг — E0 baseline + tooling.

### R0.1 — Source × competition coverage matrix (2 дня)

**Question**: для каждого из 9 источников × T1 (APL + Top-5 + UCL + EL) × последние 3 сезона — какие entity-таблицы реально содержат данные? Где заканчивается покрытие?

**Method**: SQL-аудит существующих Bronze-таблиц + smoke-scrape новых competitions через каждый источник на 1 матчдей.

**Deliverable**: матрица coverage `docs/research/R0.1_source_coverage.md` с разбивкой `(source, entity, competition, season) → coverage%`. Primary/fallback per entity per competition.

**Разблокирует**: R5 (включает в себя), E2 (manager fallback), E3 (event source choice), E8a/b/c (T1 expansion order).

### R0.2 — Scraper extension feasibility (2 дня)

**Question**: можно ли расширить FBref/SofaScore чтобы покрыть `dim_manager` и `fct_match_rating` без переписывания базовой инфраструктуры? Каков объём работы?

**Sub-tasks**:
- **R0.2a**: FBref managers — есть ли managers на match-странице FBref (`/en/matches/<match_id>/`)? Можно ли добавить parser в `scrapers/fbref/parsers/`? Smoke-test на 5 матчах.
- **R0.2b**: SofaScore player_ratings — REST API `/api/v1/event/<id>/lineups` содержит ratings (verified known fact)? Объём работы добавить `read_player_ratings()` в существующий SofaScore scraper.
- **R0.2c**: FotMob lineups — заменяет ли FotMob WhoScored как 3-й источник lineup для E3 union?

**Deliverable**: `docs/research/R0.2_scraper_extensions.md` — feasibility per sub-task + estimated days.

**Разблокирует**: E2 (manager), E3 (lineup union), E4 (ratings).

### R0.3 — Cost & storage projection (1 день)

**Question**: при расширении на T1 (7 competitions) каков ожидаемый размер каждой Bronze/Silver/Gold таблицы? Какая HDFS-отметка реалистична для guard'а?

**Method**:
- Текущий размер каждой Bronze-таблицы (APL only) → линейная экстраполяция × competition_volume_factor (UCL = ~125 матчей/сезон, Top-5 ≈ 360 каждый, EL ≈ 200 матчей).
- Для fct_event projected size: rows × 150 байт (Parquet-compressed estimate).

**Deliverable**: `docs/research/R0.3_storage_projection.md` — table-by-table projected size, HDFS guard threshold (16 / 30 / 40 GB), trigger для disk expansion.

**Разблокирует**: E0 baseline, E8 storage-guard DAG.

### R0.4 — Schema versioning design (1-2 дня)

**Question**: как версионировать canonical-колонки (`xg_canonical`, `rating_canonical`) чтобы изменение priority в R1 не ломало downstream? Где живёт `<col>_version`?

**Method**: design-document + 1 worked example (xG priority flip). Iceberg schema evolution + Trino re-materialization runbook.

**Deliverable**: `docs/research/R0.4_schema_versioning.md`.

**Разблокирует**: R1, E3, E4, E6.

### R0.5 — Usage tracker tooling (0.5 дня)

**Question**: как автоматически идентифицировать unused Gold-таблицы для E9 deprecation?

**Method**: SQL запрос к `system.runtime.queries` (Trino) + Superset `/api/v1/dataset/` + OpenMetadata downstream lineage. Объединить в утилиту `scripts/audit_gold_usage.py`.

**Deliverable**: `docs/research/R0.5_usage_tracker.md` + рабочий скрипт.

**Разблокирует**: E0 (baseline) и E9 (deprecation).

---

## Phase 0' — Tier 1 research (после R0) — **DONE 2026-05-06**

Бюджет план/факт: **8-12 / ~1 день** (parallel team-lead режим, 4 окна одновременно). Все 4 spike'а закрыли good-enough-to-unblock rules.

| # | Research | План | Факт | Разблокирует | Verdict |
|---|---|---|---|---|---|
| **R1** | Cross-source xG/score conflict — empirical thresholds | 2 дня | <1 дня | E3, E4, E6 | **DONE** — score 727/727 exact-match; xG → Understat-only (FBref no xG); WARNING/ERROR thresholds numeric (см. R1.md) |
| **R2** | Player identity resolver — prototype v0.1 | 5 дней | <1 дня | E1, E5 | **DONE** — 10/10 known-pairs; rejection 0%/2.8%/3.3% per source (target ≤25%); orphan format `us_*`/`ws_*`/`ss_*` принят |
| **R3** | SPADL coverage on WhoScored + socceraction-vs-own | 5 дней | <1 дня | E3 | **DONE** — coverage **89.97%** (85-95% bucket); SPADL primary + `ball_recovery` proprietary supplement; socceraction deferred |
| **R4** | Trino partition-by-partition vs Spark — benchmark | 2 дня | <1 дня | E3 | **DONE** — Trino: 2.7s wall-clock (3 порядка под target), 47MB peak (60× под target). **Spark исключён** |
| ~~R5~~ | ~~Cross-source coverage matrix~~ | — | — | — | **Поглощён R0.1** |

### R1 — verdict (done 2026-05-06)

FBref xG availability — **0%** (settled R0.1 / FBref не имеет xG-колонок). Cross-source FBref↔Understat невозможна; R1 переориентирован на 4 distribution:
- D1 Understat internal (schedule.xG vs SUM(shots.xG)): p95=0.158, p99=0.503 (на 2526) → WARNING@p95>0.20, ERROR@p95>0.50.
- D2 Silver↔Bronze per-player xG: ВСЕ 0.0 → ERROR@p99>0.01 (любое отклонение — bug).
- D3 Match score (FBref/Sofa/MH): **0 mismatches на 727 matches** → ERROR-on-ANY-mismatch policy.
- D4 goals/cards/subs |fb-ws|: WARNING@|diff|>2 (goals) / 1 (cards/subs); ERROR@|diff|>3.
- Bronze sanity prerequisites (R1 surfaced): no_duplicates(understat_schedule, key=game_id) ERROR; no_duplicates(fbref_match_events,...) WARNING. **Должно быть закрыто в E0/E1 до production cross-source DQ**.

### R3 — fallback decision-tree (verdict)

| WhoScored→SPADL coverage | Decision |
|---|---|
| ≥ 95% | SPADL primary, raw в shadow-column для аудита |
| **85-95% ← VERDICT (89.97%)** | **SPADL primary, добавить open-action mapping в proprietary supplement (`ball_recovery`)** |
| 70-85% | Proprietary 22-action mapper c явной фиксацией fidelity-loss |
| < 70% | fct_event хранит **raw WhoScored taxonomy**, SPADL живёт как Gold view; отложить normalization |

**Mapping (R3 D2)**: 39 distinct WhoScored types → 22 SPADL + `ball_recovery` proprietary + `'unknown'` (meta — Card/Substitution/Goal/Start/End/FormationSet/etc., 17,550 events / 2.52%). Spec — R3.md D2 mapping table; реализация — `dags/sql/silver/whoscored_events_spadl.sql` в E3.

### R2 — verdict (done 2026-05-06)

Prototype в `scripts/r2_resolver_proto.py` (FBref как spine, unidecode normalize, rapidfuzz token_sort_ratio threshold=90, 4-tier cascade). Output: `iceberg.default.r2_xref_player_proto` (1615 rows). **Rejection 0% / 2.8% / 3.3% per source** (FBref/Understat/WhoScored) — намного лучше план-target 15-25%. 32 orphan'а triaged: 14 mid-season transfers, 7 loans, 5 FBref squad lag, 6 borderline. Cross-league (UCL / non-APL) и SofaScore — отложено в `R2-followup` Phase 1.5. E1/E5 поддерживают `us_*`/`ws_*`/`ss_*` orphan IDs без падения.

---

## Tier 2 research — параллельно или отложить

| # | Research | Что исследуем | Разблокирует |
|---|---|---|---|
| R6 | Manager stint boundaries | Caretaker'ы (interim), gaps в FBref schedule, SCD-2 corner cases (увольнение mid-season) | E2 |
| R7 | Standings point deductions | SofaScore/ESPN trust-check на Everton -10, Forest -4, Juventus -10. Manual override table? | E2 |
| R8 | Referee fuzzy match cross-locale | Accent normalization (Çakir, García, Müller) | E2 |
| R9 | ML feature importance baseline | Brier score backtest старого predictions_input + новых features | E6 |
| R10 | Storage tiering | Какие таблицы вечно vs rolling 3 сезона, snapshot policy | E8 |
| R2-followup | Production-quality player resolver | Транслитерация, mid-season трансферы, jersey reuse, common-surname collisions | Post-Phase-1 |

---

## Этапы реализации (re-ordered)

> **Порядок изменён** относительно v1: аддитивные dims/facts первыми (E2, E5), refactor xref (E1) — после стабильного DQ-baseline. ML-приоритет E5 — раньше тяжёлого E3.

### E0 — Baseline + tooling (1-2 дня)

- Snapshot HDFS / Iceberg sizes / row counts по всем Bronze/Silver/Gold таблицам.
- Iceberg snapshot retention 30 d (страховка для time-travel rollback).
- Feature ветка.
- **NEW**: запустить `scripts/audit_gold_usage.py` (R0.5) для baseline-снимка какие таблицы реально читаются.
- **NEW**: cost-projection (R0.3) → определить current HDFS usage % и выставить storage guard threshold.

### E2 — Master-data dims (3-5 дней) — _was: после E1, теперь первый_ — **Phase A done 2026-05-08**

**Зависит от**: R0.1 (coverage), R0.2a (manager parser), R6 (Tier 2, может идти параллельно), R7, R8.

- ✅ `dim_referee` (FBref schedule + ESPN matchsheet, fuzzy resolution отложено в R8).
- ⏸️ `dim_manager` SCD-2 — **deferred to Phase 1.5** (R0.2c FALLBACK — FotMob endpoint hardened; см. `docs/research/R0.2_scraper_extensions.md` recon update). R0.2a FBref parser path остаётся feasibility-confirmed для Phase 1.5 reattempt.
- ✅ `dim_standings` (SofaScore league_table snapshot — не derive!).
- ✅ `dim_venue` (FBref schedule).
- ✅ `dim_competition`, ✅ `dim_season`, ⏸️ `dim_stage` (`whoscored_season_stages.stage` всё NULL → defer to E8a).

**DoD текущей итерации**: 5 dim'ов merged onto `feature/medallion-e0-on-e5` (commit `cc1c026`); 820 pytest passed; DagBag green в Airflow контейнере; `CHECK.scd2_no_overlap()` primitive готов для Phase 1.5 dim_manager re-attempt. Postmortem `docs/decisions/E2-postmortem.md` секция «E2 follow-up: rebase onto E0/E5 + R0.2c FALLBACK».

**Verdict 2026-05-08** — 5 of 7 dims в production; SCD-2 + stage отложены без блокирования E1/E3/E5.

### E5 — Player availability (2-3 дня) — _**MOVED EARLIER** (was после E4)_ — **DONE 2026-05-07**

**Зависит от**: dim_player (существует), `whoscored_missing_players` Bronze (✅ есть).

- `fct_player_unavailable` (injury/suspension из `whoscored.missing_players`).
- **Высший ML-приоритет** — top-предиктор пропущенных выходов.
- Поддержка orphan player IDs (`ws_*` если R2 не успел).

**DoD**: date-range integrity, ref_integrity к dim_player (или orphan-handling), fed в feat_team_form как `unavailable_count_l5`.

**Verdict 2026-05-07** — postmortem `docs/decisions/E5-postmortem.md`. Silver+Gold двухуровневая (cross-source `match_id` bridge через `dim_match.(date, home_team_id, away_team_id)`); `unavailable_count_l5` = L5 AVG OVER (point-in-time mask); orphan `ws_*` IDs до E1; **T7 BI/catalog metadata deferred** в phase-2-bi-catalog (configs не в master); E6 propagation (`fct_match.home/away_unavailable_count_l5` + `predictions_input`) — отдельный PR. Pytest 16/16 PASSED, integration 2/2 SKIPPED на хосте (pass в контейнере).

### E1 — Foundation: identity & xref refactor (3-5 дней) — _MOVED LATER (was первый)_ — **DONE 2026-05-08**

**Зависит от**: R2 prototype, R0.1 (coverage matrix), стабильный DQ-baseline на E2/E5.

- ✅ Конфиги competitions/team-aliases вынесены в YAML (`configs/medallion/team_aliases.yaml`, `competitions.yaml`) + loader `dags/utils/medallion_config.py`.
- ✅ xref-логика перенесена из Gold в Silver: 5 granular CTAS (`silver.xref_team`, `xref_match`, `xref_referee`, `xref_player`; `xref_manager` STUB до Phase 1.5).
- ✅ **Dual-run parity check** реализован в `dags/utils/xref_dq.py` + runbook `docs/decisions/E1-dual-run-parity-runbook.md`. `gold.entity_xref` остаётся, cutover в E1.5.
- ✅ **xref_player** — port `scripts/r2_resolver_proto.py` в `dags/utils/xref_player_resolver.py` + DAG callable; orphan IDs `fb_*`/`us_*`/`ws_*`/`ss_*` поддерживаются через canonical_id schema.

**Verdict 2026-05-08** — postmortem `docs/decisions/E1-postmortem.md`; pytest 205/205 PASSED (40 medallion_config + 32 resolver + 43 xref SQL + 20 xref_dq + 70 pre-existing); 9 integration tests PASSED in container; smoke run `airflow dags test dag_transform_xref 2026-05-09` green.

**DoD результаты**:
- ✅ Existing pipeline зелёный; новые xref-таблицы материализованы (xref_team 626 / xref_match 3801 / xref_referee 342 / xref_manager 0 STUB / xref_player ~1500 per season).
- ⚠️ **Dual-run parity reframe**: xref_match canonical_id_match_pct = **100%** vs gold (DoD ✅); xref_team **78.5%** — divergence intended (alias-canonicalisation: gold legacy `manchester_utd` vs новый `manchester_united` через aliases YAML). Runbook reframes DoD на FBref-only subset.
- ⚠️ Player rejection: FBref **0%** ✅, Understat **6.94%** (выше 5% placeholder, ниже 25% R2 baseline), WhoScored **4.89%** ✅ — promotion teams 2024-25 (Ipswich/Leicester/Southampton) требуют YAML alias updates.

**Next**: E1.5 cutover (≥3 дня green-parity на FBref subset → replace `gold.entity_xref` references в `dim_match`/`dim_team`/`dim_player`/`feat_*` на `silver.xref_*`).

### E3 — Core match facts (5-7 дней + 5-10 дней backfill = E3.5) — **DONE 2026-05-08**

**Зависит от**: R3 (SPADL decision-tree), R4 (Trino vs Spark), R0.4 (schema versioning), E1 (xref готов).

- `fct_lineup` (FBref + ESPN + **FotMob** union — R0.2c подтвердит замену WhoScored).
- `fct_event` — **SPADL-normalized + `ball_recovery` proprietary** (R3 verdict 89.97% coverage). Schema: `action_canonical / action_source='whoscored_spadl_proprietary_v1' / action_version='v1'`. Mapping logic: `dags/sql/silver/whoscored_events_spadl.sql` (spec — R3.md D2 mapping table, 39 types).
- `fct_shot` (Understat shot-grained xG/xA).

**Execution path**: **Trino partition-by-partition INSERT** (R4 verdict — 528K rows за 2.7s, peak 47MB). Spark исключён. Per-partition target wall-clock <30s (10× safety от 2.7s baseline). 30 партиций (5 seasons × 6 leagues) sequential ≈ <2 min.

**DoD**: row-count sanity, PK uniqueness, SPADL coverage = 89.97% verified; `action_canonical` enum включает 22 SPADL + `ball_recovery` + `'unknown'`; schema-version поля заполнены; per-partition wall-clock <30s.

**Verdict 2026-05-08** — postmortem `docs/decisions/E3-postmortem.md`. 4-volna parallel-subagent-execution: trino-specialist (E3.1/2/4/5), data-platform-architect (E3.3), airflow-expert (E3.6/7/10), data-quality-agent (E3.8), testing-agent (E3.9). Smoke `airflow dags test dag_transform_e3 2026-05-09` ✅ all 8 tasks SUCCESS, 36/39 DQ passed (0 ERROR, 3 WARNING — alias-drift / ESPN orphan), end-to-end **~14 секунд** (R4 baseline outperformed). Materialized: `silver.whoscored_events_spadl=695,144`, `silver.espn_lineup=15,150`, `gold.fct_event=695,144` (Bronze→Gold parity), `gold.fct_shot=47,105`, `gold.fct_lineup=159,445` (FBref+ESPN). FotMob lineup deferred Phase 1.5 (R0.2c FALLBACK). Match_id passthrough `'whoscored_raw' v0_unbridged` для fct_event — bridging deferred к E1.5/Phase B.

### E3.5 — Historical backfill (NEW, 5-10 дней, может идти параллельно с E4-E6)

**Зависит от**: E3 production-стабильность ≥3 дня.

- Backfill `fct_event`, `fct_shot`, `fct_lineup` для **3 предыдущих сезонов APL** (2022-23, 2023-24, 2024-25).
- Per-season Airflow run; HDFS-monitor включён.
- Validation: row-count vs Bronze, PK uniqueness, point-in-time для rolling features в E6.

**DoD**: 3 сезона backfill'ены, predictions_input использует исторические event-features.

**Status — DONE 2026-05-08** (postmortem `docs/decisions/E3.5-postmortem.md`): Wave 1 (research) + Wave 2 (infra: R4 type unification, parametric `dag_e3_backfill`, DQ extensions) + Wave 3 light scrape (Understat 2122 + ESPN 2122/2223/2324 + 2425 redo = 74,666 rows) + Wave 4 transforms `fct_lineup` (218,961 total: +59,516) + `fct_shot` (56,880 total: +9,775 за 2122) — **CLOSED**. Per-season DQ 9/10 PASS / 0 ERROR / 1 WARNING (~0.9% orphan match_ids — alias drift). **Scope adjustment 2026-05-08**: `fct_event` historical 2122/2223/2324 — **OUT OF SCOPE** (heavy WhoScored CF-bypass scrape ~95h не оправдан); current season 2025-26 уже end-to-end (528,691 events). Phase 2 может вернуться к историческим events. Followups: Task #14 (ESPN read_lineup integration), Task #15 (Iceberg writer auto-chunk on OOM).

### E4 — Narrow facts + ratings + odds + stage (3-5 дней) — **DONE 2026-05-09**

**Зависит от**: E3 (fct_event для view'ов), R0.2b (SofaScore ratings extension).

- ✅ `fct_goal` (UNION fct_shot.result='goal' ⊕ FBref own_goal, 6,525 rows) / `fct_card` (13,608 rows) / `fct_substitution` (25,615 rows). НЕ view'ы — UNION FBref⊕WhoScored с FBref-priority в Silver.
- ✅ `fct_match_rating` (R0.2b SofaScore extension full path — `tls_requests` + residential proxy + ban-counter retry, 200 rows / 5 матчей APL 2526 smoke).
- ✅ `fct_match_odds` (47,012 rows / 100% bridge / tall format 30 bookmaker×market×closing).
- ⏸️ `fct_match_stage` — **deferred Phase 1.5** (whoscored_season_stages.stage всё NULL; ценность низкая до E8a).

**Verdict 2026-05-09** — postmortem `docs/decisions/E4-postmortem.md`. Smoke `airflow dags test dag_transform_e4 2026-05-09` ✅ all 12 tasks SUCCESS, end-to-end ~32 sec, DQ baseline **73/81 passed, 0 ERROR, 8 WARN** (6× ref_integrity Trino alias bug + 2× DoD threshold mis-frame). 9 архитектурных решений (D1-D9): own_goal team_id_canonical = goal-receiving team (FBref convention), tall odds format, MatchHistory bridge через inline 63 team_aliases (RHS = `dim_match` slug, не YAML-canonical), canonical-trio R0.4 (`*_canonical/_source/_version='v1'`), xxhash64 + ROW_NUMBER seq tiebreaker.

### E6 — Features + ML parity + predictions_input v2 (4-6 дней) — **DONE 2026-05-09**

**Зависит от**: E1 (xref), E2-E5 (новые сущности), R9 (Tier 2, baseline Brier).

- ✅ Новые rolling features: `feat_referee_bias` (L10, 6 cols inline-hash referee_id), `feat_team_event_style` (L5, 10 SPADL action-share cols + empty-stub fallback), `feat_team_xg_form` (L5/L10, уже в E5/E6), `unavailable_count_l5` (через `feat_team_form` propagate в `fct_match`).
- ⏸️ `feat_manager_form` / `feat_lineup_strength` — **deferred Phase 1.5** (dim_manager STUB пуст; full SofaScore ratings scrape required).
- ✅ **predictions_input_v2** (104 cols = v1 80 + ref 6 + event_style 20) materialized параллельно с v1; Airflow Variable `predictions_serving_active_version` (default `'v1'`) — manual flip после ≥2 недель green DQ (target 2026-05-23+).
- ✅ `fct_match` 74→104 cols (3 LEFT JOIN'а: rb / hes / aes); `predictions_input` v1 +36 missing cols (gap fix для schema parity v1↔v2).
- ✅ Новый DQ kind `schema_parity` (`CHECK.schema_parity(tables, ignore_cols, severity)`) валидирует column_name+data_type parity между train/test/inference.
- ✅ Point-in-time leak DQ для всех новых feat (`data_quality.point_in_time` × 16 columns).

**Verdict 2026-05-09** — postmortem `docs/decisions/E6-postmortem.md`. 52 unit-тестов pass на хосте (3.04s); 2 wave-execution (W1-W9). R9 Brier baseline — отложен в Phase 1.5 (не блокирует E6 DoD). `predictions_input_v2` cutover — pending dual-run gate (2026-05-23+).

### E7 — BI + Catalog (3-5 дней) — **shipped 2026-05-09 / ≥3d DQ-gate pending до 2026-05-12+**

**Зависит от**: E2-E6 stable.

- ✅ 3 новых mart'а: `mart_scouting_radar` (100,793 rows, per-(player,match) + L5 rolling), `mart_referee_dashboard` (218 rows, per-(referee,season,league)), `mart_event_heatmap` (25,895 rows, SPADL 12×8 zone grid). Денормализованные для прямого Superset usage.
- ⏸️ `mart_manager_profile` — **deferred Phase 1.5** (dim_manager STUB пуст).
- ✅ Superset infra: `import_dashboards.py` (REST client, idempotent CREATE-IF-NOT-EXISTS), `datasources.yaml` (18 datasets), 3 декларативных Python dashboards.
- ✅ OpenMetadata infra: `apply_descriptions.py` (JWT auth, idempotent), 31 YAML descriptions (Tier.Gold + Domain.Football tags + relationships для ER-diagram).
- ✅ Tests + DQ: 483/488 pytest pass, mart row counts verified, no_duplicates+freshness+value_range+point_in_time для каждого mart'а.

**Verdict 2026-05-09** — postmortem `docs/decisions/E7-postmortem.md`. 7 архитектурных решений (D1-D7): денормализованные mart'ы, empty-stub fallback'и, idempotent REST clients, OM relationships в YAML, Superset SDK dashboards в Python (vs JSON exports), 12×8 SPADL zone grid (FIFA Connect compatible), light DQ для mart'ов. **≥3d green-gate перед E8a — opened**, target close 2026-05-12+ (3 successful master_pipeline runs без DQ ERROR).

### E8 — T1 multi-competition expansion (split, was 5-10 дней → 15-25 дней) ⚠️

**Зависит от**: R0.1 (coverage matrix), R0.3 (storage guard), E0-E7 stable, **proactive storage guard DAG** запущен.

#### E8a — UCL only (5-7 дней)
- ~125 матчей/сезон — manageable scale.
- Расширить `_team_aliases.sql` для UCL (~80 European clubs).
- Storage guard DAG: Telegram alert ≥75% HDFS, auto-pause ingestion ≥85%.
- **30-day green DQ gate перед E8b**.

#### E8b — Top-5 European leagues (5-10 дней)
- La Liga, Serie A, Bundesliga, Ligue 1, Eredivisie.
- ~360 матчей/сезон каждый. Aliases × 5 leagues.
- **30-day green DQ gate перед E8c**.

#### E8c — EL + cup competitions (5-8 дней)
- Europa League + национальные кубки T1.

**DoD каждой подфазы**: 7+ дней green, HDFS под guard threshold, обязательная **очистка queued runs** перед unpause (Cloudflare backlog rule из памяти проекта).

### E9 — Deprecation (5-10 дней + 30 d wait)

**Зависит от**: T1 (E8c) ≥30 дней green DQ, `audit_gold_usage.py` (R0.5) baseline + post-T1 snapshot.

- Старые FBref-only DAG'и → no-op shims.
- `gold.entity_xref` → VIEW поверх Silver xref'ов.
- Drop legacy gold таблиц с **нулевыми query-counts за 30 дней** (verified через R0.5 tooling).
- Update CLAUDE.md.

**DoD**: T1 ≥30 дней green, legacy deprecated, OpenMetadata lineage updated.

---

## Сводка timeline

| # | Этап | Дней | Риск |
|---|---|---|---|
| **R0.1-R0.5** | Source/feasibility research (NEW) | 5-7 | — |
| **R1-R4** | Tier 1 research (after R0) | 8-12 | — |
| E0 | Baseline + tooling | 1-2 | — |
| E2 | Master-data dims (manager scraper ext.) | 3-5 + R0.2a feedback | medium |
| E5 | Player availability | 2-3 | **DONE 2026-05-07** |
| E1 | xref refactor (Gold→Silver) | 3-5 | **DONE 2026-05-08** |
| E3 | Core match facts | 5-7 | **DONE 2026-05-08** |
| E3.5 | Historical backfill (NEW) | 5-10 | medium (storage) |
| E4 | Narrow facts + ratings + odds | 3-5 + R0.2b feedback | **DONE 2026-05-09** |
| E6 | Features + ML + predictions v2 | 4-6 | **DONE 2026-05-09** (v2 cutover pending 2026-05-23+) |
| E7 | BI + Catalog | 3-5 | **shipped 2026-05-09 / DQ-gate pending 2026-05-12+** |
| E8a | UCL expansion | 5-7 | high |
| E8b | Top-5 leagues | 5-10 | **very high** |
| E8c | EL + cups | 5-8 | high |
| E9 | Deprecation | 5-10 + 30d | medium |
| R6-R10 | Tier 2 research (parallel/deferred) | 6-10 | — |
| R2-followup | Production player resolver | 5-10 | — |

**Total**: ~75-115 рабочих дней (~15-23 недели calendar) включая всю research-фазу, расщеплённый E8 и backfill.

> **Изменение от v1**: было 39-65 дней (~6-11 недель). Реалистичный budget на 25-75% выше из-за:
> - Phase 0 source-research (5-7 дней) — был пропущен в v1
> - R2 / R3 пересмотрены до 5 дней каждый (было 1-2)
> - E3.5 backfill добавлен (5-10 дней)
> - E8 расщеплён на 3 подфазы с 30d green-gate каждая

**Rollback стратегия**:
- Research-фаза без production-impact (только decision docs).
- E1-E7 additive (revert commit + drop new tables, legacy не затронут).
- E8a/b/c — revert config tier per подфаза.
- E9 — Iceberg time-travel rollback (snapshot retention 30 d из E0).

**Гейты прогресса**:
- Каждый E-этап имеет `≥3 дня green DQ` гейт перед следующим.
- E8 подфазы — `≥30 дней green DQ` каждая.
- Postmortem обязателен после каждого E-этапа.

---

## Не входит в scope (отдельные roadmaps)

- **Phase 1.5** — `R2-followup` production player resolver, `fct_match_rating` если R0.2b даст negative.
- **Phase 2** — set pieces / possession sequences / xT-VAEP / weather / FIFA attrs / Elo history.
- **Phase 3** — Transfermarkt scraper для transfer / market value / contract / awards.

---

## Пример: Bukayo Saka — путь через Medallion (Arsenal, APL 2024-25)

### Bronze — сырые данные из 4 источников, у каждого свой ID

| Источник (таблица) | player_id | Что лежит | Пример колонок |
|---|---|---|---|
| `bronze.fbref_player_match_stats` | `bc7dc64d` | per-match stats | minutes=90, goals=1, xg=0.42, key_passes=2 |
| `bronze.understat_player_match_xg` | `1006` | per-match shot-xG | xG=0.48, xA=0.21, shots=3 |
| `bronze.whoscored_events` | `47820` | per-event Opta-derived (~80/матч) | type='Pass', x=72.3, y=45.1, minute=23 |
| `bronze.sofascore_player_ratings` _(после R0.2b)_ | `801021` | subjective rating | rating=8.4 |

**Проблема**: 4 разных ID одного физического игрока. JOIN'ить нельзя.

### Silver — identity resolution + унификация

**`silver.xref_player`** (E1):

| canonical_id | source | source_id | confidence |
|---|---|---|---|
| `fb_bc7dc64d` | fbref | `bc7dc64d` | exact |
| `fb_bc7dc64d` | understat | `1006` | name_team |
| `fb_bc7dc64d` | whoscored | `47820` | name_team_jersey |
| `fb_bc7dc64d` | sofascore | `801021` | name_team |

**`silver.player`** (E1, unified master):

| player_id | name | nation | dob | position | team_id |
|---|---|---|---|---|---|
| `fb_bc7dc64d` | Bukayo Saka | ENG | 2001-09-05 | RW | arsenal |

После Silver — **один ID `fb_bc7dc64d`** во всём downstream. Игроки, не разрешённые resolver'ом — `us_<understat_id>` orphan.

### Gold — бизнес-модель, всё JOIN'ится по `player_id`

**`dim_player`** (master, SCD-2 в E2):

| player_id | name | position | team_id | valid_from | valid_to |
|---|---|---|---|---|---|
| `fb_bc7dc64d` | Bukayo Saka | RW | arsenal | 2024-08-01 | NULL |

**`fct_player_match`** (per match, _rating подтянулся из SofaScore через xref_):

| match_id | player_id | minutes | goals | xg | rating |
|---|---|---|---|---|---|
| `cc5b4244` | `fb_bc7dc64d` | 90 | 1 | 0.42 | 8.4 |

**`fct_lineup`** (E3, FBref+ESPN+FotMob union):

| match_id | player_id | jersey | position | is_starter | minutes |
|---|---|---|---|---|---|
| `cc5b4244` | `fb_bc7dc64d` | 7 | RW | true | 90 |

**`fct_event`** (E3, SPADL ИЛИ raw-WhoScored — зависит от R3):

| match_id | event_id | player_id | type | x | y | result |
|---|---|---|---|---|---|---|
| `cc5b4244` | 42 | `fb_bc7dc64d` | pass | 72.3 | 45.1 | success |
| `cc5b4244` | 156 | `fb_bc7dc64d` | shot | 89.5 | 52.8 | goal |

**`fct_shot`** (E3, shot-grained xG из Understat):

| match_id | shot_id | player_id | minute | body_part | xg | result |
|---|---|---|---|---|---|---|
| `cc5b4244` | 3 | `fb_bc7dc64d` | 67 | right_foot | 0.42 | goal |

**`fct_player_unavailable`** (E5 — _injury, top-предиктор для ML_):

| player_id | team_id | from | to | reason | missed |
|---|---|---|---|---|---|
| `fb_bc7dc64d` | arsenal | 2024-12-22 | 2025-02-15 | hamstring | 8 |

**`feat_player_form`** (rolling L5):

| match_id | player_id | goals_l5 | xg_l5 | rating_l5 |
|---|---|---|---|---|
| `cc5b4244` | `fb_bc7dc64d` | 3 | 2.8 | 7.6 |

### Что даёт каждый слой

| Слой | Роль | На примере Saka |
|---|---|---|
| **Bronze** | сырая правда по каждому источнику | 4 ID, 4 формата, без join'ов — для аудита/rescrape |
| **Silver** | identity resolution + типизация + dedup | `xref_player` сводит 4 ID → 1 canonical |
| **Gold** | бизнес-модель | `dim_player` + facts join'ятся по `player_id` без знания источников |

Тот же паттерн (Bronze → Silver xref → Silver unified → Gold dim/fct) повторяется для **team, match, referee, manager** — всех сущностей с cross-source ID.

---

## Пример R1: Cross-source xG conflict resolution

### Проблема

xG для одного матча/игрока приходит из 3 источников с разными моделями:

| Источник | xG-модель | Granularity | Покрытие |
|---|---|---|---|
| FBref | StatsBomb | per-player-match aggregate | Top-5 + APL |
| Understat | собственная | shot-level | 7 European top |
| WhoScored | Opta-derived | per-event | global |

На том же матче Saka можно получить 0.42 (StatsBomb) vs 0.48 (Understat) vs 0.39 (Opta-derived).

### Стратегия: store-all + canonical with priority + schema-version

**Не усреднять** (биас моделей не компенсируется). Хранить всё + явный canonical с DQ-alert при расхождениях. **Threshold — data-driven** (R1 empirical step).

#### Source priority — **VERDICT R1 (2026-05-06)**

| Metric type | Primary | Fallback | Version | Source rationale |
|---|---|---|---|---|
| Shot-level xG | Understat | — | v1 | only source (FBref no xG, WhoScored qualifier-derived deferred) |
| Per-match-team xG | Understat | — | v1 | FBref no xG (R0.1) |
| Per-player-match xG | Understat | — | v1 | FBref no xG (R0.1) |
| Match score | FBref | Sofascore→Matchhistory | v1 | exact-equality 727/727 verified |
| Goals/cards/subs count | FBref (after dedup) | WhoScored | v1 | FBref complete; ws_subs partial 2425 |
| Player rating | Sofascore | — | v1 | only source (R0.2b extension) |

> **Note**: priority инвертирование (FBref→Understat) settled R0.1 — FBref xG не существует. Re-materialization via R0.4 runbook не нужно для v1.

#### Что лежит в Gold

**`fct_player_match`** (per-player — все источники + canonical + version):

| match_id | player_id | xg_canonical | xg_source | xg_version | xg_fbref | xg_understat | xg_disagreement |
|---|---|---|---|---|---|---|---|
| `cc5b4244` | `fb_bc7dc64d` | 0.42 | fbref | v1 | 0.42 | 0.48 | 0.06 |

**`fct_match`** (team-level — то же):

| match_id | home_xg_canonical | home_xg_source | home_xg_version | home_xg_fbref | home_xg_understat | home_xg_opta |
|---|---|---|---|---|---|---|
| `cc5b4244` | 1.84 | understat | v1 | 1.71 | 1.84 | 1.79 |

#### DQ-правила — **VERDICT R1 (data-driven thresholds, APL 2425+2526)**

**Coverage**: для canonical xG ≥ 80% строк (`coverage()` two-tier уже в `data_quality.py`).

**Cross-source agreement** (numeric из R1 empirical):
- `match_score`: ERROR on ANY pairwise mismatch (FBref/Sofa/MH = 0/727 baseline).
- `goals_count`: WARNING `|fb-ws|>2`, ERROR `|fb-ws|>3`.
- `cards_count`: WARNING `|fb-ws|>1`, ERROR `|fb-ws|>3`.
- `subs_count`: WARNING `|fb-ws|>1` (excl. ws_subs=0 partial-ingestion), ERROR `|fb-ws|>3`.

**Internal-consistency** (Understat self-checks):
- `understat_match_xg vs SUM(shots.xg)`: WARNING p95>0.20, ERROR p95>0.50.
- `silver.understat_player_match_xg vs bronze.player_match_stats`: ERROR p99>0.01 (любой отлёт от 0 = bug).

**Bronze sanity prerequisites** (must be enforced BEFORE cross-source DQ runs):
- `no_duplicates(understat_schedule, key=[game_id])` — ERROR (R1 surfaced ×10 dup bug).
- `no_duplicates(fbref_match_events, key=[match_id,minute,event_type,player,secondary_player])` — WARNING (R1 surfaced ~18% near-dups).
- `data_completeness(whoscored_events, min_subs_per_match=4)` — ERROR (catches Apr-30 backlog gaps).

**Audit**: `_canonical / _source / _version='v1'` колонки обязательны для backtest'ов и re-materialization workflow.

#### ML-implication

В `predictions_input` `xg_source` включается как категориальный признак. Модели типа CatBoost/LightGBM учат, что shap для каждого источника отличается — корректирует bias автоматически.

`xg_version` используется в re-materialization workflow: при изменении priority в R1 — старые `_v1` features keep, новые `_v2` живут параллельно ≥2 недели (как `predictions_input_v2`).

Этот же паттерн (store-all + canonical-with-priority + version + cross-source DQ) применяется к match scores, ratings, goals/cards/subs.
