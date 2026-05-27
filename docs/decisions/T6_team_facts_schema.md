# T6 — Cross-source team facts (season + match) schema contract

**Status**: Draft (gate для всей волны T6.*)
**Подготовлено**: 2026-05-27
**Closes**: [#90](https://github.com/sergeykuznetsov1995/data-platform-football/issues/90)
**Sub-issues**: [#91](https://github.com/sergeykuznetsov1995/data-platform-football/issues/91), [#92](https://github.com/sergeykuznetsov1995/data-platform-football/issues/92), [#93](https://github.com/sergeykuznetsov1995/data-platform-football/issues/93), [#94](https://github.com/sergeykuznetsov1995/data-platform-football/issues/94), [#95](https://github.com/sergeykuznetsov1995/data-platform-football/issues/95), [#96](https://github.com/sergeykuznetsov1995/data-platform-football/issues/96); [#97](https://github.com/sergeykuznetsov1995/data-platform-football/issues/97) (FotMob, Phase 2)
**Cited research / decisions**:
- `docs/research/RX2_xg_source_selection.md` — Understat primary для xG/xA
- `docs/research/RX_cross_source_player_profile.md` — wide single-column паттерн
- `docs/research/R1_cross_source_thresholds.md` — DQ thresholds для cross-source diff
- `docs/decisions/RX-implementation-plan.md` — паттерн player-wave T3-T5

---

## 1. Кратко

В Gold сейчас асимметрия: для игроков уже привезён 5-source `gold.fct_player_season_stats` (2551 rows outfield) + 4-source `gold.fct_player_match` (волна T3-T5, май 2026), а для команд — только FBref-only `gold.fct_team_match` и **нет** `gold.fct_team_season_stats` вовсе. Wave T6 устраняет это: новый `fct_team_season_stats` (4-source) + расширение `fct_team_match` до 4-source.

Документ — **gate** перед T6.2-T6.6 (issues #91–#96): фиксирует column contract, JOIN-keys, audit-стратегию и source priority до того как кто-то пишет SQL. После merge этого PR в body каждой sub-issue появляется ссылка `Design contract: docs/decisions/T6_team_facts_schema.md`.

Ключевые решения:
- **Spine** — FBref: `silver.fbref_team_season_profile` (season) и `silver.fbref_match_enriched` (match). У FBref полное покрытие APL и каноническая роль в `silver.xref_team`.
- **4 источника** в Phase 1: **FBref + Understat + WhoScored + SofaScore**. FotMob = Phase 2 slot (#97 blocked) — резервируется в column-mapping и COALESCE-цепочках, чтобы при подключении не делать SQL-refactor.
- **Audit-таблицы** — отдельные `<table>_audit` (separate from business fct), INNER JOIN на FBref ∩ secondary source, WARNING-only DQ (см. `feedback_audit_in_separate_table.md`).
- **PK** — natural composite (без xxhash64): `(team_id_canonical, league, season)` для season-таблицы, `(match_id_canonical, team_id_canonical)` для match-таблицы. Оба компонента non-NULL по конструкции INNER spine.
- **xref JOIN predicate** — ВСЕГДА `(source, source_id, league, season)`; пропуск `(league, season)` даёт row fan-out 1.5-4× (см. `feedback_xref_join_season_predicate.md`).

---

## 2. Архитектура

```
                                    silver.xref_team (8-source UNION, PK=(source, source_id, league, season))
                                                       │
        ┌─────────────────┬──────────────────────┬─────┴────────┬──────────────────────────┐
        │                 │                      │              │                          │
        ▼                 ▼                      ▼              ▼                          ▼
silver.fbref_team_   silver.understat_     silver.whoscored_   silver.sofascore_     silver.fotmob_
season_profile       team_season           team_season         team_season           team_season
(spine, existing,    (#91 — NEW,           (#92 — NEW,         (#93 — NEW,           (Phase 2 — #97)
 70+ cols)            GROUP BY player      GROUP BY            PIVOT match_stats
                      match → team)        whoscored_events    or event_player_stats
                                           SPADL → team)       → team)
        │                 │                      │              │                          ┊
        └─────────────────┴──────────────────────┴──────────────┴──────────────────────────┘
                                                       │
                                                       ▼
                                       gold.fct_team_season_stats (#94 — NEW)
                                       gold.fct_team_season_stats_audit (#94 — NEW)
                                          INNER JOIN FBref ∩ secondary, WARNING-only DQ


                                    silver.xref_team
                                                       │
        ┌─────────────────┬──────────────────────┬─────┴────────┬──────────────────────────┐
        ▼                 ▼                      ▼              ▼                          ▼
silver.fbref_match_   silver.understat_     silver.whoscored_   silver.sofascore_     (FotMob Phase 2)
enriched              team_match            team_match          team_match
(spine, existing)     (#91)                 (#92)               (#93)
        │                 │                      │              │
        └─────────────────┴──────────────────────┴──────────────┴──────────────────────────┘
                                                       │
                                                       ▼
                                       gold.fct_team_match v2 (#95 — backwards-compat extension)
                                       gold.fct_team_match_audit (#95 — NEW)
```

**Key invariants**:
- Gold = strictly one-hop от Silver. Audit-таблицы читают Silver, **не** business fct (см. `project_gold_cleanup_2026-05-12.md`).
- `fct_team_match` v2 не ломает downstream: `feat_team_form`, `feat_team_h2h`, `feat_team_xg_form`, `fct_player_unavailable` JOIN'ы → preserve v1 columns.

---

## 3. Why these specific values

| Решение | Source | Why |
|---|---|---|
| Spine = FBref | T5 precedent (`fct_player_season_stats`) | `silver.fbref_team_season_profile` имеет полное покрытие APL и 70+ team-season колонок; FBref — каноническая «правда» identity в `silver.xref_team` |
| HARD_FACT single column через COALESCE | `R1_cross_source_thresholds.md`, `feedback_audit_in_separate_table.md` | ≤1% drift между официальными источниками для целочисленных counters (goals, cards); один column + audit dif дешевле для аналитика чем 4 |
| Understat primary для xG/xA | `RX2_xg_source_selection.md` | coverage 99.2% (vs FotMob 81.7%, SS 84.6%); pairwise r ≥ 0.989; predictive quality ≈ identical |
| Audit в отдельной таблице | `feedback_audit_in_separate_table.md` | T5 v1 имел 8 diff-колонок в main fct → 50-col `SELECT *`; v2 после рефактора: main 42 business + audit 12 (PK + diffs). Аналитики жалуются на загромождение. |
| Natural composite PK (без xxhash64) | T5 precedent (`fct_player_season_stats:65–68`) | `(team_id_canonical, league, season)` low cardinality, оба NOT NULL по INNER FBref spine. xxhash64 + ROW_NUMBER tiebreaker (`feedback_hash_pk_with_null_canonical.md`) нужен только когда natural key содержит COALESCE'ed компонент — здесь не наш случай. |
| `(league, season)` predicate на ВСЕХ xref JOIN | `feedback_xref_join_season_predicate.md` | `silver.xref_team` имеет per-(source, source_id, **season**) rows; без season-condition fan-out 1.5-4× |
| Bronze DOUBLE IDs → double-cast | `feedback_bronze_double_id_cast.md` | `bronze.whoscored_events.team_id` — DOUBLE; direct `CAST(... AS varchar)` даёт `'9.5408E4'` (scientific notation) → молчаливый NULL-join на xref |
| Сохранить FBref columns в `fct_team_match` v2 | `feat_team_form.sql:50–64`, `feat_team_h2h.sql:15–27`, `feat_team_xg_form.sql:81` | Downstream features INNER-JOIN'ят `goals_for`, `goals_against`, `shots`, `shots_on_target`, `possession`, `points`, `result`, `is_home`, `opponent_id`, `gameweek`, `date` — переименование/удаление их сломает rolling features |
| FotMob = reserved slot, не Out of Scope | issue #90 + user-confirmed (Reserved slot) | #97 заблокирован (FotMob match-stats нужен `_next/data` slug-form path); резервируем колонку в column-mapping и слот в COALESCE-chain — when #97 разблокируется, SQL-refactor не нужен |

---

## 4. xref JOIN contract

**Canonical template** (одинаковый для season и match grain):

```sql
-- xref CTE — материализуем subset xref_team per source
xref_<source> AS (
    SELECT DISTINCT
        canonical_id,
        source_id                            AS <source>_team_id,  -- raw bronze identifier
        league,
        season                               AS season_slug       -- varchar '2526'
    FROM iceberg.silver.xref_team
    WHERE source = '<fbref|understat|whoscored|sofascore|fotmob>'
      AND confidence <> 'orphan'             -- exclude unmatched (orphans don't bridge)
)

-- JOIN на silver source-агрегат
LEFT JOIN iceberg.silver.<source>_team_<grain> src
    ON  src.<team_id_col> = xref_<source>.<source>_team_id
    AND src.league        = xref_<source>.league         -- REQUIRED
    AND src.season        = xref_<source>.season_slug    -- REQUIRED
```

**Required ON-predicate** — `(source_id, league, season)`. **Не пропускать `league`/`season`** — см. `feedback_xref_join_season_predicate.md` (без season-condition fan-out 1.5-4×).

**Season type discrepancy** (cм. `dags/sql/gold/fct_player_season_stats.sql:26–33`):

| Source | season type | Sample value | Conversion |
|---|---|---|---|
| `silver.xref_team` | varchar slug | `'2526'` | spine |
| `silver.fbref_team_season_profile` | bigint | `2025` | `2000 + CAST(SUBSTR(season, 1, 2) AS BIGINT)` |
| `silver.understat_team_*` (#91) | varchar slug | `'2526'` | direct match с xref slug |
| `silver.whoscored_team_*` (#92) | varchar slug | `'2526'` | direct match |
| `silver.sofascore_team_*` (#93) | varchar slug | `'2526'` | direct match |
| `silver.fotmob_team_*` (#97) | bigint | `2025` | `2000 + SUBSTR(season,1,2)` (TBD) |

**Bronze DOUBLE ID workaround**: для WhoScored Bronze (`bronze.whoscored_events.team_id` — DOUBLE) использовать `CAST(CAST(team_id AS BIGINT) AS varchar)` ДО JOIN на xref. Этот cast обычно делается уже в Silver-aggregate (#92), но дублирование в Gold-уровне в качестве защиты допустимо.

**xref_team source prefixes** (для orphan canonical_id, см. `silver/xref_team.sql.j2:147–155`):
- FBref → `fb_<slug>`
- Understat → `us_<slug>`
- WhoScored → `ws_<slug>`
- SofaScore → `ss_<slug>`
- FotMob → `fm_<slug>` (Phase 2)

---

## 5. Column contract — `gold.fct_team_season_stats`

**Grain**: `(team_id_canonical, league, season)`. **PK** = natural composite (см. §7). **Spine**: `silver.fbref_team_season_profile` через `xref_team(source='fbref', confidence<>'orphan')`. **Partitioning**: `(league, season)`.

**Section layout** (по образцу `fct_player_season_stats.sql:73–207`): Identity → HARD_FACT → MODELED → UNIQUE_FBREF → UNIQUE_UNDERSTAT → UNIQUE_WHOSCORED → UNIQUE_SOFASCORE → Lineage.

### 5.1 Column mapping table

Source legend: `fb` = FBref, `us` = Understat, `ws` = WhoScored, `ss` = SofaScore, `fm` = FotMob (#97 Phase 2 — reserved). Skipped cells mean «у этого источника метрики нет».

| target_column | dtype | fb (`fbref_team_season_profile`) | us (#91 `understat_team_season`) | ws (#92 `whoscored_team_season`) | ss (#93 `sofascore_team_season`) | fm (Phase 2 — #97) | nullable | rule |
|---|---|---|---|---|---|---|---|---|
| **Identity** | | | | | | | | |
| `team_id_canonical` | varchar | xref → spine | — | — | — | — | NO | xref_fbref.canonical_id |
| `league` | varchar | `league` | — | — | — | — | NO | spine |
| `season` | bigint | derived | — | — | — | — | NO | `2000 + SUBSTR(slug,1,2)` |
| `primary_team_name` | varchar | `team` | — | — | — | — | YES | display |
| **HARD_FACT — counters (COALESCE fb → us → ws → ss → [fm])** | | | | | | | | |
| `matches` | BIGINT | `mp` | `games_played` | `matches_seen` | `appearances` | (#97) `matches_played` | NO | COALESCE |
| `minutes` | BIGINT | `minutes` | `minutes_played` | — | `minutes_played` | (#97) `minutes_played` | NO | COALESCE |
| `goals` | BIGINT | `goals` | `goals` | — | `goals` | (#97) `goals` | NO | COALESCE |
| `goals_against` | BIGINT | derived from `gk_goals_against` | `goals_conceded` | — | `goals_conceded` | (#97) `goals_against` | YES | COALESCE |
| `assists` | BIGINT | `assists` | `assists` | — | `assists` | (#97) `assists` | YES | COALESCE |
| `yellow_cards` | BIGINT | `yellow_cards` | — | — | `yellow_cards` | — | YES | COALESCE |
| `red_cards` | BIGINT | `red_cards` | — | — | `red_cards` | — | YES | COALESCE |
| `second_yellow_cards` | BIGINT | `second_yellow_cards` | — | — | — | — | YES | fb only |
| `total_shots` | BIGINT | `total_shots` | `shots` | `shots_total` | `total_shots` | (#97) `shots` | YES | COALESCE |
| `shots_on_target` | BIGINT | `shots_on_target` | — | `shots_on_target_proxy` | `shots_on_target` | (#97) `shots_on_target` | YES | COALESCE |
| `fouls_committed` | BIGINT | `fouls_committed` | — | `fouls_committed` | `fouls` | — | YES | COALESCE |
| `fouls_drawn` | BIGINT | `fouls_drawn` | — | — | `was_fouled` | — | YES | COALESCE |
| `offsides` | BIGINT | `offsides` | — | — | `offsides` | — | YES | COALESCE |
| `crosses` | BIGINT | `crosses` | — | — | `total_crosses` | — | YES | COALESCE |
| `interceptions` | BIGINT | `interceptions` | — | `interceptions` | `interceptions` | — | YES | COALESCE |
| `tackles_won` | BIGINT | `tackles_won` | — | `tackle_won` | `tackles_won` | — | YES | COALESCE |
| `penalties_won` | BIGINT | `penalties_won` | — | — | `penalty_won` | — | YES | COALESCE |
| `penalties_conceded` | BIGINT | `penalties_conceded` | — | — | `penalty_conceded` | — | YES | COALESCE |
| `own_goals` | BIGINT | `own_goals` | — | — | — | — | YES | fb only |
| **MODELED — xG/xA (Understat primary per RX2)** | | | | | | | | |
| `expected_goals` | DOUBLE | — | `xg` (primary) | — | `expected_goals` | (#97) `expected_goals` | YES (≤1% gap) | COALESCE(us, [fm], ss) |
| `expected_goals_against` | DOUBLE | — | `xg_against` (primary) | — | `expected_goals_against` | (#97) `expected_goals_against` | YES | COALESCE(us, [fm], ss) |
| `expected_assists` | DOUBLE | — | `xa` (primary) | — | `expected_assists` | (#97) `expected_assists` | YES | COALESCE(us, [fm], ss) |
| `npxg` | DOUBLE | — | `npxg` | — | — | — | YES | us-only (issue #103) |
| **UNIQUE_FBREF** (FBref-only, no fallback) | | | | | | | | |
| `players_used` | INTEGER | `players_used` | — | — | — | — | YES | fb |
| `avg_age` | DOUBLE | `avg_age` | — | — | — | — | YES | fb |
| `possession_pct` | DOUBLE | `possession` | — | — | — | — | YES | fb |
| `goals_per_90` | DOUBLE | `goals_per_90` | — | — | — | — | YES | fb |
| `goals_assists_per_90` | DOUBLE | `goals_assists_per_90` | — | — | — | — | YES | fb |
| `non_penalty_goals_per_90` | DOUBLE | `non_penalty_goals_per_90` | — | — | — | — | YES | fb |
| `shots_per_90` | DOUBLE | `shots_per_90` | — | — | — | — | YES | fb |
| `goals_per_shot` | DOUBLE | `goals_per_shot` | — | — | — | — | YES | fb |
| `goals_per_shot_on_target` | DOUBLE | `goals_per_shot_on_target` | — | — | — | — | YES | fb |
| `complete_matches` | INTEGER | `complete_matches` | — | — | — | — | YES | fb |
| `substitutions` | INTEGER | `substitutions` | — | — | — | — | YES | fb |
| `unused_subs` | INTEGER | `unused_subs` | — | — | — | — | YES | fb |
| `points_per_match` | DOUBLE | `points_per_match` | — | — | — | — | YES | fb |
| `on_field_goals` | INTEGER | `on_field_goals` | — | — | — | — | YES | fb |
| `on_field_goals_against` | INTEGER | `on_field_goals_against` | — | — | — | — | YES | fb |
| `plus_minus` | INTEGER | `plus_minus` | — | — | — | — | YES | fb |
| `plus_minus_per_90` | DOUBLE | `plus_minus_per_90` | — | — | — | — | YES | fb |
| **UNIQUE_FBREF — goalkeeping aggregates** (team-level GK; см. §12 OOS на отдельный fct_team_keeper) | | | | | | | | |
| `gk_goals_against` | INTEGER | `gk_goals_against` | — | — | — | — | YES | fb |
| `gk_saves` | INTEGER | `gk_saves` | — | — | — | — | YES | fb |
| `gk_shots_on_target_against` | INTEGER | `gk_shots_on_target_against` | — | — | — | — | YES | fb |
| `clean_sheets` | INTEGER | `clean_sheets` | — | — | — | — | YES | fb |
| `gk_minutes` | INTEGER | `gk_minutes` | — | — | — | — | YES | fb |
| `save_pct` | DOUBLE | `save_pct` | — | — | — | — | YES | fb (weighted) |
| `gk_pk_attempts_faced` | INTEGER | `gk_pk_attempts_faced` | — | — | — | — | YES | fb |
| `gk_pk_allowed` | INTEGER | `gk_pk_allowed` | — | — | — | — | YES | fb |
| `gk_pk_saved` | INTEGER | `gk_pk_saved` | — | — | — | — | YES | fb |
| `goals_against_per_90` | DOUBLE | `goals_against_per_90` | — | — | — | — | YES | fb |
| **UNIQUE_UNDERSTAT** (pressing / depth, US-only) | | | | | | | | |
| `ppda` | DOUBLE | — | `ppda` | — | — | — | YES | us (passes-allowed per defensive action) |
| `oppda` | DOUBLE | — | `oppda` | — | — | — | YES | us (opp ppda → high press resistance) |
| `deep_completions` | INTEGER | — | `deep` | — | — | — | YES | us |
| `deep_completions_allowed` | INTEGER | — | `deep_allowed` | — | — | — | YES | us |
| `xpts` | DOUBLE | — | `xpts` | — | — | — | YES | us-only (expected points, model output) |
| **UNIQUE_WHOSCORED** (event-style, derived from `silver.whoscored_events_spadl`) | | | | | | | | |
| `pass_total` | BIGINT | — | — | `pass_total` | — | — | YES | ws aggregate |
| `pass_ok` | BIGINT | — | — | `pass_ok` | — | — | YES | ws |
| `pass_pct` | DOUBLE | — | — | `pass_pct` | — | — | YES | ws |
| `takeon_total` | BIGINT | — | — | `takeon_att` | — | — | YES | ws |
| `takeon_won` | BIGINT | — | — | `takeon_won` | — | — | YES | ws |
| `takeon_pct` | DOUBLE | — | — | `takeon_pct` | — | — | YES | ws |
| `defensive_actions_third` | BIGINT | — | — | derived | — | — | YES | ws (events_spadl GROUP BY area_of_field) |
| `set_piece_share_pct` | DOUBLE | — | — | derived | — | — | YES | ws |
| **UNIQUE_SOFASCORE** (duels, breakdowns) | | | | | | | | |
| `ground_duels_won` | BIGINT | — | — | — | `ground_duels_won` | — | YES | ss |
| `ground_duels_won_pct` | DOUBLE | — | — | — | `ground_duels_won_pct` | — | YES | ss |
| `aerial_duels_won` | BIGINT | — | — | — | `aerial_duels_won` | — | YES | ss |
| `aerial_duels_won_pct` | DOUBLE | — | — | — | `aerial_duels_won_pct` | — | YES | ss |
| `total_duels_won_pct` | DOUBLE | — | — | — | `total_duels_won_pct` | — | YES | ss |
| `corner_kicks` | INTEGER | — | — | — | `corners` | — | YES | ss |
| `accurate_long_balls_pct` | DOUBLE | — | — | — | `accurate_long_balls_pct` | — | YES | ss |
| **Lineage** | | | | | | | | |
| `_gold_created_at` | TIMESTAMP | — | — | — | — | — | NO | `CURRENT_TIMESTAMP` |

**Итого**: ~75 колонок (29 HARD_FACT + MODELED, 16 UNIQUE_FBREF inc. GK, 5 UNIQUE_US, 8 UNIQUE_WS, 7 UNIQUE_SS, 4 identity, 1 lineage, 4 ID stamps). Точный список финализирует #94 при имплементации (могут добавиться столбцы которые всплывут в Silver-агрегатах #91-#93).

### 5.2 SQL skeleton (для #94)

```sql
WITH
xref_fbref AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                    AS fbref_team_id,
        league,
        season                                       AS season_slug,
        2000 + CAST(SUBSTR(season, 1, 2) AS BIGINT) AS season_year
    FROM iceberg.silver.xref_team
    WHERE source = 'fbref'
      AND confidence <> 'orphan'
)
-- (analogous CTEs for us / ws / ss / [fm Phase 2])

SELECT
    xf.canonical_id                                  AS team_id_canonical,
    xf.league                                        AS league,
    xf.season_year                                   AS season,
    COALESCE(fb.team, ...)                           AS primary_team_name,

    -- HARD_FACT (COALESCE fb → us → ws → ss → [fm])
    CAST(COALESCE(fb.mp, us.games_played, ws.matches_seen, ss.appearances) AS BIGINT) AS matches,
    -- ...

    -- MODELED — Understat primary
    ROUND(COALESCE(us.xg, /* fm.expected_goals, */ ss.expected_goals), 2) AS expected_goals,

    -- UNIQUE_<source>
    fb.players_used, fb.avg_age, fb.possession AS possession_pct,
    us.ppda, us.deep,
    ws.pass_total, ws.takeon_pct,
    ss.aerial_duels_won, ss.ground_duels_won_pct,

    CURRENT_TIMESTAMP                                AS _gold_created_at

FROM xref_fbref xf
INNER JOIN iceberg.silver.fbref_team_season_profile fb
    ON  fb.team_id = xf.fbref_team_id
    AND fb.league  = xf.league
    AND fb.season  = xf.season_year
LEFT JOIN iceberg.silver.understat_team_season us
    ON  us.canonical_id = xf.canonical_id
    AND us.league       = xf.league
    AND us.season       = xf.season_slug
LEFT JOIN iceberg.silver.whoscored_team_season ws
    ON  ws.canonical_id = xf.canonical_id
    AND ws.league       = xf.league
    AND ws.season       = xf.season_slug
LEFT JOIN iceberg.silver.sofascore_team_season ss
    ON  ss.canonical_id = xf.canonical_id
    AND ss.league       = xf.league
    AND ss.season       = xf.season_slug
-- LEFT JOIN iceberg.silver.fotmob_team_season fm (Phase 2 — #97)
```

---

## 6. Column contract — `gold.fct_team_match` v2

**Grain**: `(match_id_canonical, team_id_canonical)`. **PK** = natural composite (оба non-NULL по INNER spine). **Spine**: `silver.fbref_match_enriched` + `gold.dim_match` (как в v1). **Partitioning**: `(league, season)`.

### 6.1 Backwards-compat invariant (CRITICAL)

Downstream consumers и колонки которые они INNER-JOIN'ят:

| Consumer | Columns used (must preserve) | Source |
|---|---|---|
| `gold.feat_team_form` | `match_id, team_id, opponent_id, date, gameweek, is_home, goals_for, goals_against, shots, shots_on_target, possession, points, result, league, season` | `dags/sql/gold/feat_team_form.sql:50–64` |
| `gold.feat_team_h2h` | `match_id, team_id, opponent_id, date, gameweek, goals_for, goals_against, points, result, league, season` | `feat_team_h2h.sql:15–27` |
| `gold.feat_team_xg_form` | `match_id, team_id, opponent_id, date, season, league` (через `fct_team_match` spine) | `feat_team_xg_form.sql:81` |
| `gold.fct_player_unavailable` JOIN | `match_id, team_id` | (via `team_unavail` CTE) |

**Rule**: ни одну из этих колонок **не переименовываем**, **не удаляем**, **не меняем тип**. Все новые поля — **аддитивные** (append-only).

### 6.2 Column mapping table

| target_column | dtype | v1 source | v2 new — us (#91 `understat_team_match`) | v2 new — ws (#92 `whoscored_team_match`) | v2 new — ss (#93 `sofascore_team_match`) | fm (#97) | nullable | rule |
|---|---|---|---|---|---|---|---|---|
| **v1 — preserved** | | | | | | | | |
| `match_id` | varchar | dim_match.match_id | — | — | — | — | NO | preserved |
| `team_id` | varchar | dim_match home/away | — | — | — | — | NO | preserved |
| `opponent_id` | varchar | dim_match | — | — | — | — | NO | preserved |
| `date` | DATE | dim_match | — | — | — | — | NO | preserved |
| `gameweek` | INTEGER | dim_match | — | — | — | — | YES | preserved |
| `is_home` | BOOLEAN | derived | — | — | — | — | NO | preserved |
| `goals_for` | INTEGER | `home_score`/`away_score` | — | — | — | — | YES | preserved |
| `goals_against` | INTEGER | (other side) | — | — | — | — | YES | preserved |
| `shots` | INTEGER | `home/away_shots` | — | — | — | — | YES | preserved |
| `shots_on_target` | INTEGER | `home/away_sot` | — | — | — | — | YES | preserved |
| `possession` | INTEGER | `home/away_possession` | — | — | — | — | YES | preserved |
| `yellow_cards` | INTEGER | `home/away_yellow_cards` | — | — | — | — | YES | preserved |
| `red_cards` | INTEGER | `home/away_red_cards` | — | — | — | — | YES | preserved |
| `saves` | INTEGER | `home/away_saves` | — | — | — | — | YES | preserved |
| `points` | INTEGER | CASE | — | — | — | — | NO | preserved |
| `result` | varchar | CASE | — | — | — | — | NO | preserved |
| `is_completed` | BOOLEAN | dim_match | — | — | — | — | NO | preserved |
| `league` | varchar | dim_match | — | — | — | — | NO | preserved |
| `season` | varchar | dim_match | — | — | — | — | NO | preserved |
| **v2 — new MODELED (xG/xA per match)** | | | | | | | | |
| `expected_goals` | DOUBLE | — | `xg` (primary) | — | `expected_goals` | (#97) `expected_goals` | YES | COALESCE(us,[fm],ss) |
| `expected_goals_against` | DOUBLE | — | `xg_against` | — | `expected_goals_against` | (#97) | YES | COALESCE(us,[fm],ss) |
| `expected_assists` | DOUBLE | — | `xa` | — | `expected_assists` | (#97) | YES | COALESCE |
| `npxg` | DOUBLE | — | `npxg` | — | — | — | YES | us-only |
| **v2 — new UNIQUE_UNDERSTAT** | | | | | | | | |
| `ppda` | DOUBLE | — | `ppda` | — | — | — | YES | us (per match) |
| `deep_completions` | INTEGER | — | `deep` | — | — | — | YES | us |
| **v2 — new UNIQUE_WHOSCORED** (per match, from `silver.whoscored_events_spadl` GROUP BY match+team) | | | | | | | | |
| `pass_total` | INTEGER | — | — | `pass_total` | — | — | YES | ws |
| `pass_ok` | INTEGER | — | — | `pass_ok` | — | — | YES | ws |
| `pass_pct` | DOUBLE | — | — | `pass_pct` | — | — | YES | ws |
| `tackle_att` | INTEGER | — | — | `tackle_att` | — | — | YES | ws |
| `tackle_won` | INTEGER | — | — | `tackle_won` | — | — | YES | ws |
| `takeon_att` | INTEGER | — | — | `takeon_att` | — | — | YES | ws |
| `takeon_won` | INTEGER | — | — | `takeon_won` | — | — | YES | ws |
| `touches_in_box` | INTEGER | — | — | `touches_in_box` | — | — | YES | ws |
| `key_passes_ws` | INTEGER | — | — | `key_passes` | — | — | YES | ws (suffix — SS тоже даёт `key_passes`, во избежание name-conflict) |
| **v2 — new UNIQUE_SOFASCORE** (from `bronze.sofascore_match_stats` PIVOT) | | | | | | | | |
| `total_passes` | INTEGER | — | — | — | `total_passes` | — | YES | ss |
| `accurate_passes` | INTEGER | — | — | — | `accurate_passes` | — | YES | ss |
| `accurate_passes_pct` | DOUBLE | — | — | — | `accurate_passes_pct` | — | YES | ss |
| `corner_kicks` | INTEGER | — | — | — | `corners` | — | YES | ss |
| `fouls_ss` | INTEGER | — | — | — | `fouls` | — | YES | ss (suffix — FBref tcss-name fouls_committed уже есть в v1 если когда-то добавим; для match-grain SofaScore-side метрика отдельная) |
| `offsides_ss` | INTEGER | — | — | — | `offsides` | — | YES | ss |
| `ground_duels_won` | INTEGER | — | — | — | `ground_duels_won` | — | YES | ss |
| `aerial_duels_won` | INTEGER | — | — | — | `aerial_duels_won` | — | YES | ss |
| **v2 — lineage (NEW)** | | | | | | | | |
| `_gold_created_at` | TIMESTAMP | — | — | — | — | — | NO | `CURRENT_TIMESTAMP` |

**Итого новых колонок**: ~24 (4 MODELED + 2 UNIQUE_US + 9 UNIQUE_WS + 8 UNIQUE_SS + 1 lineage). v1 имеет 19 колонок → v2 ~43.

### 6.3 SQL skeleton (для #95)

```sql
WITH
home AS (  -- preserve existing UNION ALL pattern from v1
    SELECT
        dm.match_id,
        dm.date, dm.season, dm.league, dm.gameweek,
        dm.home_team_id    AS team_id,
        dm.away_team_id    AS opponent_id,
        TRUE               AS is_home,
        m.home_score       AS goals_for,
        m.away_score       AS goals_against,
        m.home_shots       AS shots,
        m.home_sot         AS shots_on_target,
        m.home_possession  AS possession,
        m.home_yellow_cards AS yellow_cards,
        m.home_red_cards   AS red_cards,
        m.home_saves       AS saves,
        dm.is_completed
    FROM iceberg.gold.dim_match dm
    JOIN iceberg.silver.fbref_match_enriched m ON m.match_id = dm.match_id
),
away AS ( /* symmetric */ ),
unioned AS (
    SELECT *, 'home' AS side FROM home
    UNION ALL
    SELECT *, 'away' AS side FROM away
),
-- NEW: xref-resolve secondary sources
xref_us AS (
    SELECT DISTINCT canonical_id, source_id AS us_team_id, league, season
    FROM iceberg.silver.xref_team
    WHERE source = 'understat' AND confidence <> 'orphan'
),
-- (xref_ws, xref_ss analogous)

us_match AS (
    SELECT um.match_id, um.team_id_canonical AS canonical_id, um.xg, um.xg_against, um.xa, um.npxg, um.ppda, um.deep
    FROM iceberg.silver.understat_team_match um  -- #91
)
-- (ws_match, ss_match analogous)

SELECT
    -- v1 columns preserved exactly
    u.match_id, u.team_id, u.opponent_id, u.date, u.gameweek, u.is_home,
    u.goals_for, u.goals_against, u.shots, u.shots_on_target, u.possession,
    u.yellow_cards, u.red_cards, u.saves,
    CASE WHEN goals_for > goals_against THEN 3
         WHEN goals_for = goals_against THEN 1
         ELSE 0 END                                  AS points,
    CASE WHEN goals_for > goals_against THEN 'W'
         WHEN goals_for = goals_against THEN 'D'
         ELSE 'L' END                                AS result,
    u.is_completed,
    -- v2 new — MODELED
    ROUND(COALESCE(us.xg, /* fm.xg, */ ss.expected_goals), 2) AS expected_goals,
    -- (etc.)
    CURRENT_TIMESTAMP                                AS _gold_created_at,
    u.league, u.season

FROM unioned u
LEFT JOIN us_match us ON us.match_id = u.match_id AND us.canonical_id = u.team_id
LEFT JOIN ws_match ws ON ...
LEFT JOIN ss_match ss ON ...
WHERE u.match_id IS NOT NULL AND u.team_id IS NOT NULL
```

---

## 7. PK strategy

**Phase 1 — natural composite, без xxhash64**:
- `fct_team_season_stats`: `(team_id_canonical, league, season)` — оба компонента NOT NULL (`team_id_canonical` берётся из FBref spine, который имеет 100% покрытие APL).
- `fct_team_match`: `(match_id_canonical, team_id_canonical)` — оба NOT NULL по INNER JOIN на `dim_match` + `fbref_match_enriched`.

**Почему не xxhash64**: `feedback_hash_pk_with_null_canonical.md` рекомендует hash + ROW_NUMBER tiebreaker **только** когда natural key содержит COALESCE'ed компонент. В нашем случае ни `team_id_canonical`, ни `match_id_canonical` не COALESCE'ятся — оба гарантировано non-NULL по конструкции spine. xxhash64 здесь добавит cost без benefit и помешает читабельности DQ-чеков.

**When to revisit (Phase 2)**: если Phase 2 переведёт spine на не-FBref источник (например при #97 FotMob spine для тех матчей, которых нет у FBref) — тогда natural key начнёт COALESCE'иться и понадобится xxhash64 + ROW_NUMBER tiebreaker. До этого момента — не усложняем.

**DQ check** для PK uniqueness — стандартный `CHECK.no_duplicates(natural_key)` из `dags/utils/data_quality.py` (severity=ERROR).

---

## 8. Audit pattern

### 8.1 Структура

Для **каждого** business-fct создаётся отдельная таблица `<fct>_audit` (см. `fct_player_season_stats_audit.sql` как reference). Цель — DQ observability cross-source diff, без загромождения business-витрины.

| Property | Value |
|---|---|
| **Grain** | Идентичен main fct (наследует PK) |
| **JOIN strategy** | `INNER JOIN` на FBref ∩ secondary source (matched pairs only — diff = `NULL - X` бессмысленно). Other sources LEFT JOIN → NULL когда нет. |
| **Diff convention** | `<metric>_diff_<source>` = `<fbref> - <source>` для HARD_FACT. Для MODELED (xG) — `<metric>_diff_us_vs_<source>` (Understat = primary). |
| **DQ severity** | WARNING-only (`error_threshold=0.0` в `CHECK.coverage()`). Никогда не ERROR — sparse diffs нормальны. |
| **Ref_integrity** | Audit PK ⊆ main PK (CHECK.ref_integrity, severity=WARNING) |
| **Read source** | Silver (НЕ gold.fct_*) — one-hop rule (`project_gold_cleanup_2026-05-12.md`) |

### 8.2 Audit columns — `fct_team_season_stats_audit`

INNER JOIN spine = `xref_fbref` ∩ `xref_understat` (Understat = primary secondary для team-level coverage). WhoScored, SofaScore, FotMob — LEFT JOIN.

```
PK (наследует main):
    team_id_canonical, league, season

Understat diffs (INNER, всегда non-NULL):
    matches_diff_understat, minutes_diff_understat, goals_diff_understat,
    shots_diff_understat, goals_against_diff_understat, assists_diff_understat

WhoScored diffs (LEFT, NULL when absent):
    matches_diff_whoscored, shots_diff_whoscored, tackles_won_diff_whoscored,
    interceptions_diff_whoscored, fouls_committed_diff_whoscored,
    pass_total_diff_whoscored  (primary = none, ws solo)

SofaScore diffs (LEFT):
    matches_diff_sofascore, goals_diff_sofascore, shots_diff_sofascore,
    yellow_cards_diff_sofascore, red_cards_diff_sofascore,
    tackles_won_diff_sofascore, interceptions_diff_sofascore,
    corner_kicks_diff_sofascore  (no FBref baseline → not applicable)

MODELED diffs (cross-source xG comparison, RX2):
    xg_diff_us_vs_ss, xg_diff_us_vs_fm[Phase 2],
    xa_diff_us_vs_ss

FotMob diffs (Phase 2 — #97, reserved):
    matches_diff_fotmob, shots_diff_fotmob, expected_goals_diff_us_vs_fm

Lineage:
    _gold_created_at
```

### 8.3 Audit columns — `fct_team_match_audit`

INNER JOIN spine = FBref ∩ Understat (match-grain). WS, SS — LEFT. Convention одинаковый.

```
PK: match_id_canonical, team_id_canonical
Understat diffs (INNER): shots_diff_understat, goals_diff_understat
WhoScored diffs (LEFT): pass_total_diff_(none), key_passes_ws_diff_ss
SofaScore diffs (LEFT): shots_diff_sofascore, fouls_diff_sofascore,
                       yellow_cards_diff_sofascore, corners_diff_sofascore
MODELED: xg_diff_us_vs_ss, xg_diff_us_vs_fm[Phase 2]
```

### 8.4 DQ severity — WARNING-only

В `dags/utils/gold_tasks.py` validate-блок для audit-таблиц:

```python
CHECK.coverage(
    audit_pk=("team_id_canonical", "league", "season"),
    condition="ABS(goals_diff_understat) <= 1 OR goals_diff_understat IS NULL",
    warn_threshold=0.95,
    error_threshold=0.0,  # NEVER raise — only WARNING
)
```

Threshold `≤1` для целочисленных counters; `≤0.2` для xG-derived (RX2 `r ≥ 0.99` → typical diff ≤0.2 per match).

---

## 9. Source priority

Наследуется из RX2 (player-wave) с минимальными изменениями для team-grain:

| Метрика | Primary | Fallback chain (in order) |
|---|---|---|
| HARD_FACT counters (matches, goals, shots, cards) | FBref | FBref → Understat → WhoScored → SofaScore → [FotMob #97] |
| xG / xA / NPxG | Understat | Understat → [FotMob #97] → SofaScore |
| Possession % | FBref | FBref → SofaScore (no US/WS equivalent) |
| Pressing (PPDA, deep) | Understat | Understat only |
| Pass-style (pass_total, takeon_pct) | WhoScored | WhoScored only (FBref не отдаёт; SS даёт но per match) |
| Duels / breakdown (ground/aerial) | SofaScore | SofaScore only |
| Goalkeeping team-level (clean_sheets, save_pct) | FBref | FBref only (other sources don't aggregate) |
| Match-level outcome (goals_for, points, result) | FBref-derived | FBref only — это computed columns, не from source |

**Rationale для FBref-primacy в HARD_FACT**: FBref имеет полное покрытие APL за все сезоны, является каноническим источником для `silver.xref_team`, и его метрики наименее зависят от scraping rate-limits (по сравнению с FotMob / SofaScore которые могут терять matches при API drift).

---

## 10. Acceptance criteria для T6.* sub-issues

Каждый dependent issue должен пройти через эти gate-checks; design-doc — single source of truth.

### T6.2 (#91 — understat team aggregates) — Silver
- [ ] Создана `silver.understat_team_season` с колонками по §5 (помеченными `us`)
- [ ] Создана `silver.understat_team_match` с колонками по §6 (помеченными `us`)
- [ ] PK + partitioning: `(team_id, league, season)` / `(match_id, team_id)`, partitioned by `(league, season)`
- [ ] Dedup через `ROW_NUMBER` (см. `feedback_replace_partitions_required.md`)
- [ ] DQ: `no_nulls(team_id)`, `freshness`, `no_duplicates`
- [ ] Подключение к `silver.xref_team` проходит без orphan-spike (≤10%)

### T6.3 (#92 — whoscored team aggregates) — Silver
- [ ] Создана `silver.whoscored_team_season` (GROUP BY канонических team из `whoscored_events_spadl`)
- [ ] Создана `silver.whoscored_team_match` (GROUP BY match)
- [ ] Bronze DOUBLE → BIGINT → varchar cast применён (§4)
- [ ] Колонки соответствуют §5 / §6 в строках `ws`
- [ ] DQ-чеки идентичны T6.2

### T6.4 (#93 — sofascore team aggregates) — Silver
- [ ] Создана `silver.sofascore_team_season` (rollup from match)
- [ ] Создана `silver.sofascore_team_match` (PIVOT из `bronze.sofascore_match_stats`)
- [ ] Колонки соответствуют §5 / §6 в строках `ss`
- [ ] DQ-чеки идентичны T6.2

### T6.5 (#94 — fct_team_season_stats) — Gold
- [ ] Создана `gold.fct_team_season_stats` по §5 column-mapping
- [ ] PK `(team_id_canonical, league, season)`, `no_duplicates` GREEN (severity=ERROR)
- [ ] Создана `gold.fct_team_season_stats_audit` по §8.2
- [ ] Audit DQ — WARNING-only, GREEN на R1 thresholds
- [ ] Smoke на APL 2025/26: row_count ≈ 20 (10 teams × 2 sides? no — 20 teams в сезоне)

### T6.6 (#95 — fct_team_match v2) — Gold
- [ ] Backwards-compat invariant verified (см. §6.1) — `feat_team_form` / `feat_team_h2h` / `feat_team_xg_form` runs GREEN
- [ ] Новые колонки добавлены аддитивно
- [ ] Создана `gold.fct_team_match_audit` по §8.3
- [ ] Smoke на APL 2025/26: row_count ≈ 380×2 (один row per (match, team))

### T6.7 (#96 — OM+Superset)
- [ ] Column descriptions для новых таблиц в `configs/openmetadata/`
- [ ] Дашборд `team_overview.py` обновлён (берёт из нового `fct_team_season_stats`)

---

## 11. Verification (для этого design-doc PR)

Так как это design-only PR:
- **Self-check**: SQL skeleton'ы (§5.2 / §6.3) валидны структурно — CTE pattern совпадает с `fct_player_season_stats.sql:40–229`.
- **Cross-reference completeness**: каждое решение в §3 ссылается на конкретный memory/research-doc.
- **Column-mapping completeness**: ≥90% колонок из `silver.fbref_team_season_profile` и `silver.fbref_match_enriched` покрыты (отсутствующие — в §12 Out of scope).
- **Backwards-compat audit (§6.1)**: каждая колонка v1 присутствует в v2.
- **Manual review by user** — этот документ ревьюится по структуре, не SQL-валидируется (SQL — это будущая работа #94/#95).

---

## 12. Out of scope

- **FotMob** (Phase 2, #97) — слот зарезервирован в column-mapping и COALESCE-цепочках; разблокируется когда #97 ship'нет FotMob match-stats через `_next/data` slug-form path.
- **SCD-2 для team attributes** — T6 = snapshot per `(team, season|match)`. Time-series атрибуты (kit colors, manager, stadium) — отдельная R3 phase.
- **Transfermarkt market value** для команды — отдельный followup (R3 phase), не входит в T6.
- **Cross-competition aggregates** — текущий T6 = `(league, season)`. Cup / Europa / national team metrics out of scope.
- **`fct_team_keeper_season_stats`** — голкипинг team-aggregate уже живёт в `fct_team_season_stats` через `silver.fbref_team_season_profile` GK-блок (см. §5.1 UNIQUE_FBREF). Отдельной таблицы не создаём — в отличие от player-wave (`fct_keeper_season_stats`), где per-position fan-out у игроков требует отдельной витрины.
- **`silver.fbref_shot_events` dependency** — `feat_team_xg_form.sql:51` сейчас читает `silver.fbref_shot_events`, которая **не существует** (FBref Feb 2026 restriction, см. `feedback_fbref_shot_events_unavailable.md`). После T6 эта feat-таблица должна быть переписана: spine = `fct_team_match` с готовыми `expected_goals` / `expected_goals_against` колонками (Understat-derived). Отдельный followup issue после #95 merge.
- **FBref Bronze columns не covered**:
  - `goals_assists`, `goals_non_penalty`, `goals_assists_minus_penalty_per_90` — derived, легко вычислить `(goals + assists)` в Superset; не нужно тащить в Gold.
  - `attendance`, `venue`, `referee` — уже в `gold.dim_match`, дублирование избыточно.
  - `lineups_agg` (`home_starters`, `away_starters`, `home_bench`, `away_bench`) — match-grain lineup metadata, скорее тема для `fct_lineup` (#E3).

---

## 13. Dependencies

- **Blocking**: ничего, design-only gate.
- **Blocks**: #91, #92, #93, #94, #95, #96. Phase 2 unblock for #97 (FotMob).
- **Memory references** (load when working on T6.x):
  - `project_t5_fct_season_stats.md` — паттерн player-wave fct
  - `project_t5_extension_ws_us_2026-05-16.md` — добавление WS+US в существующий fct
  - `project_xg_rx2_2026-05-22.md` — Understat = xG primary
  - `feedback_audit_in_separate_table.md` — audit pattern
  - `feedback_hash_pk_with_null_canonical.md` — PK NULL-tolerant tiebreaker
  - `feedback_xref_join_season_predicate.md` — (league, season) predicate REQUIRED
  - `feedback_bronze_double_id_cast.md` — WhoScored team_id DOUBLE → BIGINT → varchar
  - `feedback_fbref_shot_events_unavailable.md` — feat_team_xg_form rewrite followup
- **Code references**:
  - `dags/sql/silver/fbref_team_season_profile.sql` — spine для season
  - `dags/sql/silver/fbref_match_enriched.sql` — spine для match
  - `dags/sql/silver/xref_team.sql.j2` — bridge таблица
  - `dags/sql/gold/fct_team_match.sql` — v1 (to be extended)
  - `dags/sql/gold/fct_player_season_stats.sql` — структурный образец
  - `dags/sql/gold/fct_player_season_stats_audit.sql` — audit-template
