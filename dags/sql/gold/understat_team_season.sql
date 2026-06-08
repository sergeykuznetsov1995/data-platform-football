-- =============================================================================
-- Gold: understat_team_season (per-source season aggregate; migrated from Silver, #370)
-- =============================================================================
--
-- One row per (team_id_canonical, league, season) — Understat team-level season
-- aggregate. Rollup from `silver.understat_team_match` (long-form, one row per
-- (match, team)) via SUM / COUNT / weighted-AVG. Mirrors the shape that
-- `gold.fct_team_season_stats` (#94) expects on the Understat block.
--
-- Issue #91 (T6 wave, T6.2). Design contract:
-- docs/decisions/T6_team_facts_schema.md §5.1 — us-marked columns.
--
-- Why rollup-from-match (not a separate bronze.understat_teams):
--   soccerdata's `Understat.read_team_match_stats` is the only team-level
--   endpoint and is inherently match-grain. There is no native season-aggregate
--   from Understat that exposes wins/draws/losses/xpts. The legacy
--   `bronze.understat_teams` is actually the same wide-form table (verified
--   2026-05-27 — identical schema, identical row count). We therefore
--   derive the season view in Silver via SQL rollup; xpts / wins / draws /
--   losses become first-class SUM(...) outputs.
--
-- Sources:
--   silver.understat_team_match  (this module's match-grain sibling)
--
-- Notes:
--   * No xref JOIN needed — `silver.understat_team_match` is already keyed by
--     `team_id_canonical`. league/season are passthrough.
--   * PPDA / OPPDA are season-level metrics that ideally should be
--     SUM(passes) / SUM(defensive_actions); soccerdata does not expose those
--     numerators/denominators, so we use the simple mean across matches as a
--     pragmatic approximation. Document this in the column doc when wiring
--     OpenMetadata (issue #96).
--   * `non_penalty_goals_against` is omitted: NPxG is derivable from shots
--     but actual non-penalty goals-against is not in Bronze (penalty rebounds
--     would need event data). Out of scope for T6.2.
-- =============================================================================

WITH per_match AS (
    SELECT *
    FROM iceberg.silver.understat_team_match
)

SELECT
    -- ========= Identity =========
    team_id_canonical,
    -- Most recent display name (tie-break by max match_id which is monotonic for Understat ids)
    MAX(team_name)                                  AS primary_team_name,

    -- ========= HARD_FACT counters (SUM / COUNT) =========
    CAST(COUNT(*) AS INTEGER)                       AS games_played,
    CAST(SUM(CASE WHEN points = 3 THEN 1 ELSE 0 END) AS INTEGER) AS wins,
    CAST(SUM(CASE WHEN points = 1 THEN 1 ELSE 0 END) AS INTEGER) AS draws,
    CAST(SUM(CASE WHEN points = 0 THEN 1 ELSE 0 END) AS INTEGER) AS losses,
    CAST(SUM(goals)         AS INTEGER)             AS goals,
    CAST(SUM(goals_against) AS INTEGER)             AS goals_against,
    CAST(SUM(points)        AS INTEGER)             AS points,

    -- ========= MODELED xG (Understat primary per RX2) =========
    SUM(xg)                                         AS xg,
    SUM(xg_against)                                 AS xg_against,
    SUM(npxg)                                       AS npxg,
    SUM(npxg_against)                               AS npxg_against,
    SUM(xpts)                                       AS xpts,

    -- ========= Pressing / depth =========
    -- Simple mean: PPDA / OPPDA per match averaged (see file header note).
    AVG(ppda)                                       AS ppda,
    AVG(oppda)                                      AS oppda,
    CAST(SUM(deep_completions)         AS INTEGER)  AS deep_completions,
    CAST(SUM(deep_completions_allowed) AS INTEGER)  AS deep_completions_allowed,

    -- ========= Lineage =========
    MAX(_bronze_ingested_at)                        AS _bronze_ingested_at,

    -- ========= Partition keys =========
    league,
    season

FROM per_match
GROUP BY team_id_canonical, league, season
