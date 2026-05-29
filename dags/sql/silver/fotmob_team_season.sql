-- =============================================================================
-- Silver: fotmob_team_season
-- =============================================================================
--
-- One row per (team_id, league, season) — FotMob team-level season aggregate.
-- Rollup from `silver.fotmob_team_match` (long-form, one row per (match, team))
-- via SUM / COUNT / weighted-AVG. Mirrors the shape that
-- `gold.fct_team_season_stats` (#94) expects on the FotMob block.
--
-- Issue #97 Phase B (T6 wave). Design contract:
-- docs/decisions/T6_team_facts_schema.md §5 — fm-marked columns.
--
-- Why rollup-from-match (not a separate bronze season endpoint):
--   FotMob exposes deep stats only at match grain (`bronze.fotmob_match_details`
--   via the _next/data slug-form path). team-grain xA is reconstructed per match
--   in `silver.fotmob_team_match` (SUM of per-player xA). The season view is
--   therefore a pure SQL rollup; expected_assists / big_chances become
--   first-class SUM(...) outputs.
--
-- Sources:
--   silver.fotmob_team_match  (this module's match-grain sibling; season is slug '2526')
--
-- Notes:
--   * No xref JOIN needed — team_id is the FotMob team NAME (passthrough),
--     resolved to canonical only in Gold (fct_team_season_stats).
--   * season is already a slug ('2526') in fotmob_team_match — passthrough.
--   * possession_pct / accurate_passes_pct are rate metrics: possession averaged
--     across matches; accurate_passes_pct recomputed from season SUM numerator /
--     denominator (more correct than averaging per-match percentages).
--   * FotMob does NOT expose team-grain expected_goals_against at match grain, so
--     there is no xg_against column here (US remains the xGA primary in Gold).
--   * INCOMPLETE-SEASON caveat: fotmob_team_match keeps only matches with stats_json
--     (~89% of fixtures; cancelled / not-yet-played excluded). So SUM counters
--     (goals / shots) under-count vs FBref's full-season totals — `appearances`
--     exposes the gap (e.g. 17 vs 19). Gold keeps FBref/US as COALESCE primary, so
--     FotMob only feeds expected_assists + UNIQUE_FOTMOB; the season audit
--     *_diff_fotmob WARNINGs surface this drift (WARNING-only by design).
-- =============================================================================

WITH per_match AS (
    SELECT *
    FROM iceberg.silver.fotmob_team_match
)

SELECT
    -- ========= Identity =========
    team_id,

    -- ========= HARD_FACT counters (SUM / COUNT) =========
    CAST(COUNT(*)                AS INTEGER) AS appearances,
    CAST(SUM(goals_for)          AS INTEGER) AS goals,
    CAST(SUM(goals_against)      AS INTEGER) AS goals_conceded,
    CAST(SUM(total_shots)        AS INTEGER) AS total_shots,
    CAST(SUM(shots_on_target)    AS INTEGER) AS shots_on_target,
    CAST(SUM(total_passes)       AS INTEGER) AS total_passes,
    CAST(SUM(accurate_passes)    AS INTEGER) AS accurate_passes,
    CASE
        WHEN SUM(total_passes) > 0
            THEN ROUND(100.0 * SUM(accurate_passes) / SUM(total_passes), 2)
        ELSE NULL
    END                                      AS accurate_passes_pct,
    CAST(SUM(yellow_cards)       AS INTEGER) AS yellow_cards,
    CAST(SUM(red_cards)          AS INTEGER) AS red_cards,
    CAST(SUM(fouls)              AS INTEGER) AS fouls_committed,
    CAST(SUM(offsides)           AS INTEGER) AS offsides,
    CAST(SUM(corner_kicks)       AS INTEGER) AS corner_kicks,
    CAST(SUM(tackles)            AS INTEGER) AS tackles,
    CAST(SUM(interceptions)      AS INTEGER) AS interceptions,
    CAST(SUM(clearances)         AS INTEGER) AS clearances,
    CAST(SUM(saves)              AS INTEGER) AS saves,
    -- Possession is a rate → mean across matches.
    ROUND(AVG(possession_pct), 2)            AS possession_pct,

    -- ========= MODELED — xG / xA family =========
    ROUND(SUM(expected_goals),   4)          AS expected_goals,
    ROUND(SUM(npxg),             4)          AS npxg,
    ROUND(SUM(xgot),             4)          AS xgot,
    -- The whole point of issue #97 at season grain: team xA, unavailable elsewhere.
    ROUND(SUM(expected_assists), 4)          AS expected_assists,

    -- ========= UNIQUE_FOTMOB =========
    CAST(SUM(big_chances)        AS INTEGER) AS big_chances,
    CAST(SUM(big_chances_missed) AS INTEGER) AS big_chances_missed,
    CAST(SUM(touches_in_box)     AS INTEGER) AS touches_in_box,
    CAST(SUM(shots_inside_box)   AS INTEGER) AS shots_inside_box,
    CAST(SUM(shots_outside_box)  AS INTEGER) AS shots_outside_box,
    CAST(SUM(blocked_shots)      AS INTEGER) AS blocked_shots,
    CAST(SUM(shots_off_target)   AS INTEGER) AS shots_off_target,

    -- ========= Lineage =========
    MAX(_bronze_ingested_at)                 AS _bronze_ingested_at,

    -- ========= Partition keys =========
    league,
    season

FROM per_match
GROUP BY team_id, league, season
