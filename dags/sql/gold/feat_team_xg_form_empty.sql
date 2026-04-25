-- =============================================================================
-- Gold: feat_team_xg_form  — EMPTY FALLBACK
-- =============================================================================
-- Materialized when iceberg.silver.fbref_shot_events is NOT available
-- (Bronze fbref_shot_events not ingested yet — valid MVP scenario).
--
-- Goal: keep the Gold contract intact so downstream fct_match LEFT JOINs
-- continue to resolve. Schema MUST mirror feat_team_xg_form.sql exactly:
--   keys (match_id, team_id, opponent_id, match_date, match_rn) +
--   12 xG / PSxG rolling columns (L5 + L10) all CAST to DOUBLE NULL +
--   partition columns (league, season) emitted last.
--
-- Spine: gold.fct_team_match — already one row per (match_id, team_id) with
-- date + season + league. ROW_NUMBER over (team_id, season) ORDER BY date
-- reproduces match_rn so any downstream rolling-aware query keeps working.
--
-- xG / PSxG values are NULL by construction (no shot data) — point-in-time
-- DQ checks pass trivially (a NULL feature can never leak future info).
-- =============================================================================

SELECT
    match_id,
    team_id,
    opponent_id,
    date                                AS match_date,
    ROW_NUMBER() OVER (
        PARTITION BY team_id, season
        ORDER BY date, match_id
    )                                   AS match_rn,

    -- ----- L5 xG / PSxG (NULL — shot_events not available) -----
    CAST(NULL AS DOUBLE) AS xg_for_l5_avg,
    CAST(NULL AS DOUBLE) AS xg_against_l5_avg,
    CAST(NULL AS DOUBLE) AS xg_diff_l5_avg,
    CAST(NULL AS DOUBLE) AS psxg_for_l5_avg,
    CAST(NULL AS DOUBLE) AS psxg_against_l5_avg,
    CAST(NULL AS DOUBLE) AS psxg_diff_l5_avg,

    -- ----- L10 xG / PSxG (NULL — shot_events not available) -----
    CAST(NULL AS DOUBLE) AS xg_for_l10_avg,
    CAST(NULL AS DOUBLE) AS xg_against_l10_avg,
    CAST(NULL AS DOUBLE) AS xg_diff_l10_avg,
    CAST(NULL AS DOUBLE) AS psxg_for_l10_avg,
    CAST(NULL AS DOUBLE) AS psxg_against_l10_avg,
    CAST(NULL AS DOUBLE) AS psxg_diff_l10_avg,

    league,
    season
FROM iceberg.gold.fct_team_match
WHERE match_id IS NOT NULL
  AND team_id  IS NOT NULL
