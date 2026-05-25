-- =============================================================================
-- Gold: feat_team_event_style  — EMPTY FALLBACK
-- =============================================================================
-- Materialized when iceberg.silver.whoscored_events_spadl is NOT available
-- (Bronze whoscored_events not ingested yet, or E1 xref not materialised —
-- valid MVP scenario). Selected by the Gold DAG via `require_silver` registry
-- gate (see gold_tasks.py + dag_transform_fbref_gold.py).
--
-- Goal: keep the Gold contract intact so downstream fct_match LEFT JOINs
-- continue to resolve. Schema MUST mirror feat_team_event_style.sql exactly:
--   keys (match_id, team_id, date, season, league) +
--   10 share-rolling columns (L5) all CAST to DOUBLE NULL.
-- Column order, names, and types are validated by the W8 unit-test parity
-- check (test_feat_team_event_style_sql.py::test_schema_parity).
--
-- Spine: gold.fct_team_match — already one row per (match_id, team_id) with
-- date + season + league. By LEFTing onto it and emitting NULL share columns
-- we preserve full row coverage; downstream fct_match LEFT JOINs see NULLs
-- (treated identically to "first 5 matches" mask state — model gracefully
-- degrades to features that ignore event-style signal).
--
-- Share values are NULL by construction (no event data) — point-in-time DQ
-- checks pass trivially (a NULL feature can never leak future info).
-- =============================================================================

SELECT
    match_id,
    team_id,
    date,
    season,
    league,

    -- ----- 6 action shares (L5, NULL — no events) -----
    CAST(NULL AS DOUBLE) AS pass_share_l5_avg,
    CAST(NULL AS DOUBLE) AS dribble_share_l5_avg,
    CAST(NULL AS DOUBLE) AS tackle_share_l5_avg,
    CAST(NULL AS DOUBLE) AS interception_share_l5_avg,
    CAST(NULL AS DOUBLE) AS cross_share_l5_avg,
    CAST(NULL AS DOUBLE) AS shot_share_l5_avg,

    -- ----- success rate (L5, NULL — no outcomes) -----
    CAST(NULL AS DOUBLE) AS success_rate_l5_avg,

    -- ----- 3 shot-mix shares (L5, NULL — no shot data via this path) -----
    CAST(NULL AS DOUBLE) AS set_piece_share_l5_avg,
    CAST(NULL AS DOUBLE) AS open_play_share_l5_avg,
    CAST(NULL AS DOUBLE) AS header_share_l5_avg
FROM iceberg.gold.fct_team_match
WHERE match_id IS NOT NULL
  AND team_id  IS NOT NULL
