-- =============================================================================
-- Gold: fct_shot_audit
-- =============================================================================
-- Cross-source DQ-audit for shot-level consistency between Understat (the
-- gold.fct_shot spine) and SofaScore (silver.sofascore_shots). NOT a business
-- mart: only technical diff-columns + PK. WARNING-only DQ.
--
-- Issue #602 — the deliverable that makes bronze.sofascore_event_shotmap worth
-- scraping (audit #476): a second independent shot source to validate xG / shot
-- count / shots-on-target against Understat.
--
-- Grain: (match_id, team_id) — one row per canonical team x match.
--   Spine = Understat (gold.fct_shot aggregated). SofaScore LEFT JOIN -> NULL
--   diffs when absent (has_sofascore = FALSE). This keeps the invariant
--   audit ⊆ fct_shot (every audit key exists in fct_shot), mirroring how the
--   other *_audit tables relate to their main fct (gold_tasks ref_integrity).
--
-- Coverage note (2026-06-16): fct_shot spans APL 2021-2526; SofaScore shotmap
-- has 2526 only -> has_sofascore = TRUE only for 2526 (~380 matches x 2 teams,
-- full bridge). Historical seasons carry NULL ss_* diffs by design.
--
-- Convention: diff = Understat - SofaScore (US is the established xG primary,
-- RX2). ABS(diff) small => sources agree. Used by:
--   1. DQ WARNING — ABS(xg_diff) within tolerance for >= X% of overlap rows.
--   2. Engineer-debug when shot/xG totals disagree across dashboards.
--
-- Source-read deviation (documented): the Understat side reads gold.fct_shot
-- (gold-on-gold) because there is NO silver.understat_shots — fct_shot IS the
-- canonical Understat shot projection. Other *_audit tables re-read Silver, but
-- here Silver does not exist for this source, so fct_shot is the closest spine.
--
-- PK: (match_id, team_id) — natural composite, both non-NULL by construction
-- (us_agg filters team_id IS NOT NULL).
-- Partitioning: (league, season) — applied externally by Python CTAS.
-- =============================================================================

WITH
-- ===== Understat spine: aggregate gold.fct_shot to team x match =====
us_agg AS (
    SELECT
        match_id,
        team_id,
        league,
        season,
        CAST(COUNT(*) AS bigint)                                          AS us_shots,
        ROUND(SUM(xg), 4)                                                 AS us_xg,
        -- goals/sot counted via `result` (NOT is_goal) and own_goal EXCLUDED:
        -- own-goal team attribution is a source-convention difference, not a DQ
        -- signal — counting it inflates goals_diff with ~own-goal noise.
        CAST(SUM(CASE WHEN result = 'goal' THEN 1 ELSE 0 END) AS bigint)  AS us_goals,
        CAST(SUM(CASE WHEN result IN ('goal', 'saved') THEN 1 ELSE 0 END) AS bigint) AS us_sot
    FROM iceberg.gold.fct_shot
    WHERE team_id IS NOT NULL
    GROUP BY match_id, team_id, league, season
),

-- ===== SofaScore: aggregate silver.sofascore_shots to team x match =====
ss_agg AS (
    SELECT
        match_id,
        team_id,
        league,
        season,
        CAST(COUNT(*) AS bigint)                                          AS ss_shots,
        ROUND(SUM(xg), 4)                                                 AS ss_xg,
        -- same result-based, own_goal-excluded logic as us_agg (apples-to-apples)
        CAST(SUM(CASE WHEN result = 'goal' THEN 1 ELSE 0 END) AS bigint)  AS ss_goals,
        CAST(SUM(CASE WHEN result IN ('goal', 'saved') THEN 1 ELSE 0 END) AS bigint) AS ss_sot
    FROM iceberg.silver.sofascore_shots
    WHERE team_id IS NOT NULL
    GROUP BY match_id, team_id, league, season
)

SELECT
    -- ========= PK =========
    us.match_id,
    us.team_id,

    -- ========= shot counts =========
    us.us_shots,
    ss.ss_shots,
    (us.us_shots - ss.ss_shots)                          AS shots_diff,

    -- ========= xG (US primary) =========
    us.us_xg,
    ss.ss_xg,
    ROUND(us.us_xg - ss.ss_xg, 4)                        AS xg_diff,

    -- ========= goals =========
    us.us_goals,
    ss.ss_goals,
    (us.us_goals - ss.ss_goals)                          AS goals_diff,

    -- ========= shots on target =========
    us.us_sot,
    ss.ss_sot,
    (us.us_sot - ss.ss_sot)                              AS sot_diff,

    -- ========= coverage flag =========
    (ss.match_id IS NOT NULL)                            AS has_sofascore,

    -- ========= Partition keys (LAST) =========
    us.league,
    us.season

FROM us_agg us
LEFT JOIN ss_agg ss
       ON ss.match_id = us.match_id
      AND ss.team_id  = us.team_id
      AND ss.league   = us.league
      AND ss.season   = us.season
