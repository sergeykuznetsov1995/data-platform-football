-- =============================================================================
-- Gold: feat_referee_bias  — EMPTY FALLBACK
-- =============================================================================
-- Materialized when upstream sources are unavailable for the referee-bias
-- feature build (e.g. dim_match.referee column unpopulated, fct_card /
-- fct_goal not yet materialized for the active wave).
--
-- Goal: keep the Gold contract intact so downstream LEFT JOINs continue to
-- resolve. Schema MUST mirror feat_referee_bias.sql exactly:
--   business cols (referee_id, match_id, date, league)
--   + 6 rolling cols (all DOUBLE NULL)
--   + partition col (season) emitted last.
--
-- All 6 rolling values are NULL by construction → strict point-in-time DQ
-- passes trivially (a NULL feature can never leak future info). WHERE 1=0
-- guarantees zero output rows while preserving column metadata for
-- partitioned-write planning by Trino.
--
-- Type alignment with feat_referee_bias.sql:
--   referee_id        varchar
--   match_id          varchar           (dim_match.match_id is varchar)
--   date              date              (dim_match.date is date)
--   league            varchar
--   ref_*_per_match_l10 / ref_home_win_rate_l10  double
--   season            varchar           (dim_match.season is slug '2425' after #404)
-- =============================================================================

SELECT
    CAST(NULL AS varchar)  AS referee_id,
    CAST(NULL AS varchar)  AS match_id,
    CAST(NULL AS date)     AS date,
    CAST(NULL AS varchar)  AS league,
    CAST(NULL AS double)   AS ref_yellow_per_match_l10,
    CAST(NULL AS double)   AS ref_red_per_match_l10,
    CAST(NULL AS double)   AS ref_cards_per_match_l10,
    CAST(NULL AS double)   AS ref_goals_per_match_l10,
    CAST(NULL AS double)   AS ref_home_win_rate_l10,
    CAST(NULL AS double)   AS ref_pen_per_match_l10,
    CAST(NULL AS varchar)  AS season  -- #404: slug, matches populated sibling
WHERE 1 = 0
