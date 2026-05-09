-- =============================================================================
-- Gold: mart_scouting_radar — EMPTY FALLBACK
-- =============================================================================
-- Schema MUST mirror mart_scouting_radar.sql exactly. Materialised when
-- upstream sources (fct_player_match / fct_shot / dim_player) are unavailable
-- so downstream Superset queries continue to resolve.
-- =============================================================================

SELECT
    CAST(NULL AS varchar) AS player_id,
    CAST(NULL AS varchar) AS player_name,
    CAST(NULL AS varchar) AS position,
    CAST(NULL AS varchar) AS team_id,
    CAST(NULL AS varchar) AS team_name,
    CAST(NULL AS varchar) AS match_id,
    CAST(NULL AS date)    AS match_date,
    CAST(NULL AS bigint)  AS minutes,
    CAST(NULL AS bigint)  AS goals,
    CAST(NULL AS bigint)  AS assists,
    CAST(NULL AS bigint)  AS shots,
    CAST(NULL AS bigint)  AS shots_on_target,
    CAST(NULL AS bigint)  AS tackles_won,
    CAST(NULL AS bigint)  AS interceptions,
    CAST(NULL AS bigint)  AS defensive_actions,
    CAST(NULL AS double)  AS xg,
    CAST(NULL AS double)  AS xa,
    CAST(NULL AS double)  AS xg_l5,
    CAST(NULL AS double)  AS xa_l5,
    CAST(NULL AS double)  AS shots_l5,
    CAST(NULL AS double)  AS defensive_l5,
    CAST(NULL AS varchar) AS league,
    CAST(NULL AS bigint)  AS season
WHERE 1 = 0
