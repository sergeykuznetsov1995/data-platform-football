-- =============================================================================
-- Gold: fct_player_fifa_rating (EMPTY FALLBACK)
-- =============================================================================
-- Identical schema to fct_player_fifa_rating.sql, zero rows.
-- Routed by gold_tasks.run_gold_transform when require_silver source
-- `sofifa_player_profile` is absent (env without SoFIFA ingest).
-- =============================================================================

SELECT
    CAST(NULL AS varchar)       AS player_id,
    CAST(NULL AS varchar)       AS player_name,
    CAST(NULL AS varchar)       AS fifa_edition,
    CAST(NULL AS integer)       AS overall,
    CAST(NULL AS integer)       AS potential,
    CAST(NULL AS integer)       AS pace,
    CAST(NULL AS integer)       AS shooting,
    CAST(NULL AS integer)       AS passing,
    CAST(NULL AS integer)       AS dribbling,
    CAST(NULL AS integer)       AS defending,
    CAST(NULL AS integer)       AS physical,
    CAST(NULL AS integer)       AS gk_diving,
    CAST(NULL AS integer)       AS gk_handling,
    CAST(NULL AS integer)       AS gk_kicking,
    CAST(NULL AS integer)       AS gk_positioning,
    CAST(NULL AS integer)       AS gk_reflexes,
    CAST(NULL AS bigint)        AS value_eur,
    CAST(NULL AS bigint)        AS wage_eur,
    CAST(NULL AS timestamp(6))  AS _bronze_ingested_at
WHERE 1 = 0
