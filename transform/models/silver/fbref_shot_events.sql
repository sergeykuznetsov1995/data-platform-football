{{
  config(
    materialized = 'table',
    on_table_exists = 'replace',
    properties = {
      "partitioning": "ARRAY['league', 'season']"
    }
  )
}}

-- Ported from dags/sql/silver/fbref_shot_events.sql (Tier-2 dbt-trino pilot).
-- Mechanical port: iceberg.bronze.X -> source('bronze','X'); the
-- _silver_created_at column mirrors run_silver_transform()'s CTAS wrapper.
-- Inner SQL logic is byte-identical to the original.

select *, current_timestamp as _silver_created_at
from (

-- =============================================================================
-- Silver: fbref_shot_events
-- =============================================================================
--
-- Detailed per-shot rows with xG data for each match.
--
-- Source:
--   {{ source('bronze', 'fbref_shot_events') }}
--
-- Deduplication:
--   ROW_NUMBER() OVER (PARTITION BY match_id, minute, player_id, outcome
--                       ORDER BY _ingested_at DESC)  =>  rn = 1
--
-- Notes:
--   * Bronze table may not exist yet — caller must check before executing.
--   * xg, psxg are DOUBLE; distance is INTEGER; rest are VARCHAR.
--   * Partitioning by (league, season) is applied externally by Python CTAS.
-- =============================================================================

WITH src AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY match_id, minute, player_id, outcome
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM {{ source('bronze', 'fbref_shot_events') }}
)

SELECT
    match_id,
    minute,
    player,
    player_id,
    team,
    TRY_CAST(xg AS DOUBLE)        AS xg,
    TRY_CAST(psxg AS DOUBLE)      AS psxg,
    outcome,
    TRY_CAST(distance AS INTEGER) AS distance,
    body_part,
    notes,

    -- ========= Lineage =========
    _ingested_at                   AS _bronze_ingested_at,

    -- ========= Partition Keys =========
    league,
    season

FROM src
WHERE rn = 1

)
