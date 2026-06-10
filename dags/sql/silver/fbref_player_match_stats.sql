-- =============================================================================
-- Silver: fbref_player_match_stats
-- =============================================================================
--
-- Deduplicated per-match individual player statistics.
--
-- Source:
--   iceberg.bronze.fbref_match_player_stats
--
-- Deduplication:
--   ROW_NUMBER() OVER (PARTITION BY match_id, COALESCE(player_id, player), team
--                       ORDER BY _ingested_at DESC)  =>  rn = 1
--
-- Notes:
--   * player_id is extracted from HTML links (added Apr 2026).
--   * Fallback to player name for older Bronze rows without player_id.
--   * Numeric columns are already BIGINT in Bronze (not VARCHAR).
--   * Column "int" (interceptions) and "off" (offsides) are reserved words => quoted.
--   * pkwon and pkcon are DOUBLE in Bronze.
--   * Partitioning by (league, season) is applied externally by Python CTAS.
-- =============================================================================

WITH src AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY match_id, COALESCE(player_id, player), team
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fbref_match_player_stats
)

SELECT
    -- ========= Identification =========
    match_id,
    player_id,
    player,
    team,
    team_side,
    nation,
    pos,
    age,

    -- ========= Performance =========
    TRY_CAST(min AS INTEGER)           AS minutes,
    TRY_CAST(gls AS INTEGER)           AS goals,
    TRY_CAST(ast AS INTEGER)           AS assists,
    TRY_CAST(pk AS INTEGER)            AS penalty_goals,
    TRY_CAST(pkatt AS INTEGER)         AS penalty_attempts,
    TRY_CAST(sh AS INTEGER)            AS shots,
    TRY_CAST(sot AS INTEGER)           AS shots_on_target,
    TRY_CAST(crdy AS INTEGER)          AS yellow_cards,
    TRY_CAST(crdr AS INTEGER)          AS red_cards,
    TRY_CAST(crs AS INTEGER)           AS crosses,
    TRY_CAST(fls AS INTEGER)           AS fouls_committed,
    TRY_CAST(fld AS INTEGER)           AS fouls_drawn,
    TRY_CAST("off" AS INTEGER)         AS offsides,
    TRY_CAST(tklw AS INTEGER)          AS tackles_won,
    TRY_CAST("int" AS INTEGER)         AS interceptions,
    TRY_CAST(og AS INTEGER)            AS own_goals,
    pkwon                          AS penalties_won,
    pkcon                          AS penalties_conceded,

    -- ========= Lineage =========
    _ingested_at                   AS _bronze_ingested_at,

    -- ========= Partition Keys =========
    -- season → slug ('2425'); FBref bronze stores year-start bigint (2024).
    league,
    LPAD(CAST(MOD(season,     100) AS varchar), 2, '0')
        || LPAD(CAST(MOD(season + 1, 100) AS varchar), 2, '0') AS season

FROM src
WHERE rn = 1
