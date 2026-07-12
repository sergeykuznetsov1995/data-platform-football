-- =============================================================================
-- Silver: fbref_player_match_stats
-- =============================================================================
--
-- Deduplicated per-match individual player statistics.
--
-- Source:
--   iceberg.bronze.fbref_match_player_stats
--
-- Identity / deduplication:
--   Blank source IDs are resolved through silver.fbref_player_identity before
--   ROW_NUMBER() OVER (PARTITION BY match_id, resolved_player_id, team
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

WITH identity AS (
    SELECT i.*,
           regexp_replace(
               regexp_replace(normalize(lower(TRIM(i.player_name)), NFD), '\p{Mn}+', ''),
               '[^a-z0-9]+', ''
           ) AS normalized_name,
           regexp_replace(
               regexp_replace(normalize(lower(TRIM(COALESCE(i.team_name, ''))), NFD), '\p{Mn}+', ''),
               '[^a-z0-9]+', ''
           ) AS normalized_team
    FROM iceberg.silver.fbref_player_identity i
),

src_identified AS (
    SELECT b.*, i.player_id AS resolved_player_id,
           i.id_resolution AS identity_resolution,
           i.is_synthetic AS identity_is_synthetic,
           i.id_evidence_datasets AS identity_evidence_datasets
    FROM iceberg.bronze.fbref_match_player_stats b
    INNER JOIN identity i
        ON  i.league = b.league
        AND i.season = CASE
            WHEN REGEXP_LIKE(COALESCE(b.source_season_id, ''), '^\d{4}$')
                THEN b.source_season_id
            WHEN REGEXP_LIKE(COALESCE(b.source_season_id, ''), '^\d{4}-\d{4}$')
                THEN SUBSTR(b.source_season_id, 3, 2) || SUBSTR(b.source_season_id, 8, 2)
            WHEN NULLIF(TRIM(b.source_season_id), '') IS NOT NULL
                THEN TRIM(b.source_season_id)
            WHEN b.league = 'INT-World Cup'
                THEN LPAD(CAST(b.season AS varchar), 4, '0')
            ELSE LPAD(CAST(MOD(b.season, 100) AS varchar), 2, '0')
                 || LPAD(CAST(MOD(b.season + 1, 100) AS varchar), 2, '0')
        END
        AND i.normalized_name = regexp_replace(
            regexp_replace(normalize(lower(TRIM(b.player)), NFD), '\p{Mn}+', ''),
            '[^a-z0-9]+', ''
        )
        AND i.normalized_team = regexp_replace(
            regexp_replace(normalize(lower(TRIM(COALESCE(b.team, ''))), NFD), '\p{Mn}+', ''),
            '[^a-z0-9]+', ''
        )
        AND (
            (NULLIF(TRIM(b.player_id), '') IS NOT NULL
                AND i.player_id = NULLIF(TRIM(b.player_id), ''))
            OR (NULLIF(TRIM(b.player_id), '') IS NULL
                AND i.id_resolution <> 'source_native')
        )
),

src AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY match_id, resolved_player_id, team
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM src_identified
)

SELECT
    -- ========= Identification =========
    match_id,
    resolved_player_id               AS player_id,
    identity_resolution              AS player_id_resolution,
    identity_is_synthetic            AS player_id_is_synthetic,
    identity_evidence_datasets       AS player_id_evidence_datasets,
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
    -- #913 Phase 2
    CASE
         WHEN REGEXP_LIKE(COALESCE(source_season_id, ''), '^\d{4}$')
             THEN source_season_id
         WHEN REGEXP_LIKE(COALESCE(source_season_id, ''), '^\d{4}-\d{4}$')
             THEN SUBSTR(source_season_id, 3, 2) || SUBSTR(source_season_id, 8, 2)
         WHEN NULLIF(TRIM(source_season_id), '') IS NOT NULL
             THEN TRIM(source_season_id)
         WHEN league = 'INT-World Cup'
             THEN LPAD(CAST(season AS varchar), 4, '0')
         ELSE LPAD(CAST(MOD(season, 100) AS varchar), 2, '0')
              || LPAD(CAST(MOD(season + 1, 100) AS varchar), 2, '0')
    END AS season

FROM src
WHERE rn = 1
