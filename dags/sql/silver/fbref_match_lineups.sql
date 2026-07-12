-- =============================================================================
-- Silver: fbref_match_lineups
-- =============================================================================
--
-- Detailed per-player lineup entries for each match.
--
-- Source:
--   iceberg.bronze.fbref_lineups
--
-- Identity / deduplication (#463, #916):
--   Blank source IDs are resolved through silver.fbref_player_identity before
--   ROW_NUMBER() OVER (PARTITION BY match_id, resolved_player_id, team
--                       ORDER BY _ingested_at DESC, _batch_id DESC)  =>  rn = 1
--   COALESCE guards legacy rows without player_id (otherwise every NULL row
--   of a match collapses into one partition); team guards same-named players
--   on opposite sides (mirrors fbref_player_match_stats.sql).
--
-- Notes:
--   * is_starter is already BOOLEAN in Bronze.
--   * jersey_number is TRY_CAST to INTEGER (source is VARCHAR).
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
    FROM iceberg.bronze.fbref_lineups b
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
               ORDER BY _ingested_at DESC, _batch_id DESC
           ) AS rn
    FROM src_identified
)

SELECT
    match_id,
    team,
    player,
    resolved_player_id               AS player_id,
    identity_resolution              AS player_id_resolution,
    identity_is_synthetic            AS player_id_is_synthetic,
    identity_evidence_datasets       AS player_id_evidence_datasets,
    is_starter,
    position,
    TRY_CAST(number AS INTEGER)    AS jersey_number,

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
