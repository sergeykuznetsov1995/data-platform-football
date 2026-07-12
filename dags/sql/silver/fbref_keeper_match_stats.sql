-- =============================================================================
-- Silver: fbref_keeper_match_stats
-- =============================================================================
-- Typed per-match goalkeeper metrics from the two keeper_stats_<team_id>
-- tables parsed on every FBref match page.
--
-- Grain: (match_id, resolved player_id, team_side). Blank source IDs are
-- resolved through silver.fbref_player_identity before deduplication.
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
    FROM iceberg.bronze.fbref_match_keeper_stats b
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
            regexp_replace(normalize(lower(TRIM(b."Player")), NFD), '\p{Mn}+', ''),
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
               PARTITION BY match_id, resolved_player_id, team_side
               ORDER BY _ingested_at DESC, _batch_id DESC
           ) AS rn
    FROM src_identified
)

SELECT
    match_id,
    resolved_player_id                            AS player_id,
    identity_resolution                           AS player_id_resolution,
    identity_is_synthetic                         AS player_id_is_synthetic,
    identity_evidence_datasets                    AS player_id_evidence_datasets,
    "Player"                                      AS player_name,
    team                                          AS team_name,
    team_side,
    "Nation"                                      AS nation,
    "Age"                                         AS age,
    TRY_CAST("Min" AS INTEGER)                    AS minutes,
    TRY_CAST("Shot Stopping_SoTA" AS INTEGER)     AS shots_on_target_against,
    TRY_CAST("Shot Stopping_GA" AS INTEGER)       AS goals_against,
    TRY_CAST("Shot Stopping_Saves" AS INTEGER)    AS saves,
    TRY_CAST("Shot Stopping_Save%" AS DOUBLE)     AS save_pct,
    _ingested_at                                  AS _bronze_ingested_at,
    league,
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
  AND match_id IS NOT NULL
  AND team_side IN ('home', 'away')
