-- =============================================================================
-- Silver: fbref_player_identity
-- =============================================================================
-- Deterministic FBref identity universe shared by season profiles and xref.
--
-- Grain: (player_id, team_name, league, season)
--
-- Resolution is deliberately exact-only:
--   1. preserve a native FBref id;
--   2. for a missing id, recover only when normalized (league, season, name)
--      has exactly one native id across every FBref player-bearing dataset;
--   3. when the name is ambiguous, require exactly one native id after adding
--      normalized team;
--   4. otherwise emit a stable noid_<xxhash64> id.  No fuzzy merge is allowed.
-- =============================================================================

WITH source_rows AS (
    SELECT player_id, player AS player_name, squad AS team_name,
           league, season, source_season_id, _ingested_at, 'player_stats' AS dataset
    FROM iceberg.bronze.fbref_player_stats
    UNION ALL
    SELECT player_id, player, squad, league, season, source_season_id, _ingested_at, 'player_shooting'
    FROM iceberg.bronze.fbref_player_shooting
    UNION ALL
    SELECT player_id, player, squad, league, season, source_season_id, _ingested_at, 'player_playingtime'
    FROM iceberg.bronze.fbref_player_playingtime
    UNION ALL
    SELECT player_id, player, squad, league, season, source_season_id, _ingested_at, 'player_misc'
    FROM iceberg.bronze.fbref_player_misc
    UNION ALL
    SELECT player_id, player, squad, league, season, source_season_id, _ingested_at, 'keeper_keeper'
    FROM iceberg.bronze.fbref_keeper_keeper
    UNION ALL
    SELECT player_id, player, team, league, season, source_season_id, _ingested_at, 'match_player_stats'
    FROM iceberg.bronze.fbref_match_player_stats
    UNION ALL
    SELECT player_id, player, team, league, season, source_season_id, _ingested_at, 'lineups'
    FROM iceberg.bronze.fbref_lineups
    UNION ALL
    SELECT player_id, "Player", team, league, season, source_season_id, _ingested_at, 'match_keeper_stats'
    FROM iceberg.bronze.fbref_match_keeper_stats
),

normalized AS (
    SELECT
        NULLIF(TRIM(player_id), '') AS native_player_id,
        NULLIF(TRIM(player_name), '') AS player_name,
        NULLIF(TRIM(team_name), '') AS team_name,
        regexp_replace(
            regexp_replace(normalize(lower(TRIM(player_name)), NFD), '\p{Mn}+', ''),
            '[^a-z0-9]+', ''
        ) AS normalized_name,
        regexp_replace(
            regexp_replace(normalize(lower(TRIM(COALESCE(team_name, ''))), NFD), '\p{Mn}+', ''),
            '[^a-z0-9]+', ''
        ) AS normalized_team,
        league,
        CASE
             WHEN REGEXP_LIKE(COALESCE(source_season_id, ''), '^\d{4}$')
                 THEN source_season_id
             WHEN REGEXP_LIKE(COALESCE(source_season_id, ''), '^\d{4}-\d{4}$')
                 THEN SUBSTR(source_season_id, 3, 2)
                      || SUBSTR(source_season_id, 8, 2)
             WHEN NULLIF(TRIM(source_season_id), '') IS NOT NULL
                 THEN TRIM(source_season_id)
             WHEN league = 'INT-World Cup'
                 THEN LPAD(CAST(season AS varchar), 4, '0')
             ELSE LPAD(CAST(MOD(season, 100) AS varchar), 2, '0')
                  || LPAD(CAST(MOD(season + 1, 100) AS varchar), 2, '0')
        END AS season,
        _ingested_at,
        dataset
    FROM source_rows
    WHERE NULLIF(TRIM(player_name), '') IS NOT NULL
),

native_by_name AS (
    SELECT
        league,
        season,
        normalized_name,
        COUNT(DISTINCT native_player_id) AS candidate_count,
        MIN(native_player_id) AS unique_player_id
    FROM normalized
    WHERE native_player_id IS NOT NULL
    GROUP BY league, season, normalized_name
),

native_by_name_team AS (
    SELECT
        league,
        season,
        normalized_name,
        normalized_team,
        COUNT(DISTINCT native_player_id) AS candidate_count,
        MIN(native_player_id) AS unique_player_id
    FROM normalized
    WHERE native_player_id IS NOT NULL
    GROUP BY league, season, normalized_name, normalized_team
),

resolved AS (
    SELECT
        n.*,
        COALESCE(
            n.native_player_id,
            CASE WHEN bn.candidate_count = 1 THEN bn.unique_player_id END,
            CASE WHEN bnt.candidate_count = 1 THEN bnt.unique_player_id END,
            'noid_' || LOWER(TO_HEX(XXHASH64(TO_UTF8(
                n.league || '|' || n.season || '|' || n.normalized_name || '|' || n.normalized_team
            ))))
        ) AS resolved_player_id,
        CASE
            WHEN n.native_player_id IS NOT NULL THEN 'source_native'
            WHEN bn.candidate_count = 1 OR bnt.candidate_count = 1
                THEN 'recovered_unique_native'
            ELSE 'synthetic_residual'
        END AS row_resolution
    FROM normalized n
    LEFT JOIN native_by_name bn
        ON  bn.league          = n.league
        AND bn.season          = n.season
        AND bn.normalized_name = n.normalized_name
    LEFT JOIN native_by_name_team bnt
        ON  bnt.league          = n.league
        AND bnt.season          = n.season
        AND bnt.normalized_name = n.normalized_name
        AND bnt.normalized_team = n.normalized_team
)

SELECT
    resolved_player_id AS player_id,
    MAX_BY(player_name, _ingested_at) AS player_name,
    MAX_BY(team_name, _ingested_at) AS team_name,
    league,
    season,
    STARTS_WITH(resolved_player_id, 'noid_') AS is_synthetic,
    CASE
        WHEN STARTS_WITH(resolved_player_id, 'noid_') THEN 'synthetic_residual'
        WHEN COUNT_IF(row_resolution = 'recovered_unique_native') > 0
            THEN 'recovered_unique_native'
        ELSE 'source_native'
    END AS id_resolution,
    ARRAY_SORT(ARRAY_DISTINCT(ARRAY_AGG(dataset))) AS id_evidence_datasets,
    MAX(_ingested_at) AS _bronze_ingested_at
FROM resolved
GROUP BY resolved_player_id, normalized_team, league, season
