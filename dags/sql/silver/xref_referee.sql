-- =============================================================================
-- Silver: xref_referee
-- =============================================================================
-- Referee cross-reference. No fuzzy matching at E1 (R8 spike deferred to
-- Tier 2). For each source we emit one row per DISTINCT referee name with
-- a deterministic slug-style canonical_id derived from the referee name.
--
-- No cross-source dedup at this stage: each source contributes its own
-- (canonical_id, source, source_id, league, season) row even when two
-- sources spell the same referee identically. Phase B (post-E1) can add
-- a second-pass UNION ALL to merge same-canonical rows from different
-- sources once we have a fuzzy-matching strategy in place.
--
-- DAG-integration note: T4 will wrap this SELECT in
-- `CREATE TABLE iceberg.silver.xref_referee AS ...` via
-- `silver_tasks.run_silver_transform()`. This file MUST stay a pure SELECT.
--
-- =============================================================================
-- Schema (frozen for E1 dual-run)
-- =============================================================================
--   canonical_id   varchar  -- LOWER(REGEXP_REPLACE(strip_diacritics(name),'[^a-zA-Z0-9]+','_'))
--   source         varchar  -- 'fbref' | 'matchhistory'
--   source_id      varchar  -- raw referee name as stored in Bronze
--   display_name   varchar  -- == source_id (no canonical names yet)
--   league         varchar
--   season         varchar  -- normalised (Bronze stores BIGINT)
--   confidence     varchar  -- always 'name_normalize' (no alias map yet)
--   match_score    double   -- always NULL
--
-- Testable invariants (T5):
--   * PK = (source, source_id, league, season).
--   * canonical_id is NEVER NULL.
--   * source ∈ {'fbref', 'matchhistory'}.
--   * confidence == 'name_normalize' for every row.
--
-- =============================================================================
-- Bronze column-name reference (verified via DESCRIBE on 2026-05-08)
-- =============================================================================
--   fbref_schedule.referee     : varchar
--   matchhistory_games.referee : varchar  (column is LOWERCASE in Bronze;
--                                          the source CSV is CamelCase but
--                                          soccerdata normalises to lower)
-- =============================================================================

SELECT
    -- Transliterate diacritics before slugging (issue #215, same root cause as
    -- #201/xref_manager): FBref / matchhistory may emit the same referee both
    -- with and without accents. A bare `[^a-zA-Z0-9]+ -> _` collapses each
    -- accent to '_', yielding DIFFERENT canonical_ids for one referee and
    -- risking the same SCD-2 split that broke dim_manager. NORMALIZE(NFD)
    -- decomposes "é" -> "e" + combining mark, `\p{Mn}+` strips the mark.
    -- Same idiom as xref_team.sql.j2 / xref_manager.sql.
    LOWER(REGEXP_REPLACE(
        REGEXP_REPLACE(NORMALIZE(referee_name, NFD), '\p{Mn}+', ''),
        '[^a-zA-Z0-9]+', '_'))                                 AS canonical_id,
    source,
    referee_name                                               AS source_id,
    referee_name                                               AS display_name,
    league,
    CAST(season AS varchar)                                    AS season,
    'name_normalize'                                           AS confidence,
    CAST(NULL AS double)                                       AS match_score
FROM (
    SELECT 'fbref' AS source, referee AS referee_name, league, season
      FROM iceberg.bronze.fbref_schedule
     WHERE referee IS NOT NULL AND referee <> ''
    UNION
    SELECT 'matchhistory' AS source, referee AS referee_name, league, season
      FROM iceberg.bronze.matchhistory_games
     WHERE referee IS NOT NULL AND referee <> ''
) raw_refs
GROUP BY
    -- Group by all output columns so the GROUP BY also serves as DISTINCT.
    1, source, referee_name, league, season
