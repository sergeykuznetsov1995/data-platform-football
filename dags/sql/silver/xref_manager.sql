-- =============================================================================
-- Silver: xref_manager
-- =============================================================================
-- Manager (head-coach) cross-reference. Single-source spine (FBref) at
-- Phase 1.5; mirror UNION blocks can be added later when a second source
-- with stable manager metadata appears (FotMob `/coachId` was hardened
-- 2026-05-08, see feedback_fotmob_endpoint_hardened.md).
--
-- Source: iceberg.bronze.fbref_match_managers (populated by FBref scorebox
-- parser, scrapers/fbref/parsers/finders.py::parse_match_managers).
--
-- DAG-integration note: T4 wraps this SELECT in
-- `CREATE TABLE iceberg.silver.xref_manager AS ...` via
-- `silver_tasks.run_silver_transform()`. This file MUST stay a pure SELECT.
--
-- =============================================================================
-- Schema (frozen — identical to xref_team / xref_referee)
-- =============================================================================
--   canonical_id   varchar  -- LOWER(REGEXP_REPLACE(strip_diacritics(name),'[^a-zA-Z0-9]+','_'))
--   source         varchar  -- 'fbref'
--   source_id      varchar  -- raw manager_name as stored in Bronze
--   display_name   varchar  -- == source_id (no canonical names yet)
--   league         varchar
--   season         varchar  -- normalised (Bronze stores BIGINT)
--   confidence     varchar  -- always 'name_normalize' (no alias map yet)
--   match_score    double   -- always NULL
--
-- Testable invariants (T5 / xref_dq.build_xref_manager_checks):
--   * PK = (source, source_id, league, season).
--   * canonical_id is NEVER NULL.
--   * source ∈ {'fbref'}.
--   * confidence == 'name_normalize' for every row.
-- =============================================================================

SELECT
    -- Transliterate diacritics before slugging (issue #201): FBref emits the
    -- same manager name both with and without accents ("Régis Le Bris" vs
    -- "Regis Le Bris"). A bare `[^a-zA-Z0-9]+ -> _` collapses each accent to
    -- '_', so the two spellings yield DIFFERENT canonical_ids and dim_manager's
    -- LAG-based stint detector then splits one continuous stint into several,
    -- breaking the (manager, team, valid_from) PK. NORMALIZE(NFD) decomposes
    -- "é" -> "e" + combining mark, `\p{Mn}+` strips the mark, leaving ASCII
    -- "e". Same idiom as xref_team.sql.j2.
    LOWER(REGEXP_REPLACE(
        REGEXP_REPLACE(NORMALIZE(manager_name, NFD), '\p{Mn}+', ''),
        '[^a-zA-Z0-9]+', '_'))                                  AS canonical_id,
    'fbref'                                                     AS source,
    manager_name                                                AS source_id,
    manager_name                                                AS display_name,
    league,
    CAST(season AS varchar)                                     AS season,
    'name_normalize'                                            AS confidence,
    CAST(NULL AS double)                                        AS match_score
FROM iceberg.bronze.fbref_match_managers
WHERE manager_name IS NOT NULL AND manager_name <> ''
GROUP BY
    -- GROUP BY all output columns — also serves as DISTINCT
    1, manager_name, league, season
