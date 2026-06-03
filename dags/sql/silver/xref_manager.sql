-- =============================================================================
-- Silver: xref_manager
-- =============================================================================
-- Manager (head-coach) cross-reference. Two sources (issue #144):
--   * FBref  — iceberg.bronze.fbref_match_managers (scorebox parser,
--              scrapers/fbref/parsers/finders.py::parse_match_managers). Spine;
--              has only a manager NAME (no stable id).
--   * FotMob — iceberg.bronze.fotmob_player_details WHERE is_coach = true.
--              Carries a STABLE coachId (player_id) which we keep verbatim in
--              source_id, plus the coach name for cross-source gluing.
--
-- Cross-source identity (issue #144):
--   Both sources derive canonical_id with the SAME diacritic-stripping
--   name-normalize idiom, so the same coach spelled identically lands on one
--   canonical_id (FBref "Mikel Arteta" == FotMob "Mikel Arteta"). A FotMob row
--   whose canonical_id has NO FBref counterpart in the same league is flagged
--   confidence='orphan' (read by xref_dq.evaluate_orphan_rate_per_source +
--   dag_transform_xref Phase 2) so a maintainer can see what failed to glue.
--
-- Why name-normalize, not a rapidfuzz resolver (cf. xref_player): managers are
-- tiny (~20 per league-season) and mostly spelled identically across sources;
-- a full fuzzy resolver is over-engineering here. canonical_id stays NAME-based
-- (not coachId-anchored) on purpose — anchoring on coachId would rewrite every
-- existing FBref canonical_id and change dim_manager output, breaking the
-- "refactor without behaviour change" contract. coachId is preserved in
-- source_id for a future ID-anchor endgame (mirrors xref_team team_id, #141).
--
-- DAG-integration note: this SELECT is wrapped in
-- `CREATE OR REPLACE TABLE iceberg.silver.xref_manager AS ...` via
-- `silver_tasks.run_silver_transform()`. This file MUST stay a pure SELECT.
--
-- =============================================================================
-- Schema (frozen — identical to xref_team / xref_referee)
-- =============================================================================
--   canonical_id   varchar  -- LOWER(REGEXP_REPLACE(strip_diacritics(name),'[^a-zA-Z0-9]+','_'))
--   source         varchar  -- 'fbref' | 'fotmob'
--   source_id      varchar  -- FBref: raw manager_name; FotMob: stable coachId
--   display_name   varchar  -- raw name as stored in the source
--   league         varchar
--   season         varchar  -- normalised (Bronze stores BIGINT)
--   confidence     varchar  -- 'name_normalize' | 'orphan' (FotMob w/o FBref glue)
--   match_score    double   -- always NULL (no fuzzy scoring)
--
-- Testable invariants (xref_dq.build_xref_manager_checks):
--   * PK = (source, source_id, league, season).
--   * canonical_id is NEVER NULL.
--   * source ∈ {'fbref', 'fotmob'}.
--   * confidence ∈ {'name_normalize', 'orphan'}.
-- =============================================================================

WITH fbref_canon AS (
    -- Distinct FBref canonical_ids per league — the glue target for FotMob.
    -- DISTINCT keeps the LEFT JOIN below 1:1 (no fan-out of FotMob rows).
    SELECT DISTINCT
        LOWER(REGEXP_REPLACE(
            REGEXP_REPLACE(NORMALIZE(manager_name, NFD), '\p{Mn}+', ''),
            '[^a-zA-Z0-9]+', '_'))                              AS canonical_id,
        league
    FROM iceberg.bronze.fbref_match_managers
    WHERE manager_name IS NOT NULL AND manager_name <> ''
),

fotmob_coach AS (
    -- One coach row per (player_id, league, season). bronze.fotmob_player_details
    -- can carry >1 physical row per player across scrape runs (same dedup idiom
    -- as fotmob_player_profile.sql) — collapse to one so source_id=player_id
    -- stays unique for the (source, source_id, league, season) PK.
    SELECT player_id, name, league, season
    FROM (
        SELECT
            CAST(player_id AS varchar)                          AS player_id,
            name,
            league,
            season,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(player_id AS varchar), league, season
                ORDER BY name
            )                                                   AS rn
        FROM iceberg.bronze.fotmob_player_details
        WHERE is_coach = true
          AND name IS NOT NULL AND name <> ''
    )
    WHERE rn = 1
)

-- ===== fbref (spine) =====
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

UNION ALL

-- ===== fotmob (coachId mirror) =====
SELECT
    LOWER(REGEXP_REPLACE(
        REGEXP_REPLACE(NORMALIZE(d.name, NFD), '\p{Mn}+', ''),
        '[^a-zA-Z0-9]+', '_'))                                  AS canonical_id,
    'fotmob'                                                    AS source,
    CAST(d.player_id AS varchar)                                AS source_id,
    d.name                                                      AS display_name,
    d.league,
    CAST(d.season AS varchar)                                   AS season,
    -- 'name_normalize' when this coach glues to an FBref canonical in the same
    -- league; 'orphan' otherwise (surfaced by the Phase 2 orphan-rate report).
    CASE WHEN fc.canonical_id IS NOT NULL
         THEN 'name_normalize'
         ELSE 'orphan'
    END                                                         AS confidence,
    CAST(NULL AS double)                                        AS match_score
FROM fotmob_coach d
LEFT JOIN fbref_canon fc
    ON  fc.canonical_id = LOWER(REGEXP_REPLACE(
            REGEXP_REPLACE(NORMALIZE(d.name, NFD), '\p{Mn}+', ''),
            '[^a-zA-Z0-9]+', '_'))
    AND fc.league = d.league
