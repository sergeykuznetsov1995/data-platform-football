-- =============================================================================
-- Silver: xref_match
-- =============================================================================
-- Match-level cross-reference. FBref is the spine: `match_id` (an 8-char hex
-- assigned by FBref) is stable and globally unique, so for the E1 MVP we
-- only emit `source = 'fbref'` rows.
--
-- Cross-source bridging (understat/whoscored/sofascore game-id → fbref
-- match_id) is a Phase B follow-up: it requires a resolved `xref_team`
-- to fuzzy-match `(date, home_canonical_id, away_canonical_id)`. Doing it
-- here would create a circular dependency (xref_team depends on the
-- alias loader, xref_match would depend on xref_team materialised), so
-- the Phase B implementation will live in a SEPARATE follow-up CTAS that
-- UNION ALL's the bridged sources on top of this baseline.
--
-- DAG-integration note: T4 will wrap this SELECT in
-- `CREATE TABLE iceberg.silver.xref_match AS ...` via
-- `silver_tasks.run_silver_transform()`. This file MUST stay a pure SELECT.
--
-- =============================================================================
-- Schema (frozen for E1 dual-run)
-- =============================================================================
--   canonical_id   varchar  -- == fbref match_id (stable hex)
--   source         varchar  -- 'fbref' (only value in MVP)
--   source_id      varchar  -- == fbref match_id
--   display_name   varchar  -- 'Manchester City vs Arsenal' (for debug)
--   league         varchar
--   season         varchar  -- normalised (FBref bronze stores BIGINT)
--   confidence     varchar  -- always 'exact' (match_id is authoritative)
--   match_score    double   -- always NULL (no fuzzy step here)
--
-- Testable invariants (T5):
--   * PK = match_id  (one row per FBref match).
--   * Only source='fbref' present.
--   * canonical_id is NEVER NULL and equals source_id.
--   * confidence == 'exact' for every row.
--
-- =============================================================================
-- Bronze column-name reference (verified via DESCRIBE on 2026-05-08)
-- =============================================================================
--   fbref_schedule has NO `match_id` column. It must be derived from
--   `match_url` (hex slug after `/matches/`). For future fixtures the
--   match page does not exist yet, so we fall back to a deterministic
--   `fut_<xxhash64>` pseudo-id — IDENTICAL to the derivation used in
--   `dags/sql/silver/fbref_match_enriched.sql` (kept in sync so xref_match
--   joins match_enriched 1:1 on canonical_id).
--
--   fbref_schedule.match_url : varchar  (e.g. '/en/matches/cc5b4244/...')
--   fbref_schedule.wk        : varchar
--   fbref_schedule.date      : varchar
--   fbref_schedule.home      : varchar
--   fbref_schedule.away      : varchar
--   fbref_schedule.league    : varchar
--   fbref_schedule.season    : bigint   (CAST to varchar for unified schema)
-- =============================================================================

WITH derived AS (
    SELECT
        COALESCE(
            NULLIF(REGEXP_EXTRACT(match_url, '/matches/([a-f0-9]+)/', 1), ''),
            'fut_' || LOWER(TO_HEX(XXHASH64(TO_UTF8(
                COALESCE(NULLIF(CAST(wk   AS varchar), ''), CAST(date AS varchar))
                || '|' || COALESCE(CAST(season AS varchar), '')
                || '|' || COALESCE(CAST(home   AS varchar), '')
                || '|' || COALESCE(CAST(away   AS varchar), '')
            ))))
        )                                AS match_id,
        home,
        away,
        league,
        season
    FROM iceberg.bronze.fbref_schedule
)

SELECT
    match_id                              AS canonical_id,
    'fbref'                               AS source,
    match_id                              AS source_id,
    CONCAT(home, ' vs ', away)            AS display_name,
    league,
    CAST(season AS varchar)               AS season,
    'exact'                               AS confidence,
    CAST(NULL AS double)                  AS match_score
FROM derived
WHERE match_id IS NOT NULL
GROUP BY
    -- Defensive dedup: Bronze can pick up duplicate rows across re-scrapes
    -- before any de-dup step. Group by (match_id, league, season, home, away)
    -- to guarantee one row per match while keeping the home/away pair
    -- available for display_name.
    match_id, league, season, home, away
