-- =============================================================================
-- Gold: fct_match_officials   (issue #613)
-- =============================================================================
-- One row per (match_id, role) — the officiating crew per match, extending
-- coverage beyond the main referee (already on dim_match.referee_id) to
-- ar1 / ar2 / fourth_official / var.
--
-- Grain / PK: (match_id, role).   role ∈ {referee, ar1, ar2, fourth_official, var}
-- FK: match_id   -> gold.dim_match      (ERROR ref_integrity)
--     referee_id -> gold.dim_referee    (WARNING, rate-mode — see below)
--
-- referee_id is resolved best-effort through the EXISTING silver.xref_referee
-- (source='fbref'), with the FULL (league, season) predicate — xref rows are
-- per-(source, source_id, league, season); omitting season fans out N×
-- (memory: feedback_xref_join_season_predicate; same pattern as
-- dim_match.sql.j2:139-143). The main referee and any assistant who also
-- referees elsewhere resolve automatically (their names already enter
-- xref_referee from fbref_schedule). Pure assistants / VAR-only officials stay
-- referee_id = NULL but keep official_name — they are single-source (FBref),
-- so a canonical_id would be redundant (no cross-source identity to glue).
-- Hence the dim_referee FK is WARNING rate-mode, NULLs are by design.
--
-- Pure SELECT: the Gold runner wraps CREATE OR REPLACE + partitioning
-- (league, season) + audit column. This file MUST stay a pure SELECT.
-- =============================================================================

SELECT
    o.match_id,
    o.role,
    ref_x.canonical_id  AS referee_id,
    o.official_name,
    o.league,
    o.season
FROM iceberg.silver.fbref_match_officials o
INNER JOIN iceberg.gold.dim_match match_scope
    ON match_scope.match_id = o.match_id
LEFT JOIN iceberg.silver.xref_referee ref_x
    ON  ref_x.source    = 'fbref'
    AND ref_x.source_id = TRIM(o.official_name)
    AND ref_x.league    = o.league
    AND ref_x.season    = o.season
