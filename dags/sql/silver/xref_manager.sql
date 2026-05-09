-- =============================================================================
-- Silver: xref_manager   (STUB — empty placeholder)
-- =============================================================================
-- Manager (head-coach) xref is intentionally empty at E1.
--
-- Why empty?
--   E2 dim_manager was deferred to Phase 1.5 (R0.2c FALLBACK):
--     * R0.2c attempt — FotMob `/match/<id>?coachId=...` endpoint hardening
--       — partially fixed but still flaky on legacy fixtures.
--     * R0.2a fallback — FBref match-page parser — not yet implemented.
--   Until either path lands a stable Bronze source, no row can be emitted.
--   See:  docs/decisions/E2-postmortem.md  +  feedback_fotmob_endpoint_hardened.md
--
-- Why materialise the table at all?
--   Downstream Gold dims (dim_manager / fct_team_match.head_coach_id)
--   will JOIN against `iceberg.silver.xref_manager`. Materialising it as
--   an empty table with the correct schema means:
--     1. T4 DAG-task does not branch on "table exists?" — the CTAS just
--        produces zero rows.
--     2. T5 schema-drift tests can validate the column set today and
--        flag a regression the moment Phase 1.5 starts populating it.
--     3. JOINs in downstream code never panic with "relation not found".
--
-- DAG-integration note: T4 will wrap this SELECT in
-- `CREATE TABLE iceberg.silver.xref_manager AS ...` via
-- `silver_tasks.run_silver_transform()`. This file MUST stay a pure SELECT.
--
-- =============================================================================
-- Schema (frozen for E1 dual-run; identical to xref_team / xref_referee)
-- =============================================================================
--   canonical_id   varchar
--   source         varchar
--   source_id      varchar
--   display_name   varchar
--   league         varchar
--   season         varchar
--   confidence     varchar
--   match_score    double
--
-- Testable invariants (T5):
--   * row_count == 0   (WHERE 1=0 guarantees this).
--   * Schema (column names + types) matches xref_team and xref_referee.
-- =============================================================================

SELECT
    CAST(NULL AS varchar)  AS canonical_id,
    CAST(NULL AS varchar)  AS source,
    CAST(NULL AS varchar)  AS source_id,
    CAST(NULL AS varchar)  AS display_name,
    CAST(NULL AS varchar)  AS league,
    CAST(NULL AS varchar)  AS season,
    CAST(NULL AS varchar)  AS confidence,
    CAST(NULL AS double)   AS match_score
WHERE 1 = 0
