-- =============================================================================
-- Gold: fct_event
-- =============================================================================
-- Per-event SPADL fact derived from `iceberg.silver.whoscored_events_spadl`
-- with cross-source identity resolution via E1 xref tables.
--
-- One row per WhoScored event — full row-count parity with Silver (NO filtering
-- on action='unknown'; per R3.D5 those rows are kept for audit).
--
-- Sources:
--   iceberg.silver.whoscored_events_spadl   (input — frozen E3.1 deliverable)
--   iceberg.bronze.whoscored_schedule_current (Opta team id -> full source name)
--   iceberg.silver.xref_team                (team_id resolution)
--   iceberg.silver.xref_player              (player_id resolution)
--
-- DAG-integration note: T3 wraps this SELECT in
-- `CREATE TABLE iceberg.gold.fct_event AS ... WITH (partitioning=ARRAY['league','season'])`
-- via `gold_tasks.run_gold_transform()`. This file MUST stay a pure SELECT
-- (no CREATE TABLE, no DDL). `_silver_created_at` (or analogous lineage column)
-- is appended by the gold_tasks wrapper — do NOT add it here.
--
-- =============================================================================
-- Output schema (frozen for E3 wave-1 — see plan R3.E3.3)
-- =============================================================================
--   match_id    varchar     resolved match canonical (see ADR #1)
--   match_id_source       varchar     'fbref' when bridged via silver.xref_match,
--                                     else 'whoscored_raw' (orphan)
--   match_id_version      varchar     'v1' when bridged, else 'v0_unbridged' (orphan)
--   event_id              varchar     passthrough from silver (synthetic stable PK)
--   team_id     varchar     via xref_team — orphan-tolerant LEFT JOIN
--   team_id_source        varchar     'whoscored'
--   team_id_version       varchar     'v1'
--   player_id   varchar     via xref_player — NULL when player_id missing
--   player_id_source      varchar     'whoscored'
--   player_id_version     varchar     'v1'
--   period                varchar     passthrough
--   minute                integer     renamed from silver.expanded_minute
--   x, y, end_x, end_y    double      pitch coordinates (passthrough)
--   action                varchar     SPADL 24-value enum (passthrough)
--   action_source         varchar     'whoscored_spadl_proprietary_v1'
--   action_version        varchar     'v1'
--   _action_confidence    varchar     'high'/'medium'/'low'/'unmappable'
--   _action_source_note   varchar     audit trail (orig WhoScored type + paired flags)
--   outcome_success       boolean     passthrough
--   league                varchar     partition key
--   season                varchar     partition key (silver stores varchar)
--
-- Primary key:  (match_id, event_id)
--   Note: match_id == raw match_id at v0_unbridged stage, so PK is
--   effectively (match_id, event_id) which silver guarantees unique
--   (166,453 rows / 166,453 distinct keys verified on 2425 corpus).
--
-- =============================================================================
-- ADR — architectural decisions documented inline
-- =============================================================================
--
-- ADR-1: match_id bridging — Phase B SHIPPED
-- ------------------------------------------
-- silver.xref_match now carries one row per WhoScored game (source='whoscored').
-- Bridged rows expose the FBref canonical hex; rows that have no FBref
-- counterpart (mid-week / non-APL fixture not in FBref spine) carry an
-- orphan canonical of the form 'ws_<game_id>' with confidence='orphan'.
--
-- This CTAS LEFT JOINs xref_match and resolves the triplet:
--   * match_id = COALESCE(xm.canonical_id, e.match_id)
--   * match_id_source    = 'fbref' when bridged AND non-orphan, else 'whoscored_raw'
--   * match_id_version   = 'v1'    when bridged AND non-orphan, else 'v0_unbridged'
--
-- The orphan branch preserves R3.D5 row-count parity (ADR-4) — silver-level
-- whoscored events with no FBref bridge still flow through Gold.
--
-- ref_integrity for match_id → silver.xref_match.canonical_id is
-- now ENABLED in `dags/utils/e3_dq.py::_build_fct_event_checks` because
-- every WhoScored game_id has a row in xref_match (bridged or orphan).
--
-- ADR-2: numeric team id bridges event short names to schedule full names
-- -----------------------------------------------------------------------
-- silver.whoscored_events_spadl carries both the raw Opta team id and the raw
-- event team name. Event names are often abbreviations (`Man City`, `PSG`,
-- `Bayern`, `RBL`), while xref_team's WhoScored universe is built from schedule
-- full names. The manifest-filtered schedule current view supplies the stable
-- numeric-id -> full-name bridge without scanning append-only event history.
-- Duplicate observations of the same name collapse to one row. Conflicting
-- names for one (league, season, team_id) deliberately produce NULL rather
-- than an arbitrary identity; a missing schedule mapping alone may fall back
-- to the raw event name.
--
-- ADR-3: player_id orphan-tolerant LEFT JOIN
-- ------------------------------------------
-- xref_player resolver writes one row per (source, source_id) pair —
-- including unresolved WhoScored players who get
-- `(canonical_id='ws_<sid>', source='whoscored', source_id='<sid>')`
-- with confidence='orphan' (see xref_player_resolver.py
-- `cascade_resolve` -> `_orphan_prefix`). So a LEFT JOIN
-- on (source='whoscored', source_id=player_id_raw) always returns
-- *some* canonical_id when player_id_raw is present in xref_player.
--
-- player_id IS NULL only when:
--   (a) silver event has player_id_raw IS NULL (substitutions / meta /
--       team-only events — kept by R3.D5 row-count parity); or
--   (b) xref_player has not been re-materialised after a new bronze
--       player appeared (timing race — DQ in E3.8 surfaces this as
--       orphan rate; target ≤7%).
--
-- Both cases are tolerated; downstream ML can filter on
-- player_id IS NOT NULL when needed.
--
-- ADR-4: NO row dropping (R3.D5)
-- ------------------------------
-- WHERE filters are forbidden in this CTAS. Silver writes one row per
-- bronze event (including action_canonical='unknown' meta/marker rows).
-- Dropping any row here breaks the bronze→silver→gold row-count parity
-- audit. Consumers filter unknown via WHERE action != 'unknown'
-- in their own queries.
--
-- =============================================================================

WITH ws_team_name_observations AS (
    SELECT
        league,
        season,
        CAST(home_team_id AS varchar) AS ws_team_id,
        home_team AS ws_team_name
    FROM iceberg.bronze.whoscored_schedule_current
    WHERE home_team_id IS NOT NULL
      AND home_team IS NOT NULL
      AND TRIM(home_team) <> ''

    UNION ALL

    SELECT
        league,
        season,
        CAST(away_team_id AS varchar) AS ws_team_id,
        away_team AS ws_team_name
    FROM iceberg.bronze.whoscored_schedule_current
    WHERE away_team_id IS NOT NULL
      AND away_team IS NOT NULL
      AND TRIM(away_team) <> ''
),
ws_team_id_to_name AS (
    SELECT
        league,
        season,
        ws_team_id,
        MIN(ws_team_name) AS ws_team_name,
        COUNT(DISTINCT ws_team_name) AS team_name_count
    FROM ws_team_name_observations
    GROUP BY league, season, ws_team_id
)
SELECT
    -- ============================================================
    -- match identity (R0.4 canonical / source / version triplet)
    -- ============================================================
    -- ADR-1: bridged via silver.xref_match LEFT JOIN. When the cascade
    -- found a FBref counterpart for this whoscored game we emit FBref
    -- hex + ('fbref','v1'). When no counterpart was found, xm.canonical_id
    -- is the orphan-prefixed 'ws_<id>' (confidence='orphan'); we surface
    -- it as match_id for ref_integrity but keep the legacy
    -- ('whoscored_raw','v0_unbridged') labels so downstream consumers
    -- can detect unbridged rows.
    COALESCE(xm.canonical_id, e.match_id)        AS match_id,
    CASE
        WHEN xm.canonical_id IS NOT NULL AND xm.confidence != 'orphan'
            THEN CAST('fbref'         AS varchar)
        ELSE CAST('whoscored_raw' AS varchar)
    END                                          AS match_id_source,
    CASE
        WHEN xm.canonical_id IS NOT NULL AND xm.confidence != 'orphan'
            THEN CAST('v1'            AS varchar)
        ELSE CAST('v0_unbridged'  AS varchar)
    END                                          AS match_id_version,

    -- ============================================================
    -- event_id passthrough (silver guarantees PK uniqueness)
    -- ============================================================
    e.event_id                                   AS event_id,

    -- ============================================================
    -- team identity — resolved from the Silver raw-name passthrough
    -- ============================================================
    -- xt.canonical_id is NULL when:
    --   * team_name_raw missing in the Silver passthrough (defensive — should
    --     not happen on a validated successful match batch); OR
    --   * xref_team has no row for (whoscored, team_name) — alias
    --     YAML drift; DQ in E3.8 catches this.
    xt.canonical_id                              AS team_id,
    CAST('whoscored' AS varchar)                 AS team_id_source,
    CAST('v1'        AS varchar)                 AS team_id_version,

    -- ============================================================
    -- player identity — orphan-tolerant LEFT JOIN on xref_player
    -- ============================================================
    -- ADR-3: player_id is non-null when player_id_raw
    -- exists in xref_player (orphan rows have ws_<sid> canonical,
    -- so non-resolved players still get a stable key downstream).
    -- Genuinely NULL only for player_id_raw IS NULL events (subs /
    -- markers / meta — R3.D5 row-count parity).
    xp.canonical_id                              AS player_id,
    CAST('whoscored' AS varchar)                 AS player_id_source,
    CAST('v1'        AS varchar)                 AS player_id_version,

    -- ============================================================
    -- Time / pitch coordinates (passthrough)
    -- ============================================================
    e.period                                     AS period,
    e.expanded_minute                            AS minute,
    e.x                                          AS x,
    e.y                                          AS y,
    e.end_x                                      AS end_x,
    e.end_y                                      AS end_y,

    -- ============================================================
    -- SPADL action vocabulary (passthrough — see silver header for enum)
    -- ============================================================
    e.action_canonical                           AS action,
    e.action_source                              AS action_source,
    e.action_version                             AS action_version,
    e._action_confidence                         AS _action_confidence,
    e._action_source_note                        AS _action_source_note,

    -- ============================================================
    -- Outcome flag (passthrough)
    -- ============================================================
    e.outcome_success                            AS outcome_success,

    -- ============================================================
    -- Partition keys (passthrough)
    -- ============================================================
    e.league                                     AS league,
    e.season                                     AS season

FROM iceberg.silver.whoscored_events_spadl e

-- ---- numeric WhoScored team id -> schedule full name ----
LEFT JOIN ws_team_id_to_name wsn
    ON wsn.ws_team_id = e.team_id_raw
   AND wsn.league     = e.league
   AND wsn.season     = e.season

-- ---- team xref (schedule full name -> canonical) ----
-- #506: exclude confidence='orphan' rows — they carry a non-NULL source-
-- prefixed canonical ('ws_<slug>') that would leak through as a resolved id.
-- xref_team.sql.j2 contract: orphans are excluded from every cross-source JOIN.
LEFT JOIN iceberg.silver.xref_team xt
    ON xt.source    = 'whoscored'
   AND xt.source_id = CASE
       WHEN wsn.team_name_count = 1 THEN wsn.ws_team_name
       WHEN wsn.team_name_count IS NULL THEN e.team_name_raw
   END
   AND xt.league    = e.league
   AND xt.season    = e.season
   AND xt.confidence <> 'orphan'

-- ---- player xref (whoscored numeric id -> canonical or ws_ orphan) ----
-- xref_player has one row per (source, source_id, season) — the resolver
-- writes a row per season to capture mid-career club moves. Without the
-- season predicate, a player who played 2425+2526 fans the JOIN out 2x.
LEFT JOIN iceberg.silver.xref_player xp
    ON xp.source    = 'whoscored'
   AND xp.source_id = e.player_id_raw
   AND xp.league    = e.league
   AND xp.season    = e.season

-- ---- match xref (whoscored game_id -> FBref hex via Phase B cascade) ----
-- Phase B (Task 2.1) materialises a 7-source xref_match. For every WhoScored
-- game silver.xref_match has a row (bridged FBref or orphan-prefixed
-- 'ws_<id>'), so the JOIN here resolves match_id to a non-NULL
-- value matched by ref_integrity.
LEFT JOIN iceberg.silver.xref_match xm
    ON xm.source    = 'whoscored'
   AND xm.source_id = e.match_id
   AND xm.league    = e.league
   AND xm.season    = e.season
