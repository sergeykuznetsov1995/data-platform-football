-- =============================================================================
-- Gold: fct_event
-- =============================================================================
-- Per-event SPADL fact derived from `iceberg.silver.whoscored_events_spadl`
-- with cross-source identity resolution via E1 xref tables.
--
-- One row per WhoScored event — full row-count parity with Silver (NO filtering
-- on action_canonical='unknown'; per R3.D5 those rows are kept for audit).
--
-- Sources:
--   iceberg.silver.whoscored_events_spadl   (input — frozen E3.1 deliverable)
--   iceberg.silver.xref_team                (team_id resolution)
--   iceberg.silver.xref_player              (player_id resolution)
--   iceberg.bronze.whoscored_events         (only for team_id → team_name bridge)
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
--   match_id_canonical    varchar     resolved match canonical (see ADR #1)
--   match_id_source       varchar     'whoscored_raw' (cross-source bridging deferred)
--   match_id_version      varchar     'v0_unbridged' (will become 'v1' after Phase B)
--   event_id              varchar     passthrough from silver (synthetic stable PK)
--   team_id_canonical     varchar     via xref_team — orphan-tolerant LEFT JOIN
--   team_id_source        varchar     'whoscored'
--   team_id_version       varchar     'v1'
--   player_id_canonical   varchar     via xref_player — NULL when player_id missing
--   player_id_source      varchar     'whoscored'
--   player_id_version     varchar     'v1'
--   period                varchar     passthrough
--   minute                integer     renamed from silver.expanded_minute
--   x, y, end_x, end_y    double      pitch coordinates (passthrough)
--   action_canonical      varchar     SPADL 24-value enum (passthrough)
--   action_source         varchar     'whoscored_spadl_proprietary_v1'
--   action_version        varchar     'v1'
--   _action_confidence    varchar     'high'/'medium'/'low'/'unmappable'
--   _action_source_note   varchar     audit trail (orig WhoScored type + paired flags)
--   outcome_success       boolean     passthrough
--   league                varchar     partition key
--   season                varchar     partition key (silver stores varchar)
--
-- Primary key:  (match_id_canonical, event_id)
--   Note: match_id_canonical == raw match_id at v0_unbridged stage, so PK is
--   effectively (match_id, event_id) which silver guarantees unique
--   (166,453 rows / 166,453 distinct keys verified on 2425 corpus).
--
-- =============================================================================
-- ADR — architectural decisions documented inline
-- =============================================================================
--
-- ADR-1: match_id bridging — DEFERRED to Phase B (post-E1.5)
-- ----------------------------------------------------------
-- E1 D6 fixes `xref_match` as FBref-only (`source='fbref'`, no whoscored
-- mapping). Bridging WhoScored game_id → FBref match_id requires a
-- materialised `xref_team` + `(date, home_canonical_id, away_canonical_id)`
-- fuzzy join — that follow-up CTAS lives in Phase B (between E1.5 and E3
-- per the E1 postmortem deferred-list).
--
-- Decision for E3.3: emit `match_id_canonical = raw match_id` with
-- `match_id_source='whoscored_raw'` and `match_id_version='v0_unbridged'`.
-- The version label is the load-bearing signal for downstream consumers:
-- after Phase B ships, fct_event will rebuild with `_version='v1'` and
-- canonical IDs swapped to FBref hex. R0.4 schema-versioning rules cover
-- the migration runbook (Iceberg ADD COLUMN is backward-compatible; CTAS
-- rebuild rewrites Parquet).
--
-- Why not LEFT JOIN xref_match anyway? It would always return NULL on the
-- whoscored side (no rows with source='whoscored'), forcing every consumer
-- to COALESCE(canonical, raw). Embedding the raw id directly with an
-- explicit version label is cleaner and matches the R0.4 pattern.
--
-- ADR-2: team_id_raw bridge through bronze.whoscored_events
-- ---------------------------------------------------------
-- silver.whoscored_events_spadl carries `team_id_raw` as a numeric Opta id
-- (CAST to varchar). xref_team.source_id is the *team name* (e.g.
-- 'Manchester City') — name-based join surface across all 8 sources.
-- The two are NOT directly joinable.
--
-- Bridge: bronze.whoscored_events has BOTH the numeric `team_id` and the
-- string `team` columns on every row, so we build a (game_id, team_id) →
-- team_name mapping in a CTE and JOIN through that. Per-game uniqueness
-- of the pair (a fixture has exactly 2 teams, never re-aliased) makes
-- this a 1:1 bridge.
--
-- Why not bake the team_name into silver? Silver is a pure normaliser
-- (R3.D scope) — cross-source xref join is a Gold-job concern (silver
-- file header line 173-174 is explicit about this).
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
-- player_id_canonical IS NULL only when:
--   (a) silver event has player_id_raw IS NULL (substitutions / meta /
--       team-only events — kept by R3.D5 row-count parity); or
--   (b) xref_player has not been re-materialised after a new bronze
--       player appeared (timing race — DQ in E3.8 surfaces this as
--       orphan rate; target ≤7%).
--
-- Both cases are tolerated; downstream ML can filter on
-- player_id_canonical IS NOT NULL when needed.
--
-- ADR-4: NO row dropping (R3.D5)
-- ------------------------------
-- WHERE filters are forbidden in this CTAS. Silver writes one row per
-- bronze event (including action_canonical='unknown' meta/marker rows).
-- Dropping any row here breaks the bronze→silver→gold row-count parity
-- audit. Consumers filter unknown via WHERE action_canonical != 'unknown'
-- in their own queries.
--
-- ADR-5: dedup of bronze bridge
-- -----------------------------
-- The bronze.whoscored_events bridge CTE GROUP BYs on (game_id, team_id)
-- to collapse multi-event rows of the same team in the same fixture into
-- a single row. MAX(team) is safe because Opta does not re-name a team
-- mid-fixture (verified — silver/whoscored_events_spadl preamble notes
-- 100% PK uniqueness on 15-col natural key).
--
-- =============================================================================

WITH event_team_names AS (
    -- ------- ADR-2 bridge: numeric Opta team_id -> team name -------
    -- One row per (game_id, team_id) pair. ~2 rows per fixture.
    -- This CTE is small (~760 rows for APL 2425 — 380 fixtures × 2 teams)
    -- so the downstream JOIN cost is negligible.
    --
    -- bronze.whoscored_events.team_id and game_id are DOUBLE; direct
    -- CAST AS varchar yields scientific notation ('9.5408E4') which
    -- breaks string equality against the silver-side team_id_raw
    -- (silver casts via BIGINT first to keep digit form '95408').
    -- Mirror that double-cast here so the JOIN below resolves.
    SELECT
        CAST(CAST(game_id AS BIGINT) AS varchar)  AS match_id,
        CAST(CAST(team_id AS BIGINT) AS varchar)  AS team_id_raw,
        MAX(team)                                 AS team_name_raw,
        league,
        season
    FROM iceberg.bronze.whoscored_events
    WHERE team_id IS NOT NULL
      AND team    IS NOT NULL
    GROUP BY
        CAST(CAST(game_id AS BIGINT) AS varchar),
        CAST(CAST(team_id AS BIGINT) AS varchar),
        league,
        season
)

SELECT
    -- ============================================================
    -- match identity (R0.4 canonical / source / version triplet)
    -- ============================================================
    -- ADR-1: bridging to FBref match_id deferred to Phase B; emit
    -- raw whoscored game_id with explicit unbridged version label
    -- so downstream consumers can detect and migrate atomically.
    e.match_id                                   AS match_id_canonical,
    CAST('whoscored_raw'   AS varchar)           AS match_id_source,
    CAST('v0_unbridged'    AS varchar)           AS match_id_version,

    -- ============================================================
    -- event_id passthrough (silver guarantees PK uniqueness)
    -- ============================================================
    e.event_id                                   AS event_id,

    -- ============================================================
    -- team identity — resolved via bronze name bridge + xref_team
    -- ============================================================
    -- xt.canonical_id is NULL when:
    --   * team_name_raw missing in bronze bridge (defensive — should
    --     not happen on validated bronze); OR
    --   * xref_team has no row for (whoscored, team_name) — alias
    --     YAML drift; DQ in E3.8 catches this.
    xt.canonical_id                              AS team_id_canonical,
    CAST('whoscored' AS varchar)                 AS team_id_source,
    CAST('v1'        AS varchar)                 AS team_id_version,

    -- ============================================================
    -- player identity — orphan-tolerant LEFT JOIN on xref_player
    -- ============================================================
    -- ADR-3: player_id_canonical is non-null when player_id_raw
    -- exists in xref_player (orphan rows have ws_<sid> canonical,
    -- so non-resolved players still get a stable key downstream).
    -- Genuinely NULL only for player_id_raw IS NULL events (subs /
    -- markers / meta — R3.D5 row-count parity).
    xp.canonical_id                              AS player_id_canonical,
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
    e.action_canonical                           AS action_canonical,
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

-- ---- bronze name bridge — per-fixture team_id -> team_name ----
LEFT JOIN event_team_names etn
    ON etn.match_id    = e.match_id
   AND etn.team_id_raw = e.team_id_raw
   AND etn.league      = e.league
   AND etn.season      = e.season

-- ---- team xref (whoscored team-name -> canonical) ----
LEFT JOIN iceberg.silver.xref_team xt
    ON xt.source    = 'whoscored'
   AND xt.source_id = etn.team_name_raw
   AND xt.league    = e.league
   AND xt.season    = e.season

-- ---- player xref (whoscored numeric id -> canonical or ws_ orphan) ----
-- xref_player has one row per (source, source_id, season) — the resolver
-- writes a row per season to capture mid-career club moves. Without the
-- season predicate, a player who played 2425+2526 fans the JOIN out 2x.
LEFT JOIN iceberg.silver.xref_player xp
    ON xp.source    = 'whoscored'
   AND xp.source_id = e.player_id_raw
   AND xp.league    = e.league
   AND xp.season    = e.season
