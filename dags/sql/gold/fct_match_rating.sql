-- =============================================================================
-- Gold: fct_match_rating
-- =============================================================================
-- Per-player match rating fact (one row per match × player × team_side).
-- Pure passthrough from `iceberg.silver.sofascore_player_ratings` with
-- canonical-trio columns appended.
--
-- Sources:
--   iceberg.silver.sofascore_player_ratings   (E4.4 deliverable — typed +
--                                              dedup + xref_player canonical
--                                              + sched_bridge to FBref hex.
--                                              Smoke run on APL 2526 = 200
--                                              bronze rows.)
--
-- E4.1 (web-scraping) shipped the FULL bronze ingestion path (no schema-stub
-- fallback), so this CTAS reads real data. If a future ingest run starts
-- failing, the OPTIONAL_BRONZE_TABLES gate in silver_tasks already protects
-- the silver layer; gold here will simply yield 0 rows for empty silver.
--
-- DAG-integration note: T4 wraps this SELECT in
-- `CREATE TABLE iceberg.gold.fct_match_rating AS ... WITH
-- (partitioning=ARRAY['league','season'])` via `gold_tasks.run_gold_transform()`.
-- This file MUST stay a pure SELECT (no CREATE TABLE, no DDL).
--
-- =============================================================================
-- Output schema (frozen for E4 wave-1)
-- =============================================================================
--   match_id              varchar         FBref hex when bridged; else
--                                         'sofascore_<raw>' v0_unbridged
--   player_id             varchar         xref_player canonical; else 'ss_<raw>'
--   team_side             varchar         'home' | 'away'
--   rating                decimal(3,1)    valid range 0.1–10.0 (silver drops 0.0)
--   position              varchar         passthrough (no canonicalisation MVP)
--   rating_id             varchar         xxhash64 synthetic PK
--   rating_source         varchar         literal 'sofascore'
--   rating_version        varchar         literal 'v1'
--   league                varchar         partition key
--   season                varchar         partition key (silver varchar slug '2526')
--   _ingested_at          timestamp(6)    bronze provenance
--
-- Logical PK: rating_id
--   xxhash64 over (match || player || team_side). team_side is part of the
--   key because a player can theoretically appear on both sides of an
--   inter-club friendly (extremely rare in APL — guard kept for hygiene).
--
-- =============================================================================
-- Season type
-- =============================================================================
-- silver.sofascore_player_ratings.season is a varchar slug ('2526'), per charter
-- S2 (see silver/sofascore_player_ratings.sql final SELECT block). Direct
-- passthrough — the Gold column is varchar slug too.
-- =============================================================================

SELECT
    s.match_id_canonical                     AS match_id,
    s.player_id_canonical                    AS player_id,
    s.team_side,
    s.rating,
    s.position,

    -- ============================================================
    -- canonical-trio: synthetic PK + provenance
    -- ============================================================
    lower(to_hex(xxhash64(to_utf8(
        s.match_id_canonical
        || '|' || s.player_id_canonical
        || '|' || s.team_side
    ))))                                     AS rating_id,
    CAST('sofascore' AS varchar)             AS rating_source,
    CAST('v1'        AS varchar)             AS rating_version,

    s.league,
    s.season,
    s._ingested_at
FROM iceberg.silver.sofascore_player_ratings s
