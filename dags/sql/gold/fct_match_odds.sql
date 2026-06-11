-- =============================================================================
-- Gold: fct_match_odds
-- =============================================================================
-- Pre-match betting odds, tall format (one row per bookmaker × market ×
-- closing_flag). Pure passthrough from `iceberg.silver.matchhistory_match_odds`
-- with canonical-trio columns appended.
--
-- Sources:
--   iceberg.silver.matchhistory_match_odds   (E4.3 deliverable — bridged
--                                             football-data.co.uk → FBref hex
--                                             via inline team_aliases CTE,
--                                             ~47,013 rows on the 5-season
--                                             APL corpus)
--
-- DAG-integration note: T4 wraps this SELECT in
-- `CREATE TABLE iceberg.gold.fct_match_odds AS ... WITH
-- (partitioning=ARRAY['league','season'])` via `gold_tasks.run_gold_transform()`.
-- This file MUST stay a pure SELECT (no CREATE TABLE, no DDL).
--
-- =============================================================================
-- Output schema (frozen — star design §4.7, issue #426)
-- =============================================================================
--   match_id              varchar          == gold.dim_match.match_id
--   bookmaker             varchar          'B365' | 'PS' | 'WH' | 'VC' | 'IW'
--                                          | 'BW' | 'AVG' | 'MAX'
--   market                varchar          '1x2' | 'ah' | 'ou_2_5'
--   odds_home             decimal(6,3)     1x2.home OR ah.home OR ou.over
--   odds_draw             decimal(6,3)     1x2.draw; NULL for ah / ou_2_5
--   odds_away             decimal(6,3)     1x2.away OR ah.away OR ou.under
--   ah_line               decimal(4,2)     handicap line; NULL outside 'ah'
--   ou_line               decimal(4,2)     O/U line (=2.5); NULL outside 'ou_2_5'
--   is_closing            boolean          TRUE = closing-line odds
--   odds_canonical        varchar          xxhash64 synthetic PK
--   odds_source           varchar          'matchhistory'
--   odds_version          varchar          literal 'v1'
--   league                varchar          partition key
--   season                varchar          partition key (silver passthrough,
--                                          slug '2425' after #404)
--   _ingested_at          timestamp(6)     bronze provenance
--
-- PK: (match_id, bookmaker, market, is_closing); odds_canonical is the same
--   quadruple hashed (xxhash64) — hash input unchanged across #426 renames.
--   One row per quadruple is the silver invariant — PK uniqueness in gold
--   falls out for free.
--
-- =============================================================================
-- Season type
-- =============================================================================
-- silver.matchhistory_match_odds.season is slug varchar ('2425') after #404.
-- Direct passthrough — no cast needed.
-- =============================================================================

SELECT
    s.match_id_canonical                     AS match_id,
    s.bookmaker_code                         AS bookmaker,
    s.market,
    s.odds_h                                 AS odds_home,
    s.odds_d                                 AS odds_draw,
    s.odds_a                                 AS odds_away,
    s.ah_handicap                            AS ah_line,
    s.ou_line,
    s.closing_flag                           AS is_closing,

    -- ============================================================
    -- canonical-trio: synthetic PK + provenance
    -- ============================================================
    -- closing_flag is boolean → CAST to varchar so the hash input is
    -- deterministic regardless of locale/format.
    lower(to_hex(xxhash64(to_utf8(
        s.match_id_canonical
        || '|' || s.bookmaker_code
        || '|' || s.market
        || '|' || CAST(s.closing_flag AS varchar)
    ))))                                     AS odds_canonical,
    CAST('matchhistory' AS varchar)          AS odds_source,
    CAST('v1'           AS varchar)          AS odds_version,

    s.league,
    s.season,
    s._ingested_at
FROM iceberg.silver.matchhistory_match_odds s
