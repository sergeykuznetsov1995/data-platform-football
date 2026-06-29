-- =============================================================================
-- Gold: fct_player_salary  (player wages, issue #430)
-- =============================================================================
-- One row per (player_id, league, season). Pure projection of
-- silver.capology_player_salaries — canonical_id is already resolved in Silver
-- (LEFT JOIN xref_player, source='capology'), so there is NO xref hop here
-- (mirrors fct_transfer reading silver.transfermarkt_transfers directly).
--
-- Orphan-ID strategy (design §6 rule 2 — rows are never dropped): canonical_id
-- is NULL for ≈9.5% of Capology players. Capology is a contract roster snapshot;
-- FBref is appearance data, so suspended / injured / backup-GK / youth /
-- loan-out players exist in Capology with no FBref counterpart — a STRUCTURAL
-- floor, unjoinable by any resolver. Every such row keeps a deterministic
-- 'cap_'-prefixed id instead of a NULL PK component:
--
--   player_id = COALESCE(canonical_id, 'cap_' || player_slug)
--
-- 'cap_' mirrors xref_player_resolver._orphan_prefix. The soft FK to dim_player
-- is therefore WARNING-severity (rate-mode), not ERROR.
--
-- Coverage: APL only (Capology ingest scope). All three salary currencies
-- (GBP/EUR/USD) arrive inline as wide columns from the single Silver row
-- (issue #195) — no currency partition.
--
-- PK:           (player_id, league, season)
-- FK:           player_id -> dim_player (soft, WARNING rate-mode)
-- Partitioning: (league, season)
-- =============================================================================

SELECT
    COALESCE(s.canonical_id, 'cap_' || s.player_slug)    AS player_id,
    s.player_name,
    s.club_name,
    s.weekly_gross_eur,
    s.annual_gross_eur,
    s.weekly_gross_gbp,
    s.annual_gross_gbp,
    s.weekly_gross_usd,
    s.annual_gross_usd,
    s.status                                             AS contract_status,
    s.verified                                           AS is_verified,
    s._bronze_ingested_at,
    s.league,
    s.season
FROM iceberg.silver.capology_player_salaries s
