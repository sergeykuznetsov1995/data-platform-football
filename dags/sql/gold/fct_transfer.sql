-- =============================================================================
-- Gold: fct_transfer  (player transfers, issue #429)
-- =============================================================================
-- One row per player transfer event. Pure projection of
-- silver.transfermarkt_transfers — NO JOINs — so the gold row count must
-- exactly equal the silver row count (DoD: rows are never dropped).
--
-- Orphan-ID strategy (design §5.6: «оставляем строки с orphan-ID, не
-- выбрасываем»): canonical ids are NULL for ≈18% of players (U21 / loan-out /
-- new signings not in the FBref spine) and for most clubs (from ≈18%,
-- to ≈31% resolved — TM club names are resolved via team_aliases only, and
-- non-APL clubs have no alias). NULLs in PK columns would collide two
-- same-day moves of one unresolved player, so every PK column falls back to
-- a 'tm_'-prefixed deterministic id:
--
--   player_id    = COALESCE(canonical_id, 'tm_' || player_id)        -- raw TM id
--   from_team_id = COALESCE(from_club_id_canonical, 'tm_' || slug(from_club_name))
--   to_team_id   = COALESCE(to_club_id_canonical,   'tm_' || slug(to_club_name))
--
-- slug() = LOWER(REGEXP_REPLACE(name, '[^a-zA-Z0-9]+', '_')) — the same
-- formula team_aliases uses for canonical ids, so RESOLVED clubs match
-- dim_team.team_id exactly; orphans are recognizable by the 'tm_' prefix
-- (precedent: 'whoscored_raw_<id>' in fct_match_timeline). Soft FK checks
-- to dim_player / dim_team are therefore WARNING-severity.
--
-- is_loan covers all loan-related movements: going on loan ('loan transfer',
-- 'Loan fee: €X') AND returning from one ('End of loan').
-- market_value_at_transfer_eur = player's TM market value at transfer time.
--
-- PK:           (player_id, transfer_date, from_team_id, to_team_id)
-- FK:           player_id → dim_player (soft), from/to_team_id → dim_team (soft)
-- Partitioning: NONE  (small table, ~750 rows for APL '2526')
-- =============================================================================

SELECT
    COALESCE(t.canonical_id, 'tm_' || t.player_id)        AS player_id,
    t.transfer_date,
    COALESCE(
        t.from_club_id_canonical,
        'tm_' || LOWER(REGEXP_REPLACE(t.from_club_name, '[^a-zA-Z0-9]+', '_'))
    )                                                     AS from_team_id,
    COALESCE(
        t.to_club_id_canonical,
        'tm_' || LOWER(REGEXP_REPLACE(t.to_club_name, '[^a-zA-Z0-9]+', '_'))
    )                                                     AS to_team_id,
    t.from_club_name,
    t.to_club_name,
    t.fee_eur,
    t.market_value_eur                                    AS market_value_at_transfer_eur,
    t.is_loan,
    t.is_upcoming,
    t._bronze_ingested_at,
    t.league,
    t.season
FROM iceberg.silver.transfermarkt_transfers t
