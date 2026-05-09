-- =============================================================================
-- Gold: fct_card
-- =============================================================================
-- Card-grained narrow fact (yellow / red / second_yellow). Pure passthrough
-- from `iceberg.silver.match_cards` with the standard canonical-trio columns
-- (`<entity>_canonical`, `<entity>_source`, `<entity>_version`) appended.
--
-- Sources:
--   iceberg.silver.match_cards   (E4.2 deliverable — FBref+WhoScored unified,
--                                 FBref-priority dedup, ~13,608 rows on the
--                                 5-season APL corpus)
--
-- DAG-integration note: T4 wraps this SELECT in
-- `CREATE TABLE iceberg.gold.fct_card AS ... WITH (partitioning=ARRAY['league','season'])`
-- via `gold_tasks.run_gold_transform()`. This file MUST stay a pure SELECT
-- (no CREATE TABLE, no DDL). `_silver_created_at` lineage column is appended
-- by the gold_tasks wrapper — do NOT add it here.
--
-- =============================================================================
-- Output schema (frozen for E4 wave-1)
-- =============================================================================
--   match_id_canonical    varchar     resolved match id (FBref hex when bridged;
--                                     'whoscored_raw_<game_id>' fallback)
--   team_id_canonical     varchar     via xref_team — orphan-tolerant (NULL ok)
--   player_id_canonical   varchar     via xref_player — orphan-tolerant (NULL ok)
--   minute                integer     event minute
--   card_type             varchar     'yellow' | 'red' | 'second_yellow'
--   card_canonical        varchar     xxhash64-derived synthetic PK (varchar)
--   card_source           varchar     'fbref' | 'whoscored' (silver.source pass-thru)
--   card_version          varchar     literal 'v1'
--   league                varchar     partition key
--   season                bigint      partition key (bigint year-of-start;
--                                     silver stores compact 'YYYY' varchar →
--                                     parsed via 2000 + first-2-digits)
--   _ingested_at          timestamp(6)  bronze provenance (silver pass-thru)
--
-- Logical PK: card_canonical
--   Built from xxhash64 over the natural cross-source dedup key
--   (match || minute || player_canon || card_type). For NULL-canonical
--   players (rare; see silver.match_cards header §"Primary key relaxation")
--   we COALESCE the player slot with `'?'` so distinct NULL-rows don't
--   collide on the same minute/match/card_type — DQ in E4.10 surfaces such
--   collisions, but the relaxation matches silver behaviour 1:1.
--
-- =============================================================================
-- Season normalisation: silver compact 'YYYY' → bigint year-of-start
-- =============================================================================
-- silver.match_cards.season is the unified compact 4-char form ('2425' for
-- the 2024-25 season — see silver/match_cards.sql R4 unification block).
-- gold.dim_match.season is bigint year-of-start (2024). This file aligns to
-- dim_match: `2000 + CAST(SUBSTR(season, 1, 2) AS bigint)` parses the first
-- two digits as the start-year offset century-2000. Defensive TRY_CAST so a
-- malformed season (legacy single-year '2021' from earlier ingest) still
-- yields a usable value via fallback.
-- =============================================================================

WITH typed AS (
    SELECT
        s.match_id_canonical,
        s.team_id_canonical,
        s.player_id_canonical,
        s.minute,
        s.card_type,
        s.source            AS card_source,
        s.league,
        -- Compact 'YYYY' → bigint year-of-start. Cards silver only emits the
        -- compact form, but the COALESCE branch keeps the path tolerant of a
        -- legacy '2021'-style row that may sneak in during backfills.
        COALESCE(
            CASE
                WHEN length(s.season) = 4
                 AND TRY_CAST(s.season AS bigint) BETWEEN 2000 AND 2100
                    THEN TRY_CAST(s.season AS bigint)
                ELSE 2000 + TRY_CAST(substr(s.season, 1, 2) AS bigint)
            END,
            TRY_CAST(s.season AS bigint)
        )                   AS season,
        s._ingested_at
    FROM iceberg.silver.match_cards s
)

SELECT
    match_id_canonical,
    team_id_canonical,
    player_id_canonical,
    minute,
    card_type,

    -- ============================================================
    -- canonical-trio: synthetic PK + provenance
    -- ============================================================
    -- xxhash64 returns varbinary; lower(to_hex(...)) gives a stable 16-char
    -- hex string (mirrors the pattern used downstream in audit/lineage).
    -- COALESCE on the player+team slots guarantees a deterministic key when
    -- canonical IDs are NULL (silver corner case ~0.4%). ROW_NUMBER tiebreaker
    -- is required because two yellow cards on the same (match, team, minute)
    -- with NULL player_id_canonical are legitimately distinct events
    -- (e.g. multiple cards in a 90+x stoppage scuffle) — without the seq
    -- they would collapse to identical hashes and trip no_duplicates DQ.
    lower(to_hex(xxhash64(to_utf8(
        match_id_canonical
        || '|' || COALESCE(team_id_canonical, '?')
        || '|' || CAST(minute AS varchar)
        || '|' || COALESCE(player_id_canonical, '?')
        || '|' || card_type
        || '|' || CAST(ROW_NUMBER() OVER (
              PARTITION BY match_id_canonical,
                           COALESCE(team_id_canonical, '?'),
                           minute,
                           COALESCE(player_id_canonical, '?'),
                           card_type
              ORDER BY card_source
          ) AS varchar)
    ))))                                     AS card_canonical,
    card_source,
    CAST('v1' AS varchar)                    AS card_version,

    league,
    season,
    _ingested_at
FROM typed
