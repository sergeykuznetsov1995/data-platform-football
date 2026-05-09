-- =============================================================================
-- Gold: fct_substitution
-- =============================================================================
-- Substitution-grained narrow fact (one row per swap). Pure passthrough from
-- `iceberg.silver.match_substitutions` with canonical-trio columns appended.
--
-- Sources:
--   iceberg.silver.match_substitutions   (E4.2 deliverable — FBref+WhoScored
--                                         unified, FBref-priority dedup,
--                                         ~25,615 rows on the 5-season APL
--                                         corpus)
--
-- DAG-integration note: T4 wraps this SELECT in
-- `CREATE TABLE iceberg.gold.fct_substitution AS ... WITH
-- (partitioning=ARRAY['league','season'])` via `gold_tasks.run_gold_transform()`.
-- This file MUST stay a pure SELECT (no CREATE TABLE, no DDL).
--
-- =============================================================================
-- Output schema (frozen for E4 wave-1)
-- =============================================================================
--   match_id_canonical          varchar     resolved match id (FBref hex when
--                                           bridged; 'whoscored_raw_<game_id>'
--                                           fallback)
--   team_id_canonical           varchar     via xref_team — orphan-tolerant
--   player_in_canonical         varchar     player coming ON  (xref_player)
--   player_out_canonical        varchar     player going OFF  (xref_player)
--   minute                      integer     event minute
--   substitution_canonical      varchar     xxhash64 synthetic PK
--   substitution_source         varchar     'fbref' | 'whoscored'
--   substitution_version        varchar     literal 'v1'
--   league                      varchar     partition key
--   season                      bigint      partition key (silver compact
--                                           'YYYY' → bigint year-of-start)
--   _ingested_at                timestamp(6)  bronze provenance
--
-- Logical PK: substitution_canonical
--   xxhash64 over (match || minute || player_in || player_out). NULL-canonical
--   players are COALESCE'd to '?' so distinct unresolved swaps in the same
--   minute do not collide. Mirrors silver.match_substitutions PK semantics.
--
-- =============================================================================
-- Season normalisation: see header in fct_card.sql — same compact-YYYY parser.
-- =============================================================================

WITH typed AS (
    SELECT
        s.match_id_canonical,
        s.team_id_canonical,
        s.player_in_canonical,
        s.player_out_canonical,
        s.minute,
        s.source            AS substitution_source,
        s.league,
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
    FROM iceberg.silver.match_substitutions s
)

SELECT
    match_id_canonical,
    team_id_canonical,
    player_in_canonical,
    player_out_canonical,
    minute,

    -- ============================================================
    -- canonical-trio: synthetic PK + provenance
    -- ============================================================
    -- COALESCE on team+players + ROW_NUMBER tiebreaker: two FBref subs at the
    -- same minute from different teams with NULL player_in/_out canonicals
    -- (silver corner case ~1.8%) would otherwise collapse to the same hash.
    lower(to_hex(xxhash64(to_utf8(
        match_id_canonical
        || '|' || COALESCE(team_id_canonical, '?')
        || '|' || CAST(minute AS varchar)
        || '|' || COALESCE(player_in_canonical,  '?')
        || '|' || COALESCE(player_out_canonical, '?')
        || '|' || CAST(ROW_NUMBER() OVER (
              PARTITION BY match_id_canonical,
                           COALESCE(team_id_canonical, '?'),
                           minute,
                           COALESCE(player_in_canonical,  '?'),
                           COALESCE(player_out_canonical, '?')
              ORDER BY substitution_source
          ) AS varchar)
    ))))                                     AS substitution_canonical,
    substitution_source,
    CAST('v1' AS varchar)                    AS substitution_version,

    league,
    season,
    _ingested_at
FROM typed
