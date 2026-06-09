-- =============================================================================
-- Silver: espn_lineup
-- =============================================================================
--
-- Per-player lineup entries from ESPN matchsheet API (via soccerdata.ESPN).
--
-- Source:
--   iceberg.bronze.espn_lineup
--
-- Target consumer:
--   gold/fct_lineup (E3.5) — UNION ALL with silver.fbref_match_lineups.
--   Output schema matches silver.fbref_match_lineups column-for-column, EXCEPT
--   ``season``: here it is a varchar slug ('2526'), while fbref Silver carries
--   bigint year-start. fct_lineup reconciles the two per-branch (each CTE casts
--   season to a unified 4-char varchar), so the UNION stays type-aligned at Gold.
--
-- =============================================================================
-- Bronze column reference (soccerdata.ESPN.read_lineup output, post _add_metadata)
-- =============================================================================
--   league            varchar   -- e.g. 'ENG-Premier League'
--   season            varchar   -- e.g. '2526' (soccerdata 2-digit slug form)
--   game              varchar   -- ESPN match display ("YYYY-MM-DD Home-Away") — no native game_id in lineup df
--   team              varchar   -- ESPN team displayName (post TEAMNAME_REPLACEMENTS)
--   player            varchar   -- player displayName
--   is_home           boolean
--   position          varchar   -- ESPN position name (raw, not canonical)
--   formation_place   varchar   -- '1'..'11' starter slot, '0' for bench (NEVER NULL —
--                                  do NOT use as is_starter signal)
--   sub_in            varchar   -- 'start' | minute (string-cast in Bronze) | NULL
--   sub_out           varchar   -- 'end'   | minute (string-cast in Bronze) | NULL
--   ...dynamic stats columns from ESPN boxscore (ignored here)
--   _ingested_at      timestamp -- standard lineage column
--   _source           varchar
--   _entity_type      varchar
--   _batch_id         varchar
--
-- =============================================================================
-- Schema parity with silver.fbref_match_lineups (target — see file)
-- =============================================================================
--   match_id              varchar    -- ESPN has no FBref hex → derive 'espn_<xxhash64>'
--                                       from (league, season, game). xref_match Phase B
--                                       will bridge espn_* → fbref hex via fuzzy
--                                       (date, home_canonical_id, away_canonical_id).
--   team                  varchar    -- direct
--   player                varchar    -- direct (FBref column name preserved)
--   player_id             varchar    -- ESPN has NO native player_id → NULL.
--                                       xref_player resolves by (name, team) for ESPN
--                                       in a follow-up; fct_lineup carries 'es_<hash>'
--                                       orphan id assigned downstream if unresolved.
--   is_starter            boolean    -- derived ONLY from sub_in = 'start'.
--                                       formation_place is NOT a usable fallback
--                                       (bench rows have '0', not NULL).
--   position              varchar    -- raw ESPN position (no canonicalisation in MVP —
--                                       FBref Silver also keeps raw position; alignment
--                                       deferred to Gold dim_position if ever needed)
--   jersey_number         integer    -- ESPN matchsheet has NO jersey number → NULL
--   _bronze_ingested_at   timestamp  -- direct
--   league                varchar    -- partition key
--   season                varchar    -- partition key. ESPN Bronze stores season as
--                                       VARCHAR slug ('2526'); kept as varchar slug
--                                       here (charter S2). The slug↔year-start
--                                       reconciliation with FBref happens at Gold
--                                       (fct_lineup), not in Silver.
--
-- =============================================================================
-- Deduplication
-- =============================================================================
--   ESPN lineup grain is (game, team, player). We synthesise match_id from
--   (league, season, game), so dedup key is (match_id, player) — but player is a
--   display name, NOT a stable id. To match the FBref dedup contract (one row
--   per match_id × player_id) we use (match_id, team, player) inside ROW_NUMBER
--   to avoid collapsing namesake players across teams. Surfaced column
--   `player_id` is set to NULL (resolver runs later).
--
-- =============================================================================
-- DAG-integration note
-- =============================================================================
--   silver_tasks.run_silver_transform() wraps this SELECT in
--   `CREATE TABLE iceberg.silver.espn_lineup AS ...` with partitioning by
--   (league, season). This file MUST stay a pure SELECT.
--
--   If iceberg.bronze.espn_lineup does not yet exist (R0.2c FALLBACK is still
--   in scoping), the DAG task will fail at planning time — that is the
--   intended behaviour: missing Bronze should surface as a hard failure, not
--   a silent empty-Silver. Wire-up of the ESPN lineup ingestion is tracked
--   separately in R0.2c / E3.7.
-- =============================================================================

WITH src AS (
    SELECT
        league,
        CAST(season AS varchar)              AS season,
        game,
        team,
        player,
        position,
        sub_in,
        _ingested_at,

        -- Deterministic match_id derivation. Same xxhash64-hex pattern used
        -- by xref_match.sql for future fixtures, prefixed 'espn_' so xref_match
        -- Phase B can filter ESPN-origin pseudo-ids and replace them with the
        -- canonical FBref hex when fuzzy bridging succeeds.
        'espn_' || LOWER(TO_HEX(XXHASH64(TO_UTF8(
            COALESCE(league, '')
            || '|' || COALESCE(CAST(season AS varchar), '')
            || '|' || COALESCE(game, '')
        ))))                                  AS match_id,

        ROW_NUMBER() OVER (
            PARTITION BY
                'espn_' || LOWER(TO_HEX(XXHASH64(TO_UTF8(
                    COALESCE(league, '')
                    || '|' || COALESCE(CAST(season AS varchar), '')
                    || '|' || COALESCE(game, '')
                )))),
                team,
                player
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.espn_lineup
)

SELECT
    match_id,
    team,
    player,
    CAST(NULL AS varchar)                     AS player_id,

    -- ========= is_starter =========
    -- ESPN: sub_in = 'start' is the authoritative starter marker (8338 rows
    -- across 380 matches in 2526 ≈ 22 starters / match — matches reality).
    -- formation_place is NOT a usable fallback — it is populated for ALL
    -- players: starters get '1'..'11', bench get '0'. So a non-NULL
    -- formation_place tells us nothing about starting status.
    -- COALESCE(..., FALSE): some bench rows have sub_in IS NULL (player
    -- never came on as substitute); treat as not-a-starter so the boolean
    -- column has zero NULLs (DQ no_nulls is ERROR-severity).
    COALESCE(sub_in = 'start', FALSE)         AS is_starter,

    position,
    CAST(NULL AS integer)                     AS jersey_number,

    -- ========= Lineage =========
    _ingested_at                              AS _bronze_ingested_at,

    -- ========= Partition Keys =========
    league,
    season

FROM src
WHERE rn = 1
