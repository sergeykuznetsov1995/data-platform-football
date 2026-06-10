-- =============================================================================
-- Gold: fct_card
-- =============================================================================
-- Card-grained narrow fact (yellow / red / second_yellow). Cross-source
-- assembly (FBref primary + WhoScored fallback union, FBref-priority dedup)
-- is folded directly into this Gold fact — it reads bronze + xref straight
-- (#382, prior intermediate `silver.match_cards` removed). The standard
-- canonical-trio columns (`<entity>_canonical`, `<entity>_source`,
-- `<entity>_version`) are appended on top.
--
-- Sources:
--   iceberg.bronze.fbref_match_events       — primary (event_type ∈ card set)
--   iceberg.bronze.whoscored_events         — fallback (type='Card', qualifiers
--                                              JSON carries Yellow/Red/SecondYellow)
--   iceberg.bronze.whoscored_schedule       — bridge spine: game_id → date /
--                                              home_team / away_team for the
--                                              WhoScored→FBref match_id resolve
--   iceberg.silver.fbref_match_enriched     — bridge target: FBref hex match_id
--                                              keyed on (league, date, home, away)
--   iceberg.silver.xref_match               — FBref-only canonical id lookup
--   iceberg.silver.xref_team                — team-name canonicalisation
--   iceberg.silver.xref_player              — player canonical resolver
--   (~13,608 rows on the 5-season APL corpus)
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
--   players (rare ~0.4%; xref_player out-of-scope seasons leave the slot NULL)
--   we COALESCE the player slot with `'?'` so distinct NULL-rows don't
--   collide on the same minute/match/card_type — DQ in E4.10 surfaces such
--   collisions. ROW_NUMBER tiebreaker keeps the folded dedup behaviour 1:1.
--
-- =============================================================================
-- Season normalisation: compact 'YYYY' → bigint year-of-start
-- =============================================================================
-- The folded `fb_cards` CTE emits the unified compact 4-char form ('2425' for
-- the 2024-25 season; FBref bigint year-of-start → compact via format()).
-- gold.dim_match.season is bigint year-of-start (2024). This file aligns to
-- dim_match: `2000 + CAST(SUBSTR(season, 1, 2) AS bigint)` parses the first
-- two digits as the start-year offset century-2000. Defensive TRY_CAST so a
-- malformed season (legacy single-year '2021' from earlier ingest) still
-- yields a usable value via fallback.
-- =============================================================================

WITH
-- ============================================================================
-- Cross-source assembly (folded from former silver.match_cards — #382)
-- ============================================================================
-- 1) WhoScored event-side dedup (bronze re-scrape duplicates)
ws_events_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY game_id, period, minute, second, type,
                            team_id, player_id, qualifiers
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.whoscored_events
    WHERE type = 'Card'
),

-- 2) FBref event-side dedup (bronze re-scrape duplicates)
fb_events_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY match_id, minute, player_id, event_type
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fbref_match_events
    WHERE event_type IN ('yellow_card', 'red_card', 'second_yellow_card')
),

-- 3) WhoScored schedule dedup (provides bridge spine)
ws_schedule_dedup AS (
    SELECT
        game_id,
        date,
        home_team,
        away_team,
        league,
        season,
        ROW_NUMBER() OVER (
            PARTITION BY game_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.whoscored_schedule
),

-- 4) WhoScored → FBref match_id bridge
-- Pre-aggregate distinct (canonical_id, source_id) pairs WITHIN a league to
-- avoid season-format mismatch — xref_team passes native season per source
-- (ws='2425', fb='2024'), so dropping season here is intentional. Cross-league
-- ambiguity is impossible because fbref_match_enriched re-applies league.
xref_team_canonical AS (
    SELECT DISTINCT source, source_id, canonical_id, league
    FROM iceberg.silver.xref_team
    WHERE canonical_id IS NOT NULL
),

ws_match_bridge AS (
    SELECT
        CAST(s.game_id AS varchar)               AS ws_game_id,   -- BIGINT → varchar
        s.league                                 AS league,
        s.season                                 AS ws_season,    -- '2425' native
        CAST(s.date AS date)                     AS match_date,
        fme.match_id                             AS fbref_match_id
    FROM ws_schedule_dedup s
    LEFT JOIN xref_team_canonical xt_home_ws
        ON xt_home_ws.source    = 'whoscored'
       AND xt_home_ws.source_id = s.home_team
       AND xt_home_ws.league    = s.league
    LEFT JOIN xref_team_canonical xt_away_ws
        ON xt_away_ws.source    = 'whoscored'
       AND xt_away_ws.source_id = s.away_team
       AND xt_away_ws.league    = s.league
    LEFT JOIN xref_team_canonical xt_home_fb
        ON xt_home_fb.source       = 'fbref'
       AND xt_home_fb.canonical_id = xt_home_ws.canonical_id
       AND xt_home_fb.league       = s.league
    LEFT JOIN xref_team_canonical xt_away_fb
        ON xt_away_fb.source       = 'fbref'
       AND xt_away_fb.canonical_id = xt_away_ws.canonical_id
       AND xt_away_fb.league       = s.league
    LEFT JOIN iceberg.silver.fbref_match_enriched fme
        ON fme.league = s.league
       AND fme.home   = xt_home_fb.source_id
       AND fme.away   = xt_away_fb.source_id
       AND fme.date   = CAST(s.date AS date)
    WHERE s.rn = 1
),

-- 5) FBref normalised cards — full APL coverage (primary)
-- season UNIFICATION (mirrors fct_lineup R4 pattern): FBref bigint year-of-
-- start (2024) → compact 'YYYY' varchar ('2425') so output matches WhoScored
-- branch and is comparable across narrow facts.
fb_cards AS (
    SELECT
        COALESCE(xm.canonical_id, fe.match_id)               AS match_id_canonical,
        xt.canonical_id                                      AS team_id_canonical,
        xp.canonical_id                                      AS player_id_canonical,
        TRY_CAST(fe.minute AS integer)                       AS minute,
        CASE fe.event_type
            WHEN 'yellow_card'         THEN 'yellow'
            WHEN 'red_card'            THEN 'red'
            WHEN 'second_yellow_card'  THEN 'second_yellow'
        END                                                  AS card_type,
        CAST('fbref' AS varchar)                             AS source,
        1                                                    AS source_priority,
        fe.league                                            AS league,
        format(
            '%02d%02d',
            mod(fe.season, 100),
            mod(fe.season + 1, 100)
        )                                                    AS season,
        fe._ingested_at                                      AS _ingested_at,
        fe.player_id                                         AS _raw_player_for_dedup
    FROM fb_events_dedup fe
    LEFT JOIN iceberg.silver.xref_match xm
        ON xm.source    = 'fbref'
       AND xm.source_id = fe.match_id
    LEFT JOIN iceberg.silver.xref_team xt
        ON xt.source    = 'fbref'
       AND xt.source_id = fe.team
       AND xt.league    = fe.league
       AND xt.season    = format('%02d%02d', mod(fe.season, 100), mod(fe.season + 1, 100))  -- #404: year-start → slug
    LEFT JOIN iceberg.silver.xref_player xp
        ON xp.source    = 'fbref'
       AND xp.source_id = fe.player_id
       AND xp.league    = fe.league
       AND xp.season    = format('%02d%02d', mod(fe.season, 100), mod(fe.season + 1, 100))  -- #404: year-start → slug
    WHERE fe.rn = 1
),

-- 6) WhoScored normalised cards — fallback (2425+2526)
ws_cards AS (
    SELECT
        COALESCE(
            mb.fbref_match_id,
            'whoscored_raw_' || CAST(CAST(we.game_id AS BIGINT) AS varchar)
        )                                                    AS match_id_canonical,
        xt.canonical_id                                      AS team_id_canonical,
        xp.canonical_id                                      AS player_id_canonical,
        TRY_CAST(we.minute AS integer)                       AS minute,
        CASE
            WHEN regexp_like(we.qualifiers, '"displayName"\s*:\s*"SecondYellow"')
                THEN 'second_yellow'
            WHEN regexp_like(we.qualifiers, '"displayName"\s*:\s*"Red"')
                THEN 'red'
            WHEN regexp_like(we.qualifiers, '"displayName"\s*:\s*"Yellow"')
                THEN 'yellow'
            ELSE NULL                                       -- card-without-colour: drop in WHERE
        END                                                  AS card_type,
        CAST('whoscored' AS varchar)                         AS source,
        2                                                    AS source_priority,
        we.league                                            AS league,
        we.season                                            AS season,        -- '2425' native varchar
        we._ingested_at                                      AS _ingested_at,
        CAST(CAST(we.player_id AS BIGINT) AS varchar)        AS _raw_player_for_dedup
    FROM ws_events_dedup we
    LEFT JOIN ws_match_bridge mb
        ON mb.ws_game_id = CAST(CAST(we.game_id AS BIGINT) AS varchar)
       AND mb.league     = we.league
       AND mb.ws_season  = we.season
    LEFT JOIN iceberg.silver.xref_team xt
        ON xt.source    = 'whoscored'
       AND xt.source_id = we.team
       AND xt.league    = we.league
       AND xt.season    = we.season
    LEFT JOIN iceberg.silver.xref_player xp
        ON xp.source    = 'whoscored'
       AND xp.source_id = CAST(CAST(we.player_id AS BIGINT) AS varchar)
       AND xp.league    = we.league
       AND xp.season    = we.season
    WHERE we.rn = 1
),

-- 7) UNION ALL + FBref-priority dedup
union_cards AS (
    SELECT * FROM fb_cards
    WHERE card_type IS NOT NULL
    UNION ALL
    SELECT * FROM ws_cards
    WHERE card_type IS NOT NULL
),

deduped AS (
    SELECT
        u.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                match_id_canonical,
                -- Player-bucket precedence: when player_id_canonical is
                -- resolved, dedup cross-source on it (FBref wins via
                -- source_priority). When unresolved (NULL), fall back to
                -- the raw native player id so two distinct unresolved
                -- players in the same team / minute are NOT collapsed.
                CASE
                    WHEN player_id_canonical IS NOT NULL
                        THEN player_id_canonical
                    ELSE source || ':'
                         || COALESCE(team_id_canonical, '?') || ':'
                         || COALESCE(_raw_player_for_dedup, '?')
                END,
                minute,
                card_type
            ORDER BY
                source_priority ASC,        -- 1=fbref wins
                _ingested_at DESC           -- freshest within source
        ) AS rn
    FROM union_cards u
),

-- Former silver.match_cards final projection (FBref-priority winner per key).
match_cards AS (
    SELECT
        match_id_canonical,
        team_id_canonical,
        player_id_canonical,
        minute,
        card_type,
        source,
        CAST('v1' AS varchar)        AS source_version,
        league,
        season,
        _ingested_at
    FROM deduped
    WHERE rn = 1
      AND match_id_canonical IS NOT NULL
      AND minute IS NOT NULL
),

typed AS (
    SELECT
        s.match_id_canonical,
        s.team_id_canonical,
        s.player_id_canonical,
        s.minute,
        s.card_type,
        s.source            AS card_source,
        s.league,
        -- #404: both branches emit slug ('2425') now → pass through unchanged.
        s.season            AS season,
        s._ingested_at
    FROM match_cards s
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
