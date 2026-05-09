-- =============================================================================
-- Silver: match_cards
-- =============================================================================
-- Card events (yellow / red / second_yellow) unified across FBref (primary,
-- full APL coverage 2016-2025) and WhoScored (fallback, 2425+2526). One row
-- per resolved (match, player, minute, card_type) — FBref wins on dedup.
--
-- Sources:
--   iceberg.bronze.fbref_match_events       — primary (event_type ∈ card set)
--   iceberg.bronze.whoscored_events         — fallback (type='Card', qualifiers
--                                              JSON carries Yellow/Red/SecondYellow)
--   iceberg.bronze.whoscored_schedule       — bridge spine: game_id → date /
--                                              home_team / away_team for the
--                                              WhoScored→FBref match_id resolve
--   iceberg.silver.fbref_match_enriched     — bridge target: provides FBref
--                                              hex match_id keyed on
--                                              (league, date, home, away)
--   iceberg.silver.xref_match               — FBref-only canonical id lookup
--   iceberg.silver.xref_team                — team-name canonicalisation
--   iceberg.silver.xref_player              — player canonical resolver
--
-- DAG-integration note: silver_tasks.run_silver_transform() wraps this SELECT
-- in `CREATE TABLE iceberg.silver.match_cards AS ... WITH (partitioning=
-- ARRAY['league','season'])`. This file MUST stay a pure SELECT (no DDL).
--
-- =============================================================================
-- Output schema (frozen for E4 wave-1)
-- =============================================================================
--   match_id_canonical    varchar     resolved match canonical (FBref hex when
--                                     bridged; 'whoscored_raw_<game_id>' fallback)
--   team_id_canonical     varchar     via xref_team — orphan-tolerant
--   player_id_canonical   varchar     via xref_player — orphan-tolerant
--   minute                integer     event minute (TRY_CAST from varchar)
--   card_type             varchar     'yellow' | 'red' | 'second_yellow'
--   source                varchar     'fbref' | 'whoscored'
--   source_version        varchar     literal 'v1'
--   league                varchar     partition key
--   season                varchar     UNIFIED 4-char 'YYYY' compact form
--                                     (FBref bigint year-of-start → '2425'
--                                      via format('%02d%02d', s%100, (s+1)%100);
--                                      WhoScored varchar slug passes through)
--   _ingested_at          timestamp(6)  bronze provenance (MAX across sources)
--
-- Primary key:  (match_id_canonical, team_id_canonical, player_id_canonical,
--                minute, card_type)  — enforced via ROW_NUMBER dedup.
--
-- =============================================================================
-- Dedup strategy (FBref > WhoScored)
-- =============================================================================
--   Group key: (match_id_canonical, dedup_player_key, minute, card_type)
--     where dedup_player_key =
--       COALESCE(player_id_canonical, source || ':' || team_canonical || ':'
--                                            || raw_native_player_id)
--   Priority:  source='fbref' wins; tie-broken by max(_ingested_at).
--
--   Resolved-player branch: when xref_player resolves both sources to the
--     same canonical_id, FBref wins (single output row). This is the desired
--     cross-source dedup pattern.
--   Unresolved-player branch: when player_id_canonical IS NULL on either side
--     (xref_player is currently scoped to R2 seasons — see project_medallion
--     _e1.md "Open follow-ups"), the fallback key includes both `source` and
--     the raw native id, so distinct unresolved players in the same team /
--     minute remain distinct rows. Side-effect: a single card covered by
--     BOTH FBref and WhoScored bronze WILL produce 2 rows when both sides
--     are unresolved (raw IDs live in different namespaces — FB hex 'e0bc6fdc'
--     vs. WS bigint '546622'). This is a known limitation of E4 wave-1 and
--     resolves automatically as xref_player coverage grows.
--
-- =============================================================================
-- Primary key relaxation (output schema, R2 prototype scope)
-- =============================================================================
--   Strict PK = (match_id_canonical, team_id_canonical, player_id_canonical,
--                minute, card_type) holds ONLY when player_id_canonical is
--   non-null. For NULL-canonical rows (legacy / out-of-scope seasons) two
--   distinct same-team-same-minute cards are intentionally kept as separate
--   rows (verified 55/13608 = 0.4% of corpus). Gold-layer fct_card SHOULD
--   either (a) filter player_id_canonical IS NOT NULL when enforcing PK, or
--   (b) extend the PK with the raw native id when NULL.
--
-- =============================================================================
-- WhoScored → FBref match_id bridge (mirrors fct_lineup ESPN bridge pattern)
-- =============================================================================
--   bronze.whoscored_events has only numeric game_id (DOUBLE) — no date / no
--   team-name pair on the event row. We bridge via:
--     1. whoscored_schedule.game_id (BIGINT) carries date + home_team + away_team
--     2. xref_team canonicalises both WhoScored team-names to canonical_id
--     3. Re-look-up canonical_id back to FBref source_id (team-name as FBref
--        spells it) within the same league
--     4. fbref_match_enriched joined on (league, date, home, away) returns
--        the authoritative FBref hex match_id
--   Bridge MISS → fallback `'whoscored_raw_<bigint(game_id)>'` keeps row alive
--   (mirrors v0_unbridged fct_event semantics) and surfaces in DQ as orphan.
--
-- =============================================================================
-- Bronze ID conventions (verified 2026-05-08)
-- =============================================================================
--   * fbref_match_events.season is BIGINT year-of-start (2024 == 2024-25)
--   * whoscored_events.season   is VARCHAR compact slug ('2425')
--   * whoscored_events.{game_id, team_id, player_id} are DOUBLE — direct
--     CAST AS varchar yields scientific notation ('9.5408E4'); ALWAYS round
--     through BIGINT first to keep digit form ('95408').
--   * whoscored_events.qualifiers is a JSON-string array; Card events carry
--     `{"type": {"value": <code>, "displayName": "Yellow"|"Red"|"SecondYellow"}}`.
-- =============================================================================

WITH
-- ============================================================================
-- 1) WhoScored event-side dedup (bronze re-scrape duplicates)
-- ============================================================================
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

-- ============================================================================
-- 2) FBref event-side dedup (bronze re-scrape duplicates)
-- ============================================================================
fb_events_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY match_id, minute, player_id, event_type
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fbref_match_events
    WHERE event_type IN ('yellow_card', 'red_card', 'second_yellow_card')
),

-- ============================================================================
-- 3) WhoScored schedule dedup (provides bridge spine)
-- ============================================================================
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

-- ============================================================================
-- 4) WhoScored → FBref match_id bridge
-- ============================================================================
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

-- ============================================================================
-- 5) FBref normalised cards — full APL coverage (primary)
-- ============================================================================
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
       AND xt.season    = CAST(fe.season AS varchar)
    LEFT JOIN iceberg.silver.xref_player xp
        ON xp.source    = 'fbref'
       AND xp.source_id = fe.player_id
       AND xp.league    = fe.league
       AND xp.season    = CAST(fe.season AS varchar)
    WHERE fe.rn = 1
),

-- ============================================================================
-- 6) WhoScored normalised cards — fallback (2425+2526)
-- ============================================================================
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

-- ============================================================================
-- 7) UNION ALL + FBref-priority dedup
-- ============================================================================
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
                -- players in the same team / minute are NOT collapsed
                -- (regression fix vs. team+minute-only key, which dropped
                -- 3 legitimate same-team-same-minute yellow events on the
                -- 5-season corpus).
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
)

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
