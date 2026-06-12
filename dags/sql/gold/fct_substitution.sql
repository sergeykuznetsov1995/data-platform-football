-- =============================================================================
-- Gold: fct_substitution
-- =============================================================================
-- Substitution-grained narrow fact (one row per swap). Cross-source assembly
-- (FBref primary + WhoScored fallback union with Off↔On pairing,
-- FBref-priority dedup) is folded directly into this Gold fact — it reads
-- bronze + xref straight (#382, prior intermediate
-- `silver.match_substitutions` removed). Canonical-trio columns are appended.
--
-- Sources:
--   iceberg.bronze.fbref_match_events       — primary; one row per swap
--                                              (player=ON, secondary_player=OFF)
--   iceberg.bronze.whoscored_events         — fallback; TWO rows per swap
--                                              (SubstitutionOff + SubstitutionOn),
--                                              paired via per-(game,minute,team) seq
--   iceberg.bronze.whoscored_schedule       — bridge spine: game_id → date /
--                                              home_team / away_team
--   iceberg.silver.fbref_match_enriched     — bridge target: FBref hex match_id
--   iceberg.silver.xref_match               — FBref-only canonical id lookup
--   iceberg.silver.xref_team                — team-name canonicalisation
--   iceberg.silver.xref_player              — player canonical resolver
--   (~25,615 rows on the 5-season APL corpus)
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
--   minute                      integer     BASE event minute (#454: FBref
--                                           '90+3' → 90; WS cumulative 93 →
--                                           90 period-aware)
--   substitution_canonical      varchar     xxhash64 synthetic PK
--   substitution_source         varchar     'fbref' | 'whoscored'
--   substitution_version        varchar     literal 'v1'
--   league                      varchar     partition key
--   season                      varchar     partition key (compact slug 'YYYY',
--                                           e.g. '2425'; #404 unified silver/gold)
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

WITH
-- ============================================================================
-- Cross-source assembly (folded from former silver.match_substitutions — #382)
-- ============================================================================
-- 1) FBref event-side dedup (re-scrape duplicates)
fb_subs_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY match_id, minute, player_id, secondary_player_id, event_type
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fbref_match_events
    WHERE event_type = 'substitution'
),

-- 2) WhoScored event-side dedup (re-scrape duplicates)
ws_subs_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY game_id, period, minute, second, type,
                            team_id, player_id, related_event_id
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.whoscored_events
    WHERE type IN ('SubstitutionOff', 'SubstitutionOn')
),

-- 3) WhoScored Off/On split + per-(game,minute,team) sequence index
-- Within (game_id, minute, team_id) we use ROW_NUMBER ordered by
-- (period, second, expanded_minute) to pair k-th Off with k-th On.
ws_off AS (
    SELECT
        CAST(CAST(game_id AS BIGINT) AS varchar)         AS ws_game_id,
        CAST(CAST(team_id AS BIGINT) AS varchar)         AS team_id_raw,
        CAST(CAST(player_id AS BIGINT) AS varchar)       AS player_out_raw,
        TRY_CAST(minute AS integer)                      AS minute,
        period,                                          -- #454: for the
        -- cumulative→base minute conversion in ws_subs (AFTER pairing —
        -- converting here would collapse two same-team stoppage swaps into
        -- one pairing partition and fan the INNER JOIN out 2×2).
        league,
        season,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY game_id, minute, team_id
            ORDER BY period, second, expanded_minute
        ) AS pair_seq
    FROM ws_subs_dedup
    WHERE type = 'SubstitutionOff' AND rn = 1
),
ws_on AS (
    SELECT
        CAST(CAST(game_id AS BIGINT) AS varchar)         AS ws_game_id,
        CAST(CAST(team_id AS BIGINT) AS varchar)         AS team_id_raw,
        CAST(CAST(player_id AS BIGINT) AS varchar)       AS player_in_raw,
        TRY_CAST(minute AS integer)                      AS minute,
        league,
        season,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY game_id, minute, team_id
            ORDER BY period, second, expanded_minute
        ) AS pair_seq
    FROM ws_subs_dedup
    WHERE type = 'SubstitutionOn' AND rn = 1
),

ws_pairs AS (
    -- INNER JOIN — drop unpaired sides (defensive; Opta normally emits both
    -- Off and On for every swap, but this keeps the contract clean).
    SELECT
        o.ws_game_id,
        o.team_id_raw,
        n.player_in_raw                                   AS player_in_raw,
        o.player_out_raw                                  AS player_out_raw,
        o.minute,
        o.period,
        o.league,
        o.season,
        GREATEST(o._ingested_at, n._ingested_at)          AS _ingested_at
    FROM ws_off o
    INNER JOIN ws_on n
        ON  n.ws_game_id  = o.ws_game_id
        AND n.team_id_raw = o.team_id_raw
        AND n.minute      = o.minute
        AND n.pair_seq    = o.pair_seq
        AND n.league      = o.league
        AND n.season      = o.season
),

-- 4) WhoScored schedule dedup (bridge spine)
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

-- 5) Distinct xref_team source/canonical pairs (within league + season)
-- #459: xref_team is season-grained (PK = source, source_id, league, season;
-- season is the compact slug '2425' for ALL sources since #404 — the old
-- "ws='2425' vs fb='2024' format mismatch" justification for dropping season
-- no longer holds). Without the season key one canonical_id expands to every
-- historical FBref name variant ('Newcastle Utd' / 'Newcastle United'); the
-- variant that misses fme.home/away produced a second bridge row with
-- fbref_match_id = NULL and duplicated WhoScored events under
-- 'whoscored_raw_<game_id>'.
xref_team_canonical AS (
    SELECT DISTINCT source, source_id, canonical_id, league, season
    FROM iceberg.silver.xref_team
    WHERE canonical_id IS NOT NULL
),

-- 6) WhoScored game_id → FBref match_id bridge (mirrors fct_lineup pattern)
ws_match_bridge AS (
    SELECT
        CAST(s.game_id AS varchar)                        AS ws_game_id,
        s.league                                          AS league,
        s.season                                          AS ws_season,
        fme.match_id                                      AS fbref_match_id
    FROM ws_schedule_dedup s
    LEFT JOIN xref_team_canonical xt_home_ws
        ON xt_home_ws.source    = 'whoscored'
       AND xt_home_ws.source_id = s.home_team
       AND xt_home_ws.league    = s.league
       AND xt_home_ws.season    = s.season          -- #459
    LEFT JOIN xref_team_canonical xt_away_ws
        ON xt_away_ws.source    = 'whoscored'
       AND xt_away_ws.source_id = s.away_team
       AND xt_away_ws.league    = s.league
       AND xt_away_ws.season    = s.season          -- #459
    LEFT JOIN xref_team_canonical xt_home_fb
        ON xt_home_fb.source       = 'fbref'
       AND xt_home_fb.canonical_id = xt_home_ws.canonical_id
       AND xt_home_fb.league       = s.league
       AND xt_home_fb.season       = s.season       -- #459
    LEFT JOIN xref_team_canonical xt_away_fb
        ON xt_away_fb.source       = 'fbref'
       AND xt_away_fb.canonical_id = xt_away_ws.canonical_id
       AND xt_away_fb.league       = s.league
       AND xt_away_fb.season       = s.season       -- #459
    LEFT JOIN iceberg.silver.fbref_match_enriched fme
        ON fme.league = s.league
       AND fme.home   = xt_home_fb.source_id
       AND fme.away   = xt_away_fb.source_id
       AND fme.date   = CAST(s.date AS date)
    WHERE s.rn = 1
),

-- 7) FBref normalised substitutions — primary (full APL 2016-2025)
-- FBref convention: `player` = player coming ON, `secondary_player` = OFF.
-- season UNIFICATION (mirrors fct_lineup R4 pattern): bigint year-of-start
-- (2024) → compact 'YYYY' varchar ('2425') for cross-fact comparability.
fb_subs AS (
    SELECT
        COALESCE(xm.canonical_id, fe.match_id)            AS match_id_canonical,
        xt.canonical_id                                   AS team_id_canonical,
        xp_in.canonical_id                                AS player_in_canonical,
        xp_out.canonical_id                               AS player_out_canonical,
        -- #454: '90+3' stoppage-time strings → base minute 90 (split_part
        -- mirrors fct_match_timeline.sql). Plain TRY_CAST returned NULL and
        -- the final WHERE silently dropped every stoppage-time sub.
        TRY_CAST(split_part(fe.minute, '+', 1) AS integer) AS minute,
        CAST('fbref' AS varchar)                          AS source,
        1                                                 AS source_priority,
        fe.league                                         AS league,
        format(
            '%02d%02d',
            mod(fe.season, 100),
            mod(fe.season + 1, 100)
        )                                                 AS season,
        fe._ingested_at                                   AS _ingested_at
    FROM fb_subs_dedup fe
    LEFT JOIN iceberg.silver.xref_match xm
        ON xm.source    = 'fbref'
       AND xm.source_id = fe.match_id
    LEFT JOIN iceberg.silver.xref_team xt
        ON xt.source    = 'fbref'
       AND xt.source_id = fe.team
       AND xt.league    = fe.league
       AND xt.season    = format('%02d%02d', mod(fe.season, 100), mod(fe.season + 1, 100))  -- #404: year-start → slug
    LEFT JOIN iceberg.silver.xref_player xp_in
        ON xp_in.source    = 'fbref'
       AND xp_in.source_id = fe.player_id              -- coming ON
       AND xp_in.league    = fe.league
       AND xp_in.season    = format('%02d%02d', mod(fe.season, 100), mod(fe.season + 1, 100))  -- #404: year-start → slug
    LEFT JOIN iceberg.silver.xref_player xp_out
        ON xp_out.source    = 'fbref'
       AND xp_out.source_id = fe.secondary_player_id   -- going OFF
       AND xp_out.league    = fe.league
       AND xp_out.season    = format('%02d%02d', mod(fe.season, 100), mod(fe.season + 1, 100))  -- #404: year-start → slug
    WHERE fe.rn = 1
),

-- 8) WhoScored normalised substitutions — fallback (2425+2526)
ws_subs AS (
    SELECT
        COALESCE(
            mb.fbref_match_id,
            'whoscored_raw_' || wp.ws_game_id
        )                                                 AS match_id_canonical,
        xt.canonical_id                                   AS team_id_canonical,
        xp_in.canonical_id                                AS player_in_canonical,
        xp_out.canonical_id                               AS player_out_canonical,
        -- #454: WS minute is the CUMULATIVE half minute (90+3 → 93). Convert
        -- to base minute (period-aware, mirrors fct_match_timeline.sql) so
        -- the dedup key aligns with the FBref branch's base minute.
        CASE
            WHEN wp.period = 'FirstHalf'  AND wp.minute > 45 THEN 45
            WHEN wp.period = 'SecondHalf' AND wp.minute > 90 THEN 90
            ELSE wp.minute
        END                                               AS minute,
        CAST('whoscored' AS varchar)                      AS source,
        2                                                 AS source_priority,
        wp.league                                         AS league,
        wp.season                                         AS season,    -- '2425' native
        wp._ingested_at                                   AS _ingested_at
    FROM ws_pairs wp
    -- ws_pairs.team_id_raw is bridged to canonical via the same
    -- bronze.whoscored_events team_id ↔ team_name mapping, BUT ws_off/ws_on
    -- here didn't keep `team` (string). Build the bridge inline by joining
    -- back to bronze.whoscored_events for the team-name lookup.
    LEFT JOIN (
        SELECT
            CAST(CAST(game_id AS BIGINT) AS varchar)      AS ws_game_id,
            CAST(CAST(team_id AS BIGINT) AS varchar)      AS team_id_raw,
            MAX(team)                                     AS team_name_raw,
            league,
            season
        FROM iceberg.bronze.whoscored_events
        WHERE team_id IS NOT NULL AND team IS NOT NULL
        GROUP BY 1, 2, league, season
    ) etn
        ON etn.ws_game_id  = wp.ws_game_id
       AND etn.team_id_raw = wp.team_id_raw
       AND etn.league      = wp.league
       AND etn.season      = wp.season
    LEFT JOIN iceberg.silver.xref_team xt
        ON xt.source    = 'whoscored'
       AND xt.source_id = etn.team_name_raw
       AND xt.league    = wp.league
       AND xt.season    = wp.season
    LEFT JOIN ws_match_bridge mb
        ON mb.ws_game_id = wp.ws_game_id
       AND mb.league     = wp.league
       AND mb.ws_season  = wp.season
    LEFT JOIN iceberg.silver.xref_player xp_in
        ON xp_in.source    = 'whoscored'
       AND xp_in.source_id = wp.player_in_raw
       AND xp_in.league    = wp.league
       AND xp_in.season    = wp.season
    LEFT JOIN iceberg.silver.xref_player xp_out
        ON xp_out.source    = 'whoscored'
       AND xp_out.source_id = wp.player_out_raw
       AND xp_out.league    = wp.league
       AND xp_out.season    = wp.season
),

-- 9) UNION + FBref-priority dedup
union_subs AS (
    SELECT * FROM fb_subs
    UNION ALL
    SELECT * FROM ws_subs
),

deduped AS (
    SELECT
        u.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                match_id_canonical,
                -- Player-bucket precedence: when both canonical IDs are
                -- present, dedup cross-source on the (in, out) pair; when
                -- either is NULL, fall back to (source || team || minute) to
                -- avoid collapsing distinct unresolved swaps.
                CASE
                    WHEN player_in_canonical IS NOT NULL
                     AND player_out_canonical IS NOT NULL
                        THEN player_in_canonical || '|' || player_out_canonical
                    ELSE source || ':'
                         || COALESCE(team_id_canonical, '?') || ':'
                         || COALESCE(CAST(minute AS varchar), '?') || ':'
                         || COALESCE(player_in_canonical, '?') || '|'
                         || COALESCE(player_out_canonical, '?')
                END,
                minute
            ORDER BY
                source_priority ASC,
                _ingested_at DESC
        ) AS rn
    FROM union_subs u
),

-- Former silver.match_substitutions final projection (FBref-priority winner).
match_substitutions AS (
    SELECT
        match_id_canonical,
        team_id_canonical,
        player_in_canonical,
        player_out_canonical,
        minute,
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
        s.player_in_canonical,
        s.player_out_canonical,
        s.minute,
        s.source            AS substitution_source,
        s.league,
        -- #404: both branches emit slug ('2425') now → pass through unchanged.
        s.season            AS season,
        s._ingested_at
    FROM match_substitutions s
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
