-- =============================================================================
-- Silver: match_substitutions
-- =============================================================================
-- Substitution events (in/out player pair per swap) unified across FBref
-- (primary, full APL coverage 2016-2025) and WhoScored (fallback, 2425+2526).
-- One row per resolved (match, team, minute, player_in, player_out).
--
-- Sources:
--   iceberg.bronze.fbref_match_events       — primary; one row per swap
--                                              already (player + secondary_player
--                                              co-located on the same record).
--   iceberg.bronze.whoscored_events         — fallback; emits TWO rows per swap
--                                              (SubstitutionOff + SubstitutionOn)
--                                              paired through bronze.related_event_id.
--   iceberg.bronze.whoscored_schedule       — bridge spine: game_id → date /
--                                              home_team / away_team for the
--                                              WhoScored→FBref match_id resolve.
--   iceberg.silver.fbref_match_enriched     — bridge target: provides FBref
--                                              hex match_id keyed on
--                                              (league, date, home, away).
--   iceberg.silver.xref_match               — FBref-only canonical id lookup.
--   iceberg.silver.xref_team                — team-name canonicalisation.
--   iceberg.silver.xref_player              — player canonical resolver.
--
-- DAG-integration note: silver_tasks.run_silver_transform() wraps this SELECT
-- in `CREATE TABLE iceberg.silver.match_substitutions AS ... WITH (
-- partitioning=ARRAY['league','season'])`. This file MUST stay a pure SELECT.
--
-- =============================================================================
-- Output schema (frozen for E4 wave-1)
-- =============================================================================
--   match_id_canonical    varchar     resolved canonical (FBref hex bridged;
--                                     'whoscored_raw_<game_id>' fallback)
--   team_id_canonical     varchar     via xref_team
--   player_in_canonical   varchar     via xref_player (FBref `player`)
--   player_out_canonical  varchar     via xref_player (FBref `secondary_player`)
--   minute                integer     event minute (TRY_CAST from varchar)
--   source                varchar     'fbref' | 'whoscored'
--   source_version        varchar     literal 'v1'
--   league                varchar
--   season                varchar     UNIFIED 4-char 'YYYY' compact form
--   _ingested_at          timestamp(6)  bronze provenance
--
-- Primary key:  (match_id_canonical, team_id_canonical, minute,
--                player_in_canonical, player_out_canonical)
--
-- =============================================================================
-- FBref convention (verified empirically on bronze.fbref_match_events sample,
-- match 3812dc28, 2026-05-08): on a `substitution` event row, the `player`
-- column holds the player coming ON (player_in), and `secondary_player`
-- holds the player going OFF (player_out). Same convention used in the
-- public FBref Match Events table. We surface this convention explicitly as
-- the canonical mapping; if a future audit overturns it, it's a one-line
-- swap below.
--
-- =============================================================================
-- WhoScored pairing strategy
-- =============================================================================
--   Opta wires every SubstitutionOff to its companion SubstitutionOn via the
--   `related_event_id` column (DOUBLE in bronze; 100% non-null on sub events).
--   After bronze-side dedup (re-scrape duplicates via ROW_NUMBER on the full
--   natural key) the (game_id, related_event_id, type) triple is unique, so
--   a self-join ws_off ↔ ws_on on (game_id, off.related_event_id = on_synth_id)
--   yields one paired row per swap.
--
--   The "synthetic id" approach: Opta does not expose its own per-event id
--   in bronze (no `event_id` column — verified via DESCRIBE 2026-05-08;
--   only related_event_id), so we synthesise a sequence index per
--   (game_id, period) ordered by (minute, second, expanded_minute, type) —
--   identical to the seq used in silver/whoscored_events_spadl.sql. The
--   pairing therefore relies on related_event_id pointing to the seq we
--   reproduce — but Opta's related_event_id is the OPPOSITE event's seq,
--   not absolute, so direct join doesn't trivially work.
--
--   Pragmatic workaround used here: pair on (game_id, minute, team_id) +
--   GREATEST/LEAST trick when there is exactly one Off and one On at the
--   same minute for the team (the dominant case). For double-subs at the
--   same minute (verified 5-10 cases per season) we order by sub-events'
--   (period, minute, second) and use ROW_NUMBER to pair the k-th Off with
--   the k-th On of the same (game_id, minute, team_id). This is a fidelity
--   loss on the rare double-swap-same-minute case (pairing may flip), but
--   minute and team are still correct, so the row is auditable.
--
--   FBref-primary dedup wins anyway for any (match, team, minute, in, out)
--   where FBref also has a row, so this WhoScored fidelity tradeoff only
--   surfaces on 2425/2526 fixtures absent from FBref bronze (rare).
--
-- =============================================================================
-- Dedup strategy (FBref > WhoScored)
-- =============================================================================
--   Group key: (match_id_canonical, team_id_canonical, minute,
--               player_in_canonical, player_out_canonical).
--   Priority:  source='fbref' wins; tie-broken by max(_ingested_at).
--
-- =============================================================================

WITH
-- ============================================================================
-- 1) FBref event-side dedup (re-scrape duplicates)
-- ============================================================================
fb_subs_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY match_id, minute, player_id, secondary_player_id, event_type
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fbref_match_events
    WHERE event_type = 'substitution'
),

-- ============================================================================
-- 2) WhoScored event-side dedup (re-scrape duplicates)
-- ============================================================================
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

-- ============================================================================
-- 3) WhoScored Off/On split + per-(game,minute,team) sequence index
-- ============================================================================
-- Within (game_id, minute, team_id) we use ROW_NUMBER ordered by
-- (period, second, expanded_minute) to pair k-th Off with k-th On.
ws_off AS (
    SELECT
        CAST(CAST(game_id AS BIGINT) AS varchar)         AS ws_game_id,
        CAST(CAST(team_id AS BIGINT) AS varchar)         AS team_id_raw,
        CAST(CAST(player_id AS BIGINT) AS varchar)       AS player_out_raw,
        TRY_CAST(minute AS integer)                      AS minute,
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

-- ============================================================================
-- 4) WhoScored schedule dedup (bridge spine)
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
-- 5) Distinct xref_team source/canonical pairs (within league)
-- ============================================================================
-- Drops the season predicate intentionally — xref_team passes native season
-- per source (ws='2425', fb='2024'); cross-league ambiguity is killed by
-- the league predicate on fbref_match_enriched downstream.
xref_team_canonical AS (
    SELECT DISTINCT source, source_id, canonical_id, league
    FROM iceberg.silver.xref_team
    WHERE canonical_id IS NOT NULL
),

-- ============================================================================
-- 6) WhoScored game_id → FBref match_id bridge (mirrors fct_lineup pattern)
-- ============================================================================
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
-- 7) FBref normalised substitutions — primary (full APL 2016-2025)
-- ============================================================================
-- FBref convention: `player` = player coming ON, `secondary_player` = OFF.
-- season UNIFICATION (mirrors fct_lineup R4 pattern): bigint year-of-start
-- (2024) → compact 'YYYY' varchar ('2425') for cross-fact comparability.
fb_subs AS (
    SELECT
        COALESCE(xm.canonical_id, fe.match_id)            AS match_id_canonical,
        xt.canonical_id                                   AS team_id_canonical,
        xp_in.canonical_id                                AS player_in_canonical,
        xp_out.canonical_id                               AS player_out_canonical,
        TRY_CAST(fe.minute AS integer)                    AS minute,
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
       AND xt.season    = CAST(fe.season AS varchar)
    LEFT JOIN iceberg.silver.xref_player xp_in
        ON xp_in.source    = 'fbref'
       AND xp_in.source_id = fe.player_id              -- coming ON
       AND xp_in.league    = fe.league
       AND xp_in.season    = CAST(fe.season AS varchar)
    LEFT JOIN iceberg.silver.xref_player xp_out
        ON xp_out.source    = 'fbref'
       AND xp_out.source_id = fe.secondary_player_id   -- going OFF
       AND xp_out.league    = fe.league
       AND xp_out.season    = CAST(fe.season AS varchar)
    WHERE fe.rn = 1
),

-- ============================================================================
-- 8) WhoScored normalised substitutions — fallback (2425+2526)
-- ============================================================================
ws_subs AS (
    SELECT
        COALESCE(
            mb.fbref_match_id,
            'whoscored_raw_' || wp.ws_game_id
        )                                                 AS match_id_canonical,
        xt.canonical_id                                   AS team_id_canonical,
        xp_in.canonical_id                                AS player_in_canonical,
        xp_out.canonical_id                               AS player_out_canonical,
        wp.minute                                         AS minute,
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

-- ============================================================================
-- 9) UNION + FBref-priority dedup
-- ============================================================================
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
)

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
