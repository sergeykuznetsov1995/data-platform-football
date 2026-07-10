-- =============================================================================
-- Silver: fbref_match_enriched
-- =============================================================================
--
-- One row per match, enriched with team stats and event aggregations.
--
-- Sources (all from iceberg.bronze):
--   fbref_schedule          (sch) -- fixture info (base table)
--   fbref_match_team_stats  (ts)  -- team-level aggregates (possession, shots...)
--   fbref_match_events             -- goal/card/sub events (aggregated in CTE)
--   fbref_lineups                  -- lineup data (aggregated in CTE)
--
-- Deduplication:
--   Each source is deduplicated independently via ROW_NUMBER before joining.
--   schedule: PARTITION BY match_id (extracted from match_url).
--   team_stats: PARTITION BY match_id ORDER BY _ingested_at DESC.
--   events & lineups: aggregated after dedup by natural key.
--
-- Notes:
--   * schedule has no match_id column — extracted via REGEXP_EXTRACT from match_url.
--   * lineups has no team_side — determined via JOIN with schedule (team = home/away).
--   * event_type values are lowercase: goal, penalty, own_goal, yellow_card, second_yellow_card, red_card, substitution.
--   * All numeric columns use TRY_CAST to enforce proper types in Silver.
--   * Score is the OFFICIAL result — see the home_score/away_score comment below (#898).
--   * Partitioning by (league, season) is applied externally by Python CTAS.
-- =============================================================================

WITH sch_raw AS (
    SELECT *,
           -- Real FBref match_id is a hex slug from /matches/<id>/.
           -- For FUTURE fixtures FBref hasn't published a match page yet, so match_url is empty.
           -- We synthesise a deterministic pseudo-id so future matches survive
           -- Silver/Gold. Prefix 'fut_' guarantees zero collision with
           -- real hex slugs and makes them trivially identifiable in downstream layers.
           --
           -- Hash inputs (in priority order):
           --   1) gameweek + season + home + away  — stable across reschedules of date/time
           --   2) date + home + away                — fallback when gameweek is NULL/empty
           -- XXHASH64 is Trino-native, faster than MD5 and gives a compact 16-hex-char id.
           COALESCE(
               NULLIF(REGEXP_EXTRACT(match_url, '/matches/([a-f0-9]+)/', 1), ''),
               'fut_' || LOWER(TO_HEX(XXHASH64(TO_UTF8(
                   COALESCE(NULLIF(CAST(wk AS VARCHAR), ''), CAST(date AS VARCHAR))
                   || '|' || COALESCE(CAST(season AS VARCHAR), '')
                   || '|' || COALESCE(CAST(home AS VARCHAR), '')
                   || '|' || COALESCE(CAST(away AS VARCHAR), '')
               ))))
           ) AS match_id
    FROM iceberg.bronze.fbref_schedule
),

sch AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY match_id
               ORDER BY _ingested_at DESC, _batch_id DESC
           ) AS rn
    FROM sch_raw
),

ts AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY match_id
               ORDER BY _ingested_at DESC, _batch_id DESC
           ) AS rn
    FROM iceberg.bronze.fbref_match_team_stats
),

-- Deduplicate events before aggregation (#463) — key MUST stay in sync with
-- fbref_match_events.sql: NULL-safe player + deterministic tiebreaker.
events_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY match_id, minute, COALESCE(player_id, player), event_type
               ORDER BY _ingested_at DESC, _batch_id DESC
           ) AS rn
    FROM iceberg.bronze.fbref_match_events
),

events_agg AS (
    SELECT
        match_id,
        COUNT(*) FILTER (WHERE event_type IN ('goal', 'penalty') AND team_side = 'home')  AS home_goals_events,
        COUNT(*) FILTER (WHERE event_type IN ('goal', 'penalty') AND team_side = 'away')  AS away_goals_events,
        COUNT(*) FILTER (WHERE event_type = 'own_goal' AND team_side = 'home')             AS home_own_goals,
        COUNT(*) FILTER (WHERE event_type = 'own_goal' AND team_side = 'away')             AS away_own_goals,
        COUNT(*) FILTER (WHERE event_type = 'yellow_card' AND team_side = 'home')          AS home_yellows,
        COUNT(*) FILTER (WHERE event_type = 'yellow_card' AND team_side = 'away')          AS away_yellows,
        COUNT(*) FILTER (WHERE event_type = 'second_yellow_card' AND team_side = 'home')    AS home_second_yellows,
        COUNT(*) FILTER (WHERE event_type = 'second_yellow_card' AND team_side = 'away')    AS away_second_yellows,
        COUNT(*) FILTER (WHERE event_type = 'red_card' AND team_side = 'home')             AS home_reds,
        COUNT(*) FILTER (WHERE event_type = 'red_card' AND team_side = 'away')             AS away_reds,
        COUNT(*) FILTER (WHERE event_type = 'substitution')                                AS total_subs
    FROM events_dedup
    WHERE rn = 1
    GROUP BY match_id
),

-- Deduplicate lineups before aggregation (#463) — key MUST stay in sync with
-- fbref_match_lineups.sql: NULL-safe player + team + deterministic tiebreaker.
lineups_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY match_id, COALESCE(player_id, player), team
               ORDER BY _ingested_at DESC, _batch_id DESC
           ) AS rn
    FROM iceberg.bronze.fbref_lineups
),

-- Determine home/away via JOIN with schedule (lineups has team name, not team_side)
lineups_agg AS (
    SELECT
        ld.match_id,
        COUNT(*) FILTER (WHERE ld.is_starter = true  AND ld.team = s.home) AS home_starters,
        COUNT(*) FILTER (WHERE ld.is_starter = true  AND ld.team = s.away) AS away_starters,
        COUNT(*) FILTER (WHERE ld.is_starter = false AND ld.team = s.home) AS home_bench,
        COUNT(*) FILTER (WHERE ld.is_starter = false AND ld.team = s.away) AS away_bench
    FROM lineups_dedup ld
    INNER JOIN sch s ON ld.match_id = s.match_id AND s.rn = 1
    WHERE ld.rn = 1
    GROUP BY ld.match_id
)

SELECT
    -- ========= Schedule (sch) — typed =========
    sch.match_id,
    TRY_CAST(TRY_CAST(sch.wk AS DOUBLE) AS INTEGER) AS gameweek,  -- wk arrives as float-strings ('1.0') from the pandas-parsed bronze schedule
    sch.round AS stage,  -- #913 Phase 3: WC stages live here (Group stage / Round of 16 / ...); club leagues usually emit league name or ''.
    sch.day,
    TRY_CAST(sch.date AS DATE)                      AS date,
    sch.time,
    sch.home,
    sch.away,
    sch.score,
    -- score holds the OFFICIAL result, separator is EN DASH (U+2013):
    --   * awarded matches carry the forfeit score ('0–3'), explained by `notes` (#898);
    --     the on-pitch result stays available as home_goals_events/away_goals_events.
    --   * a shoot-out renders as '(4) 1–1 (5)' — strip the penalty counts first, else the
    --     leading '(5)' of '(5) 0–3 (6)' is read as the home score (Düsseldorf 5–0 Bochum).
    -- Same normalisation as scripts/crossvalidate_fbref_scores.py.
    TRY_CAST(TRIM(SPLIT_PART(REGEXP_REPLACE(sch.score, '\(\d+\)\s*', ''), CHR(8211), 1)) AS INTEGER) AS home_score,
    TRY_CAST(TRIM(SPLIT_PART(REGEXP_REPLACE(sch.score, '\(\d+\)\s*', ''), CHR(8211), 2)) AS INTEGER) AS away_score,
    -- #913 Phase 3 (WC knockout): extract penalty shoot-out scores when present in score string.
    -- NULL when regular time or extra time decided the match. Used by dim_match + fct_team_match.points.
    TRY_CAST(REGEXP_EXTRACT(sch.score, '\((\d+)\)[^0-9]*\((\d+)\)', 1) AS INTEGER) AS home_penalty,
    TRY_CAST(REGEXP_EXTRACT(sch.score, '\((\d+)\)[^0-9]*\((\d+)\)', 2) AS INTEGER) AS away_penalty,
    sch.notes,
    LOWER(COALESCE(sch.notes, '')) LIKE 'match awarded to%'            AS is_awarded,
    TRY_CAST(TRY_CAST(sch.attendance AS DOUBLE) AS INTEGER) AS attendance,  -- float-strings ('34977.0') from the pandas-parsed bronze schedule
    sch.venue,
    sch.referee,
    sch.match_url,

    -- ========= Team Stats (ts) — typed =========
    TRY_CAST(ts.home_possession AS INTEGER)         AS home_possession,
    TRY_CAST(ts.away_possession AS INTEGER)         AS away_possession,
    TRY_CAST(ts.home_shots AS INTEGER)              AS home_shots,
    TRY_CAST(ts.away_shots AS INTEGER)              AS away_shots,
    TRY_CAST(ts.home_sot AS INTEGER)                AS home_sot,
    TRY_CAST(ts.away_sot AS INTEGER)                AS away_sot,
    TRY_CAST(ts.home_saves AS INTEGER)              AS home_saves,
    TRY_CAST(ts.away_saves AS INTEGER)              AS away_saves,
    TRY_CAST(ts.home_yellow_cards AS INTEGER)       AS home_yellow_cards,
    TRY_CAST(ts.away_yellow_cards AS INTEGER)       AS away_yellow_cards,
    TRY_CAST(ts.home_red_cards AS INTEGER)          AS home_red_cards,
    TRY_CAST(ts.away_red_cards AS INTEGER)          AS away_red_cards,

    -- ========= Event Aggregations (events_agg) =========
    ea.home_goals_events,
    ea.away_goals_events,
    ea.home_own_goals,
    ea.away_own_goals,
    ea.home_yellows,
    ea.away_yellows,
    ea.home_second_yellows,
    ea.away_second_yellows,
    ea.home_reds,
    ea.away_reds,
    ea.total_subs,

    -- ========= Lineup Aggregations (lineups_agg) =========
    la.home_starters,
    la.away_starters,
    la.home_bench,
    la.away_bench,

    -- ========= Lineage =========
    sch._ingested_at                                AS _bronze_ingested_at,

    -- ========= Partition Keys =========
    -- season → slug ('2425'); FBref bronze stores year-start bigint (2024).
    -- The match_id hash above keeps the native year-start input → ids unchanged.
    sch.league,
    -- #913 Phase 2: single_year (WC stores 2026 directly)
    CASE WHEN sch.league = 'INT-World Cup'
         THEN LPAD(CAST(sch.season AS varchar), 4, '0')
         ELSE LPAD(CAST(MOD(sch.season, 100) AS varchar), 2, '0')
              || LPAD(CAST(MOD(sch.season + 1, 100) AS varchar), 2, '0')
    END AS season

FROM sch
LEFT JOIN ts
    ON  sch.match_id = ts.match_id
    AND ts.rn        = 1
LEFT JOIN events_agg ea
    ON  sch.match_id = ea.match_id
LEFT JOIN lineups_agg la
    ON  sch.match_id = la.match_id
WHERE sch.rn = 1
  -- match_id is now ALWAYS non-NULL thanks to the COALESCE in sch_raw (real id or 'fut_<xxhash>').
  -- We still filter out rows missing the date (junk) and the literal header row that the FBref
  -- HTML parser occasionally captures.
  AND sch.date IS NOT NULL
  AND LOWER(sch.date) <> 'date'
