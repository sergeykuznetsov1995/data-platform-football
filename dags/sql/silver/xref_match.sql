-- =============================================================================
-- Silver: xref_match  (Phase B — 7-source cascade)
-- =============================================================================
-- Match-level cross-reference. FBref is the spine: `match_id` (an 8-char hex
-- assigned by FBref) is stable and globally unique. The 6 other sources are
-- bridged to that spine via the tuple
--
--     (match_date, home_canonical_id, away_canonical_id, league, season)
--
-- where home/away_canonical_id come from `silver.xref_team`. When the bridge
-- succeeds the row carries the FBref hex as canonical_id with
-- confidence='date_team_match'. When no FBref match is found, the row is
-- emitted as an orphan with a prefixed canonical_id (e.g. 'ws_<game_id>',
-- 'us_<game_id>', 'mh_<xxhash64>') and confidence='orphan' — mirrors the
-- xref_team orphan-prefix convention (xref_team.sql.j2:145-156).
--
-- DAG-integration note: T4 wraps this SELECT in
-- `CREATE TABLE iceberg.silver.xref_match AS ...` via
-- `silver_tasks.run_silver_transform()`. This file MUST stay a pure SELECT.
--
-- =============================================================================
-- Schema (frozen for E1.5 cutover)
-- =============================================================================
--   canonical_id   varchar  -- FBref match_id when bridged, '<prefix>_<id>' when orphan
--   source         varchar  -- 'fbref'|'whoscored'|'understat'|'sofascore'|
--                              'fotmob'|'matchhistory'|'espn'
--   source_id      varchar  -- raw match-id of that source (varchar form)
--   display_name   varchar  -- 'Manchester City vs Arsenal' (for debug)
--   league         varchar
--   season         varchar
--   confidence     varchar  -- 'exact'|'date_team_match'|'orphan'
--   match_score    double   -- always NULL (no fuzzy step here)
--
-- Testable invariants:
--   * PK = (canonical_id, source) — same FBref canonical can appear under
--     source='fbref' AND source='whoscored' (the bridged row); orphan rows
--     have a unique source-prefixed canonical_id per source.
--   * canonical_id is NEVER NULL.
--   * confidence ∈ {'exact', 'date_team_match', 'orphan'}.
--
-- =============================================================================
-- Bronze column-name reference (verified via DESCRIBE on 2026-05-08; mirrors
-- the table reference in xref_team.sql.j2:42-54)
-- =============================================================================
--   fbref_schedule        : match_url, date (varchar), home, away, league, season (BIGINT)
--   whoscored_schedule    : game_id (BIGINT), date (TIMESTAMP), home_team, away_team, league, season (varchar)
--   understat_schedule    : game_id (BIGINT), date (TIMESTAMP), home_team, away_team (varchar)
--   sofascore_schedule    : game_id (BIGINT), date (TIMESTAMP), home_team, away_team (varchar)
--   fotmob_schedule       : match_id (varchar! — NOT game_id), date (varchar), home_team, away_team (varchar), season (BIGINT)
--   matchhistory_results  : NO native match_id, match_date (TIMESTAMP), home_team, away_team (renamed via COLUMN_MAPPING), season (BIGINT)
--   espn_schedule         : game_id (BIGINT), NO date column — date prefixed in `game` column (e.g. '2026-01-06 Team-Team'); home_team, away_team (varchar), season (varchar)
--
-- Per CLAUDE.md DOUBLE-cast rule: bronze numeric ids stored as DOUBLE need
-- `CAST(CAST(x AS BIGINT) AS varchar)` to avoid scientific notation.
-- Per CLAUDE.md xref-JOIN rule: every JOIN to silver.xref_team MUST include
-- (league, season) predicates — otherwise multi-season fan-out 1.5-4×.
-- =============================================================================

WITH derived AS (
    -- Same regex/`fut_<xxhash64>` derivation as the previous xref_match.sql
    -- and as fbref_match_enriched.sql — kept in sync so xref_match joins
    -- match_enriched 1:1 on canonical_id. Also project the canonical fields
    -- needed for downstream cascade joins (date + home/away).
    SELECT
        COALESCE(
            NULLIF(REGEXP_EXTRACT(match_url, '/matches/([a-f0-9]+)/', 1), ''),
            'fut_' || LOWER(TO_HEX(XXHASH64(TO_UTF8(
                COALESCE(NULLIF(CAST(wk   AS varchar), ''), CAST(date AS varchar))
                || '|' || COALESCE(CAST(season AS varchar), '')
                || '|' || COALESCE(CAST(home   AS varchar), '')
                || '|' || COALESCE(CAST(away   AS varchar), '')
            ))))
        )                                    AS match_id,
        TRY_CAST(date AS date)               AS match_date,
        home,
        away,
        league,
        -- season → slug ('2425'); the match_id hash above keeps year-start (ids stable).
        -- #913 Phase 2
        CASE WHEN league = 'INT-World Cup'
             THEN LPAD(CAST(season AS varchar), 4, '0')
             ELSE LPAD(CAST(MOD(season, 100) AS varchar), 2, '0')
                  || LPAD(CAST(MOD(season + 1, 100) AS varchar), 2, '0')
        END  AS season
    FROM iceberg.bronze.fbref_schedule
),

fbref_base AS (
    -- FBref spine row (source='fbref', confidence='exact'). Also publishes
    -- (home_canonical_id, away_canonical_id, match_date) so the cascade
    -- blocks can JOIN against this CTE without re-reading bronze.fbref.
    SELECT
        d.match_id                                                     AS canonical_id,
        d.match_id                                                     AS source_id,
        CONCAT(d.home, ' vs ', d.away)                                 AS display_name,
        d.league,
        d.season,
        d.match_date,
        xt_h.canonical_id                                              AS home_canonical_id,
        xt_a.canonical_id                                              AS away_canonical_id
    FROM derived d
    LEFT JOIN iceberg.silver.xref_team xt_h
           ON xt_h.source    = 'fbref'
          AND xt_h.source_id = d.home
          AND xt_h.league    = d.league
          AND xt_h.season    = d.season
    LEFT JOIN iceberg.silver.xref_team xt_a
           ON xt_a.source    = 'fbref'
          AND xt_a.source_id = d.away
          AND xt_a.league    = d.league
          AND xt_a.season    = d.season
    WHERE d.match_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

ws_resolved AS (
    -- WhoScored: game_id is BIGINT in schedule (DOUBLE in events); we read
    -- from schedule, so a single BIGINT cast is sufficient.
    SELECT
        CAST(s.game_id AS varchar)                                     AS source_id,
        TRY_CAST(s.date AS date)                                       AS match_date,
        s.league,
        CAST(s.season AS varchar)                                      AS season,
        xt_h.canonical_id                                              AS home_canonical_id,
        xt_a.canonical_id                                              AS away_canonical_id,
        CONCAT(s.home_team, ' vs ', s.away_team)                       AS display_name
    FROM iceberg.bronze.whoscored_schedule s
    LEFT JOIN iceberg.silver.xref_team xt_h
           ON xt_h.source    = 'whoscored'
          AND xt_h.source_id = s.home_team
          AND xt_h.league    = s.league
          AND xt_h.season    = CAST(s.season AS varchar)
    LEFT JOIN iceberg.silver.xref_team xt_a
           ON xt_a.source    = 'whoscored'
          AND xt_a.source_id = s.away_team
          AND xt_a.league    = s.league
          AND xt_a.season    = CAST(s.season AS varchar)
    WHERE s.game_id IS NOT NULL
),

us_resolved AS (
    SELECT
        CAST(s.game_id AS varchar)                                     AS source_id,
        TRY_CAST(s.date AS date)                                       AS match_date,
        s.league,
        CAST(s.season AS varchar)                                      AS season,
        xt_h.canonical_id                                              AS home_canonical_id,
        xt_a.canonical_id                                              AS away_canonical_id,
        CONCAT(s.home_team, ' vs ', s.away_team)                       AS display_name
    FROM iceberg.bronze.understat_schedule s
    LEFT JOIN iceberg.silver.xref_team xt_h
           ON xt_h.source    = 'understat'
          AND xt_h.source_id = s.home_team
          AND xt_h.league    = s.league
          AND xt_h.season    = CAST(s.season AS varchar)
    LEFT JOIN iceberg.silver.xref_team xt_a
           ON xt_a.source    = 'understat'
          AND xt_a.source_id = s.away_team
          AND xt_a.league    = s.league
          AND xt_a.season    = CAST(s.season AS varchar)
    WHERE s.game_id IS NOT NULL
),

ss_resolved AS (
    SELECT
        CAST(s.game_id AS varchar)                                     AS source_id,
        -- #840: Bronze auto-passthrough. date is now raw epoch start_timestamp;
        -- derive here (old TIMESTAMP `date` bridged via COALESCE). home_team ->
        -- home_team_name; VALUES unchanged so xref_team join is identical.
        COALESCE(
            TRY_CAST(s.date AS date),
            CAST(from_unixtime(s.start_timestamp) AS date)
        )                                                              AS match_date,
        s.league,
        CAST(s.season AS varchar)                                      AS season,
        xt_h.canonical_id                                              AS home_canonical_id,
        xt_a.canonical_id                                              AS away_canonical_id,
        CONCAT(COALESCE(s.home_team, s.home_team_name), ' vs ',
               COALESCE(s.away_team, s.away_team_name))                AS display_name
    FROM iceberg.bronze.sofascore_schedule s
    LEFT JOIN iceberg.silver.xref_team xt_h
           ON xt_h.source    = 'sofascore'
          AND xt_h.source_id = COALESCE(s.home_team, s.home_team_name)
          AND xt_h.league    = s.league
          AND xt_h.season    = CAST(s.season AS varchar)
    LEFT JOIN iceberg.silver.xref_team xt_a
           ON xt_a.source    = 'sofascore'
          AND xt_a.source_id = COALESCE(s.away_team, s.away_team_name)
          AND xt_a.league    = s.league
          AND xt_a.season    = CAST(s.season AS varchar)
    WHERE s.game_id IS NOT NULL
),

fm_resolved AS (
    -- FotMob: native id column is `match_id` (varchar), NOT game_id. team
    -- columns are varchar (team-name strings, same as xref_team source_id).
    -- season is BIGINT in bronze, cast to varchar for JOIN alignment.
    -- date is ISO 8601 timestamp ('2026-01-08T20:00:00Z'); Trino's
    -- TRY_CAST(varchar AS date) only handles 'YYYY-MM-DD' so we slice
    -- the leading 10 chars before the cast.
    SELECT
        s.match_id                                                     AS source_id,
        TRY_CAST(SUBSTR(s.date, 1, 10) AS date)                        AS match_date,
        s.league,
        -- season → slug ('2425'); bronze fotmob_schedule is year-start bigint.
        -- #913 Phase 2
        CASE WHEN s.league = 'INT-World Cup'
             THEN LPAD(CAST(s.season AS varchar), 4, '0')
             ELSE LPAD(CAST(MOD(s.season, 100) AS varchar), 2, '0')
                  || LPAD(CAST(MOD(s.season + 1, 100) AS varchar), 2, '0')
        END   AS season,
        xt_h.canonical_id                                              AS home_canonical_id,
        xt_a.canonical_id                                              AS away_canonical_id,
        CONCAT(s.home_team, ' vs ', s.away_team)                       AS display_name
    FROM iceberg.bronze.fotmob_schedule s
    LEFT JOIN iceberg.silver.xref_team xt_h
           ON xt_h.source    = 'fotmob'
          AND xt_h.source_id = s.home_team
          AND xt_h.league    = s.league
          AND xt_h.season    = CASE WHEN s.league = 'INT-World Cup'
                                     THEN LPAD(CAST(s.season AS varchar), 4, '0')
                                     ELSE LPAD(CAST(MOD(s.season, 100) AS varchar), 2, '0')
                                          || LPAD(CAST(MOD(s.season + 1, 100) AS varchar), 2, '0')
                               END
    LEFT JOIN iceberg.silver.xref_team xt_a
           ON xt_a.source    = 'fotmob'
          AND xt_a.source_id = s.away_team
          AND xt_a.league    = s.league
          AND xt_a.season    = CASE WHEN s.league = 'INT-World Cup'
                                     THEN LPAD(CAST(s.season AS varchar), 4, '0')
                                     ELSE LPAD(CAST(MOD(s.season, 100) AS varchar), 2, '0')
                                          || LPAD(CAST(MOD(s.season + 1, 100) AS varchar), 2, '0')
                               END
    WHERE s.match_id IS NOT NULL
),

mh_resolved AS (
    -- MatchHistory has NO native match_id. We synthesise a deterministic
    -- 'mh_<xxhash64>' source_id (mirrors fbref's 'fut_' fallback) keyed on
    -- (match_date|home_team|away_team|league|season) — the (league, season)
    -- terms prevent same-day-different-league collisions.
    -- NB: home_team/away_team VALUES are single-token (matchhistory specific —
    -- e.g. 'Arsenal' not 'Arsenal FC'; see xref_team.sql.j2).
    -- #307: source switched matchhistory_games → matchhistory_results.
    -- COLUMN_MAPPING renamed date→match_date, hometeam→home_team,
    -- awayteam→away_team; team-string VALUES are identical so xref_team JOIN holds.
    SELECT
        -- match_date is raw 'DD/MM/YYYY' varchar in results → parse to ISO date
        -- so the synthetic hash key and the output column are both well-formed.
        'mh_' || LOWER(TO_HEX(XXHASH64(TO_UTF8(
            CAST(CAST(date_parse(s.match_date, '%d/%m/%Y') AS date) AS varchar)
            || '|' || COALESCE(LOWER(CAST(s.home_team AS varchar)), '')
            || '|' || COALESCE(LOWER(CAST(s.away_team AS varchar)), '')
            || '|' || s.league
            || '|' || CAST(s.season AS varchar)
        ))))                                                           AS source_id,
        CAST(date_parse(s.match_date, '%d/%m/%Y') AS date)             AS match_date,
        s.league,
        -- season → slug ('2425'); bronze matchhistory is year-start bigint.
        -- (The source_id hash above keeps year-start so ids stay stable.)
        -- #913 Phase 2
        CASE WHEN s.league = 'INT-World Cup'
             THEN LPAD(CAST(s.season AS varchar), 4, '0')
             ELSE LPAD(CAST(MOD(s.season, 100) AS varchar), 2, '0')
                  || LPAD(CAST(MOD(s.season + 1, 100) AS varchar), 2, '0')
        END   AS season,
        xt_h.canonical_id                                              AS home_canonical_id,
        xt_a.canonical_id                                              AS away_canonical_id,
        CONCAT(CAST(s.home_team AS varchar), ' vs ',
               CAST(s.away_team AS varchar))                           AS display_name
    FROM iceberg.bronze.matchhistory_results s
    LEFT JOIN iceberg.silver.xref_team xt_h
           ON xt_h.source    = 'matchhistory'
          AND xt_h.source_id = CAST(s.home_team AS varchar)
          AND xt_h.league    = s.league
          AND xt_h.season    = CASE WHEN s.league = 'INT-World Cup'
                                     THEN LPAD(CAST(s.season AS varchar), 4, '0')
                                     ELSE LPAD(CAST(MOD(s.season, 100) AS varchar), 2, '0')
                                          || LPAD(CAST(MOD(s.season + 1, 100) AS varchar), 2, '0')
                               END
    LEFT JOIN iceberg.silver.xref_team xt_a
           ON xt_a.source    = 'matchhistory'
          AND xt_a.source_id = CAST(s.away_team AS varchar)
          AND xt_a.league    = s.league
          AND xt_a.season    = CASE WHEN s.league = 'INT-World Cup'
                                     THEN LPAD(CAST(s.season AS varchar), 4, '0')
                                     ELSE LPAD(CAST(MOD(s.season, 100) AS varchar), 2, '0')
                                          || LPAD(CAST(MOD(s.season + 1, 100) AS varchar), 2, '0')
                               END
    WHERE s.match_date IS NOT NULL
),

es_resolved AS (
    -- ESPN: NO `date` column in espn_schedule. The match date is prefixed
    -- in the `game` column (format: 'YYYY-MM-DD <Home>-<Away>').
    -- Extract the leading 10-char date prefix and TRY_CAST to date.
    SELECT
        CAST(s.game_id AS varchar)                                     AS source_id,
        TRY_CAST(SUBSTR(s.game, 1, 10) AS date)                        AS match_date,
        s.league,
        CAST(s.season AS varchar)                                      AS season,
        xt_h.canonical_id                                              AS home_canonical_id,
        xt_a.canonical_id                                              AS away_canonical_id,
        CONCAT(s.home_team, ' vs ', s.away_team)                       AS display_name
    FROM iceberg.bronze.espn_schedule s
    LEFT JOIN iceberg.silver.xref_team xt_h
           ON xt_h.source    = 'espn'
          AND xt_h.source_id = s.home_team
          AND xt_h.league    = s.league
          AND xt_h.season    = CAST(s.season AS varchar)
    LEFT JOIN iceberg.silver.xref_team xt_a
           ON xt_a.source    = 'espn'
          AND xt_a.source_id = s.away_team
          AND xt_a.league    = s.league
          AND xt_a.season    = CAST(s.season AS varchar)
    WHERE s.game_id IS NOT NULL
),

-- =============================================================================
-- unioned — 7-source cascade, one block per source. Wrapped in a CTE so the
-- final SELECT can enforce the PK (canonical_id, source) invariant: a source
-- whose bronze schedule carries the same physical match under two season
-- labels (e.g. ESPN '2021' = a copy of '1920', #809) would otherwise emit two
-- rows with identical (canonical_id, source). See dedup below.
-- =============================================================================
unioned AS (

-- =============================================================================
-- FBref spine — emits the canonical row per match
-- =============================================================================
SELECT
    canonical_id,
    'fbref'                                AS source,
    source_id,
    display_name,
    league,
    season,
    'exact'                                AS confidence,
    CAST(NULL AS double)                   AS match_score
FROM fbref_base
GROUP BY canonical_id, source_id, display_name, league, season

UNION ALL

-- =============================================================================
-- WhoScored cascade
-- =============================================================================
SELECT
    COALESCE(fb.canonical_id, 'ws_' || src.source_id)                  AS canonical_id,
    'whoscored'                                                        AS source,
    src.source_id,
    src.display_name,
    src.league,
    src.season,
    CASE WHEN fb.canonical_id IS NOT NULL THEN 'date_team_match'
         ELSE 'orphan' END                                             AS confidence,
    CAST(NULL AS double)                                               AS match_score
FROM ws_resolved src
LEFT JOIN fbref_base fb
       ON fb.match_date        = src.match_date
      AND fb.home_canonical_id = src.home_canonical_id
      AND fb.away_canonical_id = src.away_canonical_id
      AND fb.league            = src.league
      -- season omitted from JOIN: all sources are slug now (#404), but
      -- date+canonical teams already uniquely identify the match, so keeping
      -- season out of the predicate is behaviour-stable (no fan-out).
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

UNION ALL

-- =============================================================================
-- Understat cascade
-- =============================================================================
SELECT
    COALESCE(fb.canonical_id, 'us_' || src.source_id)                  AS canonical_id,
    'understat'                                                        AS source,
    src.source_id,
    src.display_name,
    src.league,
    src.season,
    CASE WHEN fb.canonical_id IS NOT NULL THEN 'date_team_match'
         ELSE 'orphan' END                                             AS confidence,
    CAST(NULL AS double)                                               AS match_score
FROM us_resolved src
LEFT JOIN fbref_base fb
       ON fb.match_date        = src.match_date
      AND fb.home_canonical_id = src.home_canonical_id
      AND fb.away_canonical_id = src.away_canonical_id
      AND fb.league            = src.league
      -- season omitted from JOIN: all sources are slug now (#404), but
      -- date+canonical teams already uniquely identify the match, so keeping
      -- season out of the predicate is behaviour-stable (no fan-out).
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

UNION ALL

-- =============================================================================
-- SofaScore cascade
-- =============================================================================
SELECT
    COALESCE(fb.canonical_id, 'ss_' || src.source_id)                  AS canonical_id,
    'sofascore'                                                        AS source,
    src.source_id,
    src.display_name,
    src.league,
    src.season,
    CASE WHEN fb.canonical_id IS NOT NULL THEN 'date_team_match'
         ELSE 'orphan' END                                             AS confidence,
    CAST(NULL AS double)                                               AS match_score
FROM ss_resolved src
LEFT JOIN fbref_base fb
       ON fb.match_date        = src.match_date
      AND fb.home_canonical_id = src.home_canonical_id
      AND fb.away_canonical_id = src.away_canonical_id
      AND fb.league            = src.league
      -- season omitted from JOIN: all sources are slug now (#404), but
      -- date+canonical teams already uniquely identify the match, so keeping
      -- season out of the predicate is behaviour-stable (no fan-out).
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

UNION ALL

-- =============================================================================
-- FotMob cascade
-- =============================================================================
SELECT
    COALESCE(fb.canonical_id, 'fm_' || src.source_id)                  AS canonical_id,
    'fotmob'                                                           AS source,
    src.source_id,
    src.display_name,
    src.league,
    src.season,
    CASE WHEN fb.canonical_id IS NOT NULL THEN 'date_team_match'
         ELSE 'orphan' END                                             AS confidence,
    CAST(NULL AS double)                                               AS match_score
FROM fm_resolved src
LEFT JOIN fbref_base fb
       ON fb.match_date        = src.match_date
      AND fb.home_canonical_id = src.home_canonical_id
      AND fb.away_canonical_id = src.away_canonical_id
      AND fb.league            = src.league
      -- season omitted from JOIN: all sources are slug now (#404), but
      -- date+canonical teams already uniquely identify the match, so keeping
      -- season out of the predicate is behaviour-stable (no fan-out).
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

UNION ALL

-- =============================================================================
-- MatchHistory cascade  (synthetic source_id)
-- =============================================================================
SELECT
    COALESCE(fb.canonical_id, src.source_id)                           AS canonical_id,
    'matchhistory'                                                     AS source,
    src.source_id,
    src.display_name,
    src.league,
    src.season,
    CASE WHEN fb.canonical_id IS NOT NULL THEN 'date_team_match'
         ELSE 'orphan' END                                             AS confidence,
    CAST(NULL AS double)                                               AS match_score
FROM mh_resolved src
LEFT JOIN fbref_base fb
       ON fb.match_date        = src.match_date
      AND fb.home_canonical_id = src.home_canonical_id
      AND fb.away_canonical_id = src.away_canonical_id
      AND fb.league            = src.league
      -- season omitted from JOIN: all sources are slug now (#404), but
      -- date+canonical teams already uniquely identify the match, so keeping
      -- season out of the predicate is behaviour-stable (no fan-out).
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

UNION ALL

-- =============================================================================
-- ESPN cascade
-- =============================================================================
SELECT
    COALESCE(fb.canonical_id, 'es_' || src.source_id)                  AS canonical_id,
    'espn'                                                             AS source,
    src.source_id,
    src.display_name,
    src.league,
    src.season,
    CASE WHEN fb.canonical_id IS NOT NULL THEN 'date_team_match'
         ELSE 'orphan' END                                             AS confidence,
    CAST(NULL AS double)                                               AS match_score
FROM es_resolved src
LEFT JOIN fbref_base fb
       ON fb.match_date        = src.match_date
      AND fb.home_canonical_id = src.home_canonical_id
      AND fb.away_canonical_id = src.away_canonical_id
      AND fb.league            = src.league
      -- season omitted from JOIN: all sources are slug now (#404), but
      -- date+canonical teams already uniquely identify the match, so keeping
      -- season out of the predicate is behaviour-stable (no fan-out).
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

),  -- end unioned

-- =============================================================================
-- Dedup to the PK (canonical_id, source) — #809
-- =============================================================================
-- The cascade bridges to the FBref spine on (date, teams, league) WITHOUT
-- season (#404), emitting each source row's OWN season. When a source's bronze
-- schedule carries one physical match under two season labels (ESPN '2021' is a
-- byte-for-byte copy of '1920'), that source emits two rows sharing one
-- canonical_id — violating the declared PK. Keep exactly one row per
-- (canonical_id, source), preferring the row whose season matches the FBref
-- spine's season for that canonical (the ground truth), so the bogus duplicate
-- season is dropped rather than the real one. Single-row groups (the normal
-- case, and all orphans which carry a unique source-prefixed canonical_id) keep
-- rn=1 unchanged — behaviour-stable except for the intended dedup.
fbref_season AS (
    -- One season per canonical from the FBref spine. MIN()+GROUP BY guarantees
    -- a single row per canonical so the LEFT JOIN below cannot fan out.
    SELECT canonical_id, MIN(season) AS fb_season
    FROM unioned
    WHERE source = 'fbref'
    GROUP BY canonical_id
)
SELECT
    canonical_id,
    source,
    source_id,
    display_name,
    league,
    season,
    confidence,
    match_score
FROM (
    SELECT
        u.*,
        ROW_NUMBER() OVER (
            PARTITION BY u.canonical_id, u.source
            ORDER BY CASE WHEN u.season = fs.fb_season THEN 0 ELSE 1 END, u.season
        ) AS _rn
    FROM unioned u
    LEFT JOIN fbref_season fs ON fs.canonical_id = u.canonical_id
)
WHERE _rn = 1
