-- =============================================================================
-- Silver: sofascore_shots
-- =============================================================================
-- Shot-grained projection of SofaScore shotmap, canonicalised to the same
-- match/team/player IDs as gold.fct_shot so the two shot sources can be
-- cross-validated (gold.fct_shot_audit). One row per shot.
--
-- Issue #602 — consume the previously write-only bronze.sofascore_event_shotmap
-- (audit #476). Consumers:
--   * gold.fct_shot_audit — cross-validates Understat vs SofaScore (xg, counts).
--   * gold.fct_shot (#699) — SofaScore is a MATCH-LEVEL fallback source here.
-- Shots have no shared key across sources (Understat shot_id != SofaScore
-- shot_id for the same physical shot), so per-shot COALESCE does NOT apply; the
-- fct_shot merge instead picks ONE source per match (Understat primary). This
-- projection is canonicalised to fct_shot's IDs/enums precisely so it can drop
-- straight into that fallback branch.
--
-- Source:
--   iceberg.bronze.sofascore_event_shotmap  — shot-level events (xg, xgot, ...)
--   iceberg.bronze.sofascore_schedule       — game_id -> home/away team names
--   iceberg.silver.xref_match               — sofascore game_id -> fbref match_id
--   iceberg.silver.xref_team                — sofascore team name -> canonical id
--   iceberg.silver.xref_player              — sofascore player_id -> canonical id
--
-- PK: (match_id, shot_id)
-- Partitioning: (league, season) — applied externally by run_silver_transform.
--
-- =============================================================================
-- Bronze schema (verified via DESCRIBE on 2026-06-16)
-- =============================================================================
-- #840: Bronze is now auto-passthrough (source-key names, all fields kept).
-- Renamed keys: minute->"time", x->player_coordinates_x, y->player_coordinates_y,
-- period->reversed_period_count, outcome->incident_type. The COALESCE(old,new)
-- in the `sm` CTE bridges pre-#840 partitions until every partition re-scrapes.
--   match_id             varchar   -- SofaScore game_id, stringified (= xref source_id)
--   shot_id              varchar   -- SofaScore shot id (or composite fallback)
--   player_id            varchar   -- SofaScore player id, stringified
--   team_id              varchar   -- 100% NULL upstream -> team derived from is_home
--   is_home              boolean   -- TRUE = home side took the shot
--   "time"               bigint    -- (old: minute)
--   player_coordinates_x double    -- (old: x) on a 0..100 grid (NOT 0..1!)
--   player_coordinates_y double    -- (old: y) on a 0..100 grid (NOT 0..1!)
--   xg                   double    -- 0..1 expected goals
--   xgot                 double    -- 0..1 expected goals on target (post-shot xG proxy)
--   shot_type            varchar   -- {miss, block, save, goal, post}  <- this is the RESULT
--   incident_type        varchar   -- (old: outcome) always 'shot' upstream -> USELESS, ignored
--   goal_type            varchar   -- {NULL, regular, penalty, own} (non-NULL iff goal)
--   body_part            varchar   -- {right-foot, left-foot, head, other}
--   situation            varchar   -- {assisted, regular, fast-break, corner, free-kick,
--                                  --  penalty, set-piece, throw-in-set-piece}
--   league, season       varchar   -- (season is slug '2526')
--
-- Live-probe note (2026-06-16): bronze has APL 2526 only, 9543 shots; x in
-- [0.4, 89.3], y in [0.8, 99.5] -> 0..100 grid, normalised to 0..1 below.
--
-- =============================================================================
-- Enum normalisation -> fct_shot domain (so the audit compares like-for-like)
-- =============================================================================
--   result    (from shot_type + goal_type):
--     goal (+goal_type='own')  -> own_goal
--     goal                     -> goal
--     save                     -> saved
--     block                    -> blocked
--     miss                     -> off_target
--     post                     -> post
--   body_part: right-foot|left-foot -> foot; head -> head; else -> other
--   situation: assisted|regular|fast-break -> open_play; corner -> corner;
--     free-kick -> free_kick; penalty -> penalty;
--     set-piece|throw-in-set-piece -> set_piece
--   is_goal = shot_type = 'goal'        (own goals included — ended in net)
--   is_sot  = shot_type IN ('goal','save')  (on target = scored or keeper-saved)
--
-- =============================================================================
-- Output schema
-- =============================================================================
--   shot_id       varchar   -- raw SofaScore shot id
--   match_id      varchar   -- fbref match_id (8-hex) via xref_match, else
--                           --   'sofascore_<raw>' for unbridged
--   team_id       varchar   -- canonical via is_home -> home/away xref_team;
--                           --   NULL when team unresolved (orphan-tolerant)
--   player_id     varchar   -- canonical via xref_player; orphan 'ss_<raw>'
--   is_home       boolean
--   minute        integer
--   x, y          double    -- 0..1 normalised (raw/100); orientation NOT
--                           --   guaranteed to match Understat — coords are not
--                           --   used by the audit, kept for analysis only
--   body_part     varchar   -- foot | head | other | NULL
--   situation     varchar   -- open_play | corner | free_kick | set_piece | penalty
--   result        varchar   -- goal | saved | blocked | off_target | post | own_goal
--   is_goal       boolean
--   is_sot        boolean
--   xg            double    -- 0..1
--   xgot          double    -- 0..1 (SofaScore-unique; fct_shot.psxg is NULL)
--   shot_source   varchar   -- literal 'sofascore_v1'
--   league        varchar
--   season        varchar   -- slug '2526'
--
-- Footguns:
--   * ALL xref JOINs carry (league, season) predicate — else 1.5-4x fan-out
--     (memory: feedback_xref_join_season_predicate). PK = (source, source_id,
--     league, season) on xref_team/xref_player, (source, source_id, season) on
--     xref_match — joining on source+source_id alone fans out per season.
--   * match_id/player_id are already varchar in bronze (no double-cast).
--     schedule.game_id is BIGINT -> CAST(... AS varchar) to join.
--   * team_id in bronze is 100% NULL -> team derived from is_home.
-- =============================================================================

WITH

-- 1) Dedup bronze shotmap (re-scrape protection; replace_partitions=True) ------
sm AS (
    SELECT
        match_id,
        shot_id,
        player_id,
        is_home,
        -- #840: Bronze no longer renames/derives; do it here. COALESCE bridges
        -- old (pre-#840) partitions and freshly re-scraped ones. "time" is a
        -- Trino reserved word -> MUST be double-quoted.
        COALESCE(minute, "time")            AS minute,
        COALESCE(x, player_coordinates_x)   AS x,
        COALESCE(y, player_coordinates_y)   AS y,
        xg,
        xgot,
        shot_type,
        situation,
        goal_type,
        body_part,
        league,
        season,
        ROW_NUMBER() OVER (
            PARTITION BY match_id, shot_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.sofascore_event_shotmap
    WHERE match_id IS NOT NULL
      AND shot_id  IS NOT NULL
),

-- 2) Schedule dedup + resolve home/away team -> canonical (for is_home) -------
sched AS (
    SELECT game_id, home_team, away_team, league, season
    FROM (
        SELECT
            game_id,
            -- #840: Bronze auto-passthrough renamed home_team->home_team_name;
            -- COALESCE bridges pre-#840 partitions.
            COALESCE(home_team, home_team_name) AS home_team,
            COALESCE(away_team, away_team_name) AS away_team,
            league, season,
            ROW_NUMBER() OVER (
                PARTITION BY game_id
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.sofascore_schedule
        WHERE game_id IS NOT NULL
    )
    WHERE rn = 1
),

sched_resolved AS (
    SELECT
        CAST(s.game_id AS varchar)   AS ss_match_id,
        s.league,
        s.season,
        xth.canonical_id             AS home_canonical_id,
        xta.canonical_id             AS away_canonical_id
    FROM sched s
    LEFT JOIN iceberg.silver.xref_team xth
           ON xth.source    = 'sofascore'
          AND xth.source_id = s.home_team
          AND xth.league    = s.league
          AND xth.season    = s.season
    LEFT JOIN iceberg.silver.xref_team xta
           ON xta.source    = 'sofascore'
          AND xta.source_id = s.away_team
          AND xta.league    = s.league
          AND xta.season    = s.season
),

-- 3) SofaScore game_id -> fbref canonical match_id ---------------------------
xref_match_ss AS (
    SELECT source_id, league, season, canonical_id
    FROM iceberg.silver.xref_match
    WHERE source = 'sofascore'
)

SELECT
    s.shot_id,
    COALESCE(xm.canonical_id, 'sofascore_' || s.match_id)        AS match_id,

    -- team via is_home -> home/away canonical (orphan-tolerant)
    CASE
        WHEN s.is_home       THEN sr.home_canonical_id
        WHEN NOT s.is_home   THEN sr.away_canonical_id
        ELSE NULL
    END                                                          AS team_id,

    COALESCE(xp.canonical_id, 'ss_' || s.player_id)             AS player_id,

    s.is_home,
    CAST(s.minute AS integer)                                   AS minute,

    -- 0..100 grid -> 0..1 (orientation not guaranteed vs Understat)
    s.x / 100.0                                                 AS x,
    s.y / 100.0                                                 AS y,

    CASE
        WHEN s.body_part IN ('right-foot', 'left-foot') THEN 'foot'
        WHEN s.body_part = 'head'                       THEN 'head'
        WHEN s.body_part IS NULL                        THEN NULL
        ELSE 'other'
    END                                                         AS body_part,

    CASE
        WHEN s.situation IN ('assisted', 'regular', 'fast-break') THEN 'open_play'
        WHEN s.situation = 'corner'                               THEN 'corner'
        WHEN s.situation = 'free-kick'                            THEN 'free_kick'
        WHEN s.situation = 'penalty'                              THEN 'penalty'
        WHEN s.situation IN ('set-piece', 'throw-in-set-piece')   THEN 'set_piece'
        ELSE NULL
    END                                                         AS situation,

    CASE
        WHEN s.shot_type = 'goal' AND s.goal_type = 'own' THEN 'own_goal'
        WHEN s.shot_type = 'goal'                         THEN 'goal'
        WHEN s.shot_type = 'save'                         THEN 'saved'
        WHEN s.shot_type = 'block'                        THEN 'blocked'
        WHEN s.shot_type = 'miss'                         THEN 'off_target'
        WHEN s.shot_type = 'post'                         THEN 'post'
        ELSE NULL
    END                                                         AS result,

    (s.shot_type = 'goal')                                      AS is_goal,
    (s.shot_type IN ('goal', 'save'))                           AS is_sot,

    s.xg,
    s.xgot,

    CAST('sofascore_v1' AS varchar)                            AS shot_source,

    s.league,
    CAST(s.season AS varchar)                                  AS season

FROM sm s

LEFT JOIN xref_match_ss xm
       ON xm.source_id = s.match_id
      AND xm.league    = s.league
      AND xm.season    = s.season

LEFT JOIN sched_resolved sr
       ON sr.ss_match_id = s.match_id
      AND sr.league      = s.league
      AND sr.season      = s.season

LEFT JOIN iceberg.silver.xref_player xp
       ON xp.source    = 'sofascore'
      AND xp.source_id = s.player_id
      AND xp.league    = s.league
      AND xp.season    = s.season

WHERE s.rn = 1
