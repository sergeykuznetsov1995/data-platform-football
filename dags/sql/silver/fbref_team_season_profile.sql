-- =============================================================================
-- Silver: fbref_team_season_profile
-- =============================================================================
--
-- One wide row per team / league / season — analogue of player_season_profile,
-- intended as the primary feed for Gold team-level facts and features
-- (atk strength, def strength, possession trend, GK aggregates).
--
-- Sources (all from iceberg.bronze):
--   fbref_team_stats        (s)   -- standard team season stats (base table)
--   fbref_team_shooting     (sh)  -- team shooting stats
--   fbref_team_misc         (mi)  -- team misc stats (cards, fouls, OG, ...)
--   fbref_team_playingtime  (pt)  -- team playing-time stats
--   fbref_keeper_keeper     (gk)  -- per-player GK stats, aggregated to team level
--
-- Deduplication:
--   ROW_NUMBER() OVER (PARTITION BY team_id, league, season
--                       ORDER BY _ingested_at DESC) => rn = 1
--
-- Notes:
--   * Bronze team tables already have proper bigint/double types (unlike
--     player tables which are VARCHAR), so TRY_CAST is only needed for
--     keeper aggregates (player-level VARCHAR -> typed team aggregates).
--   * Special chars in column names need quoting: "# pl", "90s", "g+a",
--     "g-pk", "gls_1", "sot%", "g/sh", "g/sot", "2crdy", "int",
--     "mn/mp", "min%", "+/-", "+/-90", "mn/start", "mn/sub", "save%", "cs%".
--   * GK metrics aggregated by (squad, league, season) — one team has
--     multiple GKs across a season; we sum counters and weighted-average
--     save%/cs% by minutes.
--   * goals_against_avg derived from team gls vs sum of GK ga (sanity check).
--   * Partitioning by (league, season) is applied externally by Python CTAS.
-- =============================================================================

WITH s AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY team_id, league, season
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fbref_team_stats
),

sh AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY team_id, league, season
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fbref_team_shooting
),

mi AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY team_id, league, season
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fbref_team_misc
),

pt AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY team_id, league, season
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fbref_team_playingtime
),

-- Per-player GK rows -> team aggregates. Dedup first, then aggregate.
gk_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY player_id, squad, league, season
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fbref_keeper_keeper
),

gk_agg AS (
    SELECT
        squad,
        league,
        season,
        SUM(TRY_CAST(ga AS INTEGER))                                     AS gk_goals_against,
        SUM(TRY_CAST(saves AS INTEGER))                                  AS gk_saves,
        SUM(TRY_CAST(sota AS INTEGER))                                   AS gk_shots_on_target_against,
        SUM(TRY_CAST(cs AS INTEGER))                                     AS gk_clean_sheets,
        SUM(TRY_CAST(min AS INTEGER))                                    AS gk_minutes,
        -- weighted save% by SOTA (more shots = more weight)
        CASE WHEN SUM(TRY_CAST(sota AS INTEGER)) > 0
             THEN 100.0 * SUM(TRY_CAST(saves AS INTEGER))
                  / SUM(TRY_CAST(sota AS INTEGER))
             ELSE NULL END                                               AS gk_save_pct,
        SUM(TRY_CAST(pkatt AS INTEGER))                                  AS gk_pk_attempts_faced,
        SUM(TRY_CAST(pka AS INTEGER))                                    AS gk_pk_allowed,
        SUM(TRY_CAST(pksv AS INTEGER))                                   AS gk_pk_saved
    FROM gk_dedup
    WHERE rn = 1
    GROUP BY squad, league, season
)

SELECT
    -- ========= Identification =========
    s.squad                                          AS team,
    s.team_id,
    s.league,
    -- season → slug ('2425'); FBref bronze stores year-start bigint (2024).
    -- JOINs below stay on the native year-start (all intra-FBref), convert once here.
    -- #913 Phase 2
    CASE WHEN s.league = 'INT-World Cup'
         THEN LPAD(CAST(s.season AS varchar), 4, '0')
         ELSE LPAD(CAST(MOD(s.season, 100) AS varchar), 2, '0')
              || LPAD(CAST(MOD(s.season + 1, 100) AS varchar), 2, '0')
    END AS season,

    -- ========= Standard stats (s) =========
    s."# pl"                                         AS players_used,
    s.age                                            AS avg_age,
    s.poss                                           AS possession,
    s.mp,
    s.starts,
    s.min                                            AS minutes,
    s."90s"                                          AS minutes_90s,
    s.gls                                            AS goals,
    s.ast                                            AS assists,
    s."g+a"                                          AS goals_assists,
    s."g-pk"                                         AS goals_non_penalty,
    s.pk                                             AS penalty_goals,
    s.pkatt                                          AS penalty_attempts,
    s.crdy                                           AS yellow_cards,
    s.crdr                                           AS red_cards,
    s.gls_1                                          AS goals_per_90,
    s.ast_1                                          AS assists_per_90,
    s."g+a_1"                                        AS goals_assists_per_90,
    s."g-pk_1"                                       AS non_penalty_goals_per_90,
    s."g+a-pk"                                       AS goals_assists_minus_penalty_per_90,

    -- ========= Shooting (sh) =========
    sh.sh                                            AS total_shots,
    sh.sot                                           AS shots_on_target,
    sh."sot%"                                        AS shot_on_target_pct,
    sh."sh/90"                                       AS shots_per_90,
    sh."sot/90"                                      AS shots_on_target_per_90,
    sh."g/sh"                                        AS goals_per_shot,
    sh."g/sot"                                       AS goals_per_shot_on_target,

    -- ========= Misc (mi) =========
    mi."2crdy"                                       AS second_yellow_cards,
    mi.fls                                           AS fouls_committed,
    mi.fld                                           AS fouls_drawn,
    mi.off                                           AS offsides,
    mi.crs                                           AS crosses,
    mi."int"                                         AS interceptions,
    mi.tklw                                          AS tackles_won,
    mi.pkwon                                         AS penalties_won,
    mi.pkcon                                         AS penalties_conceded,
    mi.og                                            AS own_goals,

    -- ========= Playing Time (pt) =========
    pt."mn/mp"                                       AS minutes_per_match,
    pt."min%"                                        AS minutes_pct,
    pt."mn/start"                                    AS minutes_per_start,
    pt.compl                                         AS complete_matches,
    pt.subs                                          AS substitutions,
    pt."mn/sub"                                      AS minutes_per_sub,
    pt.unsub                                         AS unused_subs,
    pt.ppm                                           AS points_per_match,
    pt.ong                                           AS on_field_goals,
    pt.onga                                          AS on_field_goals_against,
    pt."+/-"                                         AS plus_minus,
    pt."+/-90"                                       AS plus_minus_per_90,

    -- ========= Goalkeeper Aggregates (gk) =========
    gk.gk_goals_against,
    gk.gk_saves,
    gk.gk_shots_on_target_against,
    gk.gk_clean_sheets                               AS clean_sheets,
    gk.gk_minutes,
    gk.gk_save_pct                                   AS save_pct,
    gk.gk_pk_attempts_faced,
    gk.gk_pk_allowed,
    gk.gk_pk_saved,
    -- per-90 GA (defensive strength signal)
    CASE WHEN s."90s" > 0
         THEN gk.gk_goals_against / s."90s"
         ELSE NULL END                               AS goals_against_per_90,

    -- ========= Lineage =========
    s._ingested_at                                   AS _bronze_ingested_at

FROM s
LEFT JOIN sh
    ON  s.team_id = sh.team_id
    AND s.league  = sh.league
    AND s.season  = sh.season
    AND sh.rn     = 1
LEFT JOIN mi
    ON  s.team_id = mi.team_id
    AND s.league  = mi.league
    AND s.season  = mi.season
    AND mi.rn     = 1
LEFT JOIN pt
    ON  s.team_id = pt.team_id
    AND s.league  = pt.league
    AND s.season  = pt.season
    AND pt.rn     = 1
LEFT JOIN gk_agg gk
    ON  s.squad  = gk.squad
    AND s.league = gk.league
    AND s.season = gk.season
WHERE s.rn = 1
  AND s.team_id IS NOT NULL
  AND s.league  IS NOT NULL
  AND s.season  IS NOT NULL
