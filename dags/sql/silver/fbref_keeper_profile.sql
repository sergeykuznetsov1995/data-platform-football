-- =============================================================================
-- Silver: fbref_keeper_profile
-- =============================================================================
--
-- One row per goalkeeper / squad / league / season (#463).
-- FBref emits one row per (player, squad): a winter transfer inside the
-- league keeps BOTH club rows — Gold collapses them (max-minutes club).
--
-- Sources (all from iceberg.bronze):
--   fbref_player_stats      (s)  -- standard stats, filtered to pos LIKE '%GK%'
--   fbref_keeper_keeper     (k)  -- goalkeeper-specific stats
--   fbref_player_shooting   (sh) -- shooting stats (GKs occasionally shoot)
--   fbref_player_misc       (mi) -- miscellaneous stats
--
-- Deduplication (#463):
--   ROW_NUMBER() OVER (PARTITION BY player_id, squad, league, season
--                       ORDER BY _ingested_at DESC, _batch_id DESC)  =>  rn = 1
--   _batch_id breaks _ingested_at ties deterministically between rebuilds.
--   JOINs carry squad — otherwise multi-squad players fan out 2×2.
--
-- Notes:
--   * Base table (s) is filtered to goalkeepers only: s.pos LIKE '%GK%'
--   * Keeper columns use special chars: "save%", "cs%", "save%_1".
--   * misc columns "2crdy" and "int" start with digit / are reserved words => quoted.
--   * Partitioning by (league, season) is applied externally by Python CTAS.
--   * All numeric columns use TRY_CAST to enforce proper types in Silver.
-- =============================================================================

WITH s AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY player_id, squad, league, season
               ORDER BY _ingested_at DESC, _batch_id DESC
           ) AS rn
    FROM iceberg.bronze.fbref_player_stats
    WHERE pos LIKE '%GK%'
),

k AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY player_id, squad, league, season
               ORDER BY _ingested_at DESC, _batch_id DESC
           ) AS rn
    FROM iceberg.bronze.fbref_keeper_keeper
),

sh AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY player_id, squad, league, season
               ORDER BY _ingested_at DESC, _batch_id DESC
           ) AS rn
    FROM iceberg.bronze.fbref_player_shooting
),

mi AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY player_id, squad, league, season
               ORDER BY _ingested_at DESC, _batch_id DESC
           ) AS rn
    FROM iceberg.bronze.fbref_player_misc
)

SELECT
    -- ========= Identification (VARCHAR) =========
    s.player,
    s.player_id,
    s.nation,
    s.pos,
    s.squad,
    s.age,
    TRY_CAST(s.born AS INTEGER)                     AS born,

    -- ========= Standard Stats (s) =========
    TRY_CAST(s.mp AS INTEGER)                       AS mp,
    TRY_CAST(s.starts AS INTEGER)                   AS starts,
    TRY_CAST(s.min AS INTEGER)                      AS minutes,
    TRY_CAST(s."90s" AS DOUBLE)                     AS minutes_90s,
    TRY_CAST(s.gls AS INTEGER)                      AS goals,
    TRY_CAST(s.ast AS INTEGER)                      AS assists,
    TRY_CAST(s."g+a" AS INTEGER)                    AS goals_assists,
    TRY_CAST(s."g-pk" AS INTEGER)                   AS goals_non_penalty,
    TRY_CAST(s.pk AS INTEGER)                       AS penalty_goals,
    TRY_CAST(s.pkatt AS INTEGER)                    AS penalty_attempts,
    TRY_CAST(s.crdy AS INTEGER)                     AS yellow_cards,
    TRY_CAST(s.crdr AS INTEGER)                     AS red_cards,
    TRY_CAST(s.gls_1 AS DOUBLE)                     AS goals_per90,
    TRY_CAST(s.ast_1 AS DOUBLE)                     AS assists_per90,
    TRY_CAST(s."g+a_1" AS DOUBLE)                   AS goals_assists_per90,
    TRY_CAST(s."g-pk_1" AS DOUBLE)                  AS non_penalty_goals_per90,
    TRY_CAST(s."g+a-pk" AS DOUBLE)                  AS goals_assists_minus_penalty_per90,

    -- ========= Goalkeeping (k) =========
    TRY_CAST(k.ga AS INTEGER)                       AS goals_against,
    TRY_CAST(k.ga90 AS DOUBLE)                      AS goals_against_per90,
    TRY_CAST(k.sota AS INTEGER)                     AS shots_on_target_against,
    TRY_CAST(k.saves AS INTEGER)                    AS saves,
    TRY_CAST(k."save%" AS DOUBLE)                   AS save_pct,
    TRY_CAST(k.w AS INTEGER)                        AS wins,
    TRY_CAST(k.d AS INTEGER)                        AS draws,
    TRY_CAST(k.l AS INTEGER)                        AS losses,
    TRY_CAST(k.cs AS INTEGER)                       AS clean_sheets,
    TRY_CAST(k."cs%" AS DOUBLE)                     AS clean_sheet_pct,
    TRY_CAST(k.pkatt AS INTEGER)                    AS pk_faced,
    TRY_CAST(k.pka AS INTEGER)                      AS pk_allowed,
    TRY_CAST(k.pksv AS INTEGER)                     AS pk_saved,
    TRY_CAST(k.pkm AS INTEGER)                      AS pk_missed,
    TRY_CAST(k."save%_1" AS DOUBLE)                 AS pk_save_pct,

    -- ========= Shooting (sh) =========
    TRY_CAST(sh.sh AS INTEGER)                      AS shots,
    TRY_CAST(sh.sot AS INTEGER)                     AS shots_on_target,
    TRY_CAST(sh."sot%" AS DOUBLE)                   AS shot_on_target_pct,
    TRY_CAST(sh."sh/90" AS DOUBLE)                  AS shots_per90,
    TRY_CAST(sh."sot/90" AS DOUBLE)                 AS shots_on_target_per90,
    TRY_CAST(sh."g/sh" AS DOUBLE)                   AS goals_per_shot,
    TRY_CAST(sh."g/sot" AS DOUBLE)                  AS goals_per_shot_on_target,

    -- ========= Miscellaneous (mi) =========
    TRY_CAST(mi."2crdy" AS INTEGER)                 AS second_yellow,
    TRY_CAST(mi.fls AS INTEGER)                     AS fouls_committed,
    TRY_CAST(mi.fld AS INTEGER)                     AS fouls_drawn,
    TRY_CAST(mi.off AS INTEGER)                     AS offsides,
    TRY_CAST(mi.crs AS INTEGER)                     AS crosses,
    TRY_CAST(mi."int" AS INTEGER)                   AS interceptions,
    TRY_CAST(mi.tklw AS INTEGER)                    AS tackles_won,
    TRY_CAST(mi.pkwon AS INTEGER)                   AS penalties_won,
    TRY_CAST(mi.pkcon AS INTEGER)                   AS penalties_conceded,
    TRY_CAST(mi.og AS INTEGER)                      AS own_goals,

    -- ========= Lineage =========
    s._ingested_at                                  AS _bronze_ingested_at,

    -- ========= Partition Keys =========
    -- season → slug ('2425'); FBref bronze stores year-start bigint (2024).
    -- JOINs above stay on the native year-start (all intra-FBref), convert once here.
    s.league,
    LPAD(CAST(MOD(s.season,     100) AS varchar), 2, '0')
        || LPAD(CAST(MOD(s.season + 1, 100) AS varchar), 2, '0') AS season

FROM s
LEFT JOIN k
    ON  s.player_id = k.player_id
    AND s.squad     = k.squad
    AND s.league    = k.league
    AND s.season    = k.season
    AND k.rn        = 1
LEFT JOIN sh
    ON  s.player_id = sh.player_id
    AND s.squad     = sh.squad
    AND s.league    = sh.league
    AND s.season    = sh.season
    AND sh.rn       = 1
LEFT JOIN mi
    ON  s.player_id = mi.player_id
    AND s.squad     = mi.squad
    AND s.league    = mi.league
    AND s.season    = mi.season
    AND mi.rn       = 1
WHERE s.rn = 1
