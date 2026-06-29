-- =============================================================================
-- Gold: fct_standings
-- =============================================================================
-- Per-(league, season, team) league table snapshot. One row per team per
-- league/season. Carries position, points, goals, GD, points-per-game.
--
-- Design contract: docs/design/gold-star-schema.md §5.5 (issue #428).
-- #428: переименовано из dim_standings — снапшот по командам это факт, не
-- справочник (grain-конфликт с dim_*). Колонка mp → played (дизайн-имя §5.5).
-- Ограничение (честно): снапшот «на сейчас», не по турам — point-in-time
-- реконструкция из fct_team_match это этаж 2, не здесь.
--
-- Source (#702 — Gold one-hop: читаем Silver, не Bronze напрямую):
--   iceberg.silver.sofascore_league_table   (primary)
--   iceberg.silver.fotmob_team_standings     (fallback, 2-й источник)
--   iceberg.silver.xref_team                 (canonical team-id resolve)
-- PK:           (league, season, team_id)
-- Partitioning: (league, season)   -- passed by run_gold_transform()
--
-- Multi-source dedup (#702 — SofaScore-primary, FotMob whole-table fallback):
--   На каждую (league, season) берём ОДИН источник целиком: если SofaScore
--   отдал таблицу — берём SofaScore; иначе FotMob. Команды из двух источников
--   НЕ смешиваются в одной (league, season) — position считается внутри одного
--   источника. standings_source несёт провенанс строки ('sofascore'/'fotmob').
--
-- Team resolution (Migrated from gold.entity_xref to silver.xref_team in E1.5,
--                  2026-05-09; #702 added FotMob block):
--   LEFT JOIN iceberg.silver.xref_team on (source=<src>, source_id=team_name,
--                                          league, season)
--   * matched   -> team_id = xref_team.canonical_id, team_id_source='fbref_canonical'
--   * orphan    -> team_id = '<ss|fm>_<slug>',       team_id_source='<src>_orphan'
--   The JOIN excludes confidence='orphan' xref rows (#460): they carry a
--   non-NULL source-prefixed canonical_id, so without the filter they'd be
--   mislabeled 'fbref_canonical'. xref footgun: предикаты league И season
--   обязательны — иначе ×1.5-4 fan-out (CLAUDE.md / xref_team keyed per-season).
--
-- Snapshot semantics:
--   Silver tables conform APPEND-mode Bronze (dedup ROW_NUMBER уже в Silver).
--   snapshot_at = _bronze_ingested_at сохранившейся строки.
--   as_of_date  = DATE(snapshot_at) -- daily granularity for downstream joins.
--
-- Notes:
--   * SofaScore/FotMob Pts уже post-deduction; R7 trust-check deferred.
--   * position is derived (ROW_NUMBER) единообразно для обоих источников —
--     SofaScore не хранит rank; FotMob хранит, но мы пересчитываем для общего
--     code path (each (league, season) уже из одного источника).
--   * points_per_game uses NULLIF(played, 0) to guard against zero-game teams.
--   * season — 4-char slug ('2526') в обоих Silver-источниках и в xref_team (#404).
-- =============================================================================

with ss_raw as (
    -- SofaScore источник (primary), резолв canonical через xref_team.
    select
        s.league,
        s.season,
        s.team_name                                       as team_name_raw,
        s.played,
        s.wins,
        s.draws,
        s.losses,
        s.goals_for,
        s.goals_against,
        s.goal_diff,
        s.points,
        s._bronze_ingested_at                             as snapshot_at,
        x.canonical_id                                    as canonical_team_id,
        'sofascore'                                       as standings_source
    from iceberg.silver.sofascore_league_table s
    left join iceberg.silver.xref_team x
      on  x.source      = 'sofascore'
      and x.source_id   = s.team_name
      and x.league      = s.league
      and x.season      = s.season
      and x.confidence <> 'orphan'
),

fm_raw as (
    -- FotMob источник (fallback), резолв canonical через xref_team.
    select
        f.league,
        f.season,
        f.team_name                                       as team_name_raw,
        f.played,
        f.wins,
        f.draws,
        f.losses,
        f.goals_for,
        f.goals_against,
        f.goal_diff,
        f.points,
        f._bronze_ingested_at                             as snapshot_at,
        x.canonical_id                                    as canonical_team_id,
        'fotmob'                                          as standings_source
    from iceberg.silver.fotmob_team_standings f
    left join iceberg.silver.xref_team x
      on  x.source      = 'fotmob'
      and x.source_id   = f.team_name
      and x.league      = f.league
      and x.season      = f.season
      and x.confidence <> 'orphan'
),

ss_keys as (
    select distinct league, season from ss_raw
),

unioned as (
    -- SofaScore целиком; FotMob только для (league, season) без SofaScore.
    -- NOT EXISTS (а не NOT IN) — NULL-safe anti-join.
    select * from ss_raw
    union all
    select * from fm_raw f
    where not exists (
        select 1 from ss_keys k
        where k.league = f.league and k.season = f.season
    )
)

select
    league,
    season,
    coalesce(
        canonical_team_id,
        case standings_source
            when 'sofascore' then 'ss_'
            when 'fotmob'    then 'fm_'
        end || lower(regexp_replace(team_name_raw, '[^a-zA-Z0-9]+', '_'))
    )                                                     as team_id,
    case
        when canonical_team_id is not null   then 'fbref_canonical'
        when standings_source = 'sofascore'  then 'sofascore_orphan'
        else                                      'fotmob_orphan'
    end                                                   as team_id_source,
    standings_source,
    team_name_raw,
    played,
    wins,
    draws,
    losses,
    goals_for,
    goals_against,
    goal_diff,
    points,
    cast(
        row_number() over (
            partition by league, season
            order by points desc, goal_diff desc, goals_for desc
        ) as integer
    )                                                     as position,
    cast(points as double) / nullif(played, 0)            as points_per_game,
    snapshot_at,
    cast(snapshot_at as date)                             as as_of_date
from unioned
