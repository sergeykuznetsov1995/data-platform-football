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
--   iceberg.silver.sofascore_league_table   (единственный источник с #1590)
--   iceberg.silver.xref_team                 (canonical team-id resolve)
-- PK:           (league, season, team_id)   -- group_id is attribute (WC groups)
-- Partitioning: (league, season)   -- passed by run_gold_transform()
--
-- Source (#1590): FotMob whole-table fallback (#702) удалён вместе со старым
--   FotMob Silver — остаётся только SofaScore. standings_source несёт провенанс
--   строки ('sofascore'); колонка остаётся для нового FotMob Silver.
--
-- Team resolution (Migrated from gold.entity_xref to silver.xref_team in E1.5,
--                  2026-05-09):
--   LEFT JOIN iceberg.silver.xref_team on (source=<src>, source_id=team_name,
--                                          league, season)
--   * matched   -> team_id = xref_team.canonical_id, team_id_source='fbref_canonical'
--   * orphan    -> team_id = 'ss_<slug>',            team_id_source='sofascore_orphan'
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
-- #913 Phase 4: group_id (WC 12 groups of 4). position = ROW_NUMBER() partitioned
-- by (league, season, group_id) so each group has its own 1..N ranking.
-- group_id NULL for club leagues and WC knockout. PK remains (league, season, team_id).
--
-- Notes:
--   * SofaScore Pts уже post-deduction; R7 trust-check deferred.
--   * position is derived (ROW_NUMBER) — SofaScore не хранит rank.
--   * points_per_game uses NULLIF(played, 0) to guard against zero-game teams.
--   * season — 4-char slug ('2526') в Silver-источнике и в xref_team (#404).
-- =============================================================================

with ss_raw as (
    -- SofaScore источник (primary), резолв canonical через xref_team.
    select
        s.league,
        s.season,
        s.team_name                                       as team_name_raw,
        -- #1590: bigint casts keep the pre-removal column types (the FotMob
        -- UNION branch was bigint and widened SofaScore's integer counters).
        cast(s.played as bigint)                          as played,
        cast(s.wins as bigint)                            as wins,
        cast(s.draws as bigint)                           as draws,
        cast(s.losses as bigint)                          as losses,
        cast(s.goals_for as bigint)                       as goals_for,
        cast(s.goals_against as bigint)                   as goals_against,
        cast(s.goal_diff as bigint)                       as goal_diff,
        cast(s.points as bigint)                          as points,
        s.group_id,
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

unioned as (
    -- #1590: single source (FotMob fallback removed).
    select * from ss_raw
)

select
    league,
    season,
    coalesce(
        canonical_team_id,
        'ss_' || lower(regexp_replace(team_name_raw, '[^a-zA-Z0-9]+', '_'))
    )                                                     as team_id,
    case
        when canonical_team_id is not null   then 'fbref_canonical'
        else                                      'sofascore_orphan'
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
    group_id,
    cast(
        row_number() over (
            partition by league, season, coalesce(group_id, '')
            order by points desc, goal_diff desc, goals_for desc
        ) as integer
    )                                                     as position,
    cast(points as double) / nullif(played, 0)            as points_per_game,
    snapshot_at,
    cast(snapshot_at as date)                             as as_of_date
from unioned
-- Scope to the canonical season universe (gold.dim_season, rendered from
-- configs/medallion/competitions.yaml). The Silver standings source carries
-- SofaScore HISTORICAL seasons beyond the platform's FBref spine
-- (e.g. 2010/11-2015/16) that dim_season does NOT list; without this filter
-- they orphan ref_integrity[fct_standings.season -> dim_season] and fail
-- validate_gold_quality. dim_season is a tiny config dim materialised in an
-- earlier Gold layer (s2a_config_dims), so it always exists when this runs.
where season in (select season from iceberg.gold.dim_season)
