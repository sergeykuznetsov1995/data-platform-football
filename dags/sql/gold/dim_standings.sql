-- =============================================================================
-- Gold: dim_standings
-- =============================================================================
-- Per-(league, season, team) league table snapshot from SofaScore. One row per
-- team per league/season. Carries position, points, goals, GD, points-per-game.
--
-- Source:       iceberg.bronze.sofascore_league_table
-- PK:           (league, season, team_id)
-- Partitioning: (league, season)   -- passed by run_gold_transform()
--
-- Team resolution:
--   LEFT JOIN iceberg.gold.entity_xref on (entity_type='team', source='sofascore')
--   * matched   -> team_id = entity_xref.canonical_id, team_id_source='fbref_canonical'
--   * orphan    -> team_id = 'ss_<slug>',              team_id_source='sofascore_orphan'
--   Orphan prefix mirrors entity_xref.sql convention for unresolved sources.
--
-- Source-of-truth selection (R0.4 schema versioning):
--   team_id_source   carries the resolution path (no separate _source/_version
--                    column — the standings row IS sourced from SofaScore by
--                    construction; the _version applies to the snapshot rule).
--
-- Snapshot semantics:
--   Bronze table is APPEND-mode -> deduplicated here via ROW_NUMBER ordered by
--   _ingested_at DESC, taking the latest snapshot per (league, season, team).
--   snapshot_at  = _ingested_at of the surviving row.
--   as_of_date   = DATE(snapshot_at) -- daily granularity for downstream joins.
--
-- Notes:
--   * SofaScore Pts already reflects post-deduction; R7 trust-check deferred.
--   * position is derived (ROW_NUMBER) — SofaScore exposes it implicitly via
--     order in the league-table feed but does not store the rank column.
--   * points_per_game uses NULLIF(mp, 0) to guard against zero-game teams.
--   * SofaScore 'season' is encoded as 4-char label ('2526') -> normalized to
--     BIGINT 2025 to align with silver/FBref season type.
-- =============================================================================

with standings_raw as (
    select
        league,
        try_cast(substr(season, 1, 2) as bigint) + 2000  as season,
        team                                              as team_name_raw,
        cast(mp as integer)                               as mp,
        cast(w  as integer)                               as wins,
        cast(d  as integer)                               as draws,
        cast(l  as integer)                               as losses,
        cast(gf as integer)                               as goals_for,
        cast(ga as integer)                               as goals_against,
        cast(gd as integer)                               as goal_diff,
        cast(pts as integer)                              as points,
        _ingested_at                                      as snapshot_at,
        row_number() over (
            partition by league, season, team
            order by _ingested_at desc
        )                                                 as rn
    from iceberg.bronze.sofascore_league_table
    where team is not null
      and trim(team) <> ''
),

standings_latest as (
    select
        league,
        season,
        team_name_raw,
        mp,
        wins,
        draws,
        losses,
        goals_for,
        goals_against,
        goal_diff,
        points,
        snapshot_at,
        cast(snapshot_at as date)                         as as_of_date
    from standings_raw
    where rn = 1
),

resolved as (
    select
        s.*,
        x.canonical_id                                    as canonical_team_id
    from standings_latest s
    left join iceberg.gold.entity_xref x
      on  x.entity_type = 'team'
      and x.source      = 'sofascore'
      and x.source_id   = s.team_name_raw
      and x.league      = s.league
      and x.season      = s.season
)

select
    league,
    season,
    coalesce(
        canonical_team_id,
        'ss_' || lower(regexp_replace(team_name_raw, '[^a-zA-Z0-9]+', '_'))
    )                                                     as team_id,
    case
        when canonical_team_id is not null then 'fbref_canonical'
        else 'sofascore_orphan'
    end                                                   as team_id_source,
    team_name_raw,
    mp,
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
    cast(points as double) / nullif(mp, 0)                as points_per_game,
    snapshot_at,
    as_of_date
from resolved
