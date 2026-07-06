-- =============================================================================
-- Gold: fct_team_elo
-- =============================================================================
-- External team-strength rating (ELO) from ClubElo. One row per team per date.
-- Off-field fact — design contract: docs/design/gold-star-schema.md §5.10
-- (issue #431).
--
-- Source:       iceberg.bronze.clubelo_ratings
--               ∪ iceberg.bronze.clubelo_ratings_historical
--               + iceberg.silver.xref_team (source='clubelo')
-- PK:           (team_id, elo_date)
-- Partitioning: none -- passed as None by run_gold_transform()
--               (design §6 rule 5: small off-field table, no season key)
--
-- Why bronze (not silver): ClubElo has NO silver projection — it is read by
-- silver only as a name source for xref_team. This mirrors fct_standings,
-- which reads bronze.sofascore_league_table directly. ClubElo 'rank' lives
-- only in the ratings tables (clubelo_team_history carries neither rank nor
-- league), so the ratings tables are the only source bearing both design
-- columns (elo, rank) at a date grain. ratings (daily, fresh) and
-- ratings_historical (~365d weekly samples) overlap and are deduped below.
--
-- Team resolution (mirror of fct_standings):
--   LEFT JOIN silver.xref_team on (source='clubelo', source_id=team, league),
--   excluding confidence='orphan' rows (xref contract: orphans carry a non-NULL
--   source-prefixed canonical_id and must be excluded from every cross-source
--   Gold JOIN, else they'd be mislabeled 'fbref_canonical').
--   ClubElo has NO season → no season predicate (xref clubelo rows have
--   season IS NULL).
--   * matched -> team_id = xref_team.canonical_id, team_id_source='fbref_canonical'
--   * orphan  -> team_id = 'ce_<slug>',            team_id_source='clubelo_orphan'
--   Orphan prefix 'ce_' mirrors xref_team.sql.j2. Rows are never dropped
--   (design §6 rule 2); the orphan share is policed by DQ.
--
-- Scope: leagues that xref_team resolves for clubelo (rendered there from
--   configs/medallion/competitions.yaml in_scope flags). ClubElo tracks whole
--   national pyramids, so without this filter every out-of-scope club would
--   become a 'ce_' orphan and inflate the orphan rate. Sourcing the league
--   set from silver.xref_team keeps gold in lockstep with the silver scope —
--   flipping in_scope in competitions.yaml widens both layers at once.
--
-- Snapshot semantics:
--   ratings + ratings_historical overlap on (team, rating_date). Deduplicated
--   at the design PK grain via ROW_NUMBER ordered by _ingested_at DESC, taking
--   the freshest snapshot — guarantees a unique (team_id, elo_date).
-- =============================================================================

with in_scope_leagues as (
    -- League universe = what silver.xref_team resolves for clubelo (scoped by
    -- competitions.yaml in_scope at render time) — no hardcoded league here.
    select distinct league
    from iceberg.silver.xref_team
    where source = 'clubelo'
),

elo_raw as (
    select team, league, rating_date, elo, rank, _ingested_at
    from iceberg.bronze.clubelo_ratings
    where team is not null
      and league in (select league from in_scope_leagues)
    union all
    select team, league, rating_date, elo, rank, _ingested_at
    from iceberg.bronze.clubelo_ratings_historical
    where team is not null
      and league in (select league from in_scope_leagues)
),

resolved as (
    select
        coalesce(
            x.canonical_id,
            'ce_' || lower(regexp_replace(e.team, '[^a-zA-Z0-9]+', '_'))
        )                                                 as team_id,
        case
            when x.canonical_id is not null then 'fbref_canonical'
            else 'clubelo_orphan'
        end                                               as team_id_source,
        e.team                                            as team_name_raw,
        cast(e.rating_date as date)                       as elo_date,
        cast(e.elo as double)                             as elo,
        cast(e.rank as integer)                           as rank,
        e._ingested_at                                    as ingested_at
    from elo_raw e
    left join iceberg.silver.xref_team x
      on  x.source      = 'clubelo'
      and x.source_id   = e.team
      and x.league      = e.league
      and x.confidence <> 'orphan'
),

deduped as (
    select
        team_id,
        team_id_source,
        team_name_raw,
        elo_date,
        elo,
        rank,
        row_number() over (
            partition by team_id, elo_date
            order by ingested_at desc
        )                                                 as rn
    from resolved
)

select
    team_id,
    team_id_source,
    team_name_raw,
    elo_date,
    elo,
    rank
from deduped
where rn = 1
