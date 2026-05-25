-- =============================================================================
-- Gold: dim_venue
-- =============================================================================
-- Global dimension of football venues (stadiums) consolidated from FBref and
-- ESPN match feeds. One row per canonical venue.
--
-- Sources:
--   iceberg.silver.fbref_match_enriched   (sch.venue per match)
--   iceberg.bronze.espn_matchsheet        (latest snapshot per (game, team))
--
-- PK:           venue_id  ('venue_<xxhash64-hex>' from LOWER(TRIM(name)))
-- Partitioning: NONE  (small global dim, queried by venue_id / venue_slug)
--
-- Source-of-truth selection (R0.4 schema versioning):
--   venue_canonical = MAX(name) FILTER (src='fbref'),
--                     fallback MAX(name) FILTER (src='espn')
--   venue_source    = 'fbref' if FBref carried the venue, else 'espn'
--   venue_version   = 'v1'        (frozen until selection rule changes)
--
-- Hash-PK pattern mirrors entity_xref.sql:346-352 (`mh_<hash>` for MatchHistory).
-- 'venue_' prefix avoids collision with team / referee namespace.
--
-- Notes:
--   * city / country are NULL placeholders — geocoding is an R8 follow-up.
--   * league/season columns capture the most-frequent / most-recent context;
--     they are informational (MAX-arbitrary) and NOT a partition key.
--   * Venues with NULL/empty name are filtered out at source.
--   * ESPN season comes as 4-char encoded label ('2526') -> normalized to
--     BIGINT 2025 to align with silver/FBref season type.
-- =============================================================================

with fbref_venues as (
    select
        trim(venue)                                  as venue_raw,
        league,
        season,
        date                                         as match_date,
        'fbref'                                      as src
    from iceberg.silver.fbref_match_enriched
    where venue is not null
      and trim(venue) <> ''
),

espn_dedup as (
    select
        trim(venue)                                  as venue_raw,
        league,
        try_cast(substr(season, 1, 2) as bigint) + 2000  as season,
        try_cast(substr(game, 1, 10) as date)        as match_date,
        'espn'                                       as src,
        row_number() over (
            partition by trim(venue), league, season, game
            order by _ingested_at desc
        )                                            as rn
    from iceberg.bronze.espn_matchsheet
    where venue is not null
      and trim(venue) <> ''
),

espn_venues as (
    select venue_raw, league, season, match_date, src
    from espn_dedup
    where rn = 1
),

unioned as (
    select * from fbref_venues
    union all
    select * from espn_venues
),

aggregated as (
    select
        'venue_' || lower(to_hex(xxhash64(to_utf8(lower(trim(venue_raw))))))  as venue_id,
        max(venue_raw) filter (where src = 'fbref')                            as venue_fbref,
        max(venue_raw) filter (where src = 'espn')                             as venue_espn,
        count(distinct match_date)                                             as n_matches,
        min(match_date)                                                        as first_seen_date,
        max(match_date)                                                        as last_seen_date,
        max(league)                                                            as league,
        max(season)                                                            as season,
        bool_or(src = 'fbref')                                                 as has_fbref
    from unioned
    group by lower(trim(venue_raw))
)

select
    venue_id,
    coalesce(venue_fbref, venue_espn)                                          as venue_canonical,
    case when has_fbref then 'fbref' else 'espn' end                           as venue_source,
    'v1'                                                                       as venue_version,
    venue_fbref,
    venue_espn,
    lower(regexp_replace(coalesce(venue_fbref, venue_espn), '[^a-zA-Z0-9]+', '_'))  as venue_slug,
    cast(null as varchar)                                                      as city,
    cast(null as varchar)                                                      as country,
    n_matches,
    first_seen_date,
    last_seen_date,
    league,
    season
from aggregated
