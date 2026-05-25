-- =============================================================================
-- Gold: dim_referee
-- =============================================================================
-- Global dimension of match referees observed in FBref schedules. One row per
-- canonical referee.
--
-- Source:       iceberg.silver.fbref_match_enriched (referee per match)
-- PK:           referee_id  ('ref_<xxhash64-hex>' from LOWER(TRIM(name)))
-- Partitioning: NONE  (small global dim, queried by referee_id / referee_slug)
--
-- Source-of-truth selection (R0.4 schema versioning):
--   referee_canonical = MIN(referee_raw)   -- single source, MIN==stable
--   referee_source    = 'fbref'            -- only available source today
--   referee_version   = 'v1'
--
-- Hash-PK pattern mirrors entity_xref.sql:346-352 ('mh_<hash>' for MatchHistory).
-- 'ref_' prefix avoids collision with team / venue namespace.
--
-- KNOWN LIMITATION (R8 follow-up):
--   Hash key is ASCII-folded LOWER(TRIM(name)) — collisions across diacritics
--   are NOT handled. e.g. "Çakır" and "Cakir" hash to *different* venue_ids
--   even though they likely represent the same official. Cross-source name
--   reconciliation (FotMob / WhoScored / SofaScore) is deferred to R8 once a
--   second referee feed lands and a fuzzy-match resolver is justified.
-- =============================================================================

with referees_raw as (
    select
        trim(referee)                                as referee_raw,
        league,
        season,
        date                                         as match_date
    from iceberg.silver.fbref_match_enriched
    where referee is not null
      and trim(referee) <> ''
),

aggregated as (
    select
        'ref_' || lower(to_hex(xxhash64(to_utf8(lower(trim(referee_raw))))))  as referee_id,
        min(referee_raw)                                                       as referee_canonical,
        count(*)                                                               as n_matches,
        min(match_date)                                                        as first_seen_date,
        max(match_date)                                                        as last_seen_date,
        array_agg(distinct league)                                             as leagues,
        array_agg(distinct cast(season as integer))                            as seasons
    from referees_raw
    group by lower(trim(referee_raw))
)

select
    referee_id,
    referee_canonical,
    'fbref'                                                                    as referee_source,
    'v1'                                                                       as referee_version,
    lower(regexp_replace(referee_canonical, '[^a-zA-Z0-9]+', '_'))             as referee_slug,
    n_matches,
    first_seen_date,
    last_seen_date,
    leagues,
    seasons
from aggregated
