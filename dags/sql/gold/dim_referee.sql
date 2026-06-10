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
-- Diacritic folding (issue #228, same idiom as xref_team.sql.j2 / #215):
--   Hash key, GROUP BY key and referee_slug all wrap the name in
--   NORMALIZE(NFD) + REGEXP_REPLACE('\p{Mn}+','') so "Çakır" and "Cakir"
--   fold to the SAME referee_id / slug. On pure-ASCII names this is a no-op,
--   so every existing referee_id is unchanged.
--
-- KNOWN LIMITATION (R8 follow-up):
--   Cross-source name reconciliation (FotMob / WhoScored / SofaScore) — fuzzy
--   matching of differently-spelled names for one official — is deferred to R8
--   once a second referee feed lands and a fuzzy-match resolver is justified.
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
        'ref_' || lower(to_hex(xxhash64(to_utf8(
            regexp_replace(normalize(lower(trim(referee_raw)), NFD), '\p{Mn}+', '')
        ))))                                                                  as referee_id,
        min(referee_raw)                                                       as referee_canonical,
        count(*)                                                               as n_matches,
        min(match_date)                                                        as first_seen_date,
        max(match_date)                                                        as last_seen_date,
        array_agg(distinct league)                                             as leagues,
        array_agg(distinct season)                                             as seasons  -- slug '2425' after #404 (was cast-to-int year-start)
    from referees_raw
    -- Fold diacritics in the dedup key so it matches the hash input above —
    -- otherwise "Çakır"/"Cakir" form two groups sharing one referee_id (PK dup).
    group by regexp_replace(normalize(lower(trim(referee_raw)), NFD), '\p{Mn}+', '')
)

select
    referee_id,
    referee_canonical,
    'fbref'                                                                    as referee_source,
    'v1'                                                                       as referee_version,
    lower(regexp_replace(
        regexp_replace(normalize(referee_canonical, NFD), '\p{Mn}+', ''),
        '[^a-zA-Z0-9]+', '_'))                                                as referee_slug,
    n_matches,
    first_seen_date,
    last_seen_date,
    leagues,
    seasons
from aggregated
