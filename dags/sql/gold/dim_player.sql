-- =============================================================================
-- Gold: dim_player
-- =============================================================================
-- Canonical player dimension, aligned to the star-schema design (issue #425).
-- One row per player — NO season in the grain: the dim keeps only what a
-- player carries "forever" (name, dob, nationality, height, foot, position).
-- Age is derived from dob at query time; current team lives in fct_lineup /
-- fct_player_season_stats, not here.
--
-- Sources: FBref spine (silver.xref_player, source='fbref') enriched via
-- COALESCE from FotMob / SofaScore / Transfermarkt / SoFIFA — same per-source
-- CTE pattern as dim_player_attributes.sql (which stays as the no-winner
-- per-source snapshot; THIS dim picks winning values).
--
-- Priorities per attribute (memory: T4 dim_player_attributes):
--   dob:         FotMob > SofaScore > Transfermarkt > SoFIFA
--   nationality: FotMob > SofaScore > Transfermarkt > SoFIFA > FBref code
--   height_cm:   Transfermarkt (official club profile) > FotMob > SofaScore > SoFIFA
--   foot:        Transfermarkt > FotMob > SofaScore
--   position:    SoFIFA > FBref (first token of 'pos', e.g. 'MF,FW' -> 'MF')
--
-- PK:           player_id  (canonical 'fb_<fbref_id>' from silver.xref_player)
-- Partitioning: NONE  (global dim — star design: dims are unpartitioned)
--
-- Coverage caveat: FotMob/SofaScore/TM/SoFIFA profiles cover current-APL
-- players only — dob/height/foot stay NULL for most historical players, and
-- nationality falls back to the FBref 3-letter code ('ENG') instead of the
-- full name. Mixed format is a deliberate completeness trade-off; the
-- code->name mapping is a tracked followup.
--
-- Season is NOT a JOIN key anywhere here (snapshot grain — MAX_BY(.., season)
-- takes each source's freshest value), so the xref (league, season) fan-out
-- footgun does not apply.
-- =============================================================================

WITH
fbref_latest AS (
    SELECT
        player_id,
        MAX_BY(player, season)  AS player_name,
        MAX_BY(nation, season)  AS nation,
        MAX_BY(pos,    season)  AS pos
    FROM iceberg.silver.fbref_player_season_profile
    WHERE player_id IS NOT NULL
    GROUP BY player_id
),

fotmob_latest AS (
    SELECT
        player_id,
        MAX_BY(player_name,   season) AS player_name,
        MAX_BY(date_of_birth, season) AS date_of_birth,
        MAX_BY(nationality,   season) AS nationality,
        MAX_BY(height_cm,     season) AS height_cm,
        MAX_BY(foot,          season) AS foot
    FROM iceberg.silver.fotmob_player_profile
    WHERE player_id IS NOT NULL
    GROUP BY player_id
),

-- SofaScore / Transfermarkt / SoFIFA silver already carry canonical_id
-- (filled at Silver materialisation via the xref_player bridge) — direct
-- JOIN, no xref hop.
sofascore_latest AS (
    SELECT
        canonical_id,
        MAX_BY(player_name,    season) AS player_name,
        MAX_BY(date_of_birth,  season) AS date_of_birth,
        MAX_BY(nationality,    season) AS nationality,
        MAX_BY(height_cm,      season) AS height_cm,
        MAX_BY(preferred_foot, season) AS preferred_foot
    FROM iceberg.silver.sofascore_player_profile
    WHERE canonical_id IS NOT NULL
    GROUP BY canonical_id
),

transfermarkt_latest AS (
    SELECT
        canonical_id,
        MAX_BY(name,        season) AS player_name,
        MAX_BY(dob,         season) AS dob,
        MAX_BY(nationality, season) AS nationality,
        MAX_BY(height_cm,   season) AS height_cm,
        MAX_BY(foot,        season) AS foot
    FROM iceberg.silver.transfermarkt_players
    WHERE canonical_id IS NOT NULL
    GROUP BY canonical_id
),

sofifa_latest AS (
    SELECT
        canonical_id,
        MAX_BY(dob,         season) AS dob,
        MAX_BY(nationality, season) AS nationality,
        MAX_BY(height_cm,   season) AS height_cm,
        MAX_BY(position,    season) AS position
    FROM iceberg.silver.sofifa_player_profile
    WHERE canonical_id IS NOT NULL
    GROUP BY canonical_id
),

-- FBref-spine: один row per canonical_id (canonical = 'fb_' || source_id by
-- resolver construction; orphan filter kept for symmetry).
xref_fbref AS (
    SELECT DISTINCT
        canonical_id,
        source_id AS fbref_player_id
    FROM iceberg.silver.xref_player
    WHERE source = 'fbref'
      AND confidence <> 'orphan'
),

xref_fotmob_latest AS (
    SELECT canonical_id, fotmob_player_id
    FROM (
        SELECT
            canonical_id,
            source_id AS fotmob_player_id,
            ROW_NUMBER() OVER (
                PARTITION BY canonical_id
                ORDER BY season DESC
            ) AS rn
        FROM iceberg.silver.xref_player
        WHERE source = 'fotmob'
    )
    WHERE rn = 1
)

SELECT
    xf.canonical_id                                                AS player_id,
    COALESCE(fb.player_name, fm.player_name,
             ss.player_name, tm.player_name)                       AS player_name,
    -- FotMob/SoFIFA dob are source-typed passthroughs — TRY_CAST keeps the
    -- COALESCE chain DATE-typed (non-ISO strings degrade to NULL, and the
    -- next source wins).
    COALESCE(TRY_CAST(fm.date_of_birth AS DATE),
             ss.date_of_birth,
             tm.dob,
             TRY_CAST(sf.dob AS DATE))                             AS dob,
    COALESCE(fm.nationality, ss.nationality, tm.nationality,
             sf.nationality,
             REGEXP_EXTRACT(fb.nation, '[A-Z]{3}'))                AS nationality,
    COALESCE(tm.height_cm, fm.height_cm,
             ss.height_cm, sf.height_cm)                           AS height_cm,
    LOWER(COALESCE(tm.foot, fm.foot, ss.preferred_foot))           AS preferred_foot,
    -- First position token: SoFIFA 'ST, RW' -> 'ST'; FBref 'MF,FW' -> 'MF'.
    COALESCE(REGEXP_EXTRACT(sf.position, '^[A-Za-z]+'),
             REGEXP_EXTRACT(fb.pos,      '^[A-Za-z]+'))            AS primary_position
FROM xref_fbref xf
LEFT JOIN fbref_latest fb
    ON fb.player_id = xf.fbref_player_id
LEFT JOIN xref_fotmob_latest xfm
    ON xfm.canonical_id = xf.canonical_id
LEFT JOIN fotmob_latest fm
    ON fm.player_id = xfm.fotmob_player_id
LEFT JOIN sofascore_latest ss
    ON ss.canonical_id = xf.canonical_id
LEFT JOIN transfermarkt_latest tm
    ON tm.canonical_id = xf.canonical_id
LEFT JOIN sofifa_latest sf
    ON sf.canonical_id = xf.canonical_id
