-- =============================================================================
-- Gold: dim_manager (SCD-2, per-match granularity)
-- =============================================================================
-- One row per (manager × team × stint). A "stint" is a continuous run of
-- matches in which the same manager_canonical_id headed the same
-- team_canonical_id. The boundary between two stints is detected via
-- LAG(manager) over (team ORDER BY match_date) — any row where the
-- manager differs from the previous row starts a new stint.
--
-- Sources:
--   iceberg.bronze.fbref_match_managers   (FBref scorebox parser)
--   iceberg.silver.fbref_match_enriched   (typed match_date, dedup'd schedule)
--   iceberg.silver.xref_manager           (canonical manager id per source-season)
--   iceberg.silver.xref_team              (canonical team id per source-season)
--
-- PK:           (manager_id_canonical, team_id_canonical, valid_from)
-- Partitioning: NONE  (master-data dim, ~50-200 rows for APL)
-- Validity:     [valid_from, valid_to)  — closed-open intervals.
--               valid_to IS NULL means the stint is current.
--
-- Multi-season stints (e.g. Arteta @ Arsenal 2019-12-20 → present): one row,
-- season = season of the FIRST match in the stint. Downstream "manager X
-- in season Y" queries should JOIN on
--   valid_from <= season_end AND (valid_to IS NULL OR valid_to > season_start)
-- rather than equality on dim_manager.season.
--
-- KNOWN LIMITATION: a manager who quit Team A and later returned to the same
-- team will produce TWO stints with the same (manager_id_canonical,
-- team_id_canonical) pair distinguished by valid_from. PK includes
-- valid_from for exactly this reason.
-- =============================================================================

WITH manager_match_log AS (
    -- Resolve team_canonical_id via the silver schedule view rather than
    -- joining xref_team on the raw scorebox team label. The scorebox uses
    -- the long form ("Brighton & Hove Albion"), while bronze.fbref_schedule
    -- uses the short form ("Brighton") — xref_team is built from schedule,
    -- so a direct join on m.team would silently drop ~5-15% of rows.
    -- Going through silver.fbref_match_enriched.{home,away} gives us the
    -- short form that xref_team actually has.
    SELECT
        m.match_id,
        m.team                                            AS raw_team,
        m.manager_name                                    AS raw_manager_name,
        m.league,
        m.season,
        s.date                                            AS match_date,
        CASE m.side WHEN 'home' THEN s.home ELSE s.away END  AS schedule_team,
        xm.canonical_id                                   AS manager_canonical_id,
        xm.display_name                                   AS manager_display_name
    FROM iceberg.bronze.fbref_match_managers m
    -- Match date + schedule short-team-name come from the typed/dedup'd
    -- Silver view.
    INNER JOIN iceberg.silver.fbref_match_enriched s
        ON s.match_id = m.match_id
    -- xref_manager has per-(source, source_id, league, season) rows; the
    -- (league, season) predicate is mandatory — without it, a manager who
    -- worked across seasons would fan-out 1.5-4× (memory:
    -- feedback_xref_join_season_predicate.md).
    INNER JOIN iceberg.silver.xref_manager xm
        ON  xm.source     = 'fbref'
        AND xm.source_id  = m.manager_name
        AND xm.league     = m.league
        AND xm.season     = CAST(m.season AS varchar)
    WHERE m.manager_name IS NOT NULL
      AND m.manager_name <> ''
),

manager_match_log_resolved AS (
    SELECT
        mml.*,
        xt.canonical_id                                   AS team_canonical_id
    FROM manager_match_log mml
    INNER JOIN iceberg.silver.xref_team xt
        ON  xt.source     = 'fbref'
        AND xt.source_id  = mml.schedule_team
        AND xt.league     = mml.league
        AND xt.season     = CAST(mml.season AS varchar)
),

-- Detect stint boundaries: a NEW stint starts whenever the manager for a
-- given team changes compared to the previous chronological match (or it's
-- the first match for that team).
stints_marked AS (
    SELECT
        manager_canonical_id,
        manager_display_name,
        team_canonical_id,
        match_date,
        league,
        season,
        CASE
            WHEN LAG(manager_canonical_id) OVER (
                PARTITION BY team_canonical_id ORDER BY match_date
            ) = manager_canonical_id THEN 0
            ELSE 1
        END                                               AS is_new_stint
    FROM manager_match_log_resolved
),

-- Cumulative SUM of the new-stint marker = a unique stint_id within each
-- team. Classic "islands and gaps" pattern.
stints_grouped AS (
    SELECT
        manager_canonical_id,
        manager_display_name,
        team_canonical_id,
        match_date,
        league,
        season,
        SUM(is_new_stint) OVER (
            PARTITION BY team_canonical_id ORDER BY match_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                 AS stint_id
    FROM stints_marked
),

stint_boundaries AS (
    SELECT
        manager_canonical_id,
        ANY_VALUE(manager_display_name)                   AS display_name,
        team_canonical_id,
        stint_id,
        MIN(match_date)                                   AS valid_from,
        MIN(league)                                       AS league,
        CAST(MIN(season) AS varchar)                      AS season
    FROM stints_grouped
    GROUP BY manager_canonical_id, team_canonical_id, stint_id
),

with_valid_to AS (
    SELECT
        manager_canonical_id,
        display_name,
        team_canonical_id,
        league,
        season,
        valid_from,
        LEAD(valid_from) OVER (
            PARTITION BY team_canonical_id ORDER BY valid_from
        )                                                 AS valid_to
    FROM stint_boundaries
)

SELECT
    manager_canonical_id                                  AS manager_id_canonical,
    display_name,
    team_canonical_id                                     AS team_id_canonical,
    league,
    season,
    valid_from,
    valid_to,
    valid_to IS NULL                                      AS is_current
FROM with_valid_to
