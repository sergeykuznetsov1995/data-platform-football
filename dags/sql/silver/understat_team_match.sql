-- =============================================================================
-- Silver: understat_team_match
-- =============================================================================
--
-- One row per (match_id, team_id_canonical) — Understat team-side match facts
-- (xG, NPxG, PPDA, deep completions, points/xPts, goals). Bronze is wide-form
-- (one row per match with home_*/away_* columns), so we UNION ALL home + away
-- to a long form, then JOIN `silver.xref_team` (source='understat') to expose
-- the canonical team id.
--
-- Issue #91 (T6 wave, T6.2). Design contract:
-- docs/decisions/T6_team_facts_schema.md §6.2 — us-marked columns.
--
-- Sources:
--   bronze.understat_team_match_stats (wide-form: home_* + away_* columns)
--   silver.xref_team                  (canonical_id ↔ understat team_id bridge)
--
-- Notes:
--   * (league, season) JOIN predicate against xref_team is mandatory
--     (see CLAUDE.md / feedback_xref_join_season_predicate.md) — without it
--     a team active across multiple seasons fan-outs 1.5-4×.
--   * Orphan rows (confidence='orphan' in xref_team) are filtered out:
--     they cannot bridge to FBref-spine in Gold layer anyway.
--   * Bronze dedup: defensive ROW_NUMBER on (game_id, league, season). Understat
--     ingest historically had append-mode duplicates (see CLAUDE.md /
--     feedback_replace_partitions_required.md); keep dedup even after the fix.
--   * `xg_against`, `npxg_against`, `oppda`, `deep_completions_allowed` are
--     derived from the opposite side of the same row.
--   * Season convention: passthrough varchar slug ('2526') — matches xref_team.
-- =============================================================================

WITH bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY game_id, league, season
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.understat_team_match_stats b
        WHERE game_id      IS NOT NULL
          AND home_team_id IS NOT NULL
          AND away_team_id IS NOT NULL
    )
    WHERE rn = 1
),

xt AS (
    SELECT canonical_id, source_id, league, season
    FROM iceberg.silver.xref_team
    WHERE source = 'understat'
      AND confidence <> 'orphan'
),

-- Home perspective: team = home, opponent = away
home AS (
    SELECT
        xt.canonical_id                                    AS team_id_canonical,
        CAST(b.game_id      AS varchar)                    AS match_id,
        CAST(b.home_team_id AS varchar)                    AS team_id,
        CAST(b.away_team_id AS varchar)                    AS opponent_team_id,
        TRUE                                               AS is_home,
        b.home_team                                        AS team_name,
        -- MODELED xG (Understat primary)
        b.home_xg                                          AS xg,
        b.away_xg                                          AS xg_against,
        b.home_np_xg                                       AS npxg,
        b.away_np_xg                                       AS npxg_against,
        -- Pressing / depth
        b.home_ppda                                        AS ppda,
        b.away_ppda                                        AS oppda,
        b.home_deep_completions                            AS deep_completions,
        b.away_deep_completions                            AS deep_completions_allowed,
        -- HARD_FACT counters
        CAST(b.home_goals  AS INTEGER)                     AS goals,
        CAST(b.away_goals  AS INTEGER)                     AS goals_against,
        CAST(b.home_points AS INTEGER)                     AS points,
        b.home_expected_points                             AS xpts,
        -- Lineage + partition keys
        b._ingested_at                                     AS _bronze_ingested_at,
        b.league,
        b.season
    FROM bronze_dedup b
    JOIN xt
      -- xref_team.source_id for Understat = display name string (e.g. 'Liverpool'),
      -- NOT the numeric `team_id`. See `silver/xref_team.sql.j2`.
      ON b.home_team = xt.source_id
     AND b.league    = xt.league
     AND b.season    = xt.season
),

-- Away perspective: team = away, opponent = home
away AS (
    SELECT
        xt.canonical_id                                    AS team_id_canonical,
        CAST(b.game_id      AS varchar)                    AS match_id,
        CAST(b.away_team_id AS varchar)                    AS team_id,
        CAST(b.home_team_id AS varchar)                    AS opponent_team_id,
        FALSE                                              AS is_home,
        b.away_team                                        AS team_name,
        b.away_xg                                          AS xg,
        b.home_xg                                          AS xg_against,
        b.away_np_xg                                       AS npxg,
        b.home_np_xg                                       AS npxg_against,
        b.away_ppda                                        AS ppda,
        b.home_ppda                                        AS oppda,
        b.away_deep_completions                            AS deep_completions,
        b.home_deep_completions                            AS deep_completions_allowed,
        CAST(b.away_goals  AS INTEGER)                     AS goals,
        CAST(b.home_goals  AS INTEGER)                     AS goals_against,
        CAST(b.away_points AS INTEGER)                     AS points,
        b.away_expected_points                             AS xpts,
        b._ingested_at                                     AS _bronze_ingested_at,
        b.league,
        b.season
    FROM bronze_dedup b
    JOIN xt
      ON b.away_team = xt.source_id
     AND b.league    = xt.league
     AND b.season    = xt.season
)

SELECT
    team_id_canonical,
    match_id,
    team_id,
    opponent_team_id,
    is_home,
    team_name,
    xg,
    xg_against,
    npxg,
    npxg_against,
    ppda,
    oppda,
    CAST(deep_completions         AS INTEGER) AS deep_completions,
    CAST(deep_completions_allowed AS INTEGER) AS deep_completions_allowed,
    goals,
    goals_against,
    points,
    xpts,
    _bronze_ingested_at,
    league,
    season
FROM home

UNION ALL

SELECT
    team_id_canonical,
    match_id,
    team_id,
    opponent_team_id,
    is_home,
    team_name,
    xg,
    xg_against,
    npxg,
    npxg_against,
    ppda,
    oppda,
    CAST(deep_completions         AS INTEGER) AS deep_completions,
    CAST(deep_completions_allowed AS INTEGER) AS deep_completions_allowed,
    goals,
    goals_against,
    points,
    xpts,
    _bronze_ingested_at,
    league,
    season
FROM away
