-- =============================================================================
-- Gold: fct_sofascore_team_match_post_shot_xg
-- =============================================================================
-- Explicit SofaScore-only team-match post-shot xG fact (#876).
--
-- Grain / PK: (match_id, team_id).
-- Metric: SUM(shotmap.xgot) over on-target shots for one team in one match.
--
-- This table is intentionally separate from cross-source fct_team_match and
-- from shot-level fct_shot.psxg.  SofaScore xGOT is not COALESCE-compatible
-- with a different provider/model.  A downstream model may select it with an
-- explicit source policy, but may not silently mix it into another source's
-- metric.  We publish only matches with exactly two teams and only team rows
-- whose every on-target shot has an xgot value; zero on-target shots is a real
-- 0.0, while an unmodelled on-target shot suppresses the row instead of being
-- naively treated as zero.
-- =============================================================================

WITH source_shots AS (
    SELECT
        match_id,
        team_id,
        shot_id,
        is_sot,
        xgot,
        league,
        season
    FROM iceberg.silver.sofascore_shots
    WHERE shot_source = 'sofascore_v1'
      AND match_id IS NOT NULL
      AND team_id IS NOT NULL
),

valid_matches AS (
    SELECT match_id, league, season
    FROM source_shots
    GROUP BY match_id, league, season
    HAVING COUNT(DISTINCT team_id) = 2
),

team_rollup AS (
    SELECT
        s.match_id,
        s.team_id,
        COUNT(*)                                            AS shot_count,
        COUNT_IF(s.is_sot)                                  AS shots_on_target,
        COUNT_IF(s.is_sot AND s.xgot IS NOT NULL)           AS modeled_shots_on_target,
        COUNT_IF(s.is_sot AND s.xgot IS NULL)               AS unmodeled_shots_on_target,
        SUM(
            CASE
                WHEN s.is_sot THEN s.xgot
                ELSE CAST(0.0 AS double)
            END
        )                                                   AS post_shot_xg,
        s.league,
        s.season
    FROM source_shots s
    INNER JOIN valid_matches v
        ON v.match_id = s.match_id
       AND v.league   = s.league
       AND v.season   = s.season
    GROUP BY s.match_id, s.team_id, s.league, s.season
),

complete_team_rollup AS (
    SELECT *
    FROM team_rollup
    WHERE unmodeled_shots_on_target = 0
      AND post_shot_xg IS NOT NULL
)

SELECT
    team.match_id,
    team.team_id,
    opponent.team_id                                      AS opponent_id,
    team.post_shot_xg,
    opponent.post_shot_xg                                 AS post_shot_xg_against,
    team.shot_count,
    team.shots_on_target,
    team.modeled_shots_on_target,
    CAST('sofascore' AS varchar)                          AS metric_source,
    CAST('sum_shotmap_xgot_on_target' AS varchar)         AS metric_definition,
    CAST('sofascore_xgot_v1' AS varchar)                  AS metric_version,
    team.league,
    team.season

FROM complete_team_rollup team
INNER JOIN complete_team_rollup opponent
    ON opponent.match_id = team.match_id
   AND opponent.league   = team.league
   AND opponent.season   = team.season
   AND opponent.team_id <> team.team_id
