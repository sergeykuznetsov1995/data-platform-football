-- =============================================================================
-- Gold: match_outcomes
-- =============================================================================
-- ML target table — one row per match with classification/regression labels
-- ONLY (no features). Kept separate from `fct_match` so backtesting code can
-- join features <-> targets explicitly and never accidentally leak labels into
-- training inputs.
--
-- Sources: iceberg.silver.fbref_match_enriched + iceberg.silver.xref_team
-- PK: match_id
-- Partitioning: (league, season)  -- applied externally by gold_tasks CTAS wrapper
--
-- Migrated from gold.entity_xref to silver.xref_team in E1.5 (2026-05-09 prep).
-- Both silver.fbref_match_enriched.season and silver.xref_team.season are slug
-- varchar ('2425') after #404 — the JOIN is a direct season = season equality.
-- The (league, season) predicate prevents the 1.5-4x fan-out documented in
-- feedback_xref_join_season_predicate.md.
--
-- Targets (NULL while is_completed = false → safe for inference rows):
--   result_1x2  'H' | 'D' | 'A'
--   home_win, draw, away_win  BOOLEAN
--   total_goals INTEGER
--   btts        BOOLEAN  (both teams to score)
--   over_2_5    BOOLEAN
--   over_3_5    BOOLEAN
--
-- Notes:
--   * Silver is already deduplicated by match_id — straight SELECT, no ROW_NUMBER.
--   * `is_completed` lets backtesting filter out future fixtures while still
--     keeping them in the table for inference joins.
--   * Targets are emitted unconditionally; for !is_completed rows the score
--     comparisons evaluate to NULL (NULL > NULL → NULL) → all targets NULL.
-- =============================================================================

SELECT
    m.match_id,
    m.season,
    m.league,
    m.date            AS match_date,
    m.gameweek,

    m.home            AS home_team,
    m.away            AS away_team,
    home_x.canonical_id AS home_team_id,
    away_x.canonical_id AS away_team_id,

    m.home_score,
    m.away_score,

    -- ========= Classification targets =========
    CASE
        WHEN m.home_score > m.away_score THEN 'H'
        WHEN m.home_score < m.away_score THEN 'A'
        WHEN m.home_score = m.away_score THEN 'D'
    END                                                  AS result_1x2,
    (m.home_score >  m.away_score)                       AS home_win,
    (m.home_score =  m.away_score)                       AS draw,
    (m.home_score <  m.away_score)                       AS away_win,

    -- ========= Regression / totals targets =========
    (m.home_score + m.away_score)                        AS total_goals,
    (m.home_score >= 1 AND m.away_score >= 1)            AS btts,
    ((m.home_score + m.away_score) > 2)                  AS over_2_5,
    ((m.home_score + m.away_score) > 3)                  AS over_3_5,

    -- ========= Inference / backtesting filter =========
    -- Future fixtures arrive in Silver with NULL scores; treat them as "not
    -- completed" so backtesting drops them while inference keeps them.
    (m.home_score IS NOT NULL AND m.away_score IS NOT NULL) AS is_completed,

    -- ========= Lineage =========
    m._silver_created_at AS _silver_ingested_at

FROM iceberg.silver.fbref_match_enriched m
LEFT JOIN iceberg.silver.xref_team home_x
    ON home_x.source    = 'fbref'
   AND home_x.source_id = m.home
   AND home_x.league    = m.league
   AND home_x.season    = m.season
LEFT JOIN iceberg.silver.xref_team away_x
    ON away_x.source    = 'fbref'
   AND away_x.source_id = m.away
   AND away_x.league    = m.league
   AND away_x.season    = m.season
