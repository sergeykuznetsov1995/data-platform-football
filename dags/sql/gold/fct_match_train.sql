-- =============================================================================
-- Gold: fct_match_train
-- =============================================================================
-- ML training split. Earliest ~80% of completed matches per season.
--
-- Sources:
--   iceberg.gold.fct_match        — wide pre-match features + base targets
--   iceberg.gold.match_outcomes   — full target set (over_2_5, over_3_5, etc.)
--
-- PK: match_id
-- Partitioning: (season)
--
-- Split logic (time-based, leakage-safe):
--   * Only completed matches participate (is_completed = TRUE).
--   * Within each season, rank matches by date DESCENDING.
--   * The most recent CEIL(season_total * 0.2) rows -> test.
--   * Everything else -> train.
--
-- WHY time-based per-season split: random splits leak future info via rolling
-- features (a match in train can have features computed from a match in test).
-- WHY per-season: a 20% global tail would put one whole season in test;
-- per-season keeps coverage uniform across cohorts.
-- WHY only completed matches: future fixtures have NULL targets and belong to
-- the inference dataset, not training/evaluation.
-- WHY split_ids CTE keyed on match_outcomes (not fct_match): keeps the m.*
-- projection clean (no leftover ROW_NUMBER columns) and Trino has no SELECT
-- ... EXCEPT (col) syntax to drop them post-hoc.
-- =============================================================================

WITH split_ids AS (
    SELECT
        o.match_id,
        ROW_NUMBER() OVER (
            PARTITION BY o.season
            ORDER BY o.match_date DESC, o.match_id DESC
        ) AS recency_rn,
        COUNT(*) OVER (PARTITION BY o.season) AS season_total
    FROM iceberg.gold.match_outcomes o
    WHERE o.is_completed = TRUE
)
SELECT
    m.*,
    -- Targets sourced from match_outcomes (fct_match has the basic ones;
    -- this adds over/under markets and the explicit win/draw indicators).
    o.over_2_5,
    o.over_3_5,
    o.home_win,
    o.draw,
    o.away_win
FROM iceberg.gold.fct_match m
JOIN iceberg.gold.match_outcomes o
    ON o.match_id = m.match_id
JOIN split_ids s
    ON s.match_id = m.match_id
WHERE m.is_completed = TRUE
  AND o.is_completed = TRUE
  -- Train = everything OUTSIDE the recent-20% tail.
  AND s.recency_rn > CAST(CEIL(CAST(s.season_total AS DOUBLE) * 0.2) AS BIGINT)
