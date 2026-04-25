-- =============================================================================
-- Gold: fct_match_test
-- =============================================================================
-- ML test split — most recent ~20% of completed matches per season. Mirror of
-- fct_match_train (same source query, opposite filter on recency_rn).
--
-- Sources:
--   iceberg.gold.fct_match        — wide pre-match features + base targets
--   iceberg.gold.match_outcomes   — full target set (over_2_5, over_3_5, etc.)
--
-- PK: match_id
-- Partitioning: (season)
--
-- Disjointness with fct_match_train: enforced via the same recency_rn boundary
-- (rn <= ceil(N*0.2) here vs. > ceil(N*0.2) there). DQ runs an explicit
-- INNER JOIN sanity check on top of that to catch any drift.
--
-- Future / inference matches are intentionally excluded — those go to a
-- separate inference dataset (T4.2), never into evaluation.
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
  -- Test = the recent-20% tail per season.
  AND s.recency_rn <= CAST(CEIL(CAST(s.season_total AS DOUBLE) * 0.2) AS BIGINT)
