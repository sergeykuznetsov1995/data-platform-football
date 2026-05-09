-- =============================================================================
-- Gold: mart_event_heatmap (E7 BI mart)
-- =============================================================================
-- Per-(team, season, league, zone, action) SPADL action distribution for
-- pitch-heatmap visualisation. 12 column bins along x (length) × 8 row bins
-- along y (width) — gives a 12×8 grid that renders cleanly as a Superset
-- heatmap chart.
--
-- Source:
--   iceberg.gold.fct_event   -- ~695k rows, x/y in 0-100 SPADL convention
--                               (silver.whoscored_events_spadl — Opta-derived).
--   iceberg.gold.dim_team    -- team_name lookup (denormalised for BI).
--
-- Bin formula: FLOOR(x / (100.0 / 12)) clamped to [0, 11]; same for y/8.
-- Rows with NULL x or y are dropped (gives partial coverage instead of fake
-- 0,0 zone bias).
--
-- Filter: action_canonical != 'unknown' (drop meta-events: substitutions,
-- formation flags, etc. — they have no spatial meaning).
--
-- PK: (team_id, season, league, zone_x, zone_y, action_canonical)
-- =============================================================================

WITH binned AS (
    SELECT
        team_id_canonical                                          AS team_id,
        league,
        season,
        action_canonical,
        outcome_success,
        LEAST(11, GREATEST(0, CAST(FLOOR(x / (100.0 / 12)) AS integer))) AS zone_x,
        LEAST(7,  GREATEST(0, CAST(FLOOR(y / (100.0 / 8))  AS integer))) AS zone_y
    FROM iceberg.gold.fct_event
    WHERE action_canonical    <> 'unknown'
      AND team_id_canonical   IS NOT NULL
      AND x                   IS NOT NULL
      AND y                   IS NOT NULL
)

SELECT
    b.team_id,
    dt.team_name,
    b.season,
    b.league,
    b.zone_x,
    b.zone_y,
    b.action_canonical,
    COUNT(*)                                                       AS event_count,
    AVG(CAST(b.outcome_success AS double))                         AS success_rate
FROM binned b
LEFT JOIN iceberg.gold.dim_team dt
       ON dt.team_id = b.team_id
GROUP BY
    b.team_id,
    dt.team_name,
    b.season,
    b.league,
    b.zone_x,
    b.zone_y,
    b.action_canonical
