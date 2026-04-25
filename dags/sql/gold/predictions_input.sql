-- =============================================================================
-- Gold: predictions_input  (T4.2 — inference feature snapshot)
-- =============================================================================
-- One row per UPCOMING match within the next 7 days, carrying the same
-- pre-match features that fct_match_train/_test expose for completed matches.
--
-- Why a separate table (instead of querying fct_match directly at serve time):
--   * Materialised snapshot is O(1) for the model server.
--   * Schema is locked: any drift between train and inference is caught here.
--   * DAG cadence (every 2 h) matches a sensible feature TTL — no surprise
--     staleness from ad-hoc queries hitting the wide mart mid-Gold-rebuild.
--
-- Source: iceberg.gold.fct_match
--   * fct_match already JOINs dim_match (incl. future fixtures, scores NULL)
--     with feat_team_form / feat_team_xg_form / feat_team_h2h.
--   * Rolling features for a future match are valid: the feat_* windows use
--     ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING ORDER BY date — by definition
--     they aggregate ONLY prior matches, never the target. Same point-in-time
--     guarantee as in training (T3.4 DQ verifies this on every Gold run).
--
-- Filter:
--   * is_completed = FALSE        — score not yet known (true inference set).
--   * date in [CURRENT_DATE, +7d] — short serving horizon; feature freshness
--                                   degrades for matches further out anyway.
--
-- Targets (result_1x2, total_goals, btts, ...) are emitted unconditionally
-- and will all be NULL for these rows — that's the inference contract: the
-- model consumes features only, never reads a target column at serve time.
--
-- PK: match_id
-- Partitioning: (season)
-- =============================================================================

-- WHY explicit column list (instead of `m.*`):
--   1. The CTAS runner appends `_silver_created_at` to every Gold table; if we
--      propagate fct_match's own `_silver_created_at` via `m.*`, Trino fails
--      with DUPLICATE_COLUMN_NAME (Trino has no SELECT * EXCEPT syntax).
--   2. The explicit list is self-documenting — anyone adding a feature to
--      fct_match.sql now also adds it here, preserving train/inference parity.
--      A column missing from this list = silent feature drift between the
--      training set the model was fit on and the inference snapshot it scores.
--
-- HOW TO EXTEND: when fct_match.sql gains a new feature column, add it here
-- in the same order. The "values_match" DQ check in gold_tasks does not yet
-- enforce equality — keep this list in sync manually until then.
SELECT
    m.match_id,
    m.date,
    m.gameweek,
    m.home_team_id,
    m.away_team_id,
    m.home_team_name,
    m.away_team_name,
    m.home_score,
    m.away_score,
    m.result_1x2,
    m.total_goals,
    m.btts,
    m.is_completed,
    -- Home pre-match form (last 5)
    m.home_l5_goals_for_avg,
    m.home_l5_goals_against_avg,
    m.home_l5_shots_avg,
    m.home_l5_sot_avg,
    m.home_l5_possession_avg,
    m.home_l5_form_points,
    m.home_l5_wins,
    m.home_l5_losses,
    m.home_l5_draws,
    m.home_matches_played_so_far,
    m.home_rest_days,
    -- Away pre-match form (last 5)
    m.away_l5_goals_for_avg,
    m.away_l5_goals_against_avg,
    m.away_l5_shots_avg,
    m.away_l5_sot_avg,
    m.away_l5_possession_avg,
    m.away_l5_form_points,
    m.away_l5_wins,
    m.away_l5_losses,
    m.away_l5_draws,
    m.away_matches_played_so_far,
    m.away_rest_days,
    -- H2H from home perspective
    m.h2h_goals_diff_avg,
    m.h2h_goals_for_avg,
    m.h2h_goals_against_avg,
    m.h2h_home_wins,
    m.h2h_home_losses,
    m.h2h_draws,
    m.h2h_matches_prior,
    -- Partition keys
    m.league,
    m.season
    -- NB: T3.2 (xG/PSxG L5+L10) and T3.3 (volatility/trend std + form_trend)
    -- columns are NOT yet present in the live fct_match materialisation
    -- (Gold DAG hasn't been re-run since those features were added). Once
    -- fct_match is rebuilt, append the new home_*/away_* columns here.
FROM iceberg.gold.fct_match m
WHERE m.is_completed = FALSE
  AND m.date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7' DAY
