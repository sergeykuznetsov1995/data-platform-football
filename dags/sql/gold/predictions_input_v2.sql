-- =============================================================================
-- Gold: predictions_input_v2  (E6 — dual-run with v1)
-- =============================================================================
-- Inference feature snapshot, V2 schema. Strict superset of predictions_input
-- (v1): every column carried by v1 is mirrored here in the SAME order, plus
-- the new E6 features that v1 deliberately omits to keep its frozen contract:
--
--   * Referee bias rolling (L10) — 6 cols, no home/away split (one ref per match)
--       ref_yellow_per_match_l10, ref_red_per_match_l10, ref_cards_per_match_l10,
--       ref_goals_per_match_l10, ref_home_win_rate_l10, ref_pen_per_match_l10
--   * Team event-style rolling (L5) — 10 home + 10 away = 20 cols
--       {home,away}_{pass,dribble,tackle,interception,cross,shot,
--                    success_rate,set_piece,open_play,header}_share_l5_avg
--       (success_rate is named *_share_l5_avg in fct_match for column-style
--       symmetry; it is a rolling rate, not a share — see feat_team_event_style.)
--
-- Total: 80 (v1) + 6 (ref) + 20 (event style) = 106 projected columns from m.
--
-- Why dual-run instead of in-place schema bump:
--   * The model server pinned to v1 schema must keep scoring through the E6
--     rollout — a column-set change is a backwards-incompatible API break.
--   * dag_serve_predictions materialises BOTH tables every 2 h. ML consumers
--     read EXACTLY ONE of them; the active version is held in the Airflow
--     Variable `predictions_serving_active_version` (values: 'v1' | 'v2').
--   * Cutover (manual flip of the Variable) happens after >=2 weeks of green
--     DQ on v2 — schema_parity vs fct_match_train + value_range + freshness.
--   * After cutover, v1 stays around as a rollback path until trains are
--     retrained on v2 features.
--
-- Source: iceberg.gold.fct_match  (W3 wide build with E6 columns).
-- Same pre-match guarantees as v1: rolling features for is_completed=FALSE
-- rows are point-in-time-safe — feat_team_form / feat_team_xg_form /
-- feat_team_event_style / feat_referee_bias all use ROWS BETWEEN 5 (or 10)
-- PRECEDING AND 1 PRECEDING ORDER BY date.
--
-- Filter: identical to v1.
--   * is_completed = FALSE
--   * date in [CURRENT_DATE, CURRENT_DATE + 7d]
-- ML targets are emitted unconditionally and will all be NULL (inference set).
--
-- PK: match_id
-- Partitioning: (season)
--
-- Schema parity with fct_match_train / fct_match_test (over the v2 column set)
-- is enforced by the validate_predictions_input_v2 DQ check (W8).
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
-- AND in v1 (unless the new column is intentionally v2-only — document why).
-- The "schema_parity" DQ check (W8) does not yet enforce equality across
-- fct_match_train ↔ predictions_input_v2 — keep this list in sync manually.
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
    -- T3.3: home volatility + trend
    m.home_l5_goals_for_std,
    m.home_l5_goals_against_std,
    m.home_l5_points_std,
    m.home_l5_form_trend,
    m.home_matches_played_so_far,
    m.home_rest_days,
    -- E5: rolling player availability count
    m.home_unavailable_count_l5,
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
    -- T3.3: away volatility + trend
    m.away_l5_goals_for_std,
    m.away_l5_goals_against_std,
    m.away_l5_points_std,
    m.away_l5_form_trend,
    m.away_matches_played_so_far,
    m.away_rest_days,
    -- E5: rolling player availability count
    m.away_unavailable_count_l5,
    -- H2H from home perspective
    m.h2h_goals_diff_avg,
    m.h2h_goals_for_avg,
    m.h2h_goals_against_avg,
    m.h2h_home_wins,
    m.h2h_home_losses,
    m.h2h_draws,
    m.h2h_matches_prior,
    -- ===== NEW (E6): Referee bias rolling (L10) =====
    -- One referee per match -> single block, no home/away split.
    m.ref_yellow_per_match_l10,
    m.ref_red_per_match_l10,
    m.ref_cards_per_match_l10,
    m.ref_goals_per_match_l10,
    m.ref_home_win_rate_l10,
    m.ref_pen_per_match_l10,
    -- T3.2: home xG / PSxG rolling (L5, L10)
    m.home_xg_for_l5_avg,
    m.home_xg_against_l5_avg,
    m.home_xg_diff_l5_avg,
    m.home_psxg_for_l5_avg,
    m.home_psxg_against_l5_avg,
    m.home_psxg_diff_l5_avg,
    m.home_xg_for_l10_avg,
    m.home_xg_against_l10_avg,
    m.home_xg_diff_l10_avg,
    m.home_psxg_for_l10_avg,
    m.home_psxg_against_l10_avg,
    m.home_psxg_diff_l10_avg,
    -- ===== NEW (E6): Home event style rolling (L5) =====
    -- SPADL action-share / success rate rolling 5 prior matches per home team.
    m.home_pass_share_l5_avg,
    m.home_dribble_share_l5_avg,
    m.home_tackle_share_l5_avg,
    m.home_interception_share_l5_avg,
    m.home_cross_share_l5_avg,
    m.home_shot_share_l5_avg,
    m.home_success_rate_l5_avg,
    m.home_set_piece_share_l5_avg,
    m.home_open_play_share_l5_avg,
    m.home_header_share_l5_avg,
    -- T3.2: away xG / PSxG rolling (L5, L10)
    m.away_xg_for_l5_avg,
    m.away_xg_against_l5_avg,
    m.away_xg_diff_l5_avg,
    m.away_psxg_for_l5_avg,
    m.away_psxg_against_l5_avg,
    m.away_psxg_diff_l5_avg,
    m.away_xg_for_l10_avg,
    m.away_xg_against_l10_avg,
    m.away_xg_diff_l10_avg,
    m.away_psxg_for_l10_avg,
    m.away_psxg_against_l10_avg,
    m.away_psxg_diff_l10_avg,
    -- ===== NEW (E6): Away event style rolling (L5) =====
    m.away_pass_share_l5_avg,
    m.away_dribble_share_l5_avg,
    m.away_tackle_share_l5_avg,
    m.away_interception_share_l5_avg,
    m.away_cross_share_l5_avg,
    m.away_shot_share_l5_avg,
    m.away_success_rate_l5_avg,
    m.away_set_piece_share_l5_avg,
    m.away_open_play_share_l5_avg,
    m.away_header_share_l5_avg,
    -- Partition keys
    m.league,
    m.season
FROM iceberg.gold.fct_match m
WHERE m.is_completed = FALSE
  AND m.date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7' DAY
