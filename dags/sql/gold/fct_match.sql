-- =============================================================================
-- Gold: fct_match
-- =============================================================================
-- Wide-form per-match view ready for ML (1X2 / BTTS / totals).
--
-- Combines dim_match (labels + attributes) with pre-match features from
-- feat_team_form (home + away) and feat_team_h2h.
--
-- Sources:
--   iceberg.gold.dim_match
--   iceberg.gold.feat_team_form
--   iceberg.gold.feat_team_h2h
--   iceberg.gold.feat_team_xg_form         — xG / PSxG rolling (L5, L10)
--   iceberg.gold.feat_referee_bias         — referee rolling biases (L10)
--   iceberg.gold.feat_team_event_style     — SPADL event-style shares rolling (L5)
--
-- PK: match_id
-- Partitioning: (league, season)
--
-- ML targets (nullable until is_completed=true):
--   result_1x2   'H' | 'A' | 'D'
--   total_goals  INTEGER
--   btts         BOOLEAN
--
-- Features: home_l5_*, away_l5_*, h2h_*, home/away xG (L5, L10),
--           ref_*_l10 (referee bias), home/away event-style shares (L5).
-- =============================================================================

SELECT
    -- #425: dim_match slimmed to the star "passport". Output schema of
    -- fct_match is UNCHANGED (downstream predictions_input / train / test
    -- read m.*): date keeps its name via alias, team names come from the
    -- Silver spine, total_goals/btts are derived from the score in place.
    dm.match_id,
    dm.match_date              AS date,
    dm.gameweek,
    dm.home_team_id,
    dm.away_team_id,
    sm.home                    AS home_team_name,
    sm.away                    AS away_team_name,

    -- ========= ML targets =========
    dm.home_score,
    dm.away_score,
    dm.result_1x2,
    dm.home_score + dm.away_score                  AS total_goals,
    (dm.home_score > 0 AND dm.away_score > 0)      AS btts,
    dm.is_completed,

    -- ========= Home team pre-match form (last 5) =========
    hf.l5_goals_for_avg       AS home_l5_goals_for_avg,
    hf.l5_goals_against_avg   AS home_l5_goals_against_avg,
    hf.l5_shots_avg           AS home_l5_shots_avg,
    hf.l5_sot_avg             AS home_l5_sot_avg,
    hf.l5_possession_avg      AS home_l5_possession_avg,
    hf.l5_form_points         AS home_l5_form_points,
    hf.l5_wins                AS home_l5_wins,
    hf.l5_losses              AS home_l5_losses,
    hf.l5_draws               AS home_l5_draws,
    hf.l5_goals_for_std       AS home_l5_goals_for_std,
    hf.l5_goals_against_std   AS home_l5_goals_against_std,
    hf.l5_points_std          AS home_l5_points_std,
    hf.l5_form_trend          AS home_l5_form_trend,
    hf.matches_played_so_far  AS home_matches_played_so_far,
    hf.rest_days              AS home_rest_days,
    hf.unavailable_count_l5   AS home_unavailable_count_l5,

    -- ========= Away team pre-match form (last 5) =========
    af.l5_goals_for_avg       AS away_l5_goals_for_avg,
    af.l5_goals_against_avg   AS away_l5_goals_against_avg,
    af.l5_shots_avg           AS away_l5_shots_avg,
    af.l5_sot_avg             AS away_l5_sot_avg,
    af.l5_possession_avg      AS away_l5_possession_avg,
    af.l5_form_points         AS away_l5_form_points,
    af.l5_wins                AS away_l5_wins,
    af.l5_losses              AS away_l5_losses,
    af.l5_draws               AS away_l5_draws,
    af.l5_goals_for_std       AS away_l5_goals_for_std,
    af.l5_goals_against_std   AS away_l5_goals_against_std,
    af.l5_points_std          AS away_l5_points_std,
    af.l5_form_trend          AS away_l5_form_trend,
    af.matches_played_so_far  AS away_matches_played_so_far,
    af.rest_days              AS away_rest_days,
    af.unavailable_count_l5   AS away_unavailable_count_l5,

    -- ========= H2H (from home team's perspective) =========
    h2h.h2h_goals_diff_avg,
    h2h.h2h_goals_for_avg,
    h2h.h2h_goals_against_avg,
    h2h.h2h_wins              AS h2h_home_wins,
    h2h.h2h_losses            AS h2h_home_losses,
    h2h.h2h_draws             AS h2h_draws,
    h2h.h2h_matches_prior,

    -- ========= Referee bias rolling (L10) =========
    rb.ref_yellow_per_match_l10,
    rb.ref_red_per_match_l10,
    rb.ref_cards_per_match_l10,
    rb.ref_goals_per_match_l10,
    rb.ref_home_win_rate_l10,
    rb.ref_pen_per_match_l10,

    -- ========= Home team xG / PSxG rolling (L5, L10) =========
    hxg.xg_for_l5_avg          AS home_xg_for_l5_avg,
    hxg.xg_against_l5_avg      AS home_xg_against_l5_avg,
    hxg.xg_diff_l5_avg         AS home_xg_diff_l5_avg,
    hxg.psxg_for_l5_avg        AS home_psxg_for_l5_avg,
    hxg.psxg_against_l5_avg    AS home_psxg_against_l5_avg,
    hxg.psxg_diff_l5_avg       AS home_psxg_diff_l5_avg,
    hxg.xg_for_l10_avg         AS home_xg_for_l10_avg,
    hxg.xg_against_l10_avg     AS home_xg_against_l10_avg,
    hxg.xg_diff_l10_avg        AS home_xg_diff_l10_avg,
    hxg.psxg_for_l10_avg       AS home_psxg_for_l10_avg,
    hxg.psxg_against_l10_avg   AS home_psxg_against_l10_avg,
    hxg.psxg_diff_l10_avg      AS home_psxg_diff_l10_avg,

    -- ========= Home team event style rolling (L5) =========
    hes.pass_share_l5_avg          AS home_pass_share_l5_avg,
    hes.dribble_share_l5_avg       AS home_dribble_share_l5_avg,
    hes.tackle_share_l5_avg        AS home_tackle_share_l5_avg,
    hes.interception_share_l5_avg  AS home_interception_share_l5_avg,
    hes.cross_share_l5_avg         AS home_cross_share_l5_avg,
    hes.shot_share_l5_avg          AS home_shot_share_l5_avg,
    hes.success_rate_l5_avg        AS home_success_rate_l5_avg,
    hes.set_piece_share_l5_avg     AS home_set_piece_share_l5_avg,
    hes.open_play_share_l5_avg     AS home_open_play_share_l5_avg,
    hes.header_share_l5_avg        AS home_header_share_l5_avg,

    -- ========= Away team xG / PSxG rolling (L5, L10) =========
    axg.xg_for_l5_avg          AS away_xg_for_l5_avg,
    axg.xg_against_l5_avg      AS away_xg_against_l5_avg,
    axg.xg_diff_l5_avg         AS away_xg_diff_l5_avg,
    axg.psxg_for_l5_avg        AS away_psxg_for_l5_avg,
    axg.psxg_against_l5_avg    AS away_psxg_against_l5_avg,
    axg.psxg_diff_l5_avg       AS away_psxg_diff_l5_avg,
    axg.xg_for_l10_avg         AS away_xg_for_l10_avg,
    axg.xg_against_l10_avg     AS away_xg_against_l10_avg,
    axg.xg_diff_l10_avg        AS away_xg_diff_l10_avg,
    axg.psxg_for_l10_avg       AS away_psxg_for_l10_avg,
    axg.psxg_against_l10_avg   AS away_psxg_against_l10_avg,
    axg.psxg_diff_l10_avg      AS away_psxg_diff_l10_avg,

    -- ========= Away team event style rolling (L5) =========
    aes.pass_share_l5_avg          AS away_pass_share_l5_avg,
    aes.dribble_share_l5_avg       AS away_dribble_share_l5_avg,
    aes.tackle_share_l5_avg        AS away_tackle_share_l5_avg,
    aes.interception_share_l5_avg  AS away_interception_share_l5_avg,
    aes.cross_share_l5_avg         AS away_cross_share_l5_avg,
    aes.shot_share_l5_avg          AS away_shot_share_l5_avg,
    aes.success_rate_l5_avg        AS away_success_rate_l5_avg,
    aes.set_piece_share_l5_avg     AS away_set_piece_share_l5_avg,
    aes.open_play_share_l5_avg     AS away_open_play_share_l5_avg,
    aes.header_share_l5_avg        AS away_header_share_l5_avg,

    dm.league,
    dm.season

FROM iceberg.gold.dim_match dm
-- Team display names (FBref short form) — dim_match no longer carries them.
LEFT JOIN iceberg.silver.fbref_match_enriched sm
    ON sm.match_id = dm.match_id
LEFT JOIN iceberg.gold.feat_team_form hf
    ON hf.match_id = dm.match_id AND hf.team_id = dm.home_team_id
LEFT JOIN iceberg.gold.feat_team_form af
    ON af.match_id = dm.match_id AND af.team_id = dm.away_team_id
LEFT JOIN iceberg.gold.feat_team_h2h h2h
    ON h2h.match_id = dm.match_id
   AND h2h.team_id     = dm.home_team_id
   AND h2h.opponent_id = dm.away_team_id
LEFT JOIN iceberg.gold.feat_team_xg_form hxg
    ON hxg.match_id = dm.match_id AND hxg.team_id = dm.home_team_id
LEFT JOIN iceberg.gold.feat_team_xg_form axg
    ON axg.match_id = dm.match_id AND axg.team_id = dm.away_team_id
LEFT JOIN iceberg.gold.feat_referee_bias rb
    ON rb.match_id = dm.match_id
LEFT JOIN iceberg.gold.feat_team_event_style hes
    ON hes.match_id = dm.match_id AND hes.team_id = dm.home_team_id
LEFT JOIN iceberg.gold.feat_team_event_style aes
    ON aes.match_id = dm.match_id AND aes.team_id = dm.away_team_id
