"""
FBref Schema Definitions
========================

PyArrow schemas for FBref data tables.
"""

import pyarrow as pa
from scrapers.schemas.common import create_schema_with_metadata

# Schedule/Fixtures schema
FBREF_SCHEDULE_SCHEMA = create_schema_with_metadata([
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('match_date', pa.date32(), nullable=True),
    pa.field('match_time', pa.string(), nullable=True),
    pa.field('home_team', pa.string(), nullable=True),
    pa.field('away_team', pa.string(), nullable=True),
    pa.field('home_goals', pa.int32(), nullable=True),
    pa.field('away_goals', pa.int32(), nullable=True),
    pa.field('home_xg', pa.float64(), nullable=True),
    pa.field('away_xg', pa.float64(), nullable=True),
    pa.field('venue', pa.string(), nullable=True),
    pa.field('referee', pa.string(), nullable=True),
    pa.field('attendance', pa.int32(), nullable=True),
    pa.field('match_id', pa.string(), nullable=True),
    pa.field('match_url', pa.string(), nullable=True),
    # WC and some cups use these (FBref schedule table has Wk + Round columns).
    # They were present in Bronze but missing from the declared PyArrow schema (#913 Phase 1).
    pa.field('wk', pa.string(), nullable=True),
    pa.field('round', pa.string(), nullable=True),
])

# Player stats (season summary)
FBREF_PLAYER_STATS_SCHEMA = create_schema_with_metadata([
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('player', pa.string(), nullable=True),
    pa.field('player_id', pa.string(), nullable=True),
    pa.field('nation', pa.string(), nullable=True),
    pa.field('team', pa.string(), nullable=True),
    pa.field('position', pa.string(), nullable=True),
    pa.field('age', pa.int32(), nullable=True),
    pa.field('born', pa.int32(), nullable=True),
    pa.field('matches_played', pa.int32(), nullable=True),
    pa.field('starts', pa.int32(), nullable=True),
    pa.field('minutes', pa.int32(), nullable=True),
    pa.field('goals', pa.int32(), nullable=True),
    pa.field('assists', pa.int32(), nullable=True),
    pa.field('goals_assists', pa.int32(), nullable=True),
    pa.field('non_penalty_goals', pa.int32(), nullable=True),
    pa.field('penalty_goals', pa.int32(), nullable=True),
    pa.field('penalty_attempts', pa.int32(), nullable=True),
    pa.field('yellow_cards', pa.int32(), nullable=True),
    pa.field('red_cards', pa.int32(), nullable=True),
    pa.field('xg', pa.float64(), nullable=True),
    pa.field('npxg', pa.float64(), nullable=True),
    pa.field('xa', pa.float64(), nullable=True),
    pa.field('progressive_carries', pa.int32(), nullable=True),
    pa.field('progressive_passes', pa.int32(), nullable=True),
    pa.field('progressive_passes_received', pa.int32(), nullable=True),
    pa.field('goals_per_90', pa.float64(), nullable=True),
    pa.field('assists_per_90', pa.float64(), nullable=True),
    pa.field('xg_per_90', pa.float64(), nullable=True),
    pa.field('xa_per_90', pa.float64(), nullable=True),
])

# Team stats (season summary)
FBREF_TEAM_STATS_SCHEMA = create_schema_with_metadata([
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('team', pa.string(), nullable=True),
    pa.field('team_id', pa.string(), nullable=True),
    pa.field('matches_played', pa.int32(), nullable=True),
    pa.field('wins', pa.int32(), nullable=True),
    pa.field('draws', pa.int32(), nullable=True),
    pa.field('losses', pa.int32(), nullable=True),
    pa.field('goals_for', pa.int32(), nullable=True),
    pa.field('goals_against', pa.int32(), nullable=True),
    pa.field('goal_difference', pa.int32(), nullable=True),
    pa.field('points', pa.int32(), nullable=True),
    pa.field('xg', pa.float64(), nullable=True),
    pa.field('xga', pa.float64(), nullable=True),
    pa.field('xg_difference', pa.float64(), nullable=True),
    pa.field('attendance_home', pa.int32(), nullable=True),
])

# Shooting stats
FBREF_SHOOTING_SCHEMA = create_schema_with_metadata([
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('player', pa.string(), nullable=True),
    pa.field('player_id', pa.string(), nullable=True),
    pa.field('team', pa.string(), nullable=True),
    pa.field('shots', pa.int32(), nullable=True),
    pa.field('shots_on_target', pa.int32(), nullable=True),
    pa.field('shot_on_target_pct', pa.float64(), nullable=True),
    pa.field('goals', pa.int32(), nullable=True),
    pa.field('shots_per_90', pa.float64(), nullable=True),
    pa.field('shots_on_target_per_90', pa.float64(), nullable=True),
    pa.field('goals_per_shot', pa.float64(), nullable=True),
    pa.field('goals_per_shot_on_target', pa.float64(), nullable=True),
    pa.field('avg_shot_distance', pa.float64(), nullable=True),
    pa.field('free_kick_shots', pa.int32(), nullable=True),
    pa.field('penalty_goals', pa.int32(), nullable=True),
    pa.field('penalty_attempts', pa.int32(), nullable=True),
    pa.field('xg', pa.float64(), nullable=True),
    pa.field('npxg', pa.float64(), nullable=True),
    pa.field('npxg_per_shot', pa.float64(), nullable=True),
    pa.field('goals_minus_xg', pa.float64(), nullable=True),
    pa.field('npg_minus_npxg', pa.float64(), nullable=True),
])

# Passing stats
FBREF_PASSING_SCHEMA = create_schema_with_metadata([
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('player', pa.string(), nullable=True),
    pa.field('player_id', pa.string(), nullable=True),
    pa.field('team', pa.string(), nullable=True),
    pa.field('passes_completed', pa.int32(), nullable=True),
    pa.field('passes_attempted', pa.int32(), nullable=True),
    pa.field('pass_completion_pct', pa.float64(), nullable=True),
    pa.field('total_pass_distance', pa.int32(), nullable=True),
    pa.field('progressive_pass_distance', pa.int32(), nullable=True),
    pa.field('short_passes_completed', pa.int32(), nullable=True),
    pa.field('short_passes_attempted', pa.int32(), nullable=True),
    pa.field('medium_passes_completed', pa.int32(), nullable=True),
    pa.field('medium_passes_attempted', pa.int32(), nullable=True),
    pa.field('long_passes_completed', pa.int32(), nullable=True),
    pa.field('long_passes_attempted', pa.int32(), nullable=True),
    pa.field('assists', pa.int32(), nullable=True),
    pa.field('xa', pa.float64(), nullable=True),
    pa.field('key_passes', pa.int32(), nullable=True),
    pa.field('passes_into_final_third', pa.int32(), nullable=True),
    pa.field('passes_into_penalty_area', pa.int32(), nullable=True),
    pa.field('crosses_into_penalty_area', pa.int32(), nullable=True),
    pa.field('progressive_passes', pa.int32(), nullable=True),
])

# Keeper stats (basic)
FBREF_KEEPER_SCHEMA = create_schema_with_metadata([
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('player', pa.string(), nullable=True),
    pa.field('player_id', pa.string(), nullable=True),
    pa.field('team', pa.string(), nullable=True),
    pa.field('matches_played', pa.int32(), nullable=True),
    pa.field('starts', pa.int32(), nullable=True),
    pa.field('minutes', pa.int32(), nullable=True),
    pa.field('goals_against', pa.int32(), nullable=True),
    pa.field('goals_against_per_90', pa.float64(), nullable=True),
    pa.field('shots_on_target_against', pa.int32(), nullable=True),
    pa.field('saves', pa.int32(), nullable=True),
    pa.field('save_pct', pa.float64(), nullable=True),
    pa.field('clean_sheets', pa.int32(), nullable=True),
    pa.field('clean_sheet_pct', pa.float64(), nullable=True),
    pa.field('penalties_faced', pa.int32(), nullable=True),
    pa.field('penalties_allowed', pa.int32(), nullable=True),
    pa.field('penalties_saved', pa.int32(), nullable=True),
    pa.field('penalties_missed', pa.int32(), nullable=True),
    pa.field('psxg', pa.float64(), nullable=True),
    pa.field('psxg_per_shot_on_target', pa.float64(), nullable=True),
    pa.field('psxg_minus_goals_allowed', pa.float64(), nullable=True),
])


# Extended player stats (merged from all stat_types) - ~100 fields
FBREF_PLAYER_STATS_EXTENDED_SCHEMA = create_schema_with_metadata([
    # === Identification ===
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('player', pa.string(), nullable=True),
    pa.field('player_id', pa.string(), nullable=True),
    pa.field('nation', pa.string(), nullable=True),
    pa.field('team', pa.string(), nullable=True),
    pa.field('position', pa.string(), nullable=True),
    pa.field('age', pa.int32(), nullable=True),
    pa.field('born', pa.int32(), nullable=True),

    # === Playing Time (from stats + playingtime) ===
    pa.field('matches_played', pa.int32(), nullable=True),
    pa.field('starts', pa.int32(), nullable=True),
    pa.field('minutes', pa.int32(), nullable=True),
    pa.field('minutes_per_match', pa.float64(), nullable=True),
    pa.field('minutes_pct', pa.float64(), nullable=True),
    pa.field('minutes_90s', pa.float64(), nullable=True),
    pa.field('subs', pa.int32(), nullable=True),
    pa.field('unused_subs', pa.int32(), nullable=True),
    pa.field('points_per_match', pa.float64(), nullable=True),

    # === Goals & Assists (from stats) ===
    pa.field('goals', pa.int32(), nullable=True),
    pa.field('assists', pa.int32(), nullable=True),
    pa.field('goals_assists', pa.int32(), nullable=True),
    pa.field('non_penalty_goals', pa.int32(), nullable=True),
    pa.field('penalty_goals', pa.int32(), nullable=True),
    pa.field('penalty_attempts', pa.int32(), nullable=True),
    pa.field('yellow_cards', pa.int32(), nullable=True),
    pa.field('red_cards', pa.int32(), nullable=True),

    # === xG Metrics (from stats + shooting) ===
    pa.field('xg', pa.float64(), nullable=True),
    pa.field('npxg', pa.float64(), nullable=True),
    pa.field('xa', pa.float64(), nullable=True),
    pa.field('npxg_xa', pa.float64(), nullable=True),
    pa.field('goals_minus_xg', pa.float64(), nullable=True),
    pa.field('npg_minus_npxg', pa.float64(), nullable=True),

    # === Shooting (from shooting) ===
    pa.field('shots', pa.int32(), nullable=True),
    pa.field('shots_on_target', pa.int32(), nullable=True),
    pa.field('shot_on_target_pct', pa.float64(), nullable=True),
    pa.field('shots_per_90', pa.float64(), nullable=True),
    pa.field('shots_on_target_per_90', pa.float64(), nullable=True),
    pa.field('goals_per_shot', pa.float64(), nullable=True),
    pa.field('goals_per_shot_on_target', pa.float64(), nullable=True),
    pa.field('avg_shot_distance', pa.float64(), nullable=True),
    pa.field('free_kick_shots', pa.int32(), nullable=True),
    pa.field('npxg_per_shot', pa.float64(), nullable=True),

    # === Passing (from passing) ===
    pa.field('passes_completed', pa.int32(), nullable=True),
    pa.field('passes_attempted', pa.int32(), nullable=True),
    pa.field('pass_completion_pct', pa.float64(), nullable=True),
    pa.field('total_pass_distance', pa.int32(), nullable=True),
    pa.field('progressive_pass_distance', pa.int32(), nullable=True),
    pa.field('short_passes_completed', pa.int32(), nullable=True),
    pa.field('short_passes_attempted', pa.int32(), nullable=True),
    pa.field('short_pass_completion_pct', pa.float64(), nullable=True),
    pa.field('medium_passes_completed', pa.int32(), nullable=True),
    pa.field('medium_passes_attempted', pa.int32(), nullable=True),
    pa.field('medium_pass_completion_pct', pa.float64(), nullable=True),
    pa.field('long_passes_completed', pa.int32(), nullable=True),
    pa.field('long_passes_attempted', pa.int32(), nullable=True),
    pa.field('long_pass_completion_pct', pa.float64(), nullable=True),
    pa.field('key_passes', pa.int32(), nullable=True),
    pa.field('passes_into_final_third', pa.int32(), nullable=True),
    pa.field('passes_into_penalty_area', pa.int32(), nullable=True),
    pa.field('crosses_into_penalty_area', pa.int32(), nullable=True),
    pa.field('progressive_passes', pa.int32(), nullable=True),

    # === Pass Types (from passing_types) ===
    pa.field('passes_live', pa.int32(), nullable=True),
    pa.field('passes_dead', pa.int32(), nullable=True),
    pa.field('passes_free_kick', pa.int32(), nullable=True),
    pa.field('through_balls', pa.int32(), nullable=True),
    pa.field('switches', pa.int32(), nullable=True),
    pa.field('crosses', pa.int32(), nullable=True),
    pa.field('throw_ins', pa.int32(), nullable=True),
    pa.field('corner_kicks', pa.int32(), nullable=True),
    pa.field('corner_kicks_in', pa.int32(), nullable=True),
    pa.field('corner_kicks_out', pa.int32(), nullable=True),
    pa.field('corner_kicks_straight', pa.int32(), nullable=True),
    pa.field('passes_offside', pa.int32(), nullable=True),
    pa.field('passes_blocked', pa.int32(), nullable=True),

    # === GCA (from gca) ===
    pa.field('sca', pa.int32(), nullable=True),
    pa.field('sca_per_90', pa.float64(), nullable=True),
    pa.field('sca_passes_live', pa.int32(), nullable=True),
    pa.field('sca_passes_dead', pa.int32(), nullable=True),
    pa.field('sca_take_ons', pa.int32(), nullable=True),
    pa.field('sca_shots', pa.int32(), nullable=True),
    pa.field('sca_fouls_drawn', pa.int32(), nullable=True),
    pa.field('sca_defense', pa.int32(), nullable=True),
    pa.field('gca', pa.int32(), nullable=True),
    pa.field('gca_per_90', pa.float64(), nullable=True),
    pa.field('gca_passes_live', pa.int32(), nullable=True),
    pa.field('gca_passes_dead', pa.int32(), nullable=True),
    pa.field('gca_take_ons', pa.int32(), nullable=True),
    pa.field('gca_shots', pa.int32(), nullable=True),
    pa.field('gca_fouls_drawn', pa.int32(), nullable=True),
    pa.field('gca_defense', pa.int32(), nullable=True),

    # === Defense (from defense) ===
    pa.field('tackles', pa.int32(), nullable=True),
    pa.field('tackles_won', pa.int32(), nullable=True),
    pa.field('tackles_def_3rd', pa.int32(), nullable=True),
    pa.field('tackles_mid_3rd', pa.int32(), nullable=True),
    pa.field('tackles_att_3rd', pa.int32(), nullable=True),
    pa.field('challenge_tackles', pa.int32(), nullable=True),
    pa.field('challenges', pa.int32(), nullable=True),
    pa.field('challenge_tackles_pct', pa.float64(), nullable=True),
    pa.field('challenges_lost', pa.int32(), nullable=True),
    pa.field('blocks', pa.int32(), nullable=True),
    pa.field('blocked_shots', pa.int32(), nullable=True),
    pa.field('blocked_passes', pa.int32(), nullable=True),
    pa.field('interceptions', pa.int32(), nullable=True),
    pa.field('tackles_interceptions', pa.int32(), nullable=True),
    pa.field('clearances', pa.int32(), nullable=True),
    pa.field('errors', pa.int32(), nullable=True),

    # === Possession (from possession) ===
    pa.field('touches', pa.int32(), nullable=True),
    pa.field('touches_def_pen', pa.int32(), nullable=True),
    pa.field('touches_def_3rd', pa.int32(), nullable=True),
    pa.field('touches_mid_3rd', pa.int32(), nullable=True),
    pa.field('touches_att_3rd', pa.int32(), nullable=True),
    pa.field('touches_att_pen', pa.int32(), nullable=True),
    pa.field('touches_live', pa.int32(), nullable=True),
    pa.field('take_ons_attempted', pa.int32(), nullable=True),
    pa.field('take_ons_succeeded', pa.int32(), nullable=True),
    pa.field('take_ons_pct', pa.float64(), nullable=True),
    pa.field('take_ons_tackled', pa.int32(), nullable=True),
    pa.field('take_ons_tackled_pct', pa.float64(), nullable=True),
    pa.field('carries', pa.int32(), nullable=True),
    pa.field('carry_distance', pa.int32(), nullable=True),
    pa.field('carry_progressive_distance', pa.int32(), nullable=True),
    pa.field('progressive_carries', pa.int32(), nullable=True),
    pa.field('carries_into_final_third', pa.int32(), nullable=True),
    pa.field('carries_into_penalty_area', pa.int32(), nullable=True),
    pa.field('miscontrols', pa.int32(), nullable=True),
    pa.field('dispossessed', pa.int32(), nullable=True),
    pa.field('passes_received', pa.int32(), nullable=True),
    pa.field('progressive_passes_received', pa.int32(), nullable=True),

    # === Misc (from misc) ===
    pa.field('fouls_committed', pa.int32(), nullable=True),
    pa.field('fouls_drawn', pa.int32(), nullable=True),
    pa.field('offsides', pa.int32(), nullable=True),
    pa.field('penalties_won', pa.int32(), nullable=True),
    pa.field('penalties_conceded', pa.int32(), nullable=True),
    pa.field('own_goals', pa.int32(), nullable=True),
    pa.field('ball_recoveries', pa.int32(), nullable=True),
    pa.field('aerials_won', pa.int32(), nullable=True),
    pa.field('aerials_lost', pa.int32(), nullable=True),
    pa.field('aerials_won_pct', pa.float64(), nullable=True),

    # === On/Off Impact (from playingtime) ===
    pa.field('on_goals', pa.int32(), nullable=True),
    pa.field('on_goals_against', pa.int32(), nullable=True),
    pa.field('plus_minus', pa.int32(), nullable=True),
    pa.field('plus_minus_per_90', pa.float64(), nullable=True),
    pa.field('on_xg', pa.float64(), nullable=True),
    pa.field('on_xga', pa.float64(), nullable=True),
    pa.field('xg_plus_minus', pa.float64(), nullable=True),
    pa.field('xg_plus_minus_per_90', pa.float64(), nullable=True),

    # === Per 90 (from stats) ===
    pa.field('goals_per_90', pa.float64(), nullable=True),
    pa.field('assists_per_90', pa.float64(), nullable=True),
    pa.field('goals_assists_per_90', pa.float64(), nullable=True),
    pa.field('non_penalty_goals_per_90', pa.float64(), nullable=True),
    pa.field('non_penalty_goals_assists_per_90', pa.float64(), nullable=True),
    pa.field('xg_per_90', pa.float64(), nullable=True),
    pa.field('xa_per_90', pa.float64(), nullable=True),
    pa.field('xg_xa_per_90', pa.float64(), nullable=True),
    pa.field('npxg_per_90', pa.float64(), nullable=True),
    pa.field('npxg_xa_per_90', pa.float64(), nullable=True),
])


# Player match stats (per-game granularity)
FBREF_PLAYER_MATCH_STATS_SCHEMA = create_schema_with_metadata([
    # === Match Identification ===
    pa.field('match_id', pa.string(), nullable=True),
    pa.field('match_date', pa.date32(), nullable=True),
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('round', pa.string(), nullable=True),
    pa.field('day', pa.string(), nullable=True),
    pa.field('venue', pa.string(), nullable=True),
    pa.field('result', pa.string(), nullable=True),
    pa.field('home_team', pa.string(), nullable=True),
    pa.field('away_team', pa.string(), nullable=True),
    pa.field('opponent', pa.string(), nullable=True),
    pa.field('team_goals', pa.int32(), nullable=True),
    pa.field('opponent_goals', pa.int32(), nullable=True),

    # === Player Identification ===
    pa.field('player', pa.string(), nullable=True),
    pa.field('player_id', pa.string(), nullable=True),
    pa.field('team', pa.string(), nullable=True),
    pa.field('position', pa.string(), nullable=True),
    pa.field('starter', pa.bool_(), nullable=True),
    pa.field('minutes', pa.int32(), nullable=True),

    # === Goals & Assists ===
    pa.field('goals', pa.int32(), nullable=True),
    pa.field('assists', pa.int32(), nullable=True),
    pa.field('penalty_goals', pa.int32(), nullable=True),
    pa.field('penalty_attempts', pa.int32(), nullable=True),

    # === Shooting ===
    pa.field('shots', pa.int32(), nullable=True),
    pa.field('shots_on_target', pa.int32(), nullable=True),
    pa.field('xg', pa.float64(), nullable=True),
    pa.field('npxg', pa.float64(), nullable=True),
    pa.field('xa', pa.float64(), nullable=True),

    # === Passing ===
    pa.field('passes_completed', pa.int32(), nullable=True),
    pa.field('passes_attempted', pa.int32(), nullable=True),
    pa.field('pass_completion_pct', pa.float64(), nullable=True),
    pa.field('progressive_passes', pa.int32(), nullable=True),
    pa.field('key_passes', pa.int32(), nullable=True),

    # === Defense ===
    pa.field('tackles', pa.int32(), nullable=True),
    pa.field('interceptions', pa.int32(), nullable=True),
    pa.field('blocks', pa.int32(), nullable=True),
    pa.field('clearances', pa.int32(), nullable=True),

    # === Possession ===
    pa.field('touches', pa.int32(), nullable=True),
    pa.field('take_ons_attempted', pa.int32(), nullable=True),
    pa.field('take_ons_succeeded', pa.int32(), nullable=True),
    pa.field('carries', pa.int32(), nullable=True),
    pa.field('progressive_carries', pa.int32(), nullable=True),

    # === GCA/SCA ===
    pa.field('sca', pa.int32(), nullable=True),
    pa.field('gca', pa.int32(), nullable=True),

    # === Misc ===
    pa.field('fouls_committed', pa.int32(), nullable=True),
    pa.field('fouls_drawn', pa.int32(), nullable=True),
    pa.field('offsides', pa.int32(), nullable=True),
    pa.field('aerials_won', pa.int32(), nullable=True),
    pa.field('aerials_lost', pa.int32(), nullable=True),
    pa.field('yellow_card', pa.bool_(), nullable=True),
    pa.field('red_card', pa.bool_(), nullable=True),
])


# =============================================================================
# SEPARATE STAT TYPE SCHEMAS (Memory-efficient architecture)
# =============================================================================
# These schemas are used when collecting each stat_type separately
# instead of merging all into one large table.

# Common identification fields for all player stat tables
_PLAYER_IDENTIFICATION_FIELDS = [
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('player', pa.string(), nullable=True),
    pa.field('player_id', pa.string(), nullable=True),
    pa.field('nation', pa.string(), nullable=True),
    pa.field('team', pa.string(), nullable=True),
    pa.field('position', pa.string(), nullable=True),
    pa.field('age', pa.int32(), nullable=True),
    pa.field('born', pa.int32(), nullable=True),
    pa.field('stat_type', pa.string(), nullable=True),
]

# Common identification fields for all team stat tables
_TEAM_IDENTIFICATION_FIELDS = [
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('team', pa.string(), nullable=True),
    pa.field('team_id', pa.string(), nullable=True),
    pa.field('stat_type', pa.string(), nullable=True),
]

# --- Player Standard Stats (stats) ---
FBREF_PLAYER_STANDARD_SCHEMA = create_schema_with_metadata([
    *_PLAYER_IDENTIFICATION_FIELDS,
    pa.field('matches_played', pa.int32(), nullable=True),
    pa.field('starts', pa.int32(), nullable=True),
    pa.field('minutes', pa.int32(), nullable=True),
    pa.field('minutes_90s', pa.float64(), nullable=True),
    pa.field('goals', pa.int32(), nullable=True),
    pa.field('assists', pa.int32(), nullable=True),
    pa.field('goals_assists', pa.int32(), nullable=True),
    pa.field('non_penalty_goals', pa.int32(), nullable=True),
    pa.field('penalty_goals', pa.int32(), nullable=True),
    pa.field('penalty_attempts', pa.int32(), nullable=True),
    pa.field('yellow_cards', pa.int32(), nullable=True),
    pa.field('red_cards', pa.int32(), nullable=True),
    pa.field('xg', pa.float64(), nullable=True),
    pa.field('npxg', pa.float64(), nullable=True),
    pa.field('xa', pa.float64(), nullable=True),
    pa.field('npxg_xa', pa.float64(), nullable=True),
    pa.field('progressive_carries', pa.int32(), nullable=True),
    pa.field('progressive_passes', pa.int32(), nullable=True),
    pa.field('progressive_passes_received', pa.int32(), nullable=True),
    pa.field('goals_per_90', pa.float64(), nullable=True),
    pa.field('assists_per_90', pa.float64(), nullable=True),
    pa.field('xg_per_90', pa.float64(), nullable=True),
    pa.field('xa_per_90', pa.float64(), nullable=True),
])

# --- Player Shooting Stats ---
FBREF_PLAYER_SHOOTING_SCHEMA = create_schema_with_metadata([
    *_PLAYER_IDENTIFICATION_FIELDS,
    pa.field('goals', pa.int32(), nullable=True),
    pa.field('shots', pa.int32(), nullable=True),
    pa.field('shots_on_target', pa.int32(), nullable=True),
    pa.field('shot_on_target_pct', pa.float64(), nullable=True),
    pa.field('shots_per_90', pa.float64(), nullable=True),
    pa.field('shots_on_target_per_90', pa.float64(), nullable=True),
    pa.field('goals_per_shot', pa.float64(), nullable=True),
    pa.field('goals_per_shot_on_target', pa.float64(), nullable=True),
    pa.field('avg_shot_distance', pa.float64(), nullable=True),
    pa.field('free_kick_shots', pa.int32(), nullable=True),
    pa.field('penalty_goals', pa.int32(), nullable=True),
    pa.field('penalty_attempts', pa.int32(), nullable=True),
    pa.field('xg', pa.float64(), nullable=True),
    pa.field('npxg', pa.float64(), nullable=True),
    pa.field('npxg_per_shot', pa.float64(), nullable=True),
    pa.field('goals_minus_xg', pa.float64(), nullable=True),
    pa.field('npg_minus_npxg', pa.float64(), nullable=True),
])

# --- Player Passing Stats ---
FBREF_PLAYER_PASSING_SCHEMA = create_schema_with_metadata([
    *_PLAYER_IDENTIFICATION_FIELDS,
    pa.field('passes_completed', pa.int32(), nullable=True),
    pa.field('passes_attempted', pa.int32(), nullable=True),
    pa.field('pass_completion_pct', pa.float64(), nullable=True),
    pa.field('total_pass_distance', pa.int32(), nullable=True),
    pa.field('progressive_pass_distance', pa.int32(), nullable=True),
    pa.field('short_passes_completed', pa.int32(), nullable=True),
    pa.field('short_passes_attempted', pa.int32(), nullable=True),
    pa.field('short_pass_completion_pct', pa.float64(), nullable=True),
    pa.field('medium_passes_completed', pa.int32(), nullable=True),
    pa.field('medium_passes_attempted', pa.int32(), nullable=True),
    pa.field('medium_pass_completion_pct', pa.float64(), nullable=True),
    pa.field('long_passes_completed', pa.int32(), nullable=True),
    pa.field('long_passes_attempted', pa.int32(), nullable=True),
    pa.field('long_pass_completion_pct', pa.float64(), nullable=True),
    pa.field('assists', pa.int32(), nullable=True),
    pa.field('xa', pa.float64(), nullable=True),
    pa.field('assists_minus_xa', pa.float64(), nullable=True),
    pa.field('key_passes', pa.int32(), nullable=True),
    pa.field('passes_into_final_third', pa.int32(), nullable=True),
    pa.field('passes_into_penalty_area', pa.int32(), nullable=True),
    pa.field('crosses_into_penalty_area', pa.int32(), nullable=True),
    pa.field('progressive_passes', pa.int32(), nullable=True),
])

# --- Player Passing Types Stats ---
FBREF_PLAYER_PASSING_TYPES_SCHEMA = create_schema_with_metadata([
    *_PLAYER_IDENTIFICATION_FIELDS,
    pa.field('passes_attempted', pa.int32(), nullable=True),
    pa.field('passes_live', pa.int32(), nullable=True),
    pa.field('passes_dead', pa.int32(), nullable=True),
    pa.field('passes_free_kick', pa.int32(), nullable=True),
    pa.field('through_balls', pa.int32(), nullable=True),
    pa.field('switches', pa.int32(), nullable=True),
    pa.field('crosses', pa.int32(), nullable=True),
    pa.field('throw_ins', pa.int32(), nullable=True),
    pa.field('corner_kicks', pa.int32(), nullable=True),
    pa.field('corner_kicks_in', pa.int32(), nullable=True),
    pa.field('corner_kicks_out', pa.int32(), nullable=True),
    pa.field('corner_kicks_straight', pa.int32(), nullable=True),
    pa.field('passes_completed', pa.int32(), nullable=True),
    pa.field('passes_offside', pa.int32(), nullable=True),
    pa.field('passes_blocked', pa.int32(), nullable=True),
])

# --- Player GCA (Goal and Shot Creating Actions) Stats ---
FBREF_PLAYER_GCA_SCHEMA = create_schema_with_metadata([
    *_PLAYER_IDENTIFICATION_FIELDS,
    pa.field('sca', pa.int32(), nullable=True),
    pa.field('sca_per_90', pa.float64(), nullable=True),
    pa.field('sca_passes_live', pa.int32(), nullable=True),
    pa.field('sca_passes_dead', pa.int32(), nullable=True),
    pa.field('sca_take_ons', pa.int32(), nullable=True),
    pa.field('sca_shots', pa.int32(), nullable=True),
    pa.field('sca_fouls_drawn', pa.int32(), nullable=True),
    pa.field('sca_defense', pa.int32(), nullable=True),
    pa.field('gca', pa.int32(), nullable=True),
    pa.field('gca_per_90', pa.float64(), nullable=True),
    pa.field('gca_passes_live', pa.int32(), nullable=True),
    pa.field('gca_passes_dead', pa.int32(), nullable=True),
    pa.field('gca_take_ons', pa.int32(), nullable=True),
    pa.field('gca_shots', pa.int32(), nullable=True),
    pa.field('gca_fouls_drawn', pa.int32(), nullable=True),
    pa.field('gca_defense', pa.int32(), nullable=True),
])

# --- Player Defense Stats ---
FBREF_PLAYER_DEFENSE_SCHEMA = create_schema_with_metadata([
    *_PLAYER_IDENTIFICATION_FIELDS,
    pa.field('tackles', pa.int32(), nullable=True),
    pa.field('tackles_won', pa.int32(), nullable=True),
    pa.field('tackles_def_3rd', pa.int32(), nullable=True),
    pa.field('tackles_mid_3rd', pa.int32(), nullable=True),
    pa.field('tackles_att_3rd', pa.int32(), nullable=True),
    pa.field('challenge_tackles', pa.int32(), nullable=True),
    pa.field('challenges', pa.int32(), nullable=True),
    pa.field('challenge_tackles_pct', pa.float64(), nullable=True),
    pa.field('challenges_lost', pa.int32(), nullable=True),
    pa.field('blocks', pa.int32(), nullable=True),
    pa.field('blocked_shots', pa.int32(), nullable=True),
    pa.field('blocked_passes', pa.int32(), nullable=True),
    pa.field('interceptions', pa.int32(), nullable=True),
    pa.field('tackles_interceptions', pa.int32(), nullable=True),
    pa.field('clearances', pa.int32(), nullable=True),
    pa.field('errors', pa.int32(), nullable=True),
])

# --- Player Possession Stats ---
FBREF_PLAYER_POSSESSION_SCHEMA = create_schema_with_metadata([
    *_PLAYER_IDENTIFICATION_FIELDS,
    pa.field('touches', pa.int32(), nullable=True),
    pa.field('touches_def_pen', pa.int32(), nullable=True),
    pa.field('touches_def_3rd', pa.int32(), nullable=True),
    pa.field('touches_mid_3rd', pa.int32(), nullable=True),
    pa.field('touches_att_3rd', pa.int32(), nullable=True),
    pa.field('touches_att_pen', pa.int32(), nullable=True),
    pa.field('touches_live', pa.int32(), nullable=True),
    pa.field('take_ons_attempted', pa.int32(), nullable=True),
    pa.field('take_ons_succeeded', pa.int32(), nullable=True),
    pa.field('take_ons_pct', pa.float64(), nullable=True),
    pa.field('take_ons_tackled', pa.int32(), nullable=True),
    pa.field('take_ons_tackled_pct', pa.float64(), nullable=True),
    pa.field('carries', pa.int32(), nullable=True),
    pa.field('carry_distance', pa.int32(), nullable=True),
    pa.field('carry_progressive_distance', pa.int32(), nullable=True),
    pa.field('progressive_carries', pa.int32(), nullable=True),
    pa.field('carries_into_final_third', pa.int32(), nullable=True),
    pa.field('carries_into_penalty_area', pa.int32(), nullable=True),
    pa.field('miscontrols', pa.int32(), nullable=True),
    pa.field('dispossessed', pa.int32(), nullable=True),
    pa.field('passes_received', pa.int32(), nullable=True),
    pa.field('progressive_passes_received', pa.int32(), nullable=True),
])

# --- Player Playing Time Stats ---
FBREF_PLAYER_PLAYINGTIME_SCHEMA = create_schema_with_metadata([
    *_PLAYER_IDENTIFICATION_FIELDS,
    pa.field('matches_played', pa.int32(), nullable=True),
    pa.field('minutes', pa.int32(), nullable=True),
    pa.field('minutes_per_match', pa.float64(), nullable=True),
    pa.field('minutes_pct', pa.float64(), nullable=True),
    pa.field('minutes_90s', pa.float64(), nullable=True),
    pa.field('starts', pa.int32(), nullable=True),
    pa.field('minutes_per_start', pa.float64(), nullable=True),
    pa.field('complete_matches', pa.int32(), nullable=True),
    pa.field('subs', pa.int32(), nullable=True),
    pa.field('minutes_per_sub', pa.float64(), nullable=True),
    pa.field('unused_subs', pa.int32(), nullable=True),
    pa.field('points_per_match', pa.float64(), nullable=True),
    pa.field('on_goals', pa.int32(), nullable=True),
    pa.field('on_goals_against', pa.int32(), nullable=True),
    pa.field('plus_minus', pa.int32(), nullable=True),
    pa.field('plus_minus_per_90', pa.float64(), nullable=True),
    pa.field('on_xg', pa.float64(), nullable=True),
    pa.field('on_xga', pa.float64(), nullable=True),
    pa.field('xg_plus_minus', pa.float64(), nullable=True),
    pa.field('xg_plus_minus_per_90', pa.float64(), nullable=True),
])

# --- Player Misc Stats ---
FBREF_PLAYER_MISC_SCHEMA = create_schema_with_metadata([
    *_PLAYER_IDENTIFICATION_FIELDS,
    pa.field('yellow_cards', pa.int32(), nullable=True),
    pa.field('red_cards', pa.int32(), nullable=True),
    pa.field('second_yellow', pa.int32(), nullable=True),
    pa.field('fouls_committed', pa.int32(), nullable=True),
    pa.field('fouls_drawn', pa.int32(), nullable=True),
    pa.field('offsides', pa.int32(), nullable=True),
    pa.field('crosses', pa.int32(), nullable=True),
    pa.field('interceptions', pa.int32(), nullable=True),
    pa.field('tackles_won', pa.int32(), nullable=True),
    pa.field('penalties_won', pa.int32(), nullable=True),
    pa.field('penalties_conceded', pa.int32(), nullable=True),
    pa.field('own_goals', pa.int32(), nullable=True),
    pa.field('ball_recoveries', pa.int32(), nullable=True),
    pa.field('aerials_won', pa.int32(), nullable=True),
    pa.field('aerials_lost', pa.int32(), nullable=True),
    pa.field('aerials_won_pct', pa.float64(), nullable=True),
])

# --- Team Standard Stats ---
FBREF_TEAM_STANDARD_SCHEMA = create_schema_with_metadata([
    *_TEAM_IDENTIFICATION_FIELDS,
    pa.field('players_used', pa.int32(), nullable=True),
    pa.field('avg_age', pa.float64(), nullable=True),
    pa.field('possession', pa.float64(), nullable=True),
    pa.field('matches_played', pa.int32(), nullable=True),
    pa.field('starts', pa.int32(), nullable=True),
    pa.field('minutes', pa.int32(), nullable=True),
    pa.field('minutes_90s', pa.float64(), nullable=True),
    pa.field('goals', pa.int32(), nullable=True),
    pa.field('assists', pa.int32(), nullable=True),
    pa.field('goals_assists', pa.int32(), nullable=True),
    pa.field('non_penalty_goals', pa.int32(), nullable=True),
    pa.field('penalty_goals', pa.int32(), nullable=True),
    pa.field('penalty_attempts', pa.int32(), nullable=True),
    pa.field('yellow_cards', pa.int32(), nullable=True),
    pa.field('red_cards', pa.int32(), nullable=True),
    pa.field('xg', pa.float64(), nullable=True),
    pa.field('npxg', pa.float64(), nullable=True),
    pa.field('xa', pa.float64(), nullable=True),
    pa.field('npxg_xa', pa.float64(), nullable=True),
    pa.field('progressive_carries', pa.int32(), nullable=True),
    pa.field('progressive_passes', pa.int32(), nullable=True),
    pa.field('goals_per_90', pa.float64(), nullable=True),
    pa.field('assists_per_90', pa.float64(), nullable=True),
    pa.field('xg_per_90', pa.float64(), nullable=True),
    pa.field('xa_per_90', pa.float64(), nullable=True),
])

# --- Team Shooting Stats ---
FBREF_TEAM_SHOOTING_SCHEMA = create_schema_with_metadata([
    *_TEAM_IDENTIFICATION_FIELDS,
    pa.field('goals', pa.int32(), nullable=True),
    pa.field('shots', pa.int32(), nullable=True),
    pa.field('shots_on_target', pa.int32(), nullable=True),
    pa.field('shot_on_target_pct', pa.float64(), nullable=True),
    pa.field('shots_per_90', pa.float64(), nullable=True),
    pa.field('shots_on_target_per_90', pa.float64(), nullable=True),
    pa.field('goals_per_shot', pa.float64(), nullable=True),
    pa.field('goals_per_shot_on_target', pa.float64(), nullable=True),
    pa.field('avg_shot_distance', pa.float64(), nullable=True),
    pa.field('free_kick_shots', pa.int32(), nullable=True),
    pa.field('penalty_goals', pa.int32(), nullable=True),
    pa.field('penalty_attempts', pa.int32(), nullable=True),
    pa.field('xg', pa.float64(), nullable=True),
    pa.field('npxg', pa.float64(), nullable=True),
    pa.field('npxg_per_shot', pa.float64(), nullable=True),
    pa.field('goals_minus_xg', pa.float64(), nullable=True),
    pa.field('npg_minus_npxg', pa.float64(), nullable=True),
])

# --- Team Passing Stats ---
FBREF_TEAM_PASSING_SCHEMA = create_schema_with_metadata([
    *_TEAM_IDENTIFICATION_FIELDS,
    pa.field('passes_completed', pa.int32(), nullable=True),
    pa.field('passes_attempted', pa.int32(), nullable=True),
    pa.field('pass_completion_pct', pa.float64(), nullable=True),
    pa.field('total_pass_distance', pa.int32(), nullable=True),
    pa.field('progressive_pass_distance', pa.int32(), nullable=True),
    pa.field('short_passes_completed', pa.int32(), nullable=True),
    pa.field('short_passes_attempted', pa.int32(), nullable=True),
    pa.field('short_pass_completion_pct', pa.float64(), nullable=True),
    pa.field('medium_passes_completed', pa.int32(), nullable=True),
    pa.field('medium_passes_attempted', pa.int32(), nullable=True),
    pa.field('medium_pass_completion_pct', pa.float64(), nullable=True),
    pa.field('long_passes_completed', pa.int32(), nullable=True),
    pa.field('long_passes_attempted', pa.int32(), nullable=True),
    pa.field('long_pass_completion_pct', pa.float64(), nullable=True),
    pa.field('assists', pa.int32(), nullable=True),
    pa.field('xa', pa.float64(), nullable=True),
    pa.field('key_passes', pa.int32(), nullable=True),
    pa.field('passes_into_final_third', pa.int32(), nullable=True),
    pa.field('passes_into_penalty_area', pa.int32(), nullable=True),
    pa.field('crosses_into_penalty_area', pa.int32(), nullable=True),
    pa.field('progressive_passes', pa.int32(), nullable=True),
])

# --- Team Passing Types Stats ---
FBREF_TEAM_PASSING_TYPES_SCHEMA = create_schema_with_metadata([
    *_TEAM_IDENTIFICATION_FIELDS,
    pa.field('passes_attempted', pa.int32(), nullable=True),
    pa.field('passes_live', pa.int32(), nullable=True),
    pa.field('passes_dead', pa.int32(), nullable=True),
    pa.field('passes_free_kick', pa.int32(), nullable=True),
    pa.field('through_balls', pa.int32(), nullable=True),
    pa.field('switches', pa.int32(), nullable=True),
    pa.field('crosses', pa.int32(), nullable=True),
    pa.field('throw_ins', pa.int32(), nullable=True),
    pa.field('corner_kicks', pa.int32(), nullable=True),
    pa.field('corner_kicks_in', pa.int32(), nullable=True),
    pa.field('corner_kicks_out', pa.int32(), nullable=True),
    pa.field('corner_kicks_straight', pa.int32(), nullable=True),
    pa.field('passes_completed', pa.int32(), nullable=True),
    pa.field('passes_offside', pa.int32(), nullable=True),
    pa.field('passes_blocked', pa.int32(), nullable=True),
])

# --- Team GCA Stats ---
FBREF_TEAM_GCA_SCHEMA = create_schema_with_metadata([
    *_TEAM_IDENTIFICATION_FIELDS,
    pa.field('sca', pa.int32(), nullable=True),
    pa.field('sca_per_90', pa.float64(), nullable=True),
    pa.field('sca_passes_live', pa.int32(), nullable=True),
    pa.field('sca_passes_dead', pa.int32(), nullable=True),
    pa.field('sca_take_ons', pa.int32(), nullable=True),
    pa.field('sca_shots', pa.int32(), nullable=True),
    pa.field('sca_fouls_drawn', pa.int32(), nullable=True),
    pa.field('sca_defense', pa.int32(), nullable=True),
    pa.field('gca', pa.int32(), nullable=True),
    pa.field('gca_per_90', pa.float64(), nullable=True),
    pa.field('gca_passes_live', pa.int32(), nullable=True),
    pa.field('gca_passes_dead', pa.int32(), nullable=True),
    pa.field('gca_take_ons', pa.int32(), nullable=True),
    pa.field('gca_shots', pa.int32(), nullable=True),
    pa.field('gca_fouls_drawn', pa.int32(), nullable=True),
    pa.field('gca_defense', pa.int32(), nullable=True),
])

# --- Team Defense Stats ---
FBREF_TEAM_DEFENSE_SCHEMA = create_schema_with_metadata([
    *_TEAM_IDENTIFICATION_FIELDS,
    pa.field('tackles', pa.int32(), nullable=True),
    pa.field('tackles_won', pa.int32(), nullable=True),
    pa.field('tackles_def_3rd', pa.int32(), nullable=True),
    pa.field('tackles_mid_3rd', pa.int32(), nullable=True),
    pa.field('tackles_att_3rd', pa.int32(), nullable=True),
    pa.field('challenge_tackles', pa.int32(), nullable=True),
    pa.field('challenges', pa.int32(), nullable=True),
    pa.field('challenge_tackles_pct', pa.float64(), nullable=True),
    pa.field('challenges_lost', pa.int32(), nullable=True),
    pa.field('blocks', pa.int32(), nullable=True),
    pa.field('blocked_shots', pa.int32(), nullable=True),
    pa.field('blocked_passes', pa.int32(), nullable=True),
    pa.field('interceptions', pa.int32(), nullable=True),
    pa.field('tackles_interceptions', pa.int32(), nullable=True),
    pa.field('clearances', pa.int32(), nullable=True),
    pa.field('errors', pa.int32(), nullable=True),
])

# --- Team Possession Stats ---
FBREF_TEAM_POSSESSION_SCHEMA = create_schema_with_metadata([
    *_TEAM_IDENTIFICATION_FIELDS,
    pa.field('possession', pa.float64(), nullable=True),
    pa.field('touches', pa.int32(), nullable=True),
    pa.field('touches_def_pen', pa.int32(), nullable=True),
    pa.field('touches_def_3rd', pa.int32(), nullable=True),
    pa.field('touches_mid_3rd', pa.int32(), nullable=True),
    pa.field('touches_att_3rd', pa.int32(), nullable=True),
    pa.field('touches_att_pen', pa.int32(), nullable=True),
    pa.field('touches_live', pa.int32(), nullable=True),
    pa.field('take_ons_attempted', pa.int32(), nullable=True),
    pa.field('take_ons_succeeded', pa.int32(), nullable=True),
    pa.field('take_ons_pct', pa.float64(), nullable=True),
    pa.field('take_ons_tackled', pa.int32(), nullable=True),
    pa.field('take_ons_tackled_pct', pa.float64(), nullable=True),
    pa.field('carries', pa.int32(), nullable=True),
    pa.field('carry_distance', pa.int32(), nullable=True),
    pa.field('carry_progressive_distance', pa.int32(), nullable=True),
    pa.field('progressive_carries', pa.int32(), nullable=True),
    pa.field('carries_into_final_third', pa.int32(), nullable=True),
    pa.field('carries_into_penalty_area', pa.int32(), nullable=True),
    pa.field('miscontrols', pa.int32(), nullable=True),
    pa.field('dispossessed', pa.int32(), nullable=True),
    pa.field('passes_received', pa.int32(), nullable=True),
    pa.field('progressive_passes_received', pa.int32(), nullable=True),
])

# --- Team Playing Time Stats ---
FBREF_TEAM_PLAYINGTIME_SCHEMA = create_schema_with_metadata([
    *_TEAM_IDENTIFICATION_FIELDS,
    pa.field('matches_played', pa.int32(), nullable=True),
    pa.field('minutes', pa.int32(), nullable=True),
    pa.field('minutes_per_match', pa.float64(), nullable=True),
    pa.field('minutes_pct', pa.float64(), nullable=True),
    pa.field('minutes_90s', pa.float64(), nullable=True),
    pa.field('starts', pa.int32(), nullable=True),
    pa.field('minutes_per_start', pa.float64(), nullable=True),
    pa.field('complete_matches', pa.int32(), nullable=True),
    pa.field('subs', pa.int32(), nullable=True),
    pa.field('minutes_per_sub', pa.float64(), nullable=True),
    pa.field('unused_subs', pa.int32(), nullable=True),
    pa.field('points_per_match', pa.float64(), nullable=True),
    pa.field('on_goals', pa.int32(), nullable=True),
    pa.field('on_goals_against', pa.int32(), nullable=True),
    pa.field('plus_minus', pa.int32(), nullable=True),
    pa.field('plus_minus_per_90', pa.float64(), nullable=True),
    pa.field('on_xg', pa.float64(), nullable=True),
    pa.field('on_xga', pa.float64(), nullable=True),
    pa.field('xg_plus_minus', pa.float64(), nullable=True),
    pa.field('xg_plus_minus_per_90', pa.float64(), nullable=True),
])

# --- Team Misc Stats ---
FBREF_TEAM_MISC_SCHEMA = create_schema_with_metadata([
    *_TEAM_IDENTIFICATION_FIELDS,
    pa.field('yellow_cards', pa.int32(), nullable=True),
    pa.field('red_cards', pa.int32(), nullable=True),
    pa.field('second_yellow', pa.int32(), nullable=True),
    pa.field('fouls_committed', pa.int32(), nullable=True),
    pa.field('fouls_drawn', pa.int32(), nullable=True),
    pa.field('offsides', pa.int32(), nullable=True),
    pa.field('crosses', pa.int32(), nullable=True),
    pa.field('interceptions', pa.int32(), nullable=True),
    pa.field('tackles_won', pa.int32(), nullable=True),
    pa.field('penalties_won', pa.int32(), nullable=True),
    pa.field('penalties_conceded', pa.int32(), nullable=True),
    pa.field('own_goals', pa.int32(), nullable=True),
    pa.field('ball_recoveries', pa.int32(), nullable=True),
    pa.field('aerials_won', pa.int32(), nullable=True),
    pa.field('aerials_lost', pa.int32(), nullable=True),
    pa.field('aerials_won_pct', pa.float64(), nullable=True),
])

# --- Keeper Basic Stats ---
FBREF_KEEPER_BASIC_SCHEMA = create_schema_with_metadata([
    *_PLAYER_IDENTIFICATION_FIELDS,
    pa.field('matches_played', pa.int32(), nullable=True),
    pa.field('starts', pa.int32(), nullable=True),
    pa.field('minutes', pa.int32(), nullable=True),
    pa.field('minutes_90s', pa.float64(), nullable=True),
    pa.field('goals_against', pa.int32(), nullable=True),
    pa.field('goals_against_per_90', pa.float64(), nullable=True),
    pa.field('shots_on_target_against', pa.int32(), nullable=True),
    pa.field('saves', pa.int32(), nullable=True),
    pa.field('save_pct', pa.float64(), nullable=True),
    pa.field('wins', pa.int32(), nullable=True),
    pa.field('draws', pa.int32(), nullable=True),
    pa.field('losses', pa.int32(), nullable=True),
    pa.field('clean_sheets', pa.int32(), nullable=True),
    pa.field('clean_sheet_pct', pa.float64(), nullable=True),
    pa.field('penalties_faced', pa.int32(), nullable=True),
    pa.field('penalties_allowed', pa.int32(), nullable=True),
    pa.field('penalties_saved', pa.int32(), nullable=True),
    pa.field('penalties_missed', pa.int32(), nullable=True),
])

# --- Keeper Advanced Stats ---
FBREF_KEEPER_ADVANCED_SCHEMA = create_schema_with_metadata([
    *_PLAYER_IDENTIFICATION_FIELDS,
    pa.field('goals_against', pa.int32(), nullable=True),
    pa.field('penalties_allowed', pa.int32(), nullable=True),
    pa.field('free_kick_goals_against', pa.int32(), nullable=True),
    pa.field('corner_kick_goals_against', pa.int32(), nullable=True),
    pa.field('own_goals_against', pa.int32(), nullable=True),
    pa.field('psxg', pa.float64(), nullable=True),
    pa.field('psxg_per_shot_on_target', pa.float64(), nullable=True),
    pa.field('psxg_minus_goals_allowed', pa.float64(), nullable=True),
    pa.field('psxg_minus_goals_allowed_per_90', pa.float64(), nullable=True),
    pa.field('launched_completed', pa.int32(), nullable=True),
    pa.field('launched_attempted', pa.int32(), nullable=True),
    pa.field('launched_completion_pct', pa.float64(), nullable=True),
    pa.field('passes_attempted', pa.int32(), nullable=True),
    pa.field('throws_attempted', pa.int32(), nullable=True),
    pa.field('pass_launch_pct', pa.float64(), nullable=True),
    pa.field('avg_pass_length', pa.float64(), nullable=True),
    pa.field('goal_kicks_attempted', pa.int32(), nullable=True),
    pa.field('goal_kicks_launch_pct', pa.float64(), nullable=True),
    pa.field('avg_goal_kick_length', pa.float64(), nullable=True),
    pa.field('crosses_faced', pa.int32(), nullable=True),
    pa.field('crosses_stopped', pa.int32(), nullable=True),
    pa.field('crosses_stopped_pct', pa.float64(), nullable=True),
    pa.field('def_actions_outside_pen', pa.int32(), nullable=True),
    pa.field('def_actions_outside_pen_per_90', pa.float64(), nullable=True),
    pa.field('avg_def_action_distance', pa.float64(), nullable=True),
])

# =============================================================================
# LEGACY MERGED SCHEMAS (kept for backward compatibility)
# =============================================================================

# Keeper stats (basic). Advanced 'keeper_adv' fields removed in #606 — that
# stat_type is no longer scraped (all-NULL since the FBref Feb-2026 restriction).
FBREF_KEEPER_STATS_SCHEMA = create_schema_with_metadata([
    # === Identification ===
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('player', pa.string(), nullable=True),
    pa.field('player_id', pa.string(), nullable=True),
    pa.field('nation', pa.string(), nullable=True),
    pa.field('team', pa.string(), nullable=True),
    pa.field('age', pa.int32(), nullable=True),
    pa.field('born', pa.int32(), nullable=True),

    # === Playing Time ===
    pa.field('matches_played', pa.int32(), nullable=True),
    pa.field('starts', pa.int32(), nullable=True),
    pa.field('minutes', pa.int32(), nullable=True),
    pa.field('minutes_90s', pa.float64(), nullable=True),

    # === Basic Keeping (from keeper) ===
    pa.field('goals_against', pa.int32(), nullable=True),
    pa.field('goals_against_per_90', pa.float64(), nullable=True),
    pa.field('shots_on_target_against', pa.int32(), nullable=True),
    pa.field('saves', pa.int32(), nullable=True),
    pa.field('save_pct', pa.float64(), nullable=True),
    pa.field('wins', pa.int32(), nullable=True),
    pa.field('draws', pa.int32(), nullable=True),
    pa.field('losses', pa.int32(), nullable=True),
    pa.field('clean_sheets', pa.int32(), nullable=True),
    pa.field('clean_sheet_pct', pa.float64(), nullable=True),

    # === Penalties ===
    pa.field('penalties_faced', pa.int32(), nullable=True),
    pa.field('penalties_allowed', pa.int32(), nullable=True),
    pa.field('penalties_saved', pa.int32(), nullable=True),
    pa.field('penalties_missed', pa.int32(), nullable=True),
    pa.field('penalty_save_pct', pa.float64(), nullable=True),
])
