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

# Keeper stats
FBREF_KEEPER_SCHEMA = create_schema_with_metadata([
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('player', pa.string(), nullable=True),
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
