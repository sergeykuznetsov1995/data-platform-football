"""
Understat Schema Definitions
============================

PyArrow schemas for Understat xG data tables.
"""

import pyarrow as pa
from scrapers.schemas.common import create_schema_with_metadata

# Shot-level data schema
UNDERSTAT_SHOTS_SCHEMA = create_schema_with_metadata([
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('match_id', pa.string(), nullable=True),
    pa.field('shot_id', pa.string(), nullable=True),
    pa.field('player', pa.string(), nullable=True),
    pa.field('player_id', pa.string(), nullable=True),
    pa.field('team', pa.string(), nullable=True),
    pa.field('team_id', pa.string(), nullable=True),
    pa.field('minute', pa.int32(), nullable=True),
    pa.field('x', pa.float64(), nullable=True),  # X coordinate (0-1)
    pa.field('y', pa.float64(), nullable=True),  # Y coordinate (0-1)
    pa.field('xg', pa.float64(), nullable=True),  # Expected goals
    pa.field('result', pa.string(), nullable=True),  # Goal, Saved, Missed, Blocked
    pa.field('situation', pa.string(), nullable=True),  # OpenPlay, SetPiece, Penalty, etc.
    pa.field('shot_type', pa.string(), nullable=True),  # RightFoot, LeftFoot, Head
    pa.field('last_action', pa.string(), nullable=True),  # Pass, Dribble, etc.
    pa.field('is_home', pa.bool_(), nullable=True),
    pa.field('match_date', pa.date32(), nullable=True),
    pa.field('home_team', pa.string(), nullable=True),
    pa.field('away_team', pa.string(), nullable=True),
])

# Player season summary
UNDERSTAT_PLAYERS_SCHEMA = create_schema_with_metadata([
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('player', pa.string(), nullable=True),
    pa.field('player_id', pa.string(), nullable=True),
    pa.field('team', pa.string(), nullable=True),
    pa.field('position', pa.string(), nullable=True),
    pa.field('matches', pa.int32(), nullable=True),
    pa.field('minutes', pa.int32(), nullable=True),
    pa.field('goals', pa.int32(), nullable=True),
    pa.field('assists', pa.int32(), nullable=True),
    pa.field('shots', pa.int32(), nullable=True),
    pa.field('key_passes', pa.int32(), nullable=True),
    pa.field('yellow_cards', pa.int32(), nullable=True),
    pa.field('red_cards', pa.int32(), nullable=True),
    pa.field('xg', pa.float64(), nullable=True),
    pa.field('xa', pa.float64(), nullable=True),
    pa.field('xg_chain', pa.float64(), nullable=True),
    pa.field('xg_buildup', pa.float64(), nullable=True),
    pa.field('npg', pa.int32(), nullable=True),  # Non-penalty goals
    pa.field('npxg', pa.float64(), nullable=True),  # Non-penalty xG
    pa.field('xg_per_90', pa.float64(), nullable=True),
    pa.field('xa_per_90', pa.float64(), nullable=True),
])

# Team season summary
UNDERSTAT_TEAMS_SCHEMA = create_schema_with_metadata([
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('team', pa.string(), nullable=True),
    pa.field('team_id', pa.string(), nullable=True),
    pa.field('matches', pa.int32(), nullable=True),
    pa.field('wins', pa.int32(), nullable=True),
    pa.field('draws', pa.int32(), nullable=True),
    pa.field('losses', pa.int32(), nullable=True),
    pa.field('goals_for', pa.int32(), nullable=True),
    pa.field('goals_against', pa.int32(), nullable=True),
    pa.field('points', pa.int32(), nullable=True),
    pa.field('xg', pa.float64(), nullable=True),
    pa.field('xga', pa.float64(), nullable=True),
    pa.field('npxg', pa.float64(), nullable=True),
    pa.field('npxga', pa.float64(), nullable=True),
    pa.field('xg_difference', pa.float64(), nullable=True),
    pa.field('deep_completions', pa.int32(), nullable=True),  # Passes into final third
    pa.field('deep_completions_allowed', pa.int32(), nullable=True),
    pa.field('ppda', pa.float64(), nullable=True),  # Passes per defensive action
    pa.field('oppda', pa.float64(), nullable=True),  # Opponent PPDA
])

# Match-level data
UNDERSTAT_MATCHES_SCHEMA = create_schema_with_metadata([
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('match_id', pa.string(), nullable=True),
    pa.field('match_date', pa.date32(), nullable=True),
    pa.field('home_team', pa.string(), nullable=True),
    pa.field('away_team', pa.string(), nullable=True),
    pa.field('home_goals', pa.int32(), nullable=True),
    pa.field('away_goals', pa.int32(), nullable=True),
    pa.field('home_xg', pa.float64(), nullable=True),
    pa.field('away_xg', pa.float64(), nullable=True),
    pa.field('home_shots', pa.int32(), nullable=True),
    pa.field('away_shots', pa.int32(), nullable=True),
    pa.field('home_shots_on_target', pa.int32(), nullable=True),
    pa.field('away_shots_on_target', pa.int32(), nullable=True),
    pa.field('home_deep', pa.int32(), nullable=True),
    pa.field('away_deep', pa.int32(), nullable=True),
    pa.field('home_ppda', pa.float64(), nullable=True),
    pa.field('away_ppda', pa.float64(), nullable=True),
    pa.field('is_finished', pa.bool_(), nullable=True),
])
