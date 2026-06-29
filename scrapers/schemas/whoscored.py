"""
WhoScored Schema Definitions
============================

PyArrow schemas for WhoScored event data in SPADL format.
"""

import pyarrow as pa
from scrapers.schemas.common import create_schema_with_metadata

# SPADL (Soccer Player Action Description Language) event schema
WHOSCORED_EVENTS_SPADL_SCHEMA = create_schema_with_metadata([
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('game_id', pa.int64(), nullable=True),
    pa.field('match_date', pa.date32(), nullable=True),
    pa.field('home_team', pa.string(), nullable=True),
    pa.field('away_team', pa.string(), nullable=True),
    pa.field('home_team_id', pa.int64(), nullable=True),
    pa.field('away_team_id', pa.int64(), nullable=True),

    # Event identifiers
    pa.field('event_id', pa.int64(), nullable=True),
    pa.field('period_id', pa.int32(), nullable=True),  # 1=first half, 2=second half
    pa.field('time_seconds', pa.int32(), nullable=True),

    # Team and player
    pa.field('team_id', pa.int64(), nullable=True),
    pa.field('team', pa.string(), nullable=True),
    pa.field('player_id', pa.int64(), nullable=True),
    pa.field('player', pa.string(), nullable=True),

    # Location (SPADL coordinates: 0-105 x 0-68)
    pa.field('start_x', pa.float64(), nullable=True),
    pa.field('start_y', pa.float64(), nullable=True),
    pa.field('end_x', pa.float64(), nullable=True),
    pa.field('end_y', pa.float64(), nullable=True),

    # Action description
    pa.field('action_type', pa.string(), nullable=True),
    pa.field('result', pa.string(), nullable=True),
    pa.field('bodypart', pa.string(), nullable=True),

    # Original WhoScored event type
    pa.field('original_event_type', pa.string(), nullable=True),
    pa.field('original_outcome_type', pa.string(), nullable=True),

    # Additional context
    pa.field('is_goal', pa.bool_(), nullable=True),
    pa.field('is_own_goal', pa.bool_(), nullable=True),
    pa.field('is_assist', pa.bool_(), nullable=True),
    pa.field('is_key_pass', pa.bool_(), nullable=True),
])

# SPADL action types for reference
SPADL_ACTION_TYPES = [
    'pass',
    'cross',
    'throw_in',
    'freekick_crossed',
    'freekick_short',
    'corner_crossed',
    'corner_short',
    'take_on',
    'foul',
    'tackle',
    'interception',
    'shot',
    'shot_penalty',
    'shot_freekick',
    'keeper_save',
    'keeper_claim',
    'keeper_punch',
    'keeper_pick_up',
    'clearance',
    'bad_touch',
    'non_action',
    'dribble',
    'goalkick',
]

# SPADL result types for reference
SPADL_RESULT_TYPES = [
    'fail',
    'success',
    'offside',
    'owngoal',
    'yellow_card',
    'red_card',
]

# SPADL body parts for reference
SPADL_BODYPARTS = [
    'foot',
    'head',
    'other',
    'head/other',
]

# Match summary schema
WHOSCORED_MATCH_SCHEMA = create_schema_with_metadata([
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('game_id', pa.int64(), nullable=True),
    pa.field('match_date', pa.date32(), nullable=True),
    pa.field('match_url', pa.string(), nullable=True),
    pa.field('home_team', pa.string(), nullable=True),
    pa.field('away_team', pa.string(), nullable=True),
    pa.field('home_team_id', pa.int64(), nullable=True),
    pa.field('away_team_id', pa.int64(), nullable=True),
    pa.field('home_score', pa.int32(), nullable=True),
    pa.field('away_score', pa.int32(), nullable=True),
    pa.field('venue', pa.string(), nullable=True),
    pa.field('referee', pa.string(), nullable=True),
    pa.field('attendance', pa.int32(), nullable=True),

    # Match stats
    pa.field('home_possession', pa.float64(), nullable=True),
    pa.field('away_possession', pa.float64(), nullable=True),
    pa.field('home_shots', pa.int32(), nullable=True),
    pa.field('away_shots', pa.int32(), nullable=True),
    pa.field('home_shots_on_target', pa.int32(), nullable=True),
    pa.field('away_shots_on_target', pa.int32(), nullable=True),
    pa.field('home_corners', pa.int32(), nullable=True),
    pa.field('away_corners', pa.int32(), nullable=True),
    pa.field('home_fouls', pa.int32(), nullable=True),
    pa.field('away_fouls', pa.int32(), nullable=True),
])

# Player ratings schema
WHOSCORED_PLAYER_RATINGS_SCHEMA = create_schema_with_metadata([
    pa.field('league', pa.string(), nullable=False),
    pa.field('season', pa.int32(), nullable=False),
    pa.field('game_id', pa.int64(), nullable=True),
    pa.field('player_id', pa.int64(), nullable=True),
    pa.field('player', pa.string(), nullable=True),
    pa.field('team_id', pa.int64(), nullable=True),
    pa.field('team', pa.string(), nullable=True),
    pa.field('is_starter', pa.bool_(), nullable=True),
    pa.field('position', pa.string(), nullable=True),
    pa.field('minutes_played', pa.int32(), nullable=True),
    pa.field('rating', pa.float64(), nullable=True),
    pa.field('goals', pa.int32(), nullable=True),
    pa.field('assists', pa.int32(), nullable=True),
    pa.field('shots', pa.int32(), nullable=True),
    pa.field('passes', pa.int32(), nullable=True),
    pa.field('pass_accuracy', pa.float64(), nullable=True),
    pa.field('dribbles', pa.int32(), nullable=True),
    pa.field('tackles', pa.int32(), nullable=True),
    pa.field('interceptions', pa.int32(), nullable=True),
    pa.field('fouls_committed', pa.int32(), nullable=True),
    pa.field('fouls_won', pa.int32(), nullable=True),
])
