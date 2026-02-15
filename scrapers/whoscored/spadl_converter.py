"""
SPADL Converter
===============

Converts WhoScored event data to SPADL (Soccer Player Action Description Language) format.
"""

import logging
from typing import Any, Dict, Tuple

from scrapers.whoscored.constants import (
    SPADL_PITCH_LENGTH,
    SPADL_PITCH_WIDTH,
    WS_COORD_MAX,
    EVENT_TYPE_MAPPING,
)

logger = logging.getLogger(__name__)


def convert_coordinates(x: float, y: float) -> Tuple[float, float]:
    """
    Convert WhoScored coordinates to SPADL format.

    WhoScored uses 0-100 scale, SPADL uses meters (105x68).

    Args:
        x: X coordinate (0-100)
        y: Y coordinate (0-100)

    Returns:
        Tuple of (x, y) in SPADL coordinates (meters)
    """
    spadl_x = (x / WS_COORD_MAX) * SPADL_PITCH_LENGTH
    spadl_y = (y / WS_COORD_MAX) * SPADL_PITCH_WIDTH
    return spadl_x, spadl_y


def event_to_spadl(event: Dict[str, Any], match_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert WhoScored event to SPADL format.

    Args:
        event: Raw WhoScored event dictionary containing:
            - type: Event type info
            - qualifiers: List of event qualifiers
            - x, y: Start coordinates
            - endX, endY: End coordinates (optional)
            - minute, second: Event timing
            - period: Match period
            - teamId, playerId: IDs
            - outcomeType: Result of the event
        match_info: Match metadata containing:
            - league, season, match_id
            - match_date, home_team, away_team
            - home_team_id, away_team_id

    Returns:
        SPADL formatted event dictionary
    """
    event_type = event.get('type', {}).get('displayName', 'Unknown')
    qualifiers = {
        q.get('type', {}).get('displayName', ''): q.get('value')
        for q in event.get('qualifiers', [])
    }

    # Convert coordinates
    start_x, start_y = 0.0, 0.0
    end_x, end_y = 0.0, 0.0

    if 'x' in event and 'y' in event:
        start_x, start_y = convert_coordinates(
            event.get('x', 0), event.get('y', 0)
        )

    if 'endX' in event and 'endY' in event:
        end_x, end_y = convert_coordinates(
            event.get('endX', event.get('x', 0)),
            event.get('endY', event.get('y', 0))
        )
    else:
        end_x, end_y = start_x, start_y

    # Determine SPADL action type
    action_type = EVENT_TYPE_MAPPING.get(event_type, 'non_action')

    # Special cases based on qualifiers
    if 'Penalty' in qualifiers:
        action_type = 'shot_penalty'
    elif 'FreekickTaken' in qualifiers and event_type == 'Shot':
        action_type = 'shot_freekick'
    elif 'CornerTaken' in qualifiers:
        if 'ShortCorner' in qualifiers:
            action_type = 'corner_short'
        else:
            action_type = 'corner_crossed'

    # Determine result
    outcome = event.get('outcomeType', {}).get('displayName', 'Unknown')
    if outcome == 'Successful':
        result = 'success'
    elif 'Goal' in event_type or event.get('isGoal'):
        result = 'success'
    elif 'OwnGoal' in qualifiers:
        result = 'owngoal'
    elif outcome == 'Unsuccessful':
        result = 'fail'
    else:
        result = 'fail'

    # Determine body part
    bodypart = 'foot'
    if 'Head' in qualifiers:
        bodypart = 'head'
    elif 'OtherBodyPart' in qualifiers:
        bodypart = 'other'

    # Calculate time in seconds
    minute = event.get('minute', 0)
    second = event.get('second', 0)
    period = event.get('period', {}).get('value', 1)

    if period == 1:
        time_seconds = minute * 60 + second
    else:
        time_seconds = 45 * 60 + (minute - 45) * 60 + second

    # Determine team name
    team_id = event.get('teamId')
    if team_id == match_info.get('home_team_id'):
        team = match_info.get('home_team')
    else:
        team = match_info.get('away_team')

    return {
        'league': match_info.get('league'),
        'season': match_info.get('season'),
        'game_id': match_info.get('match_id'),
        'match_date': match_info.get('match_date'),
        'home_team': match_info.get('home_team'),
        'away_team': match_info.get('away_team'),
        'home_team_id': match_info.get('home_team_id'),
        'away_team_id': match_info.get('away_team_id'),
        'event_id': event.get('id'),
        'period_id': period,
        'time_seconds': time_seconds,
        'team_id': team_id,
        'team': team,
        'player_id': event.get('playerId'),
        'player': event.get('playerName', ''),
        'start_x': start_x,
        'start_y': start_y,
        'end_x': end_x,
        'end_y': end_y,
        'action_type': action_type,
        'result': result,
        'bodypart': bodypart,
        'original_event_type': event_type,
        'original_outcome_type': outcome,
        'is_goal': event.get('isGoal', False),
        'is_own_goal': 'OwnGoal' in qualifiers,
        'is_assist': 'IntentionalGoalAssist' in qualifiers or 'IntentionalAssist' in qualifiers,
        'is_key_pass': 'KeyPass' in qualifiers,
    }
