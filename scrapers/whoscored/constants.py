"""
WhoScored Constants
===================

Configuration constants for WhoScored scraper including:
- League configuration with region/tournament IDs
- Event type mapping to SPADL actions
- Known season/stage IDs for fallback
- SPADL action type definitions
"""

from typing import Dict, Tuple

# SPADL pitch dimensions (in meters)
SPADL_PITCH_LENGTH = 105.0
SPADL_PITCH_WIDTH = 68.0

# WhoScored uses 100x100 coordinate system
WS_COORD_MAX = 100.0

# Event type mapping to SPADL actions
EVENT_TYPE_MAPPING: Dict[str, str] = {
    'Pass': 'pass',
    'Cross': 'cross',
    'Throw-in': 'throw_in',
    'FreekickShort': 'freekick_short',
    'Corner': 'corner_crossed',
    'TakeOn': 'take_on',
    'Foul': 'foul',
    'Tackle': 'tackle',
    'Interception': 'interception',
    'Shot': 'shot',
    'ShotOnPost': 'shot',
    'MissedShots': 'shot',
    'SavedShot': 'shot',
    'Goal': 'shot',
    'Clearance': 'clearance',
    'BallTouch': 'dribble',
    'Aerial': 'non_action',
    'OffsidePass': 'pass',
}

# League configuration with region_id, tournament_id and URL slug
LEAGUE_CONFIG: Dict[str, Dict[str, any]] = {
    'ENG-Premier League': {
        'region_id': 252,
        'tournament_id': 2,
        'slug': 'england-premier-league',
    },
    'ESP-La Liga': {
        'region_id': 206,
        'tournament_id': 4,
        'slug': 'spain-laliga',
    },
    'GER-Bundesliga': {
        'region_id': 81,
        'tournament_id': 3,
        'slug': 'germany-bundesliga',
    },
    'ITA-Serie A': {
        'region_id': 108,
        'tournament_id': 5,
        'slug': 'italy-serie-a',
    },
    'FRA-Ligue 1': {
        'region_id': 74,
        'tournament_id': 22,
        'slug': 'france-ligue-1',
    },
}

# Fallback season/stage IDs (updated periodically)
# These allow scraping when Selenium can't interact with dropdown
# Format: (league, season) -> (season_id, stage_id)
KNOWN_SEASON_IDS: Dict[Tuple[str, int], Tuple[str, str]] = {
    # 2025/2026 season
    ('ENG-Premier League', 2025): ('10743', '24533'),
    ('ESP-La Liga', 2025): ('10803', '24622'),
    ('GER-Bundesliga', 2025): ('10720', '24478'),
    ('ITA-Serie A', 2025): ('10732', '24500'),
    ('FRA-Ligue 1', 2025): ('10792', '24609'),
    # 2024/2025 season
    ('ENG-Premier League', 2024): ('9618', '22076'),
    ('ESP-La Liga', 2024): ('9621', '22082'),
    ('GER-Bundesliga', 2024): ('9622', '22084'),
    ('ITA-Serie A', 2024): ('9620', '22080'),
    ('FRA-Ligue 1', 2024): ('9619', '22078'),
    # 2023/2024 season
    ('ENG-Premier League', 2023): ('9155', '20934'),
    ('ESP-La Liga', 2023): ('9158', '20940'),
    ('GER-Bundesliga', 2023): ('9159', '20942'),
    ('ITA-Serie A', 2023): ('9157', '20938'),
    ('FRA-Ligue 1', 2023): ('9156', '20936'),
}

# SPADL action type definitions for reference
SPADL_ACTIONS: Dict[str, str] = {
    'pass': 'Normal pass in open play',
    'cross': 'Cross into the box',
    'throw_in': 'Throw in',
    'freekick_crossed': 'Freekick crossed into the box',
    'freekick_short': 'Short freekick',
    'corner_crossed': 'Corner crossed into the box',
    'corner_short': 'Short corner',
    'take_on': 'Dribble past opponent',
    'foul': 'Foul',
    'tackle': 'Tackle',
    'interception': 'Interception',
    'shot': 'Shot from open play',
    'shot_penalty': 'Penalty kick',
    'shot_freekick': 'Direct freekick on goal',
    'keeper_save': 'Goalkeeper save',
    'keeper_claim': 'Goalkeeper catch',
    'keeper_punch': 'Goalkeeper punch',
    'keeper_pick_up': 'Goalkeeper picks up ball',
    'clearance': 'Clearance',
    'bad_touch': 'Bad touch / loss of possession',
    'non_action': 'Non-action (aerial duel, etc.)',
    'dribble': 'Dribble / carry',
    'goalkick': 'Goal kick',
}

# Base URL
BASE_URL = 'https://www.whoscored.com'
