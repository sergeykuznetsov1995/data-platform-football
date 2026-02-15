"""
FBref Constants
===============

Configuration constants for FBref scraper.
"""

# FBref base URL
BASE_URL = 'https://fbref.com'

# League configuration with competition IDs and URL slugs
LEAGUE_IDS = {
    'ENG-Premier League': {'comp_id': '9', 'slug': 'Premier-League'},
    'ESP-La Liga': {'comp_id': '12', 'slug': 'La-Liga'},
    'GER-Bundesliga': {'comp_id': '20', 'slug': 'Bundesliga'},
    'ITA-Serie A': {'comp_id': '11', 'slug': 'Serie-A'},
    'FRA-Ligue 1': {'comp_id': '13', 'slug': 'Ligue-1'},
    'UEFA-Champions League': {'comp_id': '8', 'slug': 'Champions-League'},
    'UEFA-Europa League': {'comp_id': '19', 'slug': 'Europa-League'},
    'INT-World Cup': {'comp_id': '1', 'slug': 'World-Cup'},
}

# Available stat types for players (outfield)
PLAYER_STAT_TYPES = [
    'stats',           # Standard stats
    'shooting',        # Shooting stats
    'passing',         # Passing stats
    'passing_types',   # Pass types
    'gca',             # Goal and shot creation
    'defense',         # Defensive actions
    'possession',      # Possession stats
    'playingtime',     # Playing time
    'misc',            # Miscellaneous
]

# Goalkeeper-specific stat types
KEEPER_STAT_TYPES = [
    'keeper',          # Goalkeeper basic stats
    'keeper_adv',      # Goalkeeper advanced stats
]

# Player match-level stat types (per-game statistics)
PLAYER_MATCH_STAT_TYPES = [
    'summary',         # Basic match summary
    'passing',         # Match passing stats
    'defense',         # Match defense stats
    'possession',      # Match possession stats
    'misc',            # Match misc stats
]

# Available stat types for teams (squads)
TEAM_STAT_TYPES = [
    'stats',           # Standard stats
    'shooting',        # Shooting stats
    'passing',         # Passing stats
    'passing_types',   # Pass types
    'gca',             # Goal and shot creation
    'defense',         # Defensive actions
    'possession',      # Possession stats
    'playingtime',     # Playing time
    'misc',            # Miscellaneous
]

# Default rate limit (requests per minute)
DEFAULT_RATE_LIMIT = 20
