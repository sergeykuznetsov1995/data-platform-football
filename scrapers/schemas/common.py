"""
Common Schema Definitions
=========================

Shared schema components and metadata fields for all scrapers.
"""

import pyarrow as pa

# Standard metadata fields added to all tables
METADATA_FIELDS = [
    pa.field('_source', pa.string(), nullable=False),
    pa.field('_ingested_at', pa.timestamp('us'), nullable=False),
    pa.field('_batch_id', pa.string(), nullable=False),
]

# Common column types
LEAGUE_FIELD = pa.field('league', pa.string(), nullable=False)
SEASON_FIELD = pa.field('season', pa.int32(), nullable=False)
TEAM_FIELD = pa.field('team', pa.string(), nullable=True)
PLAYER_FIELD = pa.field('player', pa.string(), nullable=True)
MATCH_ID_FIELD = pa.field('match_id', pa.string(), nullable=True)
MATCH_DATE_FIELD = pa.field('match_date', pa.date32(), nullable=True)


def create_schema_with_metadata(fields: list) -> pa.Schema:
    """
    Create PyArrow schema with standard metadata fields.

    Args:
        fields: List of PyArrow fields

    Returns:
        PyArrow Schema with metadata fields appended
    """
    return pa.schema(fields + METADATA_FIELDS)


# League name mappings for normalization
LEAGUE_ALIASES = {
    # Premier League
    'ENG-Premier League': 'ENG-Premier League',
    'Premier League': 'ENG-Premier League',
    'EPL': 'ENG-Premier League',
    'England': 'ENG-Premier League',

    # La Liga
    'ESP-La Liga': 'ESP-La Liga',
    'La Liga': 'ESP-La Liga',
    'LaLiga': 'ESP-La Liga',
    'Spain': 'ESP-La Liga',

    # Bundesliga
    'GER-Bundesliga': 'GER-Bundesliga',
    'Bundesliga': 'GER-Bundesliga',
    'Germany': 'GER-Bundesliga',

    # Serie A
    'ITA-Serie A': 'ITA-Serie A',
    'Serie A': 'ITA-Serie A',
    'Italy': 'ITA-Serie A',

    # Ligue 1
    'FRA-Ligue 1': 'FRA-Ligue 1',
    'Ligue 1': 'FRA-Ligue 1',
    'France': 'FRA-Ligue 1',

    # World Cup (INT)
    'INT-World Cup': 'INT-World Cup',
    'World Cup': 'INT-World Cup',
    'FIFA World Cup': 'INT-World Cup',
    'World Championship': 'INT-World Cup',
}


def normalize_league_name(league: str) -> str:
    """Normalize league name to standard format."""
    return LEAGUE_ALIASES.get(league, league)


# Season format helpers
def format_season(year: int) -> str:
    """Format season year to standard format (e.g., 2023 -> '2023-24')."""
    return f"{year}-{str(year + 1)[-2:]}"


def parse_season(season_str: str) -> int:
    """Parse season string to starting year (e.g., '2023-24' -> 2023)."""
    if isinstance(season_str, int):
        return season_str
    return int(season_str.split('-')[0])
