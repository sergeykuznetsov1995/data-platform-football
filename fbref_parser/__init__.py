"""
FBref Parser - Production-ready Python package for scraping football statistics from FBref.com

This package provides modular components for parsing player statistics including:
- Field player statistics (standard, shooting, passing, defense, possession, etc.)
- Goalkeeper statistics (saves, clean sheets, PSxG, plus all field player stats)
- Universal league discovery and team extraction utilities

Main exports:
    FieldPlayerParser: Parser for field players
    GoalkeeperParser: Parser for goalkeepers

League discovery utilities (new in v2.1):
    discover_all_leagues: Automatically discover all leagues from FBref
    extract_league_teams: Extract teams from any league
"""

from .parsers.field_player import FieldPlayerParser
from .parsers.goalkeeper import GoalkeeperParser
from .utils.league_discovery import discover_all_leagues
from .utils.league_helpers import extract_league_teams

__version__ = "2.1.0"
__all__ = [
    'FieldPlayerParser',
    'GoalkeeperParser',
    'discover_all_leagues',
    'extract_league_teams'
]
