"""
FBref Parser - Production-ready Python package for scraping football statistics from FBref.com

This package provides modular components for parsing player statistics including:
- Field player statistics (standard, shooting, passing, defense, possession, etc.)
- Goalkeeper statistics (saves, clean sheets, PSxG, plus all field player stats)

Main exports:
    FieldPlayerParser: Parser for field players
    GoalkeeperParser: Parser for goalkeepers
"""

from .parsers.field_player import FieldPlayerParser
from .parsers.goalkeeper import GoalkeeperParser

__version__ = "2.0.0"
__all__ = ['FieldPlayerParser', 'GoalkeeperParser']
