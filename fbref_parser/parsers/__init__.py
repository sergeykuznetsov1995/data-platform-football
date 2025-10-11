"""
Parser implementations for FBref statistics

Contains:
- BaseParser: Abstract base class with shared logic
- FieldPlayerParser: Parser for field players
- GoalkeeperParser: Parser for goalkeepers
"""

from .base_parser import BaseParser
from .field_player import FieldPlayerParser
from .goalkeeper import GoalkeeperParser

__all__ = ['BaseParser', 'FieldPlayerParser', 'GoalkeeperParser']
