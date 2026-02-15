"""
FBref Scraper (Compatibility Module)
====================================

This module is maintained for backwards compatibility.
The actual implementation has been moved to scrapers.fbref package.

For new code, import directly from:
    from scrapers.fbref import FBrefScraper
"""

# Re-export everything from the new location for backwards compatibility
from scrapers.fbref import (
    FBrefScraper,
    FBrefSeleniumScraper,
    BASE_URL,
    LEAGUE_IDS,
    PLAYER_STAT_TYPES,
    TEAM_STAT_TYPES,
)

__all__ = [
    'FBrefScraper',
    'FBrefSeleniumScraper',
    'BASE_URL',
    'LEAGUE_IDS',
    'PLAYER_STAT_TYPES',
    'TEAM_STAT_TYPES',
]
