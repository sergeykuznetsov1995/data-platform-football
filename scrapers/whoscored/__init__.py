"""
WhoScored Scraper Package
=========================

Scraper for WhoScored event data with SPADL conversion.

Modules:
- scraper: Main WhoScoredScraper class
- constants: Configuration constants and mappings
- spadl_converter: Event to SPADL format conversion
- page_navigator: Page navigation and URL extraction
"""

from scrapers.whoscored.scraper import WhoScoredScraper
from scrapers.whoscored.constants import (
    LEAGUE_CONFIG,
    EVENT_TYPE_MAPPING,
    KNOWN_SEASON_IDS,
    SPADL_ACTIONS,
    BASE_URL,
)
from scrapers.whoscored.spadl_converter import (
    convert_coordinates,
    event_to_spadl,
)
from scrapers.whoscored.page_navigator import PageNavigator

__all__ = [
    'WhoScoredScraper',
    'PageNavigator',
    'LEAGUE_CONFIG',
    'EVENT_TYPE_MAPPING',
    'KNOWN_SEASON_IDS',
    'SPADL_ACTIONS',
    'BASE_URL',
    'convert_coordinates',
    'event_to_spadl',
]
