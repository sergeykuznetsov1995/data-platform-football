"""
FBref Scraper Package
=====================

Scraper for FBref football statistics with Cloudflare bypass.

Modules:
- scraper: Main FBrefScraper class
- constants: Configuration constants (LEAGUE_IDS, STAT_TYPES)
- url_builder: URL construction functions
- html_parser: HTML parsing and table extraction

Usage:
    from scrapers.fbref import FBrefScraper

    scraper = FBrefScraper(
        leagues=['ENG-Premier League'],
        seasons=[2024],
        headless=True
    )
    result = scraper.scrape_all()
"""

from scrapers.fbref.scraper import FBrefScraper, FBrefSeleniumScraper
from scrapers.fbref.constants import (
    BASE_URL,
    LEAGUE_IDS,
    PLAYER_STAT_TYPES,
    TEAM_STAT_TYPES,
    DEFAULT_RATE_LIMIT,
)
from scrapers.fbref.url_builder import (
    format_season,
    get_schedule_url,
    get_stats_url,
)
from scrapers.fbref.html_parser import (
    extract_tables_from_comments,
    extract_player_ids_from_table,
    extract_team_ids_from_table,
    parse_table,
    find_schedule_table,
    find_team_stats_table,
    find_player_stats_table,
)

__all__ = [
    # Main scraper
    'FBrefScraper',
    'FBrefSeleniumScraper',  # Backwards compatibility alias
    # Constants
    'BASE_URL',
    'LEAGUE_IDS',
    'PLAYER_STAT_TYPES',
    'TEAM_STAT_TYPES',
    'DEFAULT_RATE_LIMIT',
    # URL builders
    'format_season',
    'get_schedule_url',
    'get_stats_url',
    # HTML parsers
    'extract_tables_from_comments',
    'extract_player_ids_from_table',
    'extract_team_ids_from_table',
    'parse_table',
    'find_schedule_table',
    'find_team_stats_table',
    'find_player_stats_table',
]
