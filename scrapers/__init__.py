"""
Football Data Scrapers
======================

This module provides scrapers for collecting football data from various sources
and writing to Apache Iceberg tables.

Sources:
- FBref: Match statistics, player stats, advanced metrics
- Understat: xG data, shot maps
- WhoScored: Event data in SPADL format
- FotMob: Match events, lineups
- SofaScore: Live scores, statistics
- SoFIFA: FIFA ratings, player attributes
- ClubElo: Historical ELO ratings
- ESPN: Schedules, results
- MatchHistory: Historical match data

Recommended Scrapers (Selenium-based):
- FBrefSeleniumScraper: For FBref data (bypasses Cloudflare)
- FotMobSeleniumScraper: For FotMob data (handles session cookies)
- MatchHistoryDirectScraper: For football-data.co.uk (with Selenium fallback)
- WhoScoredScraper: For WhoScored data (bypasses Cloudflare)
"""

from scrapers.base.base_scraper import BaseScraper, SeleniumScraper
from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.fbref_selenium_scraper import FBrefSeleniumScraper
from scrapers.fotmob_selenium_scraper import FotMobSeleniumScraper
from scrapers.matchhistory_direct_scraper import MatchHistoryDirectScraper
from scrapers.whoscored_scraper import WhoScoredScraper

__all__ = [
    # Base classes
    'BaseScraper',
    'SeleniumScraper',
    'IcebergWriter',
    # Recommended scrapers (Selenium-based)
    'FBrefSeleniumScraper',
    'FotMobSeleniumScraper',
    'MatchHistoryDirectScraper',
    'WhoScoredScraper',
]

__version__ = '0.1.0'
