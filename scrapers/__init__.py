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

Main Scrapers:
- FBrefScraper: For FBref data (bypasses Cloudflare via Selenium)
- SoccerdataFBrefScraper: Lightweight FBref scraper using soccerdata + Tor
- FotMobScraper: For FotMob data (handles session cookies via Selenium)
- MatchHistoryScraper: For football-data.co.uk (with Selenium fallback)
- WhoScoredScraper: For WhoScored data (bypasses Cloudflare)
"""

from scrapers.base.base_scraper import BaseScraper, SeleniumScraper, SoccerdataScraper
from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.fbref import FBrefScraper
from scrapers.soccerdata_fbref_scraper import SoccerdataFBrefScraper
from scrapers.fotmob_scraper import FotMobScraper
from scrapers.matchhistory_scraper import MatchHistoryScraper
from scrapers.whoscored import WhoScoredScraper

__all__ = [
    # Base classes
    'BaseScraper',
    'SeleniumScraper',
    'SoccerdataScraper',
    'IcebergWriter',
    # Main scrapers
    'FBrefScraper',
    'SoccerdataFBrefScraper',  # Lightweight Tor-based scraper
    'FotMobScraper',
    'MatchHistoryScraper',
    'WhoScoredScraper',
]

__version__ = '0.1.0'
