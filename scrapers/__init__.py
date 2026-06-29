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
- FBrefScraper: Selenium-based FBref scraper (used for match-level data:
  shot_events, match_events, lineups, match_team_stats, match_player_stats)
- NodriverFBrefScraper: Primary FBref scraper for season-level stats and
  schedule (nodriver + cf-verify Cloudflare Turnstile bypass).
- FotMobScraper: For FotMob data (handles session cookies via Selenium)
- MatchHistoryScraper: For football-data.co.uk (with Selenium fallback)
- WhoScoredScraper: For WhoScored data (bypasses Cloudflare)

Note (Apr 2026): SoccerdataFBrefScraper was removed — curl_cffi cannot bypass
Cloudflare Turnstile, so the scraper was non-functional in production.
"""

from scrapers.base.base_scraper import BaseScraper, SeleniumScraper, SoccerdataScraper
from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.fbref import FBrefScraper
from scrapers.nodriver_fbref import NodriverFBrefScraper
from scrapers.fotmob import FotMobScraper
from scrapers.matchhistory import MatchHistoryScraper
from scrapers.whoscored import WhoScoredScraper

__all__ = [
    # Base classes
    'BaseScraper',
    'SeleniumScraper',
    'SoccerdataScraper',
    'IcebergWriter',
    # Main scrapers
    'FBrefScraper',
    'NodriverFBrefScraper',
    'FotMobScraper',
    'MatchHistoryScraper',
    'WhoScoredScraper',
]

__version__ = '0.1.0'
