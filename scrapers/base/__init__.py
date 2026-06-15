"""
Base Components for Scrapers
=============================

Core classes and utilities for web scraping.

Modules:
- base_scraper: BaseScraper and SeleniumScraper base classes
- iceberg_writer: IcebergWriter for writing to Apache Iceberg via Trino
- hdfs_client: HDFSClient for WebHDFS operations (legacy, optional)
- trino_manager: TrinoTableManager for Iceberg tables
- browser/: Browser automation package (CloudflareBypass)
"""

from scrapers.base.base_scraper import BaseScraper, ReplaceGuardError, SeleniumScraper
from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.base.hdfs_client import HDFSClient, HDFSError
from scrapers.base.trino_manager import TrinoTableManager, TrinoError
from scrapers.base.browser import CloudflareBypass, browser_session, BrowserConfig

__all__ = [
    'BaseScraper',
    'ReplaceGuardError',
    'SeleniumScraper',
    'IcebergWriter',
    'HDFSClient',
    'HDFSError',
    'TrinoTableManager',
    'TrinoError',
    'CloudflareBypass',
    'BrowserConfig',
    'browser_session',
]
