"""Capology Bronze scraper package.

Public entry point: ``CapologyScraper``. Imports kept minimal so DAG
parsing remains cheap; runtime deps (BeautifulSoup, tls_requests) are
imported lazily inside ``CapologyScraper.read_*``.
"""

from scrapers.capology.scraper import CapologyScraper

__all__ = ['CapologyScraper']
