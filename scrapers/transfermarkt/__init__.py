"""Transfermarkt Bronze scraper package.

Public entry point: ``TransfermarktScraper``. Imports here are intentionally
minimal so ``import scrapers.transfermarkt`` stays cheap for Airflow DAG
parsing — the runtime imports nodriver / BeautifulSoup lazily inside
``TransfermarktScraper.read_*`` methods.
"""

from scrapers.transfermarkt.scraper import TransfermarktScraper

__all__ = ['TransfermarktScraper']
