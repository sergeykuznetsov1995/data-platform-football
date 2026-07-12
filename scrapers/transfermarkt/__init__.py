"""Transfermarkt Bronze scraper package.

Public entry point: ``TransfermarktScraper``. Imports here are intentionally
minimal so ``import scrapers.transfermarkt`` stays cheap for Airflow DAG
parsing; optional parsing/runtime dependencies stay lazy.
"""

from scrapers.transfermarkt.scraper import TransfermarktScraper

__all__ = ['TransfermarktScraper']
