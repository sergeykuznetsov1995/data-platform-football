"""Transfermarkt Bronze scraper package.

Public entry point: ``TransfermarktScraper``. Imports here are intentionally
minimal so ``import scrapers.transfermarkt`` stays cheap for Airflow DAG
parsing; optional parsing/runtime dependencies stay lazy.
"""

from scrapers.transfermarkt.models import (
    FetchOutcome,
    FetchStatus,
    ProxyRequiredError,
    TrafficBudgetExceeded,
)
from scrapers.transfermarkt.scraper import (
    TransfermarktScraper,
    materialize_legacy_coaches,
    materialize_legacy_market_value_history,
    materialize_legacy_players,
    materialize_legacy_transfers,
)
from scrapers.transfermarkt.registry import (
    CompetitionRecord,
    CrawlScope,
    EditionRecord,
    RegistrySnapshot,
    SeasonFormat,
)

__all__ = [
    'FetchOutcome',
    'FetchStatus',
    'ProxyRequiredError',
    'TrafficBudgetExceeded',
    'CompetitionRecord',
    'CrawlScope',
    'EditionRecord',
    'RegistrySnapshot',
    'SeasonFormat',
    'TransfermarktScraper',
    'materialize_legacy_coaches',
    'materialize_legacy_market_value_history',
    'materialize_legacy_players',
    'materialize_legacy_transfers',
]
