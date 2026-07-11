"""Direct-first, manifest-backed WhoScored ingestion package."""

from scrapers.whoscored.domain import SeasonFormat, WhoScoredScope
from scrapers.whoscored.service import WhoScoredIngestService

__all__ = [
    'SeasonFormat',
    'WhoScoredIngestService',
    'WhoScoredScope',
]
