"""Football data scrapers.

Package exports stay lazy so importing one lightweight production component
does not initialize every browser scraper, pandas stack, or Iceberg writer in
the Airflow scheduler process.  Direct module imports remain preferred.
"""

from __future__ import annotations

from importlib import import_module


_EXPORTS = {
    "BaseScraper": ("scrapers.base.base_scraper", "BaseScraper"),
    "SeleniumScraper": ("scrapers.base.base_scraper", "SeleniumScraper"),
    "SoccerdataScraper": ("scrapers.base.base_scraper", "SoccerdataScraper"),
    "IcebergWriter": ("scrapers.base.iceberg_writer", "IcebergWriter"),
    "FotMobScraper": ("scrapers.fotmob", "FotMobScraper"),
    "MatchHistoryScraper": ("scrapers.matchhistory", "MatchHistoryScraper"),
    "WhoScoredIngestService": ("scrapers.whoscored", "WhoScoredIngestService"),
}

__all__ = list(_EXPORTS)
__version__ = "0.1.0"


def __getattr__(name: str):
    try:
        module_name, attribute = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
