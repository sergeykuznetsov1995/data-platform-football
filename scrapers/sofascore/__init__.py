"""SofaScore package exports.

Keep the package import lightweight: discovery and catalog tooling only need
stdlib modules and must not pull pandas/soccerdata (or browser dependencies)
merely because Python initialises ``scrapers.sofascore``.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing-only import
    from scrapers.sofascore.scraper import SofaScoreScraper

__all__ = ['SofaScoreScraper']


def __getattr__(name: str):
    if name == 'SofaScoreScraper':
        from scrapers.sofascore.scraper import SofaScoreScraper

        globals()[name] = SofaScoreScraper
        return SofaScoreScraper
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
