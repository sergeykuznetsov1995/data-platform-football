"""SofaScore package exports.

Keep the package import lightweight: discovery and catalog tooling only need
stdlib modules and must not pull pandas/soccerdata (or browser dependencies)
merely because Python initialises ``scrapers.sofascore``.
"""

from importlib import import_module

__all__ = ['SofaScoreScraper']


def __getattr__(name: str):
    if name == 'SofaScoreScraper':
        SofaScoreScraper = getattr(
            import_module("scrapers.sofascore.scraper"),
            "SofaScoreScraper",
        )
        globals()[name] = SofaScoreScraper
        return SofaScoreScraper
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
