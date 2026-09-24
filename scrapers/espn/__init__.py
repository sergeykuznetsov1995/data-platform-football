"""ESPN package facade.

The legacy scraper is resolved lazily so importing native Raw/Bronze modules
does not import the historical third-party runtime path.
"""

from __future__ import annotations

from typing import Any


__all__ = ["ESPNScraper"]


def __getattr__(name: str) -> Any:
    if name != "ESPNScraper":
        raise AttributeError(name)
    from .scraper import ESPNScraper

    return ESPNScraper
