"""
Patched soccerdata.WhoScored for less aggressive Cloudflare timeouts.

Default ``sb.Driver(uc=True)`` ships with selenium's 30s ``script_timeout``,
which is too tight for WhoScored's current Cloudflare bypass — long-running
``execute_script`` calls (used inside soccerdata to harvest JS variables)
bail out before the challenge resolves and trigger a 5×retry full-driver
restart loop in ``BaseSeleniumReader._download_and_save``.

This subclass only overrides :py:meth:`_init_webdriver` to bump the script
timeout to 120 s. Everything else is unchanged.

Use this as a drop-in replacement for ``soccerdata.WhoScored`` in
:meth:`scrapers.whoscored.WhoScoredScraper._get_reader`. Used **only** for
schedule / missing_players / season_stages — events scraping bypasses
soccerdata altogether (see :mod:`scrapers.whoscored.events_fetcher`).
"""

from __future__ import annotations

import logging

import soccerdata as sd

logger = logging.getLogger(__name__)


class EnhancedWhoScored(sd.WhoScored):
    """``soccerdata.WhoScored`` with ``script_timeout=120s`` instead of 30s."""

    SCRIPT_TIMEOUT_SECONDS = 120

    @classmethod
    def _all_leagues(cls):
        # soccerdata's BaseReader._all_leagues uses ``cls.__name__`` to look up
        # leagues in LEAGUE_DICT. For our subclass that resolves to
        # 'EnhancedWhoScored' (absent from LEAGUE_DICT) → empty dict → every
        # league gets rejected as "Invalid". Forward to the parent class so the
        # lookup uses 'WhoScored'.
        return sd.WhoScored._all_leagues()

    def _init_webdriver(self):
        driver = super()._init_webdriver()
        if driver is not None:
            try:
                driver.set_script_timeout(self.SCRIPT_TIMEOUT_SECONDS)
                logger.info(
                    "EnhancedWhoScored: script_timeout=%ds",
                    self.SCRIPT_TIMEOUT_SECONDS,
                )
            except Exception as e:
                logger.warning("Failed to set script_timeout: %s", e)
        return driver
