"""
Competition-format lookups for scrapers (#920 Phase 3).

One shared implementation of the pattern sofascore pioneered: consult
``dags.utils.medallion_config`` lazily and fall back to the club default on
ANY failure. Scrapers previously hardcoded ``league == 'INT-World Cup'`` in
their season-format branches, so onboarding the NEXT tournament silently
fetched the club-formula page ('2028-2029') — the wrong-season class that
destroyed WC bronze on 2026-07-09 (#920 pinned comment).

The fallback is conservative on purpose: a wrong club default keeps today's
behavior and surfaces as a loud ID miss / empty scrape later, never as a
silently mislabelled partition. With medallion_config's repo-relative
CONFIG_DIR fallback the except-branch is truly exceptional (it used to fire
on every host run because the container path didn't exist there).
"""

import logging

logger = logging.getLogger(__name__)

_warned: set = set()


def _warn_once(what: str, exc: Exception) -> None:
    if what not in _warned:
        _warned.add(what)
        logger.warning(
            "medallion config unavailable for %s (%s) — falling back to the "
            "club default. Tournament leagues will NOT resolve correctly in "
            "this environment.", what, exc,
        )


def is_single_year(league, season) -> bool:
    """True when (league, season) is a single_year competition per
    competitions.yaml. Conservative False on any lookup failure."""
    if not league:
        return False
    try:
        from dags.utils.medallion_config import get_competition_season_format
        return get_competition_season_format(league, int(season)) == 'single_year'
    except Exception as e:  # noqa: BLE001 — never break a club scrape
        _warn_once(f'is_single_year({league!r})', e)
        return False


def is_single_year_competition(league) -> bool:
    """True when ANY configured season of the competition is single_year
    (season-independent variant, for call sites without a season at hand)."""
    if not league:
        return False
    try:
        from dags.utils.medallion_config import (
            is_single_year_competition as _impl,
        )
        return _impl(league)
    except Exception as e:  # noqa: BLE001
        _warn_once(f'is_single_year_competition({league!r})', e)
        return False


def is_group_knockout(league) -> bool:
    """True when the competition's top-level competition_format is
    group_knockout (payload-shape branches: group standings tables,
    stage-shaped calendars). Deliberately distinct from is_single_year —
    single-year round-robin leagues exist."""
    if not league:
        return False
    try:
        from dags.utils.medallion_config import get_competition_format
        return get_competition_format(league) == 'group_knockout'
    except Exception as e:  # noqa: BLE001
        _warn_once(f'is_group_knockout({league!r})', e)
        return False
