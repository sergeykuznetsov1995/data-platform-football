"""
Competition-format lookups for scrapers (#920 Phase 3).

One shared implementation of the pattern sofascore pioneered: consult
``dags.utils.medallion_config`` lazily and fall back on failure. Scrapers
previously hardcoded ``league == 'INT-World Cup'`` in their season-format
branches, so onboarding the NEXT tournament silently fetched the
club-formula page ('2028-2029') — the wrong-season class that destroyed WC
bronze on 2026-07-09 (#920 pinned comment).

The fallback is asymmetric on purpose (#920 review hardening): for a CLUB
league a failed lookup returns the club default (today's behavior — a loud
ID miss later at worst); for an ``INT-``-prefixed tournament league it
RAISES — a silent club fallback there IS the wrong-season / wrong-partition
incident class, and the INT- prefix is already a load-bearing convention
(sofifa floor filter, coherence test). With medallion_config's
repo-relative CONFIG_DIR fallback the failure branch is truly exceptional
(it used to fire on every host run because the container path didn't exist
there).
"""

import logging

logger = logging.getLogger(__name__)

_warned: set = set()


def _fallback(league, what: str, exc: Exception) -> bool:
    """Club default on lookup failure — but NEVER for a tournament league."""
    if str(league).startswith('INT-'):
        raise RuntimeError(
            f"medallion config unavailable for {what} and {league!r} is a "
            f"tournament (INT-*) — refusing the silent club fallback "
            f"(wrong-season/partition class of the 2026-07-09 incident): {exc}"
        ) from exc
    if what not in _warned:
        _warned.add(what)
        logger.warning(
            "medallion config unavailable for %s (%s) — falling back to the "
            "club default.", what, exc,
        )
    return False


def is_single_year(league, season) -> bool:
    """True when (league, season) is single_year per competitions.yaml.

    A season id missing from the yaml falls back to the COMPETITION-level
    answer: get_competition_season_format defaults unlisted seasons to
    'split_year', which for a historical tournament backfill (WC 2022,
    Euro 2024 — only current editions are configured) would silently
    reintroduce the club-formula URL/label the old literal was immune to.
    """
    if not league:
        return False
    try:
        from dags.utils.medallion_config import get_competition_season_format
        if get_competition_season_format(league, int(season)) == 'single_year':
            return True
        return is_single_year_competition(league)
    except Exception as e:  # noqa: BLE001 — club default; raises for INT-*
        return _fallback(league, f'is_single_year({league!r})', e)


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
        return _fallback(league, f'is_single_year_competition({league!r})', e)


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
        return _fallback(league, f'is_group_knockout({league!r})', e)
