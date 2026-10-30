"""Claim order of the Transfermarkt history queue (#1402).

Map of 23.09: step 1 is the previous edition of every live competition, top
leagues first; step 2 is the 10-season window (leagues, then cups and
continental cups, then national teams); deeper editions follow.  The order is
computed from the denominator file only; it never changes the target set.
"""

from __future__ import annotations

from typing import Any, Callable

from scrapers.transfermarkt.denominator import Denominator
from utils.transfermarkt_current_state import TOP_LEAGUES

_UNKNOWN = 3
_NO_TIER = 99


def history_claim_key(
    denominator: Denominator, *, top_leagues: tuple[str, ...] = TOP_LEAGUES,
) -> Callable[[Any], tuple[Any, ...]]:
    """Return a deterministic sort key for not-retry history scopes."""

    def key(scope: Any) -> tuple[Any, ...]:
        target = scope.target
        competition_id, edition_id = target.competition_id, target.edition_id
        row = denominator.row(competition_id)
        if row is None or not edition_id.isdigit():
            return (_UNKNOWN, len(top_leagues), _UNKNOWN, _NO_TIER, 0, competition_id, edition_id)
        season = int(edition_id)
        if season == row.current_saison_id - 1:
            step = 0
        elif season in row.step2_saison_ids:
            step = 1
        else:
            step = 2
        if row.competition_class == "core_national":
            kind = 2
        elif row.route == "pokal":
            kind = 1
        elif row.competition_class == "core_club":
            kind = 0
        else:
            kind = 3
        top = top_leagues.index(competition_id) if competition_id in top_leagues else len(top_leagues)
        tier = int(row.tier) if row.tier.isdigit() else _NO_TIER
        return (step, top, kind, tier, -season, competition_id, edition_id)

    return key


__all__ = ["history_claim_key"]
