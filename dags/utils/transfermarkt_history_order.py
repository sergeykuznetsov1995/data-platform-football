"""Claim order of the Transfermarkt history queue (#1402).

Map of 23.09: step 1 is the previous edition of every live competition, top
leagues first; step 2 is the 10-season window (leagues, then cups and
continental cups, then national teams); deeper editions follow.  Within a step
the issue key applies: kind, season lag, top league, league tier.  The order
never changes the target set.
"""

from __future__ import annotations

from typing import Any, Callable, Iterable

from scrapers.transfermarkt.denominator import Denominator
from utils.transfermarkt_current_state import TOP_LEAGUES

# Window of step 2 (2015/16…2024/25, calendar 2015…2024) when the denominator
# row lists no step2_saison_ids, as for cups (#1402).
HISTORY_WINDOW_FIRST = 2015
HISTORY_WINDOW_LAST = 2024
_UNKNOWN = 3
_NO_TIER = 99


def history_claim_key(
    denominator: Denominator,
    targets: Iterable[Any],
    *,
    top_leagues: tuple[str, ...] = TOP_LEAGUES,
) -> Callable[[Any], tuple[Any, ...]]:
    """Return a deterministic sort key for not-retry history scopes.

    The previous edition is the newest historical target of the competition,
    so tournaments played every 2-4 years still get one.
    """

    previous: dict[str, int] = {}
    for target in targets:
        if target.edition_id.isdigit():
            season = int(target.edition_id)
            previous[target.competition_id] = max(season, previous.get(target.competition_id, season))

    def key(scope: Any) -> tuple[Any, ...]:
        target = scope.target
        competition_id, edition_id = target.competition_id, target.edition_id
        row = denominator.row(competition_id)
        if row is None or not edition_id.isdigit():
            return (_UNKNOWN, _UNKNOWN, 0, len(top_leagues), _NO_TIER, competition_id, edition_id)
        season = int(edition_id)
        if season == previous.get(competition_id):
            step = 0
        elif (season in row.step2_saison_ids if row.step2_saison_ids
              else HISTORY_WINDOW_FIRST <= season <= HISTORY_WINDOW_LAST):
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
        return (step, kind, -season, top, tier, competition_id, edition_id)

    return key


__all__ = ["HISTORY_WINDOW_FIRST", "HISTORY_WINDOW_LAST", "history_claim_key"]
