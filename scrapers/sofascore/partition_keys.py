"""Canonical SofaScore league keys and validation for partitioned writes."""

import re
from collections.abc import Mapping
from numbers import Integral


_CANONICAL_LEAGUE = re.compile(r"SS-[1-9][0-9]*")


def canonical_league(league: str, tournament_by_canonical: Mapping[str, int]) -> str:
    if not isinstance(league, str) or not league or league != league.strip():
        raise ValueError("League must be a nonblank string without surrounding whitespace")
    if _CANONICAL_LEAGUE.fullmatch(league):
        return league
    if league.startswith("SS-"):
        raise ValueError("Malformed SofaScore league key")
    try:
        tournament_id = tournament_by_canonical[league]
    except KeyError:
        raise ValueError("Unknown legacy league key") from None
    if isinstance(tournament_id, bool) or not isinstance(tournament_id, Integral) or tournament_id <= 0:
        raise ValueError("Tournament ID must be a positive integer")
    return f"SS-{int(tournament_id)}"


def assert_canonical_leagues(frame) -> None:
    if "league" not in frame.columns:
        raise ValueError("Missing league column")
    for league in frame["league"]:
        if not isinstance(league, str) or not _CANONICAL_LEAGUE.fullmatch(league):
            raise ValueError("Every league must be a canonical SS-<positive int> key")
