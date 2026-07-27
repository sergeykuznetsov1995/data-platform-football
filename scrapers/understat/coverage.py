"""Reviewed source-level coverage exceptions for exact Understat matches.

Exceptions are intentionally match- and entity-specific.  They are admitted
only after a fresh native endpoint probe proves that the match envelope exists
but its inner roster/shot arrays are empty.  Broad league/season percentage
waivers are not allowed here.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Mapping


_EMPTY_MATCH_PAYLOAD = (
    "Understat getMatchData returned HTTP 200 but both shots and rosters "
    "were empty; verified 2026-07-27"
)

_BY_SCOPE: Mapping[
    tuple[str, str], Mapping[str, Mapping[str, Mapping[str, str]]]
] = MappingProxyType(
    {
        ("FRA-Ligue 1", "1617"): {
            "understat_shots": {"missing": {"4238": _EMPTY_MATCH_PAYLOAD}},
            "understat_player_match_stats": {
                "missing": {"4238": _EMPTY_MATCH_PAYLOAD}
            },
        },
        ("GER-Bundesliga", "2425"): {
            "understat_shots": {"missing": {"27930": _EMPTY_MATCH_PAYLOAD}},
            "understat_player_match_stats": {
                "missing": {"27930": _EMPTY_MATCH_PAYLOAD}
            },
        },
    }
)


def coverage_exceptions_for_scope(scope: Any) -> dict[str, object]:
    """Return a detached exact-match allowlist for a ScopeKey-like object."""

    league = str(getattr(scope, "league"))
    season = str(getattr(scope, "season"))
    configured = _BY_SCOPE.get((league, season), {})
    return {
        entity: {
            direction: dict(game_ids)
            for direction, game_ids in directions.items()
        }
        for entity, directions in configured.items()
    }


__all__ = ["coverage_exceptions_for_scope"]
