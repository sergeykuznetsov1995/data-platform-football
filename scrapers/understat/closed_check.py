"""Weekly change check for closed Understat seasons (#1431).

A *closed* scope is a season whose source start year is older than the
current source season (``current_source_season_id``: the season starting in
July of the calendar year, one date for all leagues).  The daily plan ingests
only non-closed scopes, including the next-season probe.  Closed scopes of the
rolling window are checked once a week (Monday) with one ``getLeagueData``
request: three fingerprints of the league response are compared with the
fingerprints recorded by the last complete attempt, and the scope is
re-ingested in full only when one of them differs.

The fingerprints are ``dataframe_content_hash`` of the frames produced by
``parse_schedule``, ``parse_player_season_stats`` and
``parse_team_match_stats`` right after the league response is parsed.  They
are deliberately not the stored manifest ``payload_hashes``: the service later
rewrites ``schedule.has_data`` from every match response, so a stored schedule
hash cannot be reproduced from one league request.  Hashing parsed frames
rather than the raw JSON also ignores source fields the pipeline never keeps.
``getTeamData`` breakdowns are outside the check by design.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable, Mapping

import pandas as pd

from .catalog import current_source_season_id
from .quality import dataframe_content_hash


LEAGUE_HASH_ENTITIES = ("schedule", "players", "team_match_stats")

WEEKLY_CHECK_WEEKDAY = 0  # Monday


def league_payload_hashes(frames: Mapping[str, pd.DataFrame]) -> dict[str, str]:
    """Fingerprint the three frames parsed from one league response."""

    return {
        entity: dataframe_content_hash(frames[entity])
        for entity in LEAGUE_HASH_ENTITIES
    }


def weekly_check_day(run_boundary: datetime | date) -> bool:
    """Whether the run boundary falls on the weekly closed-scope check day."""

    return run_boundary.weekday() == WEEKLY_CHECK_WEEKDAY


def _source_season_id(scope: Any) -> int:
    if isinstance(scope, Mapping):
        return int(scope["source_season_id"])
    return int(getattr(scope, "source_season_id"))


def is_scope_closed(scope: Any, today: date) -> bool:
    """Catalog rule: the season started before the current source season."""

    return _source_season_id(scope) < current_source_season_id(today)


def split_daily_plan(
    scopes: Iterable[Any],
    run_boundary: datetime | date,
) -> tuple[list[Any], list[Any]]:
    """Split ordered scopes into (daily current, weekly closed-check) lists.

    The closed list is empty unless the run boundary is the check day.  The
    input order is kept inside each list; callers place current scopes first.
    """

    today = run_boundary.date() if isinstance(run_boundary, datetime) else run_boundary
    current: list[Any] = []
    closed: list[Any] = []
    for scope in scopes:
        (closed if is_scope_closed(scope, today) else current).append(scope)
    if not weekly_check_day(run_boundary):
        closed = []
    return current, closed


__all__ = [
    "LEAGUE_HASH_ENTITIES",
    "is_scope_closed",
    "league_payload_hashes",
    "split_daily_plan",
    "weekly_check_day",
]
