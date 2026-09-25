"""Public facade for repository-owned, offline ESPN JSON parsing."""

from __future__ import annotations

import json

from .core_lists import (
    CoreSeason,
    RefPage,
    collect_refs,
    event_id_from_ref,
    event_ids,
    parse_event_status,
    parse_league_current_season,
    parse_ref_page,
    parse_season,
    parse_types,
)
from .discovery import (
    CatalogCandidate,
    CompetitionDetail,
    parse_competition_detail,
    parse_soccer_dropdown,
)
from .parser_common import EspnParseError, decode_object, espn_day
from .parser_contracts import (
    EntityParseState,
    LINEUP_STAT_MAP_VERSION,
    LineupRow,
    MatchsheetRow,
    MATCHSHEET_STAT_MAP_VERSION,
    PARSER_VERSION,
    STATUS_MAP_VERSION,
    ScheduleParseState,
    ScheduleRow,
    SummaryParseResult,
)
from .schedule_parser import (
    parse_all_scoreboard_day,
    parse_scoreboard_calendar,
    parse_scoreboards,
    stale_open_events,
)
from .summary_parser import (
    LINEUP_STAT_NAME_MAP,
    MATCHSHEET_STAT_NAME_MAP,
    parse_summary,
)


def parse_soccer_dropdown_bytes(raw: bytes) -> tuple[CatalogCandidate, ...]:
    if not isinstance(raw, bytes):
        raise TypeError("raw payload must be bytes")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise EspnParseError("soccer dropdown is not valid UTF-8") from exc
    try:
        document = json.loads(text)
    except json.JSONDecodeError:
        # Task 1's accepted discovery contract supports the real ESPN HTML
        # document containing window['__espnfitt__'] as well as native JSON.
        return parse_soccer_dropdown(text)
    if not isinstance(document, dict):
        raise EspnParseError("soccer dropdown JSON root must be an object")
    return parse_soccer_dropdown(document)


def parse_competition_detail_bytes(raw: bytes) -> CompetitionDetail:
    return parse_competition_detail(decode_object(raw, "competition detail"))


__all__ = [
    "CoreSeason",
    "EntityParseState",
    "EspnParseError",
    "LineupRow",
    "LINEUP_STAT_MAP_VERSION",
    "LINEUP_STAT_NAME_MAP",
    "MatchsheetRow",
    "MATCHSHEET_STAT_MAP_VERSION",
    "MATCHSHEET_STAT_NAME_MAP",
    "PARSER_VERSION",
    "STATUS_MAP_VERSION",
    "RefPage",
    "ScheduleParseState",
    "ScheduleRow",
    "SummaryParseResult",
    "collect_refs",
    "espn_day",
    "event_id_from_ref",
    "event_ids",
    "parse_all_scoreboard_day",
    "parse_competition_detail_bytes",
    "parse_event_status",
    "parse_league_current_season",
    "parse_ref_page",
    "parse_scoreboard_calendar",
    "parse_scoreboards",
    "parse_season",
    "parse_soccer_dropdown_bytes",
    "parse_summary",
    "parse_types",
    "stale_open_events",
]
