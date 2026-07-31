"""Public facade for repository-owned, offline ESPN JSON parsing."""

from __future__ import annotations

from .discovery import (
    CatalogCandidate,
    CompetitionDetail,
    parse_competition_detail,
    parse_soccer_dropdown,
)
from .parser_common import EspnParseError, decode_object
from .parser_contracts import (
    EntityParseState,
    LineupRow,
    MatchsheetRow,
    PARSER_VERSION,
    STATUS_MAP_VERSION,
    ScheduleRow,
    SummaryParseResult,
)
from .schedule_parser import parse_scoreboard_calendar, parse_scoreboards
from .summary_parser import parse_summary


def parse_soccer_dropdown_bytes(raw: bytes) -> tuple[CatalogCandidate, ...]:
    return parse_soccer_dropdown(decode_object(raw, "soccer dropdown"))


def parse_competition_detail_bytes(raw: bytes) -> CompetitionDetail:
    return parse_competition_detail(decode_object(raw, "competition detail"))


__all__ = [
    "EntityParseState",
    "EspnParseError",
    "LineupRow",
    "MatchsheetRow",
    "PARSER_VERSION",
    "STATUS_MAP_VERSION",
    "ScheduleRow",
    "SummaryParseResult",
    "parse_competition_detail_bytes",
    "parse_scoreboard_calendar",
    "parse_scoreboards",
    "parse_soccer_dropdown_bytes",
    "parse_summary",
]
