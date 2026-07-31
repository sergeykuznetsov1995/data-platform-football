"""Frozen output contracts for the ESPN offline parser."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any


PARSER_VERSION = "espn-native-parser-v1"
STATUS_MAP_VERSION = "espn-status-v1"


class EntityParseState(str, Enum):
    CAPTURED = "captured"
    VALID_EMPTY = "valid_empty"


@dataclass(frozen=True, slots=True)
class ScheduleRow:
    scope_id: str
    competition_id: int
    competition_slug: str
    source_season_year: int
    event_id: int
    kickoff: datetime
    status: str
    status_map_version: str
    terminal: bool
    played_final: bool
    terminal_nonplayed: bool
    summary_required: bool
    home_team_id: int
    home_team: str
    away_team_id: int
    away_team: str
    home_score: int | None
    away_score: int | None
    venue_id: int | None
    venue: str | None
    attendance: int | None
    league: str
    season: str
    game: str
    game_id: int
    league_id: str
    date: datetime
    match_date: datetime
    home_goals: int | None
    away_goals: int | None
    parser_version: str
    extra_json: str


@dataclass(frozen=True, slots=True)
class LineupRow:
    scope_id: str
    competition_id: int
    event_id: int
    source_season_year: int
    team_id: int
    team: str
    home_away: str
    is_home: bool
    athlete_id: int
    player: str
    jersey: str | None
    position: str | None
    formation_place: str | None
    starter: bool | None
    captain: bool | None
    subbed_in: bool | None
    subbed_out: bool | None
    sub_in: str | int | None
    sub_out: str | int | None
    substitutions_json: str
    statistics_json: str
    league: str
    season: str
    game: str
    parser_version: str
    extra_json: str


@dataclass(frozen=True, slots=True)
class MatchsheetRow:
    scope_id: str
    competition_id: int
    event_id: int
    source_season_year: int
    team_id: int
    team: str
    home_away: str
    is_home: bool
    score: int | None
    total_shots: Any
    shots_on_target: Any
    possession_pct: Any
    fouls_committed: Any
    yellow_cards: Any
    red_cards: Any
    offsides: Any
    corner_kicks: Any
    saves: Any
    statistics_json: str
    venue_id: int | None
    venue: str | None
    attendance: int | None
    referee_id: int | None
    referee: str | None
    league: str
    season: str
    game: str
    parser_version: str
    extra_json: str


@dataclass(frozen=True, slots=True)
class SummaryParseResult:
    event_id: int
    lineup: tuple[LineupRow, ...]
    matchsheet: tuple[MatchsheetRow, ...]
    lineup_state: EntityParseState
    matchsheet_state: EntityParseState
    parser_version: str
    extra_json: str
