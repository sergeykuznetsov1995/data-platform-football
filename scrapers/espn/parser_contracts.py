"""Frozen output contracts for the ESPN offline parser."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

PARSER_VERSION = "espn-native-parser-v3"
STATUS_MAP_VERSION = "espn-status-v2"
LINEUP_STAT_MAP_VERSION = "espn-lineup-stat-map-v1"
MATCHSHEET_STAT_MAP_VERSION = "espn-matchsheet-stat-map-v1"


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
    attendance: str | None
    attendance_value: int | None
    league: str
    season: str
    game: str
    game_id: int
    league_id: str
    date: datetime
    match_date: datetime
    home_goals: str | None
    away_goals: str | None
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
    sub_in: str | None
    sub_out: str | None
    appearances: float | None
    fouls_committed: float | None
    fouls_suffered: float | None
    goal_assists: float | None
    goals_conceded: float | None
    offsides: float | None
    own_goals: float | None
    red_cards: float | None
    saves: float | None
    shots_faced: float | None
    shots_on_target: float | None
    sub_ins: float | None
    total_goals: float | None
    total_shots: float | None
    yellow_cards: float | None
    substitutions_json: str
    statistics_json: str
    stat_map_version: str
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
    accurate_crosses: str | None
    accurate_long_balls: str | None
    accurate_passes: str | None
    blocked_shots: str | None
    capacity: str | None
    cross_pct: str | None
    effective_clearance: str | None
    effective_tackles: str | None
    fouls_committed: str | None
    goal_assists: str | None
    goal_difference: str | None
    goals_conceded: str | None
    interceptions: str | None
    longball_pct: str | None
    offsides: str | None
    pass_pct: str | None
    penalty_kick_goals: str | None
    penalty_kick_shots: str | None
    possession_pct: str | None
    red_cards: str | None
    roster: str | None
    saves: str | None
    shot_pct: str | None
    shots_on_target: str | None
    tackle_pct: str | None
    total_clearance: str | None
    total_crosses: str | None
    total_goals: str | None
    total_long_balls: str | None
    total_passes: str | None
    total_shots: str | None
    total_tackles: str | None
    won_corners: str | None
    yellow_cards: str | None
    corner_kicks: str | None
    statistics_json: str
    stat_map_version: str
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
