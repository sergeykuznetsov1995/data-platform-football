"""Frozen output contracts for the ESPN offline parser."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

PARSER_VERSION = "espn-native-parser-v5"
STATUS_MAP_VERSION = "espn-status-v2"
LINEUP_STAT_MAP_VERSION = "espn-lineup-stat-map-v1"
MATCHSHEET_STAT_MAP_VERSION = "espn-matchsheet-stat-map-v2"


class EntityParseState(str, Enum):
    CAPTURED = "captured"
    VALID_EMPTY = "valid_empty"
    MALFORMED = "malformed"


class SummaryDisposition(str, Enum):
    """Outcome of one Summary: every parsed match carries exactly one (#1502).

    The failure unit is the match, never the tournament-season.
    """

    CAPTURED = "captured"
    # No player in the answer at all and no team statistics (C6-F1).
    VALID_EMPTY = "valid_empty"
    # The raw body is kept, no rows are published; ``reason`` says what/where.
    SOURCE_MALFORMED = "source_malformed"
    # Rows are published; ``anomalies`` names the classes.
    LINEUP_ANOMALY = "lineup_anomaly"


LINEUP_ANOMALY_CLASSES = frozenset(
    {
        "starters_not_11",
        "contradictory_flags",
        "duplicate_player",
        "one_sided_roster",
        "one_sided_statistics",
        "formation_place_missing",
    }
)
"""Lineup anomaly classes (review C9-F1); rows are written, the match is flagged.

- ``starters_not_11``: a team's starter count differs from 11, or from
  ``format.startersPerTeam`` when ESPN declares a small-sided format (ESPN
  itself flags 12-13 starters in 2010, 10 in some 2026 matches).
- ``contradictory_flags``: one athlete is a starter and subbed in, or a bench
  athlete is subbed out without coming on.
- ``duplicate_player``: an athlete repeats within a team; the first row stays.
- ``one_sided_roster`` / ``one_sided_statistics``: one team has players or
  statistics, the other has none; rows of the non-empty side are written.
- ``formation_place_missing``: the team has a ``formation`` and only part of
  its starters carry ``formationPlace`` (no field at all is not an anomaly).
"""


class ScheduleParseState(str, Enum):
    PARSED = "parsed"
    # Unknown ESPN status name: the match waits for review, the tournament
    # still publishes (#1501, R-11).
    QUARANTINED = "quarantined"


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
    # False when ESPN marks the time invalid or the kickoff is a matchday
    # placeholder (one time for the whole round).
    kickoff_confirmed: bool = True
    # "<slug>:<year>" of the edition that owns the same native event_id.
    duplicate_of: str | None = None
    parse_state: ScheduleParseState = ScheduleParseState.PARSED


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
    cross_pct: str | None
    effective_clearance: str | None
    effective_tackles: str | None
    fouls_committed: str | None
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
    total_long_balls: str | None
    total_passes: str | None
    total_shots: str | None
    total_tackles: str | None
    won_corners: str | None
    yellow_cards: str | None
    statistics_json: str
    stat_map_version: str
    venue_id: int | None
    venue: str | None
    # ESPN reports 0 when it does not know the attendance: stored as NULL.
    attendance: int | None
    referee: str | None
    formation: str | None
    # From header competitor.linescores: 1st and 2nd entries are the halves,
    # the rest (extra time) are summed; no entries -> NULL.
    score_h1: int | None
    score_h2: int | None
    score_et: int | None
    shootout_score: int | None
    aggregate_score: int | None
    advance: bool | None
    leg: int | None
    league: str
    season: str
    game: str
    parser_version: str
    extra_json: str


@dataclass(frozen=True, slots=True)
class MatchEventRow:
    """One ``keyEvents`` or ``commentary`` record of a Summary (#1502).

    Row key: ``(event_id, kind, play_id)`` for key events and
    ``(event_id, kind, sequence)`` for commentary, whose lines can share one
    play (a foul and the free kick it wins); commentary without ``sequence``
    falls back to ``play_id``.  ``red_card``,
    ``yellow_card``, ``penalty_kick``, ``own_goal``, ``home_score`` and
    ``away_score`` are read by the ESPN play keys when present; recorded
    Summary bodies do not carry them (only core plays do, task 18), so the
    event type lives in ``type_id``/``type_text``.  Commentary plays carry team
    and athlete names without IDs: ``team_id``/``athlete_ids`` stay empty and
    the names are kept in ``extra_json``.
    """

    event_id: int
    kind: str
    play_id: str | None
    sequence: int | None
    period: int | None
    clock_value: float | None
    clock_display: str | None
    team_id: int | None
    athlete_ids: str
    type_id: str | None
    type_text: str | None
    text: str | None
    x: float | None
    y: float | None
    scoring_play: bool | None
    red_card: bool | None
    yellow_card: bool | None
    penalty_kick: bool | None
    own_goal: bool | None
    home_score: int | None
    away_score: int | None
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
    disposition: SummaryDisposition
    # What and where for SOURCE_MALFORMED; None otherwise.
    reason: str | None
    anomalies: tuple[str, ...]
    events: tuple[MatchEventRow, ...]
    advance_team_id: int | None
