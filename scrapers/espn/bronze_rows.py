"""Parser contracts -> rows of the ESPN bronze tables (#1503).

Pure functions: one ``MatchPayload`` (schedule row + optional parsed Summary +
the raw body it came from) gives one ``espn_match`` row and the child rows.
A payload carries the complete state of its match: the writer replaces every
row of the match, so a match already fetched must come with its Summary.

- no Summary yet: match row with ``pending`` states, no children;
- ``SOURCE_MALFORMED``: match row with disposition and reason, no children;
- a match gone from its day (#1504): no Summary, ``disposition`` is the
  presence value ``withdrawn`` (core answers 404) or ``moved``;
- lineup rows only when ``lineup_state`` is captured, team statistics only
  when ``matchsheet_state`` is captured (match facts of the matchsheet rows
  live on the match row), events whenever the Summary has them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any, Iterable

from .bronze_schema import (
    EVENTS_TABLE,
    LINEUP_STAT_COLUMNS,
    LINEUP_TABLE,
    MATCH_TABLE,
    TEAM_STAT_COLUMNS,
    TEAM_STATS_TABLE,
)
from .parser_contracts import (
    EntityParseState,
    LineupRow,
    MatchsheetRow,
    ScheduleRow,
    SummaryDisposition,
    SummaryParseResult,
)

SOURCE = "espn"
PENDING = "pending"
# Presence of a known match that left its day (#1504); written to
# ``disposition`` of a match without Summary, never a parser outcome.
WITHDRAWN = "withdrawn"
MOVED = "moved"
PRESENCE_VALUES = frozenset({WITHDRAWN, MOVED})

# Anomaly classes that name no single team: every team of the match is flagged.
_TEAM_ATTRIBUTED = frozenset(
    {"starters_not_11", "contradictory_flags", "formation_place_missing"}
)


@dataclass(frozen=True, slots=True)
class RawRef:
    """Raw-store body a row was parsed from (#1500)."""

    raw_uri: str
    raw_sha256: str
    fetched_at: datetime


@dataclass(frozen=True, slots=True)
class BatchStamp:
    """One batch id and one commit time for every row of a batch."""

    batch_id: str
    ingested_at: datetime


@dataclass(frozen=True, slots=True)
class MatchPayload:
    schedule: ScheduleRow
    # None while the Summary is not fetched (not played or not downloaded).
    summary: SummaryParseResult | None
    # Summary body when ``summary`` is set, else the scoreboard body.
    raw: RawRef
    # WITHDRAWN / MOVED for a match without Summary that left its day.
    presence: str | None = None

    def __post_init__(self) -> None:
        if self.presence is not None:
            if self.presence not in PRESENCE_VALUES:
                raise ValueError(f"unknown presence {self.presence!r}")
            if self.summary is not None:
                raise ValueError("presence is only for a match without Summary")


def _utc(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _lineage(*, raw: RawRef, parser_version: str, stamp: BatchStamp) -> dict[str, Any]:
    return {
        "_source": SOURCE,
        "_ingested_at": _utc(stamp.ingested_at),
        "_batch_id": stamp.batch_id,
        "_source_fetched_at": _utc(raw.fetched_at),
        "raw_uri": raw.raw_uri,
        "raw_sha256": raw.raw_sha256,
        "parser_version": parser_version,
    }


def _scope(schedule: ScheduleRow) -> dict[str, Any]:
    return {
        "competition_slug": schedule.competition_slug,
        "season_year": schedule.source_season_year,
        "event_id": schedule.event_id,
    }


def _state(value: EntityParseState) -> str:
    return value.value


def _sides(summary: SummaryParseResult) -> dict[str, MatchsheetRow]:
    return {row.home_away: row for row in summary.matchsheet}


def match_row(
    schedule: ScheduleRow,
    summary: SummaryParseResult | None,
    *,
    raw: RawRef,
    stamp: BatchStamp,
    presence: str | None = None,
) -> dict[str, Any]:
    played = schedule.played_final
    sides = _sides(summary) if summary is not None else {}
    home, away = sides.get("home"), sides.get("away")

    def side(row: MatchsheetRow | None, attr: str) -> Any:
        return getattr(row, attr) if row is not None else None

    if sides:
        facts = next(iter(sides.values()))
        venue_id, venue = facts.venue_id, facts.venue
        attendance, referee, leg = facts.attendance, facts.referee, facts.leg
    else:
        venue_id, venue = schedule.venue_id, schedule.venue
        # ESPN reports 0 when it does not know the attendance (as in #1502).
        attendance = schedule.attendance_value or None
        referee = leg = None

    if summary is None:
        disposition = presence
        reason = anomalies = None
        lineup_state = team_stats_state = events_state = PENDING
        first_fetched_at = None
        parser_version = schedule.parser_version
    else:
        disposition = summary.disposition.value
        reason = summary.reason
        anomalies = json.dumps(list(summary.anomalies))
        lineup_state = _state(summary.lineup_state)
        team_stats_state = _state(summary.matchsheet_state)
        if summary.disposition is SummaryDisposition.SOURCE_MALFORMED:
            events_state = EntityParseState.MALFORMED.value
        elif summary.events:
            events_state = EntityParseState.CAPTURED.value
        else:
            events_state = EntityParseState.VALID_EMPTY.value
        first_fetched_at = _utc(raw.fetched_at)
        parser_version = summary.parser_version

    return {
        **_scope(schedule),
        "kickoff": _utc(schedule.kickoff),
        "kickoff_confirmed": schedule.kickoff_confirmed,
        "status": schedule.status,
        "status_map_version": schedule.status_map_version,
        "terminal": schedule.terminal,
        "played_final": played,
        "terminal_nonplayed": schedule.terminal_nonplayed,
        "home_team_id": schedule.home_team_id,
        "home_team": schedule.home_team,
        "away_team_id": schedule.away_team_id,
        "away_team": schedule.away_team,
        "home_score": schedule.home_score if played else None,
        "away_score": schedule.away_score if played else None,
        "home_score_h1": side(home, "score_h1"),
        "away_score_h1": side(away, "score_h1"),
        "home_score_h2": side(home, "score_h2"),
        "away_score_h2": side(away, "score_h2"),
        "home_score_et": side(home, "score_et"),
        "away_score_et": side(away, "score_et"),
        "home_shootout": side(home, "shootout_score"),
        "away_shootout": side(away, "shootout_score"),
        "home_aggregate": side(home, "aggregate_score"),
        "away_aggregate": side(away, "aggregate_score"),
        "leg": leg,
        "advance_team_id": summary.advance_team_id if summary is not None else None,
        "home_formation": side(home, "formation"),
        "away_formation": side(away, "formation"),
        "venue_id": venue_id,
        "venue": venue,
        "attendance": attendance,
        "referee": referee,
        "disposition": disposition,
        "anomalies": anomalies,
        "reason": reason,
        "lineup_state": lineup_state,
        "team_stats_state": team_stats_state,
        "events_state": events_state,
        "deep_state": PENDING,
        "duplicate_of": schedule.duplicate_of,
        "parse_state": schedule.parse_state.value,
        "first_fetched_at": first_fetched_at,
        "rechecked_at": None,
        **_lineage(raw=raw, parser_version=parser_version, stamp=stamp),
    }


def _anomalous_teams(summary: SummaryParseResult) -> set[int]:
    """Teams whose lineup rows carry ``lineup_anomaly`` (classes are per match)."""
    classes = set(summary.anomalies)
    teams = {row.team_id for row in summary.lineup}
    if not classes:
        return set()
    if classes - _TEAM_ATTRIBUTED:
        return teams
    by_team: dict[int, list[LineupRow]] = {}
    for row in summary.lineup:
        by_team.setdefault(row.team_id, []).append(row)
    formations = {row.team_id: row.formation for row in summary.matchsheet}
    flagged: set[int] = set()
    for team_id, rows in by_team.items():
        starters = [row for row in rows if row.starter is True]
        if "starters_not_11" in classes and len(starters) != 11:
            flagged.add(team_id)
        if "contradictory_flags" in classes and any(
            (row.starter is True and row.subbed_in is True)
            or (
                row.starter is False
                and row.subbed_out is True
                and row.subbed_in is not True
            )
            for row in rows
        ):
            flagged.add(team_id)
        if "formation_place_missing" in classes and formations.get(team_id) is not None:
            missing = sum(row.formation_place is None for row in starters)
            if 0 < missing < len(starters):
                flagged.add(team_id)
    # A small-sided format (startersPerTeam) is not visible in the rows: the
    # class then names no team by these checks, so every team is flagged.
    return flagged or teams


def lineup_rows(
    schedule: ScheduleRow,
    summary: SummaryParseResult | None,
    *,
    raw: RawRef,
    stamp: BatchStamp,
) -> list[dict[str, Any]]:
    if summary is None or summary.lineup_state is not EntityParseState.CAPTURED:
        return []
    flagged = _anomalous_teams(summary)
    lineage = _lineage(raw=raw, parser_version=summary.parser_version, stamp=stamp)
    return [
        {
            **_scope(schedule),
            "team_id": row.team_id,
            "team": row.team,
            "home_away": row.home_away,
            "athlete_id": row.athlete_id,
            "player": row.player,
            "jersey": row.jersey,
            "position": row.position,
            "formation_place": row.formation_place,
            "starter": row.starter,
            "subbed_in": row.subbed_in,
            "subbed_out": row.subbed_out,
            "sub_in": row.sub_in,
            "sub_out": row.sub_out,
            **{name: getattr(row, name) for name in LINEUP_STAT_COLUMNS},
            "substitutions_json": row.substitutions_json,
            "lineup_anomaly": row.team_id in flagged,
            "deep_stats_json": None,
            **lineage,
        }
        for row in summary.lineup
    ]


# Recorded bodies give these as fractions ("0.8") and possession in percent
# ("36.2"); a "%" display the parser also accepts is brought to those units.
_FRACTION_COLUMNS = frozenset(
    {"cross_pct", "longball_pct", "pass_pct", "shot_pct", "tackle_pct"}
)


def _number(column: str, value: str | None) -> float | None:
    if value is None:
        return None
    if value.endswith("%"):
        number = float(value[:-1])
        return number / 100 if column in _FRACTION_COLUMNS else number
    return float(value)


def team_stats_rows(
    schedule: ScheduleRow,
    summary: SummaryParseResult | None,
    *,
    raw: RawRef,
    stamp: BatchStamp,
) -> list[dict[str, Any]]:
    if summary is None or summary.matchsheet_state is not EntityParseState.CAPTURED:
        return []
    lineage = _lineage(raw=raw, parser_version=summary.parser_version, stamp=stamp)
    return [
        {
            **_scope(schedule),
            "team_id": row.team_id,
            "team": row.team,
            "home_away": row.home_away,
            "formation": row.formation,
            **{name: _number(name, getattr(row, name)) for name in TEAM_STAT_COLUMNS},
            "team_stats_state": _state(summary.matchsheet_state),
            "deep_stats_json": None,
            **lineage,
        }
        for row in summary.matchsheet
        # A team without statistics (one_sided_statistics) keeps only its
        # match facts, which live on espn_match.
        if any(getattr(row, name) is not None for name in TEAM_STAT_COLUMNS)
    ]


def event_rows(
    schedule: ScheduleRow,
    summary: SummaryParseResult | None,
    *,
    raw: RawRef,
    stamp: BatchStamp,
) -> list[dict[str, Any]]:
    if summary is None:
        return []
    lineage = _lineage(raw=raw, parser_version=summary.parser_version, stamp=stamp)
    rows = []
    for row in summary.events:
        if row.kind == "key_event" or row.sequence is None:
            event_key = row.play_id
        else:
            event_key = str(row.sequence)
        rows.append(
            {
                **_scope(schedule),
                "kind": row.kind,
                "event_key": event_key,
                "play_id": row.play_id,
                "sequence": row.sequence,
                "period": row.period,
                "clock_value": row.clock_value,
                "clock_display": row.clock_display,
                "team_id": row.team_id,
                "athlete_ids": row.athlete_ids,
                "type_id": row.type_id,
                "type_text": row.type_text,
                "text": row.text,
                "x": row.x,
                "y": row.y,
                "scoring_play": row.scoring_play,
                "red_card": row.red_card,
                "yellow_card": row.yellow_card,
                "penalty_kick": row.penalty_kick,
                "own_goal": row.own_goal,
                "home_score": row.home_score,
                "away_score": row.away_score,
                **lineage,
            }
        )
    return rows


def batch_rows(
    payloads: Iterable[MatchPayload], *, stamp: BatchStamp
) -> dict[str, list[dict[str, Any]]]:
    """Rows of every table for a batch, in write order."""
    tables: dict[str, list[dict[str, Any]]] = {
        MATCH_TABLE: [],
        LINEUP_TABLE: [],
        TEAM_STATS_TABLE: [],
        EVENTS_TABLE: [],
    }
    for payload in payloads:
        if (
            payload.summary is not None
            and payload.summary.event_id != payload.schedule.event_id
        ):
            raise ValueError(
                f"summary of event {payload.summary.event_id} paired with "
                f"schedule event {payload.schedule.event_id}"
            )
        args = (payload.schedule, payload.summary)
        kwargs = {"raw": payload.raw, "stamp": stamp}
        tables[MATCH_TABLE].append(
            match_row(*args, **kwargs, presence=payload.presence)
        )
        tables[LINEUP_TABLE].extend(lineup_rows(*args, **kwargs))
        tables[TEAM_STATS_TABLE].extend(team_stats_rows(*args, **kwargs))
        tables[EVENTS_TABLE].extend(event_rows(*args, **kwargs))
    return tables


__all__ = (
    "BatchStamp",
    "MOVED",
    "MatchPayload",
    "PRESENCE_VALUES",
    "RawRef",
    "WITHDRAWN",
    "batch_rows",
    "event_rows",
    "lineup_rows",
    "match_row",
    "team_stats_rows",
)
