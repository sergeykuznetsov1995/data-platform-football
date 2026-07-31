"""Offline ESPN scoreboard calendar and event parsing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from types import MappingProxyType
from typing import Any, Iterable, Mapping

from .models import Competition, Edition
from .parser_common import (
    EspnParseError,
    canonical_json,
    clipped_date,
    decode_object,
    native_id,
    optional_nonnegative_int,
    optional_string,
    required_list,
    required_mapping,
    required_string,
    source_year,
    unknown_fields,
    utc_datetime,
)
from .parser_contracts import PARSER_VERSION, STATUS_MAP_VERSION, ScheduleRow


@dataclass(frozen=True, slots=True)
class _Status:
    terminal: bool
    played_final: bool
    terminal_nonplayed: bool


_OPEN = _Status(False, False, False)
_PLAYED = _Status(True, True, False)
_NONPLAYED = _Status(True, False, True)

# Explicit and deliberately versioned. New upstream values require review.
STATUS_MAP: Mapping[str, _Status] = MappingProxyType(
    {
        "STATUS_SCHEDULED": _OPEN,
        "STATUS_PRE_GAME": _OPEN,
        "STATUS_IN_PROGRESS": _OPEN,
        "STATUS_FIRST_HALF": _OPEN,
        "STATUS_HALFTIME": _OPEN,
        "STATUS_SECOND_HALF": _OPEN,
        "STATUS_END_PERIOD": _OPEN,
        "STATUS_OVERTIME": _OPEN,
        "STATUS_SHOOTOUT": _OPEN,
        "STATUS_DELAYED": _OPEN,
        "STATUS_RAIN_DELAY": _OPEN,
        "STATUS_FULL_TIME": _PLAYED,
        "STATUS_FINAL": _PLAYED,
        "STATUS_FINAL_AET": _PLAYED,
        "STATUS_FINAL_PEN": _PLAYED,
        "STATUS_END_OF_REGULATION": _PLAYED,
        "STATUS_END_OF_EXTRA_TIME": _PLAYED,
        "STATUS_POSTPONED": _OPEN,
        "STATUS_CANCELED": _NONPLAYED,
        "STATUS_CANCELLED": _NONPLAYED,
        "STATUS_ABANDONED": _NONPLAYED,
        "STATUS_SUSPENDED": _OPEN,
        "STATUS_FORFEIT": _NONPLAYED,
        "STATUS_WALKOVER": _NONPLAYED,
    }
)


def _validate_scope(competition: Competition, edition: Edition) -> None:
    if not isinstance(competition, Competition) or not isinstance(edition, Edition):
        raise TypeError("competition and edition must be registry models")
    if edition not in competition.editions:
        raise EspnParseError("edition is not promoted for this competition")


def _legacy(competition: Competition, edition: Edition) -> tuple[str, str]:
    if competition.legacy is None:
        return competition.name, str(edition.source_season_year)
    aliases = competition.legacy.season_aliases.get(edition.source_season_year, ())
    return competition.legacy.league, aliases[0] if aliases else str(
        edition.source_season_year
    )


def _scoreboard_league(
    payload: Mapping[str, Any], competition: Competition
) -> Mapping[str, Any]:
    leagues = required_list(payload.get("leagues"), "scoreboard.leagues")
    if len(leagues) != 1:
        raise EspnParseError(
            "scoreboard must contain exactly one root league because events "
            "have no per-league binding"
        )
    league = required_mapping(leagues[0], "scoreboard.leagues[0]")
    league_id = native_id(league.get("id"), "scoreboard.leagues[0].id")
    if league_id != competition.espn_id:
        raise EspnParseError(
            "scoreboard promoted league does not match competition "
            f"{competition.espn_id}:{competition.slug}"
        )
    if "slug" in league and league["slug"] is not None:
        slug = required_string(league["slug"], "scoreboard.leagues[0].slug")
        if slug != competition.slug:
            raise EspnParseError(
                f"scoreboard promoted league slug {slug!r} does not match "
                f"registry slug {competition.slug!r}"
            )
    return league


def _calendar_ranges(item: Any, field: str) -> list[tuple[date, date]]:
    if isinstance(item, str):
        day = utc_datetime(item, field).date()
        return [(day, day)]
    node = required_mapping(item, field)
    if "entries" in node:
        entries = required_list(node["entries"], f"{field}.entries")
        ranges: list[tuple[date, date]] = []
        for index, entry in enumerate(entries):
            ranges.extend(_calendar_ranges(entry, f"{field}.entries[{index}]"))
        return ranges
    start = utc_datetime(node.get("startDate"), f"{field}.startDate").date()
    end = utc_datetime(node.get("endDate"), f"{field}.endDate").date()
    if start > end:
        raise EspnParseError(f"{field} date range starts after it ends")
    return [(start, end)]


def parse_scoreboard_calendar(
    raw: bytes, competition: Competition, edition: Edition
) -> tuple[date, ...]:
    _validate_scope(competition, edition)
    payload = decode_object(raw, "scoreboard")
    league = _scoreboard_league(payload, competition)
    calendar = required_list(league.get("calendar"), "scoreboard league calendar")
    days: set[date] = set()
    for index, item in enumerate(calendar):
        for start, end in _calendar_ranges(item, f"scoreboard calendar[{index}]"):
            cursor = max(start, edition.start_date)
            clipped_end = min(end, edition.end_date)
            while cursor <= clipped_end:
                days.add(cursor)
                cursor += timedelta(days=1)
    return tuple(sorted(days))


def _side(raw: Any, field: str) -> tuple[str, int, str, int | None, Mapping[str, Any]]:
    competitor = required_mapping(raw, field)
    home_away = required_string(competitor.get("homeAway"), f"{field}.homeAway")
    if home_away not in {"home", "away"}:
        raise EspnParseError(f"{field}.homeAway must be home or away")
    team = required_mapping(competitor.get("team"), f"{field}.team")
    team_id = native_id(team.get("id"), f"{field}.team.id")
    team_name = required_string(team.get("displayName"), f"{field}.team.displayName")
    score = optional_nonnegative_int(competitor.get("score"), f"{field}.score")
    return home_away, team_id, team_name, score, competitor


def _event_row(
    event_raw: Any,
    *,
    competition: Competition,
    edition: Edition,
    query_start: date,
    query_end: date,
    source_extra: Mapping[str, Any],
) -> ScheduleRow | None:
    event = required_mapping(event_raw, "scoreboard event")
    event_id = native_id(event.get("id"), "scoreboard event.id")
    season = required_mapping(event.get("season"), f"event[{event_id}].season")
    event_year = source_year(season.get("year"), f"event[{event_id}].season.year")
    kickoff = utc_datetime(event.get("date"), f"event[{event_id}].date")
    if event_year != edition.source_season_year:
        return None
    if not clipped_date(kickoff, edition.start_date, edition.end_date):
        return None
    if not clipped_date(kickoff, query_start, query_end):
        return None

    status = required_mapping(event.get("status"), f"event[{event_id}].status")
    status_type = required_mapping(status.get("type"), f"event[{event_id}].status.type")
    status_name = required_string(
        status_type.get("name"), f"event[{event_id}].status.type.name"
    )
    semantics = STATUS_MAP.get(status_name)
    if semantics is None:
        raise EspnParseError(
            f"unknown ESPN status {status_name!r} in {STATUS_MAP_VERSION}"
        )

    competitions = required_list(
        event.get("competitions"), f"event[{event_id}].competitions"
    )
    if len(competitions) != 1:
        raise EspnParseError(f"event[{event_id}] must have exactly one competition")
    event_competition = required_mapping(
        competitions[0], f"event[{event_id}].competitions[0]"
    )
    competitors = required_list(
        event_competition.get("competitors"),
        f"event[{event_id}].competitions[0].competitors",
    )
    if len(competitors) != 2:
        raise EspnParseError(f"event[{event_id}] must have exactly two competitors")
    sides: dict[str, tuple[int, str, int | None, Mapping[str, Any]]] = {}
    for index, raw_side in enumerate(competitors):
        home_away, team_id, team_name, score, raw_mapping = _side(
            raw_side, f"event[{event_id}].competitors[{index}]"
        )
        if home_away in sides:
            raise EspnParseError(f"event[{event_id}] has duplicate {home_away} side")
        sides[home_away] = (team_id, team_name, score, raw_mapping)
    if set(sides) != {"home", "away"}:
        raise EspnParseError(f"event[{event_id}] must contain home and away sides")
    home, away = sides["home"], sides["away"]
    if home[0] == away[0]:
        raise EspnParseError(f"event[{event_id}] must contain two distinct team IDs")
    if semantics.played_final and (home[2] is None or away[2] is None):
        raise EspnParseError(f"played-final event[{event_id}] must have both scores")

    venue_id: int | None = None
    venue_name: str | None = None
    if "venue" in event_competition and event_competition["venue"] is not None:
        venue = required_mapping(
            event_competition["venue"], f"event[{event_id}].competition.venue"
        )
        if "id" in venue and venue["id"] is not None:
            venue_id = native_id(venue["id"], f"event[{event_id}].venue.id")
        venue_name = optional_string(
            venue.get("fullName"), f"event[{event_id}].venue.fullName"
        )
    attendance = optional_nonnegative_int(
        event_competition.get("attendance"), f"event[{event_id}].attendance"
    )
    league, legacy_season = _legacy(competition, edition)
    # Preserve soccerdata's legacy date-prefixed game key while native event_id
    # remains the authoritative identity.
    game = f"{kickoff.date().isoformat()} {home[1]}-{away[1]}"
    extras: dict[str, Any] = {}
    event_extra = unknown_fields(
        event,
        ("id", "date", "name", "shortName", "season", "status", "competitions"),
    )
    competition_extra = unknown_fields(
        event_competition, ("attendance", "venue", "competitors", "date", "id")
    )
    if event_extra:
        extras["event"] = event_extra
    if competition_extra:
        extras["competition"] = competition_extra
    season_extra = unknown_fields(season, ("year",))
    if season_extra:
        extras["season"] = season_extra
    status_extra = unknown_fields(status, ("type",))
    status_type_extra = unknown_fields(status_type, ("name",))
    if status_extra or status_type_extra:
        extras["status"] = {
            key: value
            for key, value in (
                ("status", status_extra),
                ("type", status_type_extra),
            )
            if value
        }
    side_extras: dict[str, Any] = {}
    for side_name, side_data in (("home", home), ("away", away)):
        raw_competitor = side_data[3]
        raw_team = required_mapping(
            raw_competitor["team"], f"event[{event_id}].{side_name}.team"
        )
        competitor_extra = unknown_fields(raw_competitor, ("homeAway", "score", "team"))
        team_extra = unknown_fields(raw_team, ("id", "displayName"))
        if competitor_extra or team_extra:
            side_extras[side_name] = {
                key: value
                for key, value in (
                    ("competitor", competitor_extra),
                    ("team", team_extra),
                )
                if value
            }
    if side_extras:
        extras["sides"] = side_extras
    if "venue" in event_competition and event_competition["venue"] is not None:
        raw_venue = required_mapping(
            event_competition["venue"], f"event[{event_id}].competition.venue"
        )
        venue_extra = unknown_fields(raw_venue, ("id", "fullName"))
        if venue_extra:
            extras["venue"] = venue_extra
    if source_extra:
        extras["source"] = dict(source_extra)
    return ScheduleRow(
        scope_id=competition.scope_id(edition),
        competition_id=competition.espn_id,
        competition_slug=competition.slug,
        source_season_year=edition.source_season_year,
        event_id=event_id,
        kickoff=kickoff,
        status=status_name,
        status_map_version=STATUS_MAP_VERSION,
        terminal=semantics.terminal,
        played_final=semantics.played_final,
        terminal_nonplayed=semantics.terminal_nonplayed,
        summary_required=semantics.played_final,
        home_team_id=home[0],
        home_team=home[1],
        away_team_id=away[0],
        away_team=away[1],
        home_score=home[2],
        away_score=away[2],
        venue_id=venue_id,
        venue=venue_name,
        attendance=str(attendance) if attendance is not None else None,
        attendance_value=attendance,
        league=league,
        season=legacy_season,
        game=game,
        game_id=event_id,
        league_id=competition.slug,
        date=kickoff,
        match_date=kickoff,
        home_goals=str(home[2]) if home[2] is not None else None,
        away_goals=str(away[2]) if away[2] is not None else None,
        parser_version=PARSER_VERSION,
        extra_json=canonical_json(extras),
    )


def parse_scoreboards(
    raw_payloads: bytes | Iterable[bytes],
    *,
    competition: Competition,
    edition: Edition,
    query_start: date,
    query_end: date,
) -> tuple[ScheduleRow, ...]:
    _validate_scope(competition, edition)
    if type(query_start) is not date or type(query_end) is not date:
        raise TypeError("query_start and query_end must be date values")
    if query_start > query_end:
        raise EspnParseError("query window starts after it ends")
    payloads: Iterable[bytes] = (
        [raw_payloads] if isinstance(raw_payloads, bytes) else raw_payloads
    )
    by_event: dict[int, ScheduleRow] = {}
    for payload_index, raw in enumerate(payloads):
        document = decode_object(raw, f"scoreboard[{payload_index}]")
        league = _scoreboard_league(document, competition)
        source_extra: dict[str, Any] = {}
        root_extra = unknown_fields(document, ("events", "leagues"))
        league_extra = unknown_fields(league, ("id", "slug", "calendar"))
        if root_extra:
            source_extra["scoreboard"] = root_extra
        if league_extra:
            source_extra["league"] = league_extra
        events = required_list(
            document.get("events"), f"scoreboard[{payload_index}].events"
        )
        for raw_event in events:
            row = _event_row(
                raw_event,
                competition=competition,
                edition=edition,
                query_start=query_start,
                query_end=query_end,
                source_extra=source_extra,
            )
            if row is None:
                continue
            existing = by_event.get(row.event_id)
            if existing is not None and existing != row:
                raise EspnParseError(
                    f"conflicting duplicate event_id {row.event_id} across scoreboards"
                )
            by_event[row.event_id] = row
    return tuple(sorted(by_event.values(), key=lambda row: (row.kickoff, row.event_id)))
