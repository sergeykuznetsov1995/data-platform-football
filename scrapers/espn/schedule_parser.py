"""Offline ESPN scoreboard calendar and event parsing."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
import logging
import re
from types import MappingProxyType
from typing import Any, Iterable, Mapping

from .models import Competition, Edition
from .parser_common import (
    EspnParseError,
    canonical_json,
    decode_object,
    espn_day,
    native_id,
    optional_bool,
    optional_nonnegative_int,
    optional_string,
    required_list,
    required_mapping,
    required_string,
    source_day_contains,
    source_year,
    unknown_fields,
    utc_datetime,
)
from .parser_contracts import (
    PARSER_VERSION,
    STATUS_MAP_VERSION,
    ScheduleParseState,
    ScheduleRow,
)

logger = logging.getLogger(__name__)

# One kickoff shared by this many not-yet-started matches of one tournament on
# one ESPN day is a matchday placeholder, not a confirmed time (V2.md:109,
# R-09).  A match already under way has a factual kickoff and never counts.
PLACEHOLDER_MIN_EVENTS = 4
_NOT_STARTED = frozenset({"STATUS_SCHEDULED", "STATUS_PRE_GAME"})
# Open statuses that ESPN leaves open for good once the match is moved.
STALE_OPEN_STATUSES = frozenset({"STATUS_POSTPONED", "STATUS_SUSPENDED"})
_ALL_UID_RE = re.compile(r"^s:600~l:(\d+)~e:(\d+)$")


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
    if not event:
        # ESPN intermittently emits a bare {} inside events (seen 12.08.2026 on
        # 19727:2024, where the same request had returned 16 sound events a day
        # earlier). It carries no field at all, so there is nothing to parse and
        # nothing to lose; a known event replaced by one is still caught by the
        # withdrawn-events ceiling downstream. Anything with a field stays strict.
        return None
    event_id = native_id(event.get("id"), "scoreboard event.id")
    season = required_mapping(event.get("season"), f"event[{event_id}].season")
    event_year = source_year(season.get("year"), f"event[{event_id}].season.year")
    kickoff = utc_datetime(event.get("date"), f"event[{event_id}].date")
    kickoff_date = kickoff.date()
    if event_year != edition.source_season_year:
        return None
    if not source_day_contains(kickoff_date, edition.start_date, edition.end_date):
        return None
    if not source_day_contains(kickoff_date, query_start, query_end):
        return None

    status = required_mapping(event.get("status"), f"event[{event_id}].status")
    status_type = required_mapping(status.get("type"), f"event[{event_id}].status.type")
    status_name = required_string(
        status_type.get("name"), f"event[{event_id}].status.type.name"
    )
    semantics = STATUS_MAP.get(status_name)
    parse_state = ScheduleParseState.PARSED
    if semantics is None:
        # One match waits for review; the tournament still publishes (R-11).
        logger.warning(
            "unknown ESPN status %r in %s: event %s of %s quarantined",
            status_name,
            STATUS_MAP_VERSION,
            event_id,
            competition.scope_id(edition),
        )
        semantics = _OPEN
        parse_state = ScheduleParseState.QUARANTINED

    competitions = required_list(
        event.get("competitions"), f"event[{event_id}].competitions"
    )
    if len(competitions) != 1:
        raise EspnParseError(f"event[{event_id}] must have exactly one competition")
    event_competition = required_mapping(
        competitions[0], f"event[{event_id}].competitions[0]"
    )
    time_valid = optional_bool(
        event_competition.get("timeValid"), f"event[{event_id}].timeValid"
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
        kickoff_confirmed=time_valid is not False,
        parse_state=parse_state,
    )


def _mark_placeholder_kickoffs(rows: Iterable[ScheduleRow]) -> list[ScheduleRow]:
    """Unconfirm a kickoff shared by a whole round of not-yet-started matches."""

    rows = list(rows)
    groups: dict[tuple[str, date, datetime], int] = defaultdict(int)
    for row in rows:
        if row.status in _NOT_STARTED:
            groups[(row.scope_id, espn_day(row.kickoff), row.kickoff)] += 1
    return [
        replace(row, kickoff_confirmed=False)
        if row.status in _NOT_STARTED
        and groups[(row.scope_id, espn_day(row.kickoff), row.kickoff)]
        >= PLACEHOLDER_MIN_EVENTS
        else row
        for row in rows
    ]


def _sorted_rows(by_event: Mapping[int, ScheduleRow]) -> tuple[ScheduleRow, ...]:
    rows = _mark_placeholder_kickoffs(by_event.values())
    return tuple(sorted(rows, key=lambda row: (row.kickoff, row.event_id)))


def _keep_unique(by_event: dict[int, ScheduleRow], row: ScheduleRow) -> None:
    existing = by_event.get(row.event_id)
    if existing is not None and existing != row:
        raise EspnParseError(
            f"conflicting duplicate event_id {row.event_id} across scoreboards"
        )
    by_event[row.event_id] = row


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
            _keep_unique(by_event, row)
    return _sorted_rows(by_event)


def parse_all_scoreboard_day(
    raw: bytes, targets: Mapping[int, Competition], day: date
) -> Mapping[str, tuple[ScheduleRow, ...]]:
    """Rows of one ``all/scoreboard?dates=D`` split by target tournament.

    ``all`` has no per-league root (``leagues[0]`` carries no id), so each event
    is bound through its ``uid`` ``s:600~l:<leagueId>~e:<eventId>``; events of
    non-target leagues are dropped unread.  An event of a season the registry
    has no edition for is skipped with a warning (the core season check opens
    that edition).  Every target slug is present, possibly empty.
    """

    if type(day) is not date:
        raise TypeError("day must be a date value")
    document = decode_object(raw, "all scoreboard")
    leagues = required_list(document.get("leagues"), "all scoreboard.leagues")
    if len(leagues) != 1:
        raise EspnParseError("all scoreboard must contain exactly one root league")
    required_mapping(leagues[0], "all scoreboard.leagues[0]")
    source_extra: dict[str, Any] = {}
    root_extra = unknown_fields(document, ("events", "leagues"))
    if root_extra:
        source_extra["scoreboard"] = root_extra
    by_slug: dict[str, dict[int, ScheduleRow]] = {
        competition.slug: {} for competition in targets.values()
    }
    for index, raw_event in enumerate(
        required_list(document.get("events"), "all scoreboard.events")
    ):
        event = required_mapping(raw_event, f"all scoreboard.events[{index}]")
        if not event:
            continue
        uid = required_string(event.get("uid"), f"all scoreboard.events[{index}].uid")
        match = _ALL_UID_RE.match(uid)
        if match is None:
            raise EspnParseError(f"all scoreboard event uid {uid!r} has no league")
        if native_id(match.group(2), "uid event id") != native_id(
            event.get("id"), f"all scoreboard.events[{index}].id"
        ):
            raise EspnParseError(f"all scoreboard event uid {uid!r} differs from id")
        competition = targets.get(native_id(match.group(1), "uid league id"))
        if competition is None:
            continue
        season = required_mapping(event.get("season"), f"event[{uid}].season")
        year = source_year(season.get("year"), f"event[{uid}].season.year")
        edition = next(
            (item for item in competition.editions if item.source_season_year == year),
            None,
        )
        if edition is None:
            logger.warning(
                "all scoreboard %s: %s has no edition %s, event %s skipped",
                day,
                competition.slug,
                year,
                match.group(2),
            )
            continue
        row = _event_row(
            event,
            competition=competition,
            edition=edition,
            query_start=day,
            query_end=day,
            source_extra=source_extra,
        )
        if row is not None:
            _keep_unique(by_slug[competition.slug], row)
    return MappingProxyType(
        {slug: _sorted_rows(rows) for slug, rows in sorted(by_slug.items())}
    )


def stale_open_events(
    rows: Iterable[ScheduleRow],
    now: datetime,
    older_than: timedelta = timedelta(days=3),
) -> tuple[int, ...]:
    """POSTPONED/SUSPENDED matches whose kickoff is older than ``older_than``.

    These get a daily ``competitions/{id}/status`` check (#1504); the new
    kickoff comes back through the core window list.
    """

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return tuple(
        sorted(
            row.event_id
            for row in rows
            if row.status in STALE_OPEN_STATUSES and row.kickoff < now - older_than
        )
    )
