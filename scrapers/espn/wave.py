"""Current-data waves of the ESPN contour: plan, per-tournament run, summary (#1504).

A wave (``dag_espn_current``, 00/06/12/18 UTC) reads the day statuses of
yesterday and today (UTC) from ``all/scoreboard`` (a day with exactly 1000
events is topped up league by league), compares them with bronze and plans
only the tournament-seasons with something new: a final without a captured
Summary, a changed status/score/kickoff, or a known match that left its day.
Each planned tournament runs on its own (mapped task): Summary of new finals
(cache-first, one download per match), parse, one batch write.  A failing
tournament is red with its first error; its neighbours publish.

A match that left its day is not an accident: core answers 404 -> ``withdrawn``;
core answers -> ``moved`` (new kickoff when another fetched day lists it, the
old one otherwise — ``competitions/{id}/status`` carries no date).  The wave
counts them and warns above ``ESPN_WITHDRAWN_ALERT``; it never stops on them.

Freshness meter (#1505): every match row carries ``status_checked_at`` (when
its status was read) and ``first_published_at`` (carried from the stored row,
never moved by a republication).  A match that is not a played final and
whose stored status predates kickoff + 2 h is written once more with a day
status read after that moment.  The 00 wave also reads the core event list
of every live tournament for the last three ESPN days: an event core lists
that neither bronze nor the fetched days have is taken from its league day
(``topup_days`` of its tournament) and planned like any other match.

Recheck and "not worse" (#1506, ``recheck.py``): a captured match is written
again only when its status, kickoff, score or shootout changes (a new parser
replays the stored body, 0 bytes).  The 00 wave adds the rechecks (kickoff +
7…10 days, incomplete matches only) and the 5 % sample (+24 h / +72 h); any
repeated Summary is compared, parsed, with the stored one and a poorer answer
never replaces a richer one (``downgrade_rejected`` in the recheck journal).

Pure functions: the client (``EspnHttpClient``), Trino (``TrinoTableManager``)
and the journal connection come in as parameters, the DAG is a thin shell.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta, timezone
import json
import logging
import re
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from . import editions_store, recheck, urls
from .bronze_rows import (
    MOVED,
    PENDING,
    PRESENCE_VALUES,
    WITHDRAWN,
    MatchPayload,
    RawRef,
    schedule_shootout,
)
from .bronze_schema import BRONZE_DATABASE, MATCH_TABLE
from .bronze_writer import TournamentBatch, write_tournament_batch
from .core_lists import collect_refs, event_ids as core_event_ids, parse_event_status
from .denominator import DenominatorRow
from .editions import EditionState
from .journal import flush_journal, journal_rows
from .models import (
    AgeClass,
    CapabilityState,
    Competition,
    Edition,
    EntityCapabilities,
    Gender,
)
from .parser_common import EspnParseError, espn_day
from .parser_contracts import ScheduleParseState, ScheduleRow, SummaryParseResult
from .raw_store import RawStoreError
from .schedule_parser import (
    STALE_OPEN_STATUSES,
    STATUS_MAP,
    parse_all_scoreboard_day,
    parse_scoreboards,
    stale_open_events,
)
from .summary_parser import parse_summary
from .transport_contracts import (
    AllOriginsBlocked,
    DirectTransportError,
    OriginBlocked,
    HttpStatusError,
    InvalidJsonError,
    ResponseTooLarge,
    RetryExhausted,
)

logger = logging.getLogger(__name__)

LIVE_POOL = "espn_live"
ESPN_WITHDRAWN_ALERT = 8
# The wave is red when more than this share of its tournaments is red.
RED_SHARE = 0.20
# all/scoreboard has no count: exactly this many events means it was cut.
ALL_DAY_LIMIT = 1000
STALE_AFTER = timedelta(days=3)
# A match ends ~2 h after kickoff: a status read before that is not final (#1505).
RECHECK_AFTER = timedelta(hours=2)
# The 00 wave compares the core event list of the last CORE_DAYS ESPN days.
CORE_DAYS = 3
GREEN = "green"
RED = "red"
_MATCH = f"iceberg.{BRONZE_DATABASE}.{MATCH_TABLE}"
# A request that fails for one match or league is that tournament's error; a
# single 403 (OriginBlocked) is one of them.  Gate closures (AllOriginsBlocked
# — a subclass of OriginBlocked, re-raised first —, LaneClosed,
# DailyCapExceeded) propagate and turn the wave red.
_STATUS_ERRORS = (
    DirectTransportError,
    OriginBlocked,
    EspnParseError,
    HttpStatusError,
    InvalidJsonError,
    ResponseTooLarge,
    RetryExhausted,
)
_ALL_UID = re.compile(r"^s:600~l:(\d+)~e:(\d+)$")
_UNKNOWN = EntityCapabilities(
    CapabilityState.UNKNOWN, CapabilityState.UNKNOWN, CapabilityState.UNKNOWN
)


# --------------------------------------------------------------- registry


def build_competition(
    row: DenominatorRow, edition_state: EditionState
) -> tuple[Competition, Edition]:
    """Registry row + one edition of the cache -> parser context."""

    edition = Edition(
        edition_state.year,
        edition_state.display_name,
        edition_state.start,
        edition_state.end,
        True,
        _UNKNOWN,
    )
    competition = Competition(
        row.espn_id,
        row.slug,
        row.name,
        Gender.MALE,
        AgeClass.SENIOR,
        row.live,
        (edition,),
    )
    return competition, edition


def target_competitions(
    rows: Iterable[DenominatorRow], snapshot: editions_store.EditionsSnapshot
) -> dict[int, Competition]:
    """``espn_id -> Competition`` with every open edition, for the day parser."""

    targets: dict[int, Competition] = {}
    for row in rows:
        editions = tuple(
            build_competition(row, state)[1] for state in snapshot.open_of(row.slug)
        )
        if not editions:
            logger.warning("ESPN %s has no open edition, left out of the wave", row.slug)
            continue
        targets[row.espn_id] = Competition(
            row.espn_id, row.slug, row.name, Gender.MALE, AgeClass.SENIOR, row.live, editions
        )
    return targets


def live_rows(denominator) -> list[DenominatorRow]:
    rows = []
    for slug in sorted(denominator.live_targets()):
        row = denominator.row(slug)
        if row.espn_id is None:
            logger.warning("ESPN live target %s has no espn_id, left out", slug)
            continue
        rows.append(row)
    return rows


# ------------------------------------------------------------------ bronze

_MATCH_COLUMNS = (
    "competition_slug",
    "season_year",
    "event_id",
    "kickoff",
    "kickoff_confirmed",
    "status",
    "status_map_version",
    "terminal",
    "played_final",
    "terminal_nonplayed",
    "home_team_id",
    "home_team",
    "away_team_id",
    "away_team",
    "home_score",
    "away_score",
    "venue_id",
    "venue",
    "attendance",
    "duplicate_of",
    "parse_state",
    "parser_version",
    "disposition",
    "lineup_state",
    "raw_uri",
    "raw_sha256",
    "_source_fetched_at",
    "first_published_at",
    "status_checked_at",
    "home_shootout",
    "away_shootout",
    "rechecked_at",
)


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _naive(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(tzinfo=None)


@dataclass(frozen=True, slots=True)
class BronzeMatch:
    """The fields of one ``espn_match`` row a wave compares and rebuilds."""

    competition_slug: str
    season_year: int
    event_id: int
    kickoff: datetime
    kickoff_confirmed: bool
    status: str
    status_map_version: str
    terminal: bool
    played_final: bool
    terminal_nonplayed: bool
    home_team_id: int
    home_team: str
    away_team_id: int
    away_team: str
    home_score: int | None
    away_score: int | None
    venue_id: int | None
    venue: str | None
    attendance: int | None
    duplicate_of: str | None
    parse_state: str
    parser_version: str
    disposition: str | None
    lineup_state: str
    raw_uri: str
    raw_sha256: str
    source_fetched_at: datetime
    first_published_at: datetime | None = None
    status_checked_at: datetime | None = None
    home_shootout: int | None = None
    away_shootout: int | None = None
    rechecked_at: datetime | None = None

    @classmethod
    def from_row(cls, row: Sequence[Any]) -> "BronzeMatch":
        values = dict(zip(_MATCH_COLUMNS, row))
        values["source_fetched_at"] = values.pop("_source_fetched_at")
        values["kickoff"] = _aware(values["kickoff"])
        if values["status_checked_at"] is not None:
            values["status_checked_at"] = _aware(values["status_checked_at"])
        values["season_year"] = int(values["season_year"])
        values["event_id"] = int(values["event_id"])
        return cls(**values)

    @property
    def summary_captured(self) -> bool:
        return self.lineup_state != PENDING

    def raw_ref(self) -> RawRef:
        return RawRef(self.raw_uri, self.raw_sha256, self.source_fetched_at)

    def carried(self, payload: MatchPayload) -> MatchPayload:
        """``payload`` with the stored ``first_published_at`` and
        ``rechecked_at`` carried over (a recheck sets its own)."""

        changes: dict[str, Any] = {}
        if self.first_published_at is not None:
            changes["first_published_at"] = self.first_published_at
        if payload.rechecked_at is None and self.rechecked_at is not None:
            changes["rechecked_at"] = self.rechecked_at
        return replace(payload, **changes) if changes else payload

    def schedule_row(
        self,
        competition: Competition,
        edition: Edition,
        *,
        status: str | None = None,
        scores: tuple[int, int] | None = None,
    ) -> ScheduleRow:
        """The schedule row this bronze row was written from (no day lists it).

        ``status`` replaces the stored one (a core status check), ``scores``
        the stored score of a match that became a played final; the match row
        it gives equals the stored row except for those fields.
        """

        name = status or self.status
        semantics = STATUS_MAP.get(name) if status else None
        played = semantics.played_final if semantics else self.played_final
        if scores is not None:
            home, away = scores
        else:
            home = self.home_score if played else None
            away = self.away_score if played else None
        return ScheduleRow(
            scope_id=competition.scope_id(edition),
            competition_id=competition.espn_id,
            competition_slug=self.competition_slug,
            source_season_year=self.season_year,
            event_id=self.event_id,
            kickoff=self.kickoff,
            status=name,
            status_map_version=self.status_map_version,
            terminal=semantics.terminal if semantics else self.terminal,
            played_final=played,
            terminal_nonplayed=(
                semantics.terminal_nonplayed if semantics else self.terminal_nonplayed
            ),
            summary_required=played,
            home_team_id=self.home_team_id,
            home_team=self.home_team,
            away_team_id=self.away_team_id,
            away_team=self.away_team,
            home_score=home,
            away_score=away,
            venue_id=self.venue_id,
            venue=self.venue,
            attendance=str(self.attendance) if self.attendance is not None else None,
            attendance_value=self.attendance,
            league=competition.name,
            season=str(self.season_year),
            game=f"{self.kickoff.date().isoformat()} {self.home_team}-{self.away_team}",
            game_id=self.event_id,
            league_id=self.competition_slug,
            date=self.kickoff,
            match_date=self.kickoff,
            home_goals=str(home) if home is not None else None,
            away_goals=str(away) if away is not None else None,
            parser_version=self.parser_version,
            extra_json="{}",
            kickoff_confirmed=self.kickoff_confirmed,
            duplicate_of=self.duplicate_of,
            parse_state=ScheduleParseState(self.parse_state),
        )


def _select(where: str) -> str:
    return f"SELECT {', '.join(_MATCH_COLUMNS)} FROM {_MATCH} WHERE {where}"


def bronze_window(trino, start: date, end: date) -> list[BronzeMatch]:
    """Matches with kickoff (UTC) in ``[start, end]`` days."""

    rows = trino.execute_query(
        _select("kickoff >= ? AND kickoff < ?"),
        (
            datetime.combine(start, time()),
            datetime.combine(end + timedelta(days=1), time()),
        ),
    )
    return [BronzeMatch.from_row(row) for row in rows]


def bronze_stale(trino, now: datetime) -> list[BronzeMatch]:
    """Open POSTPONED/SUSPENDED matches older than ``STALE_AFTER``."""

    statuses = ", ".join(f"'{name}'" for name in sorted(STALE_OPEN_STATUSES))
    rows = trino.execute_query(
        _select(f"status IN ({statuses}) AND NOT terminal AND kickoff < ?"),
        (_naive(now - STALE_AFTER),),
    )
    return [BronzeMatch.from_row(row) for row in rows]


def bronze_matches(
    trino, slug: str, season_year: int, event_ids: Iterable[int]
) -> dict[int, BronzeMatch]:
    ids = sorted({int(event_id) for event_id in event_ids})
    if not ids:
        return {}
    rows = trino.execute_query(
        _select(
            "competition_slug = ? AND season_year = ? AND event_id IN ("
            + ", ".join(str(event_id) for event_id in ids)
            + ")"
        ),
        (slug, int(season_year)),
    )
    return {match.event_id: match for match in map(BronzeMatch.from_row, rows)}


def bronze_edition_terminal(trino) -> dict[str, dict[int, bool]]:
    """``slug -> year -> every bronze match terminal`` (closes old editions)."""

    rows = trino.execute_query(
        f"SELECT competition_slug, season_year, bool_and(terminal) FROM {_MATCH} "
        "GROUP BY competition_slug, season_year"
    )
    result: dict[str, dict[int, bool]] = {}
    for slug, year, terminal in rows:
        result.setdefault(slug, {})[int(year)] = bool(terminal)
    return result


# -------------------------------------------------------------------- plan


@dataclass(frozen=True, slots=True)
class TournamentWork:
    """One mapped unit: a tournament-season and the matches it must write."""

    slug: str
    season_year: int
    event_ids: tuple[int, ...]
    espn_id: int
    name: str
    display_name: str
    start: date
    end: date
    days: tuple[date, ...]
    topup_days: tuple[date, ...] = ()
    # event_id -> WITHDRAWN / MOVED for matches no fetched day lists at their
    # stored day; the MOVED ones found on another day carry the new kickoff.
    presence: Mapping[int, str] = field(default_factory=dict)
    # event_id -> status from core for a match no fetched day lists.
    statuses: Mapping[int, str] = field(default_factory=dict)
    # event_id -> ISO time core answered the status of a match no fetched
    # day lists (``status_checked_at`` of its row).
    checked_at: Mapping[int, str] = field(default_factory=dict)
    # event_id -> recheck kind (#1506): the Summary is downloaded again.
    rechecks: Mapping[int, str] = field(default_factory=dict)
    # Set when planning already failed for this tournament: the mapped task
    # turns it red with this first error, its neighbours still publish.
    error: str | None = None

    def context(self) -> tuple[Competition, Edition]:
        edition = Edition(
            self.season_year, self.display_name, self.start, self.end, True, _UNKNOWN
        )
        competition = Competition(
            self.espn_id, self.slug, self.name, Gender.MALE, AgeClass.SENIOR, True, (edition,)
        )
        return competition, edition

    def to_xcom(self) -> dict[str, Any]:
        return {
            "slug": self.slug,
            "season_year": self.season_year,
            "event_ids": list(self.event_ids),
            "espn_id": self.espn_id,
            "name": self.name,
            "display_name": self.display_name,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "days": [day.isoformat() for day in self.days],
            "topup_days": [day.isoformat() for day in self.topup_days],
            "presence": {str(key): value for key, value in sorted(self.presence.items())},
            "statuses": {str(key): value for key, value in sorted(self.statuses.items())},
            "checked_at": {str(key): value for key, value in sorted(self.checked_at.items())},
            "rechecks": {str(key): value for key, value in sorted(self.rechecks.items())},
            "error": self.error,
        }

    @classmethod
    def from_xcom(cls, value: Mapping[str, Any]) -> "TournamentWork":
        return cls(
            slug=value["slug"],
            season_year=int(value["season_year"]),
            event_ids=tuple(int(item) for item in value["event_ids"]),
            espn_id=int(value["espn_id"]),
            name=value["name"],
            display_name=value["display_name"],
            start=date.fromisoformat(value["start"]),
            end=date.fromisoformat(value["end"]),
            days=tuple(date.fromisoformat(item) for item in value["days"]),
            topup_days=tuple(date.fromisoformat(item) for item in value["topup_days"]),
            presence={int(key): item for key, item in value["presence"].items()},
            statuses={int(key): item for key, item in value["statuses"].items()},
            checked_at={int(key): item for key, item in value.get("checked_at", {}).items()},
            rechecks={int(key): item for key, item in value.get("rechecks", {}).items()},
            error=value.get("error"),
        )


class WavePlanError(RuntimeError):
    """Planning already failed for this tournament (a day or its editions)."""


class SummaryFetchError(RuntimeError):
    """A new final's Summary did not download; the match row is written
    without it (pending, a miss of the freshness meter) and the task retries."""


@dataclass(frozen=True, slots=True)
class WavePlan:
    works: tuple[TournamentWork, ...]
    days: tuple[date, ...]
    topup_days: tuple[date, ...]
    status_checks: int


def _fetch(client, request: urls.EspnRequest, *, force_refresh: bool):
    return client.fetch_json(
        request.url, request.endpoint, request.params, force_refresh=force_refresh
    )


def _raw_ref(result) -> RawRef:
    return RawRef(result.raw_uri, result.content_hash, datetime.fromisoformat(result.fetched_at))


def _league_day_rows(
    body: bytes, competition: Competition, day: date
) -> list[ScheduleRow]:
    rows: list[ScheduleRow] = []
    for edition in competition.open_editions():
        rows.extend(
            parse_scoreboards(
                body,
                competition=competition,
                edition=edition,
                query_start=day,
                query_end=day,
            )
        )
    return rows


def split_day(
    body: bytes, targets: Mapping[int, Competition], day: date
) -> tuple[dict[str, list[ScheduleRow]], dict[str, str]]:
    """Rows of one ``all/scoreboard`` day per target slug, and parse errors per slug.

    The day parser fails the whole body on one broken event.  When the body
    itself is sound (one root league, an event list, every event bound to a
    league by its ``uid``), each target's events are then parsed on their own:
    only the tournament that owns the broken event is red.  A broken body or
    an event no league owns keeps the whole-day error — nothing can be
    attributed, so nothing is compared.
    """

    try:
        parsed = parse_all_scoreboard_day(body, targets, day)
        return {slug: list(rows) for slug, rows in parsed.items()}, {}
    except EspnParseError as exc:
        whole = exc
    try:
        document = json.loads(body)
    except ValueError:
        raise whole from None
    leagues = document.get("leagues") if isinstance(document, dict) else None
    events = document.get("events") if isinstance(document, dict) else None
    if (
        not isinstance(leagues, list)
        or len(leagues) != 1
        or not isinstance(leagues[0], dict)
        or not isinstance(events, list)
    ):
        raise whole
    groups: dict[int, list[Any]] = {}
    for event in events:
        if isinstance(event, dict) and not event:
            continue  # the parser skips a bare {} too
        uid = event.get("uid") if isinstance(event, dict) else None
        match = _ALL_UID.match(uid) if isinstance(uid, str) else None
        if match is None:
            raise whole
        if int(match.group(1)) in targets:
            groups.setdefault(int(match.group(1)), []).append(event)
    rows: dict[str, list[ScheduleRow]] = {c.slug: [] for c in targets.values()}
    errors: dict[str, str] = {}
    for espn_id, group in groups.items():
        competition = targets[espn_id]
        part = json.dumps({"leagues": [{}], "events": group}).encode()
        try:
            rows[competition.slug] = list(
                parse_all_scoreboard_day(part, {espn_id: competition}, day)[competition.slug]
            )
        except EspnParseError as exc:
            errors[competition.slug] = f"EspnParseError: {exc}"
    logger.warning(
        "all/scoreboard %s parsed per tournament after %s: %d red", day, whole, len(errors)
    )
    return rows, errors


def _day_rows(
    client, targets: Mapping[int, Competition], day: date
) -> tuple[dict[int, ScheduleRow], bool, dict[str, str], datetime]:
    """Rows of one day for every target; tops up by league when ``all`` is cut.

    Returns the rows, whether the day was cut, errors per slug and when the
    ``all`` body was fetched (the moment its statuses were read).
    """

    result = _fetch(client, urls.all_scoreboard_day(day), force_refresh=True)
    checked = _aware(datetime.fromisoformat(result.fetched_at))
    by_slug, errors = split_day(result.body, targets, day)
    rows: dict[int, ScheduleRow] = {}
    for group in by_slug.values():
        rows.update((row.event_id, row) for row in group)
    events = result.json_data.get("events") if isinstance(result.json_data, dict) else None
    if not isinstance(events, list) or len(events) < ALL_DAY_LIMIT:
        return rows, False, errors, checked
    logger.warning(
        "all/scoreboard %s returned %d events: topping up %d leagues",
        day,
        len(events),
        len(targets),
    )
    for competition in sorted(targets.values(), key=lambda item: item.slug):
        try:
            league = _fetch(
                client, urls.league_scoreboard_day(competition.slug, day), force_refresh=True
            )
            rows.update(
                (row.event_id, row) for row in _league_day_rows(league.body, competition, day)
            )
        except AllOriginsBlocked:
            raise
        except _STATUS_ERRORS as exc:
            errors[competition.slug] = f"{type(exc).__name__}: {exc}"
    return rows, True, errors, checked


def _needs_recheck(
    row: ScheduleRow, stored: BronzeMatch, checked_at: datetime | None
) -> bool:
    """A non-final match whose stored status predates kickoff + 2 h, seen after it.

    Written once more so that ``status_checked_at`` shows the status after
    the match should have ended (#1505); afterwards it is past that moment
    and the match stays out of the wave until something changes.
    """

    if row.played_final or checked_at is None:
        return False
    due = _aware(row.kickoff) + RECHECK_AFTER
    return checked_at >= due and (
        stored.status_checked_at is None or stored.status_checked_at < due
    )


def _changed(
    row: ScheduleRow, stored: BronzeMatch | None, checked_at: datetime | None = None
) -> bool:
    if stored is None:
        return True
    if row.played_final and not stored.summary_captured:
        return True
    if _needs_recheck(row, stored, checked_at):
        return True
    home = row.home_score if row.played_final else None
    away = row.away_score if row.played_final else None
    # A shootout the day lists (#1506); parser_version and the rest of
    # extra_json are never a reason to download or write again.
    shootout = schedule_shootout(row) if row.played_final else (None, None)
    return (
        row.status != stored.status
        or row.kickoff != stored.kickoff
        or home != stored.home_score
        or away != stored.away_score
        or (shootout[0] is not None and shootout[0] != stored.home_shootout)
        or (shootout[1] is not None and shootout[1] != stored.away_shootout)
    )


def check_presence(
    client, stored: BronzeMatch
) -> tuple[str | None, str | None, str | None]:
    """``(presence, new status, error)`` of a known match no fetched day lists.

    404 -> WITHDRAWN; any status answer -> MOVED, with the status when it
    differs from the stored one and is in the status map (a played final is
    then written with its Summary).  Any other failure leaves the match
    unmarked and names the error: its tournament is red in the wave.
    """

    request = urls.event_status(stored.competition_slug, stored.event_id)
    try:
        status = parse_event_status(_fetch(client, request, force_refresh=True).json_data)
    except HttpStatusError as exc:
        if exc.status == 404:
            return WITHDRAWN, None, None
        return None, None, f"status of {stored.event_id}: HttpStatusError: {exc}"
    except AllOriginsBlocked:
        raise
    except _STATUS_ERRORS as exc:
        return None, None, f"status of {stored.event_id}: {type(exc).__name__}: {exc}"
    if status == stored.status or status not in STATUS_MAP:
        return MOVED, None, None
    return MOVED, status, None


def _error_work(
    row: DenominatorRow,
    snapshot: editions_store.EditionsSnapshot,
    days: tuple[date, ...],
    error: str,
) -> TournamentWork:
    states = snapshot.open_of(row.slug)
    year = states[0].year if states else (row.current_season_year or 0)
    return TournamentWork(
        slug=row.slug,
        season_year=year,
        event_ids=(),
        espn_id=row.espn_id,
        name=row.name,
        display_name="-",
        start=days[0],
        end=days[-1],
        days=days,
        error=error,
    )


def _core_missing(
    client,
    competition: Competition,
    first: date,
    last: date,
    known: set[int],
) -> tuple[dict[int, tuple[date, ScheduleRow, datetime]], list[int]]:
    """Events core lists for ESPN days ``first..last`` that ``known`` lacks.

    Each is looked up on its league day (a fresh read): returns the rows
    found ``event_id -> (day, row, fetched_at)`` and the ids no league day
    lists.  Core is the denominator of the freshness meter (R-09): the day
    scoreboard is only what was downloaded.
    """

    slug = competition.slug
    listed = core_event_ids(
        collect_refs(
            lambda page: _fetch(
                client, urls.events_window(slug, first, last, page), force_refresh=True
            ).body,
            f"{slug} core events",
        )
    )
    missing = set(listed) - known
    found: dict[int, tuple[date, ScheduleRow, datetime]] = {}
    day = first
    while missing - set(found) and day <= last:
        result = _fetch(client, urls.league_scoreboard_day(slug, day), force_refresh=True)
        fetched = _aware(datetime.fromisoformat(result.fetched_at))
        for row in _league_day_rows(result.body, competition, day):
            if row.event_id in missing:
                found[row.event_id] = (day, row, fetched)
        day += timedelta(days=1)
    return found, sorted(missing - set(found))


def plan_wave(
    *,
    client,
    trino,
    rows: Sequence[DenominatorRow],
    state_path: Path,
    now: datetime,
    check_stale: bool,
    check_core: bool = False,
    check_recheck: bool = False,
) -> WavePlan:
    """Tournament-seasons with something to write in this wave.

    A tournament whose planning failed (no edition, a broken day, a failed
    top-up) comes back as a work item with ``error``: red in the wave
    summary, without touching its neighbours.  ``check_core`` (the 00 wave)
    adds the events of the core list no day and no bronze row has;
    ``check_recheck`` (the 00 wave) adds the rechecks and the sample (#1506).
    """

    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    snapshot = editions_store.load_or_refresh(
        state_path,
        client=client,
        rows=rows,
        schedule_terminal=lambda: bronze_edition_terminal(trino),
        now=now,
    )
    targets = target_competitions(rows, snapshot)
    by_slug = {row.slug: row for row in rows}
    today = now.astimezone(timezone.utc).date()
    days = (today - timedelta(days=1), today)
    errors: dict[str, str] = {
        row.slug: "no open edition: " + snapshot.failed.get(row.slug, "core season unknown")
        for row in rows
        if row.espn_id not in targets
    }

    found: dict[int, tuple[date, ScheduleRow]] = {}
    checked: dict[int, datetime] = {}
    topup: list[date] = []
    for day in days:
        day_rows, cut, day_errors, day_checked = _day_rows(client, targets, day)
        for slug, error in day_errors.items():
            errors.setdefault(slug, f"{day.isoformat()}: {error}")
        if cut:
            topup.append(day)
        # The later day wins a match both days list (its status is newer).
        found.update((event_id, (day, row)) for event_id, row in day_rows.items())
        checked.update((event_id, day_checked) for event_id in day_rows)
    stored = {
        match.event_id: match
        for match in bronze_window(trino, days[0] - timedelta(days=1), days[-1] + timedelta(days=1))
    }
    # Only tournaments whose days were read completely are compared.
    usable = {slug for slug in by_slug if slug not in errors}
    status_errors: dict[str, str] = {}
    core_days: dict[str, set[date]] = {}
    core_added = 0
    if check_core:
        first = today - timedelta(days=CORE_DAYS - 1)
        for competition in sorted(targets.values(), key=lambda item: item.slug):
            if competition.slug not in usable or not any(
                edition.start_date <= today and edition.end_date >= first
                for edition in competition.open_editions()
            ):
                continue
            try:
                added, unlisted = _core_missing(
                    client, competition, first, today, set(found) | set(stored)
                )
            except AllOriginsBlocked:
                raise
            except _STATUS_ERRORS as exc:
                status_errors.setdefault(
                    competition.slug, f"core events: {type(exc).__name__}: {exc}"
                )
                continue
            for event_id, (day, row, fetched) in added.items():
                found[event_id] = (day, row)
                checked[event_id] = fetched
                core_days.setdefault(competition.slug, set()).add(day)
            core_added += len(added)
            if unlisted:
                status_errors.setdefault(
                    competition.slug,
                    f"core lists {len(unlisted)} event(s) no league day has: "
                    + ", ".join(str(event_id) for event_id in unlisted[:5]),
                )

    event_ids: dict[tuple[str, int], set[int]] = {}
    presence: dict[int, str] = {}
    statuses: dict[int, str] = {}
    checked_at: dict[int, str] = {}

    def add(slug: str, year: int, event_id: int) -> None:
        event_ids.setdefault((slug, year), set()).add(event_id)

    for event_id, (day, row) in found.items():
        if row.competition_slug not in usable:
            continue
        known = stored.get(event_id)
        if not _changed(row, known, checked.get(event_id)):
            continue
        if (
            known is not None
            and not row.played_final
            and espn_day(row.kickoff) != espn_day(known.kickoff)
        ):
            presence[event_id] = MOVED
        add(row.competition_slug, row.source_season_year, event_id)

    absent = [
        match
        for match in stored.values()
        if match.event_id not in found
        and not match.terminal
        and match.competition_slug in usable
        and espn_day(match.kickoff) in days
        and match.disposition not in PRESENCE_VALUES
    ]
    if check_stale:
        stale = {
            match.event_id: match
            for match in bronze_stale(trino, now)
            if match.competition_slug in usable and match.disposition != WITHDRAWN
        }
        stale_ids = set(stale_open_events(stale.values(), now, STALE_AFTER))
        absent.extend(
            match
            for event_id, match in sorted(stale.items())
            if event_id in stale_ids and event_id not in found
            and event_id not in {item.event_id for item in absent}
        )
    for match in absent:
        mark, status, error = check_presence(client, match)
        if error is not None:
            logger.warning("ESPN %s: %s", match.competition_slug, error)
            status_errors.setdefault(match.competition_slug, error)
            continue
        if mark is None or (mark == match.disposition and status is None):
            continue
        if mark == MOVED:
            checked_at[match.event_id] = _naive(now).isoformat()
        if status is not None:
            statuses[match.event_id] = status
        # A played final carries its Summary disposition, not a presence.
        if status is None or not STATUS_MAP[status].played_final:
            presence[match.event_id] = mark
        add(match.competition_slug, match.season_year, match.event_id)

    rechecks: dict[int, str] = {}
    if check_recheck:
        for event_id, (slug, year, kind) in sorted(recheck.plan_recheck(trino, now).items()):
            if slug not in usable or snapshot.edition(slug, year) is None:
                logger.info("ESPN %s %s: %s skipped (not a live edition)", slug, event_id, kind)
                continue
            rechecks[event_id] = kind
            add(slug, year, event_id)

    works = [_error_work(by_slug[slug], snapshot, days, error) for slug, error in sorted(errors.items())]
    for (slug, year), ids in sorted(event_ids.items()):
        state = snapshot.edition(slug, year)
        row = by_slug[slug]
        if state is None:
            works.append(_error_work(row, snapshot, days, f"no edition {year} in the cache"))
            continue
        works.append(
            TournamentWork(
                slug=slug,
                season_year=year,
                event_ids=tuple(sorted(ids)),
                espn_id=row.espn_id,
                name=row.name,
                display_name=state.display_name,
                start=state.start,
                end=state.end,
                days=days,
                topup_days=tuple(sorted(set(topup) | core_days.get(slug, set()))),
                presence={key: presence[key] for key in sorted(ids) if key in presence},
                statuses={key: statuses[key] for key in sorted(ids) if key in statuses},
                checked_at={key: checked_at[key] for key in sorted(ids) if key in checked_at},
                rechecks={key: rechecks[key] for key in sorted(ids) if key in rechecks},
                # A failed status check: the planned matches still publish,
                # then the tournament is red with that error.
                error=status_errors.get(slug),
            )
        )
    planned = {work.slug for work in works}
    works.extend(
        _error_work(by_slug[slug], snapshot, days, error)
        for slug, error in sorted(status_errors.items())
        if slug not in planned
    )
    logger.info(
        "ESPN wave plan: %d tournament(s) (%d red at planning), %d match(es), "
        "%d status check(s), %d added from core, %d recheck(s), days %s",
        len(works),
        len(errors),
        sum(len(work.event_ids) for work in works),
        len(absent),
        core_added,
        len(rechecks),
        [day.isoformat() for day in days],
    )
    return WavePlan(tuple(works), days, tuple(topup), len(absent))


# ---------------------------------------------------------------- run


@dataclass(frozen=True, slots=True)
class TournamentOutcome:
    slug: str
    season_year: int
    state: str
    matches: int
    dispositions: Mapping[str, int]
    first_error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "slug": self.slug,
            "season_year": self.season_year,
            "state": self.state,
            "matches": self.matches,
            "dispositions": dict(self.dispositions),
            "first_error": self.first_error,
        }


def failed_outcome(work: TournamentWork, exc: BaseException) -> TournamentOutcome:
    return TournamentOutcome(
        work.slug, work.season_year, RED, 0, {}, f"{type(exc).__name__}: {exc}"
    )


def finish_requests(client, conn, *, run_id: str, task_id: str) -> None:
    """Journal of the task's requests, then the queued raw bodies."""

    flush_journal(conn, journal_rows(client.ledger, run_id=run_id, task_id=task_id))
    client.flush()


def _summary_result(client, request: urls.EspnRequest, *, captured: bool):
    if captured:
        # Status changed after capture: the batch carries the full match
        # state, so the stored Summary is read back instead of downloaded.
        try:
            return client.replay_json(request.url, request.endpoint, request.params)
        except RawStoreError:
            pass
    return _fetch(client, request, force_refresh=False)


@dataclass(frozen=True, slots=True)
class _Again:
    """A Summary downloaded (or replayed) again for a captured match (#1506)."""

    summary: SummaryParseResult
    raw: RawRef
    outcome: str
    before: Mapping[str, int] | None
    after: Mapping[str, int] | None
    # False when the body equals the stored one (nothing to journal).
    new_body: bool = True


def _stored_summary(
    client, known: BronzeMatch, schedule: ScheduleRow, competition: Competition, edition: Edition
) -> SummaryParseResult:
    """The stored Summary of ``known`` parsed again: its exact raw body."""

    body = client.raw_store.load_exact(known.raw_uri, known.raw_sha256)
    return parse_summary(body, competition=competition, edition=edition, event=schedule)


def _summary_again(
    client,
    known: BronzeMatch,
    schedule: ScheduleRow,
    competition: Competition,
    edition: Edition,
    *,
    force: bool,
) -> _Again:
    """Summary of a captured match under the "not worse" rule.

    ``force`` (recheck, sample) downloads it; a failed download keeps the
    stored parse (``failed``).  Otherwise (a status change) the stored body
    is replayed as before.  A new body is compared, parsed, with the stored
    one: a poorer answer keeps the stored parse and its raw body.
    """

    request = urls.summary(known.competition_slug, known.event_id)
    if force:
        try:
            result = _fetch(client, request, force_refresh=True)
        except AllOriginsBlocked:
            raise
        except _STATUS_ERRORS as exc:
            logger.warning(
                "ESPN recheck of %s failed: %s: %s", known.event_id, type(exc).__name__, exc
            )
            old = _stored_summary(client, known, schedule, competition, edition)
            return _Again(old, known.raw_ref(), recheck.FAILED, recheck.summary_parts(old), None)
    else:
        result = _summary_result(client, request, captured=True)
    new = parse_summary(result.body, competition=competition, edition=edition, event=schedule)
    if result.content_hash == known.raw_sha256:
        parts = recheck.summary_parts(new)
        return _Again(new, _raw_ref(result), recheck.SAME, parts, parts, new_body=False)
    old = _stored_summary(client, known, schedule, competition, edition)
    outcome = recheck.compare_parts(old, new)
    before, after = recheck.summary_parts(old), recheck.summary_parts(new)
    if outcome == recheck.DOWNGRADE_REJECTED:
        logger.warning(
            "ESPN %s %s: downgrade rejected, stored Summary kept: %s -> %s",
            known.competition_slug,
            known.event_id,
            before,
            after,
        )
        return _Again(old, known.raw_ref(), outcome, before, after)
    return _Again(new, _raw_ref(result), outcome, before, after)


def _header_scores(body: bytes) -> tuple[int, int] | None:
    """Home and away score from a Summary header; None when it has none."""

    try:
        header = json.loads(body)["header"]["competitions"][0]["competitors"]
        sides = {side["homeAway"]: int(side["score"]) for side in header}
        return sides["home"], sides["away"]
    except (KeyError, IndexError, TypeError, ValueError):
        return None


def _absent_payload(
    client,
    work: TournamentWork,
    known: BronzeMatch,
    competition: Competition,
    edition: Edition,
    *,
    presence: str | None,
) -> MatchPayload:
    """Payload of a match no fetched day lists, rebuilt from its bronze row.

    A core status that is a played final (a stuck POSTPONED match played on
    another day) is written with its Summary: the score comes from the
    Summary header, since no day lists the match.
    """

    checked = work.checked_at.get(known.event_id)
    status_checked_at = (
        datetime.fromisoformat(checked) if checked is not None else known.status_checked_at
    )
    status = work.statuses.get(known.event_id)
    if status is not None and STATUS_MAP[status].played_final:
        result = _fetch(client, urls.summary(work.slug, known.event_id), force_refresh=False)
        scores = _header_scores(result.body)
        if scores is not None:
            schedule = known.schedule_row(competition, edition, status=status, scores=scores)
            parsed = parse_summary(
                result.body, competition=competition, edition=edition, event=schedule
            )
            return known.carried(
                MatchPayload(
                    schedule, parsed, _raw_ref(result), status_checked_at=status_checked_at
                )
            )
        logger.warning("ESPN %s: final without a header score, kept as moved", known.event_id)
        status, presence = None, MOVED
    schedule = known.schedule_row(competition, edition, status=status)
    return known.carried(
        MatchPayload(
            schedule, None, known.raw_ref(), presence, status_checked_at=status_checked_at
        )
    )


def run_tournament(
    work: TournamentWork, *, client, trino, conn, run_id: str, task_id: str
) -> TournamentOutcome:
    """Write the planned matches of one tournament-season as one batch.

    Network and write errors raise (the task retries, then the tournament is
    red); Summary shapes never raise — they are dispositions.  A new final
    whose Summary does not download is written without it (pending) before
    ``SummaryFetchError`` is raised, so the match stays in the denominator of
    the freshness meter (#1505).  A repeated Summary of a captured match
    (recheck, sample, status change) goes through the "not worse" rule and
    the recheck journal (#1506).
    """

    if work.error is not None and not work.event_ids:
        raise WavePlanError(work.error)
    failure: BaseException | None = None
    try:
        competition, edition = work.context()
        targets = {competition.espn_id: competition}
        rows: dict[int, tuple[ScheduleRow, RawRef]] = {}
        for day in work.days:
            result = _fetch(client, urls.all_scoreboard_day(day), force_refresh=False)
            by_slug, errors = split_day(result.body, targets, day)
            if work.slug in errors:
                raise EspnParseError(errors[work.slug])
            for row in by_slug[work.slug]:
                rows[row.event_id] = (row, _raw_ref(result))
        for day in work.topup_days:
            result = _fetch(
                client, urls.league_scoreboard_day(work.slug, day), force_refresh=False
            )
            for row in _league_day_rows(result.body, competition, day):
                rows[row.event_id] = (row, _raw_ref(result))
        stored = bronze_matches(trino, work.slug, work.season_year, work.event_ids)

        payloads: list[MatchPayload] = []
        summary_errors: list[str] = []
        journal: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        def note(event_id: int, kind: str, again: _Again) -> None:
            journal.append(
                recheck.recheck_row(
                    checked_at=now,
                    run_id=run_id,
                    slug=work.slug,
                    season_year=work.season_year,
                    event_id=event_id,
                    kind=kind,
                    before=again.before,
                    after=again.after,
                    outcome=again.outcome,
                )
            )

        for event_id in work.event_ids:
            presence = work.presence.get(event_id)
            known = stored.get(event_id)
            found = rows.get(event_id)
            if found is not None and found[0].source_season_year != work.season_year:
                found = None
            kind = work.rechecks.get(event_id)
            if (
                kind is not None
                and known is not None
                and known.summary_captured
                and (found is None or found[0].played_final)
            ):
                if found is not None:
                    schedule, checked = found[0], found[1].fetched_at
                else:
                    schedule = known.schedule_row(competition, edition)
                    checked = known.status_checked_at
                again = _summary_again(client, known, schedule, competition, edition, force=True)
                note(event_id, kind, again)
                payloads.append(
                    known.carried(
                        MatchPayload(
                            schedule,
                            again.summary,
                            again.raw,
                            status_checked_at=checked,
                            rechecked_at=now if kind == recheck.RECHECK else None,
                        )
                    )
                )
                continue
            if found is None:
                if known is None:
                    raise LookupError(f"event {event_id}: neither a day nor bronze has it")
                payloads.append(
                    _absent_payload(
                        client, work, known, competition, edition, presence=presence
                    )
                )
                continue
            schedule, raw = found
            if not schedule.played_final:
                payload = MatchPayload(
                    schedule, None, raw, presence, status_checked_at=raw.fetched_at
                )
            else:
                captured = known is not None and known.summary_captured
                if captured:
                    # Written without it, the match would lose its children:
                    # a failure raises.
                    again = _summary_again(
                        client, known, schedule, competition, edition, force=False
                    )
                    if again.new_body:
                        note(event_id, recheck.REFRESH, again)
                    payloads.append(
                        known.carried(
                            MatchPayload(
                                schedule,
                                again.summary,
                                again.raw,
                                status_checked_at=raw.fetched_at,
                            )
                        )
                    )
                    continue
                try:
                    result = _fetch(client, urls.summary(work.slug, event_id), force_refresh=False)
                except AllOriginsBlocked:
                    raise
                except _STATUS_ERRORS as exc:
                    # The match is the failure unit (#1505): its row lands
                    # without Summary (pending), so the meter counts it as a
                    # miss instead of losing it from the denominator.
                    summary_errors.append(f"summary of {event_id}: {type(exc).__name__}: {exc}")
                    payload = MatchPayload(
                        schedule, None, raw, presence, status_checked_at=raw.fetched_at
                    )
                else:
                    parsed = parse_summary(
                        result.body, competition=competition, edition=edition, event=schedule
                    )
                    payload = MatchPayload(
                        schedule, parsed, _raw_ref(result), status_checked_at=raw.fetched_at
                    )
            payloads.append(known.carried(payload) if known is not None else payload)

        # Bronze rows never point at a raw body that is not written yet.
        client.flush()
        write_tournament_batch(
            TournamentBatch(work.slug, work.season_year, payloads), trino=trino
        )
        # After the write: a journalled outcome is what bronze holds.
        if journal:
            recheck.flush_rechecks(conn, journal)
    except BaseException as exc:
        failure = exc
        raise
    finally:
        try:
            finish_requests(client, conn, run_id=run_id, task_id=task_id)
        except Exception:
            if failure is None:
                raise
            # The tournament's own error is the one to report.
            logger.exception("ESPN request journal of %s not written", work.slug)
    if work.error is not None:
        # Its planned matches are written; the tournament is still red.
        raise WavePlanError(work.error)
    if summary_errors:
        # The rows are written; the tournament is red and the task retries.
        raise SummaryFetchError(
            f"{len(summary_errors)} Summary download(s) failed, first: {summary_errors[0]}"
        )
    dispositions = Counter(
        payload.summary.disposition.value if payload.summary is not None else payload.presence
        for payload in payloads
        if payload.summary is not None or payload.presence is not None
    )
    return TournamentOutcome(
        work.slug, work.season_year, GREEN, len(payloads), dict(sorted(dispositions.items()))
    )


# ---------------------------------------------------------------- summary


@dataclass(frozen=True, slots=True)
class WaveSummary:
    tournaments: int
    red_tournaments: int
    red: bool
    reason: str | None
    withdrawn_count: int
    moved_count: int
    warnings: tuple[str, ...]
    table: tuple[str, ...]
    duration_s: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "tournaments": self.tournaments,
            "red_tournaments": self.red_tournaments,
            "red": self.red,
            "reason": self.reason,
            "withdrawn_count": self.withdrawn_count,
            "moved_count": self.moved_count,
            "warnings": list(self.warnings),
            "table": list(self.table),
            "duration_s": self.duration_s,
        }


def summarize_wave(
    outcomes: Sequence[Mapping[str, Any]],
    failed: Sequence[str],
    *,
    plan_error: str | None = None,
    duration_s: float | None = None,
) -> WaveSummary:
    """Tournament -> state -> first error; the wave is red only when the plan
    failed or more than ``RED_SHARE`` of its tournaments are red.

    ``failed`` names mapped tasks that ended without an outcome.
    """

    table = [
        f"{item['slug']}:{item['season_year']} -> {item['state']} -> "
        f"{item.get('first_error') or '-'}"
        for item in outcomes
    ] + [f"{name} -> {RED} -> task ended without an outcome" for name in failed]
    total = len(outcomes) + len(failed)
    red_count = sum(item["state"] == RED for item in outcomes) + len(failed)
    withdrawn = sum(int(item.get("dispositions", {}).get(WITHDRAWN, 0)) for item in outcomes)
    moved = sum(int(item.get("dispositions", {}).get(MOVED, 0)) for item in outcomes)
    warnings = []
    if withdrawn > ESPN_WITHDRAWN_ALERT:
        warnings.append(
            f"{withdrawn} withdrawn matches in one wave (alert above {ESPN_WITHDRAWN_ALERT})"
        )
    if plan_error is not None:
        reason: str | None = f"plan_wave: {plan_error}"
    elif total and red_count / total > RED_SHARE:
        reason = f"{red_count} of {total} tournaments red (> {RED_SHARE:.0%})"
    else:
        reason = None
    return WaveSummary(
        tournaments=total,
        red_tournaments=red_count,
        red=reason is not None,
        reason=reason,
        withdrawn_count=withdrawn,
        moved_count=moved,
        warnings=tuple(warnings),
        table=tuple(table),
        duration_s=duration_s,
    )


__all__ = [
    "ALL_DAY_LIMIT",
    "BronzeMatch",
    "ESPN_WITHDRAWN_ALERT",
    "LIVE_POOL",
    "RED_SHARE",
    "SummaryFetchError",
    "TournamentOutcome",
    "TournamentWork",
    "WavePlan",
    "WavePlanError",
    "WaveSummary",
    "build_competition",
    "check_presence",
    "failed_outcome",
    "finish_requests",
    "live_rows",
    "plan_wave",
    "run_tournament",
    "split_day",
    "summarize_wave",
    "target_competitions",
]
