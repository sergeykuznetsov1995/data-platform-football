"""Independent schedule evidence for a closed UTC deadline day (#2340).

Read only the scheduled planner's recorded receipts and their immutable raw
objects. A day can have two Eastern scoreboard dates. The midnight core check
covers those dates and the following date; the latter can locate a core ID
without making it due on the measured day.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
import logging
import os

from . import urls
from .core_lists import collect_refs, event_ids, parse_ref_page
from .denominator import load_denominator
from .journal import JOURNAL_TABLE
from .parser_common import (
    ESPN_DAY_TZ,
    EspnParseError,
    decode_object,
    espn_day,
    native_id,
    required_list,
    required_mapping,
    required_string,
    utc_datetime,
)
from .raw_store import RAW_STORE_ENV, EspnRawStore
from .schedule_parser import STATUS_MAP, _ALL_UID_RE
from .transport_contracts import canonicalize_target
from .wave_log import WAVE_LOG_TABLE, WAVE_ROW

logger = logging.getLogger(__name__)
CORE_SCOPE_KEY = "freshness_core_scope_v1"


@dataclass(frozen=True)
class Fixture:
    slug: str
    event_id: int
    kickoff: datetime
    status: str
    observed: datetime


@dataclass
class SourceDay:
    fixtures: dict[tuple[str, int], Fixture] = field(default_factory=dict)
    gaps: set[str] = field(default_factory=set)
    # Core has an ID absent from every saved scoreboard in its three-day window.
    # The Bronze query may still locate it (e.g. a previously withdrawn match).
    unlocated: set[tuple[str, int]] = field(default_factory=set)


def _fingerprint(request):
    return canonicalize_target(request.url, request.params).url_fingerprint


def _as_utc(stamp):
    return (
        stamp.replace(tzinfo=timezone.utc)
        if stamp.tzinfo is None
        else stamp.astimezone(timezone.utc)
    )


def _board(body, slug, id_to_slug, observed):
    doc = decode_object(body, "scoreboard")
    leagues = required_list(doc.get("leagues"), "scoreboard.leagues")
    if len(leagues) != 1:
        raise EspnParseError("ambiguous scoreboard league")
    league = required_mapping(leagues[0], "scoreboard.league")
    if slug is None and (
        league.get("id") or league.get("slug") not in (None, "", "all")
    ):
        raise EspnParseError("league-specific response for all leagues")
    if slug is not None and (
        id_to_slug.get(native_id(league.get("id"), "league.id")) != slug
        or league.get("slug", slug) != slug
    ):
        raise EspnParseError("wrong scoreboard league")
    events = required_list(doc.get("events"), "scoreboard.events")
    fixtures = []
    for item in events:
        event = required_mapping(item, "scoreboard.event")
        event_id = native_id(event.get("id"), "event.id")
        target = slug
        if target is None or "uid" in event:
            uid = _ALL_UID_RE.fullmatch(str(event.get("uid", "")))
            if not uid or int(uid[2]) != event_id:
                raise EspnParseError("ambiguous event identity")
            target = id_to_slug.get(int(uid[1]))
            if slug is not None and target != slug:
                raise EspnParseError("event belongs to another league")
        if target is None:
            continue
        kickoff = utc_datetime(event.get("date"), "event.date")
        status = required_mapping(event.get("status"), "event.status")
        name = required_mapping(status.get("type"), "status.type").get("name")
        name = required_string(name, "status.type.name")
        if name not in STATUS_MAP:
            raise EspnParseError(f"unknown event status {name!r}")
        fixtures.append(Fixture(target, event_id, kickoff, name, observed))
    return fixtures, len(events) < urls.LIST_LIMIT


@lru_cache(maxsize=1)
def _store(uri):
    return EspnRawStore.from_uri(uri)


@lru_cache(maxsize=64)
def _saved_board(store, uri, digest, slug, targets, at):
    # Pace reads the same closed day each minute. Cache parsed fixtures only;
    # a changed receipt or target registry selects another entry.
    return _board(store.load_exact(uri, digest), slug, dict(targets), at)


def _out_of_season(run, day):
    try:
        from airflow.models.xcom import XCom

        scope = XCom.get_one(
            run_id=run,
            dag_id="dag_espn_current",
            task_id="plan_wave",
            key=CORE_SCOPE_KEY,
        )
    except Exception as exc:
        logger.warning("ESPN nightly scope unavailable (%s)", type(exc).__name__)
        return set()
    if not isinstance(scope, dict) or scope.get("day") != day.isoformat():
        return set()
    slugs = scope.get("out_of_season")
    return (
        set(slugs)
        if isinstance(slugs, list) and all(isinstance(s, str) for s in slugs)
        else set()
    )


@lru_cache(maxsize=512)
def _core_ids(store, slug, receipts):
    # Cache only verified IDs, never failures or the raw response bodies.
    def page(number):
        if number > len(receipts) or not receipts[number - 1]:
            raise EspnParseError("missing core page")
        at, uri, digest = receipts[number - 1]
        if number > 1 and at < receipts[number - 2][0]:
            raise EspnParseError("stale core page")
        body = store.load_exact(uri, digest)
        if number == 1:
            first_page = parse_ref_page(decode_object(body, "core"), "core")
            if first_page.page_count and first_page.page_index != 1:
                raise EspnParseError("missing first core page")
        return body

    return event_ids(collect_refs(page, f"{slug} core"))


def read_source_day(trino, day: date, targets, *, store=None, registry=None):
    registry = registry or load_denominator()
    slugs = set(targets)
    ids = {
        row.espn_id: slug
        for slug in slugs
        if (row := registry.row(slug)) and row.espn_id
    }
    result = SourceDay(gaps=slugs - set(ids.values()))
    day_start = datetime.combine(day, time.min, timezone.utc)
    close = day_start + timedelta(days=1)
    kickoff_end = close - timedelta(hours=26)
    first = espn_day(day_start - timedelta(hours=26))
    last = espn_day(kickoff_end - timedelta(microseconds=1))
    core_first = day - timedelta(days=2)
    run = f"scheduled__{(day_start - timedelta(hours=6)).isoformat()}"
    waves = trino.execute_query(
        f"SELECT slug, state FROM {WAVE_LOG_TABLE} WHERE run_id='{run}' "
        f"AND wave_started_at >= TIMESTAMP '{day} 00:00:00' "
        f"AND wave_started_at < TIMESTAMP '{close.date()} 00:00:00'"
    )
    summary = [state for slug, state in waves if slug == WAVE_ROW]
    red = {slug for slug, state in waves if slug != WAVE_ROW and state == "red"}
    if (
        len(summary) != 1
        or summary[0] not in {"green", "red"}
        or (summary[0] == "red" and not red)
        or red - set(registry.targets())
    ):
        result.gaps.update(slugs)
    result.gaps.update(red & slugs)

    # Include failure receipts so a failed final refresh cannot certify an old body.
    rows = trino.execute_query(
        f"SELECT url_fingerprint, requested_at, run_id, disposition, raw_uri, content_hash "
        f"FROM {JOURNAL_TABLE} WHERE request_date BETWEEN DATE '{core_first}' AND DATE '{day}' "
        "AND lane='live' AND task_id='plan_wave' AND endpoint='scoreboard' "
        "AND starts_with(run_id, 'scheduled__') AND attempts > 0 "
        "ORDER BY requested_at"
    )
    captures = {}
    nightly = {}
    for fp, stamp, run_id, disposition, uri, digest in rows:
        if stamp is None:
            continue
        at = _as_utc(stamp)
        if at >= close:
            continue
        capture = (
            (at, uri, digest) if disposition == "success" and uri and digest else None
        )
        captures[fp] = capture
        if run_id == run:
            nightly[fp] = capture

    store = store if store is not None else _store(os.environ[RAW_STORE_ENV])
    target_ids = tuple(sorted(ids.items()))
    for offset in range((day - core_first).days + 1):
        board_day = core_first + timedelta(days=offset)
        required = first <= board_day <= last
        end = datetime.combine(board_day + timedelta(days=1), time.min, ESPN_DAY_TZ)
        mature = min(end.astimezone(timezone.utc), kickoff_end) + timedelta(hours=2)
        covered = set()
        for slug in [None, *sorted(slugs)]:
            fp = _fingerprint(urls.league_scoreboard_day(slug or "all", board_day))
            if fp not in captures:
                continue
            try:
                capture = captures[fp]
                if capture is None:
                    raise ValueError("failed scoreboard capture")
                at, uri, digest = capture
                fixtures, complete = _saved_board(
                    store, uri, digest, slug, target_ids, at
                )
            except (ValueError, RuntimeError, OSError):
                if slug is not None and required:
                    result.gaps.add(slug)
                continue
            for fixture in fixtures:
                key = (fixture.slug, fixture.event_id)
                old = result.fixtures.get(key)
                if old is None or fixture.observed > old.observed:
                    result.fixtures[key] = fixture
                elif old.observed == fixture.observed and old != fixture:
                    result.gaps.add(fixture.slug)
            if complete and at >= mature:
                covered.update(slugs if slug is None else (slug,))
        if required:
            result.gaps.update(slugs - covered)

    # Empty Bronze or an empty scoreboard cannot prove that a live league
    # was outside the nightly planner's calendar. Old runs have no exemption.
    live = registry.live_targets() & slugs
    exempt = None
    for slug in sorted(slugs):
        fp = _fingerprint(urls.events_window(slug, core_first, day))
        if fp not in nightly:
            if slug in live:
                if exempt is None:
                    exempt = _out_of_season(run, day)
                if slug not in exempt or any(
                    f.slug == slug for f in result.fixtures.values()
                ):
                    result.gaps.add(slug)
            continue
        receipts = []
        while fp in nightly:
            receipts.append(nightly[fp])
            fp = _fingerprint(
                urls.events_window(slug, core_first, day, len(receipts) + 1)
            )
        try:
            listed = _core_ids(store, slug, tuple(receipts))
        except (ValueError, RuntimeError, OSError):
            result.gaps.add(slug)
            continue
        result.unlocated.update(
            (slug, eid) for eid in listed if (slug, eid) not in result.fixtures
        )
    return result
