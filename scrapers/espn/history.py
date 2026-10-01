"""History lane of the ESPN contour: past seasons on what the live lane leaves (#1509).

The queue is the table ``iceberg.ops.espn_history_queue_v1``, one row per
(slug, season_year, season_type); Airflow's task map never holds it.  A
run walks it in four steps, all requests through the ``history`` lane of the
gate (``TransportGate(lane="history")``):

1. inventory — ``leagues/{slug}/seasons`` once per slug: an inventory row
   (``season_year = 0``) and one season row (``season_type = 0``) per past
   season.  Past = ``year < current_season_year`` of the registry; the
   seasons that started in the last ``HISTORY_YEARS`` years (ten seasons of a
   league, the editions of ten years of a national-team tournament).  The
   scope file ``configs/espn/history_scope.json`` narrows it: a non-empty
   ``allow`` is the exact list of (slug, year); an empty one means every
   ``Denominator.targets()`` slug.  The queue follows the file: a widened
   scope inventories what it lacks, rows outside the scope wait;
2. season — ``seasons/{year}``: window (label ``year`` from core) and types;
   the season row becomes one row per type (or ``empty`` without types);
3. type — ``types/{t}/events`` over every page (``limit=100``) -> event ids;
4. Summary by id: the schedule row comes from its ``header``
   (``schedule_row_from_header``), no scoreboard.

States: ``pending -> listed -> done | red | empty``.  The publication unit is a
batch of at most ``BATCH_MATCHES`` matches of one (slug, season, type), the
failure unit is the match: a Summary that fails is counted, the type row ends
``red`` and is tried once more by a later run (``MAX_ATTEMPTS``), then stays
red.  A match already in bronze with its Summary is never downloaded again, so
an interrupted run continues where it stopped.

Cache (plan decision 4): a season closed more than ``CLOSED_AFTER`` ago
reads its season, lists and Summaries with a terminal status from the raw
store (a pre-match body the live lane stored is downloaded again); an open
season reads everything fresh.  A repeat of a closed season makes 0 network
requests.  A match whose Summary is still not terminal is written (its row
keeps its place) but counts as failed: the type row is red, not done.

Duplicates: inside a season an event listed by two types is written once
(the first type); across tournaments one SELECT per batch finds the event ids
bronze already holds under another (slug, season): the new row gets
``duplicate_of`` of that owner (``editions`` rule: whoever was written
first) — unless the new one is a main competition and the stored one a
``_qual`` slug: then the stored rows become duplicates of the new batch
(main before ``_qual``).

Never ahead of the live lane: the gate gives ``history`` only what the live
share leaves and freezes it first on 403/429 — ``LaneClosed`` ends the run
cleanly; before every batch the live debt (``history_report.LIVE_DEBT_SQL``)
is checked and any debt ends the run until the next one; the stop file
(``history_stopped``) and the time budget are checked before every batch and
between matches.  Each run appends a run row (``slug = '(run)'``, state = why
it ended, matches written) that the morning report reads.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import re
import time
import threading
from typing import Any, Callable, Iterable, Sequence

from . import urls
from .bronze_rows import MatchPayload
from .bronze_schema import BRONZE_DATABASE, MATCH_TABLE
from .bronze_writer import TournamentBatch, write_tournament_batch
from .core_lists import CoreSeason, collect_refs, event_ids as core_event_ids, parse_season
from .denominator import Denominator
from .editions import EditionState
from .history_report import render_live_debt_sql
from .journal import JOURNAL_SCHEMA, _execute, _literal, flush_journal, journal_rows
from .models import Competition, Edition, SeasonType
from .parser_common import EspnParseError
from .raw_store import RawStoreError
from .schedule_parser import STATUS_MAP, schedule_row_from_header
from .summary_parser import parse_summary
from .transport_contracts import AllOriginsBlocked, LaneClosed
from .parallel import WORKERS, bounded_fetch, interruptible_sleep, protected
from .pace_store import PaceStore
from .wave import _STATUS_ERRORS, BronzeMatch, _fetch, _raw_ref, bronze_matches, build_competition

logger = logging.getLogger(__name__)

QUEUE_TABLE = f"{JOURNAL_SCHEMA}.espn_history_queue_v1"
QUEUE_COLUMNS = (
    ("slug", "varchar"),
    ("season_year", "integer"),
    ("season_type", "integer"),
    ("state", "varchar"),
    ("matches", "integer"),
    ("done", "integer"),
    ("failed", "integer"),
    ("attempts", "integer"),
    ("last_error", "varchar"),
    ("updated_at", "timestamp(6)"),
    ("run_id", "varchar"),
)
# ``season_year`` of a slug's inventory row; ``season_type`` of a season whose
# types are not read yet; ``slug`` of the row each run appends.
INVENTORY = 0
SEASON = 0
# ``season_type`` of the inventory row made for an empty ``allow`` (every past
# season of the slug); a non-empty ``allow`` inventories under ``SEASON``.
ALL_SEASONS = 1
RUN_ROW = "(run)"
PENDING, LISTED, DONE, RED, EMPTY = "pending", "listed", "done", "red", "empty"
# Why a run ended (state of its run row).
IDLE, BUDGET, LIVE_DEBT, LANE_CLOSED, STOPPED, ERROR = (
    "idle",
    "budget",
    "live_debt",
    "lane_closed",
    "stopped",
    "error",
)
HISTORY_YEARS = 10
LIST_LIMIT = 100
BATCH_MATCHES = 400
MAX_ATTEMPTS = 2
CLOSED_AFTER = timedelta(days=7)
DEFAULT_SCOPE_PATH = (
    Path(__file__).resolve().parents[2] / "configs" / "espn" / "history_scope.json"
)
_SEASON_REF_RE = re.compile(r"/seasons/(\d+)(?:[/?#]|$)")


# ------------------------------------------------------------------ scope


def load_scope(path: Path | None = None) -> tuple[tuple[str, int], ...]:
    """``allow`` of the scope file: ``(slug, year)`` pairs; empty = all targets."""

    data = json.loads(Path(path or DEFAULT_SCOPE_PATH).read_text(encoding="utf-8"))
    if not isinstance(data, dict) or set(data) != {"allow"} or not isinstance(
        data["allow"], list
    ):
        raise ValueError("history scope must be {\"allow\": [[slug, year], ...]}")
    pairs = []
    for item in data["allow"]:
        if (
            not isinstance(item, list)
            or len(item) != 2
            or not isinstance(item[0], str)
            or type(item[1]) is not int
        ):
            raise ValueError(f"history scope item {item!r} is not [slug, year]")
        pairs.append((item[0], item[1]))
    if len(set(pairs)) != len(pairs):
        raise ValueError("history scope repeats a season")
    return tuple(pairs)


def history_years(listed: Iterable[int], current: int) -> tuple[int, ...]:
    """Past seasons that started in the last ``HISTORY_YEARS`` years, newest first."""

    return tuple(
        sorted(
            {year for year in listed if current - HISTORY_YEARS <= year < current},
            reverse=True,
        )
    )


def default_stop_file() -> Path:
    return Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow")) / "state" / "espn" / "history.off"


def history_stopped(path: Path | None) -> bool:
    """The stop file: while it exists the history lane does nothing."""

    return path is not None and Path(path).exists()


# ------------------------------------------------------------------ queue


@dataclass(frozen=True, slots=True)
class QueueRow:
    slug: str
    season_year: int
    season_type: int
    state: str
    matches: int = 0
    done: int = 0
    failed: int = 0
    attempts: int = 0
    last_error: str | None = None
    updated_at: datetime | None = None
    run_id: str | None = None

    @property
    def key(self) -> tuple[str, int, int]:
        return (self.slug, self.season_year, self.season_type)


def _query(conn, sql: str) -> list:
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        cursor.close()


def ensure_queue_table(conn) -> None:
    _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {JOURNAL_SCHEMA}")
    columns = ", ".join(f"{name} {sql_type}" for name, sql_type in QUEUE_COLUMNS)
    _execute(conn, f"CREATE TABLE IF NOT EXISTS {QUEUE_TABLE} ({columns})")


def load_queue(conn) -> dict[tuple[str, int, int], QueueRow]:
    names = ", ".join(name for name, _ in QUEUE_COLUMNS)
    rows = _query(conn, f"SELECT {names} FROM {QUEUE_TABLE} WHERE slug <> '{RUN_ROW}'")
    queue = {}
    for values in rows:
        row = QueueRow(*values)
        row = replace(
            row,
            season_year=int(row.season_year),
            season_type=int(row.season_type),
            matches=int(row.matches or 0),
            done=int(row.done or 0),
            failed=int(row.failed or 0),
            attempts=int(row.attempts or 0),
        )
        queue[row.key] = row
    return queue


def _values(rows: Sequence[QueueRow]) -> str:
    return ", ".join(
        "("
        + ", ".join(
            _literal(getattr(row, name), sql_type) for name, sql_type in QUEUE_COLUMNS
        )
        + ")"
        for row in rows
    )


def _where(keys: Iterable[tuple[str, int, int]]) -> str:
    return " OR ".join(
        f"(slug = {_literal(slug, 'varchar')} AND season_year = {int(year)} "
        f"AND season_type = {int(kind)})"
        for slug, year, kind in keys
    )


def save_rows(conn, rows: Sequence[QueueRow], drop: Iterable[tuple[str, int, int]] = ()) -> None:
    """Replace the rows by key (DELETE + INSERT); ``drop`` keys are deleted too."""

    keys = list(dict.fromkeys([*drop, *(row.key for row in rows)]))
    if keys:
        _execute(conn, f"DELETE FROM {QUEUE_TABLE} WHERE {_where(keys)}")
    if rows:
        names = ", ".join(name for name, _ in QUEUE_COLUMNS)
        _execute(conn, f"INSERT INTO {QUEUE_TABLE} ({names}) VALUES {_values(rows)}")


def append_run_row(conn, row: QueueRow) -> None:
    names = ", ".join(name for name, _ in QUEUE_COLUMNS)
    _execute(conn, f"INSERT INTO {QUEUE_TABLE} ({names}) VALUES {_values([row])}")


# ------------------------------------------------------------------ bronze

_MATCH = f"iceberg.{BRONZE_DATABASE}.{MATCH_TABLE}"


def live_debt(trino, live_targets: Iterable[str], now: datetime) -> int:
    rows = trino.execute_query(render_live_debt_sql(live_targets, now))
    return int(rows[0][0]) if rows else 0


def _is_qual(slug: str) -> bool:
    return slug.endswith("_qual")


def duplicate_owners(
    trino, slug: str, year: int, event_ids: Sequence[int]
) -> tuple[dict[int, str], dict[tuple[str, int], list[int]]]:
    """Owners of the batch ids stored under another (slug, season).

    Returns ``event_id -> "<slug>:<year>"`` for the ids this batch writes as
    duplicates, and ``(slug, year) -> ids`` of stored ``_qual`` rows that the
    batch (a main competition) takes over: they become its duplicates.
    """

    ids = sorted({int(event_id) for event_id in event_ids})
    if not ids:
        return {}, {}
    rows = trino.execute_query(
        f"SELECT event_id, competition_slug, season_year FROM {_MATCH} "
        "WHERE duplicate_of IS NULL AND event_id IN ("
        + ", ".join(str(event_id) for event_id in ids)
        + ") AND NOT (competition_slug = ? AND season_year = ?)",
        (slug, int(year)),
    )
    owners: dict[int, str] = {}
    retake: dict[tuple[str, int], list[int]] = {}
    for event_id, other_slug, other_year in rows:
        if not _is_qual(slug) and _is_qual(other_slug):
            retake.setdefault((other_slug, int(other_year)), []).append(int(event_id))
        else:
            owners.setdefault(int(event_id), f"{other_slug}:{int(other_year)}")
    for event_id in owners:
        for ids_of in retake.values():
            if event_id in ids_of:
                ids_of.remove(event_id)
    return owners, {key: value for key, value in retake.items() if value}


def mark_duplicates(trino, owner: str, slug: str, year: int, event_ids: Sequence[int]) -> None:
    """``duplicate_of = owner`` on stored match rows (children carry no such column)."""

    trino.execute_query(
        f"UPDATE {_MATCH} SET duplicate_of = ? WHERE competition_slug = ? AND season_year = ? "
        "AND event_id IN (" + ", ".join(str(int(event_id)) for event_id in sorted(event_ids)) + ")",
        (owner, slug, int(year)),
    )


def _collected(known: BronzeMatch | None) -> bool:
    return known is not None and (known.summary_captured or known.terminal_nonplayed)


# ------------------------------------------------------------------ runner


@dataclass(frozen=True, slots=True)
class HistoryRun:
    reason: str
    matches: int
    failed: int
    batches: int
    detail: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "reason": self.reason,
            "matches": self.matches,
            "failed": self.failed,
            "batches": self.batches,
            "detail": self.detail,
        }


class _Stop(Exception):
    def __init__(self, reason: str, detail: str | None = None) -> None:
        super().__init__(detail or reason)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True, slots=True)
class _Season:
    core: CoreSeason
    types: tuple[SeasonType, ...]
    closed: bool


def _header_status(data: Any) -> str | None:
    try:
        return data["header"]["competitions"][0]["status"]["type"]["name"]
    except (KeyError, IndexError, TypeError):
        return None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class _Runner:
    def __init__(
        self,
        *,
        client,
        trino,
        conn,
        denominator: Denominator,
        scope: Sequence[tuple[str, int]],
        run_id: str,
        task_id: str,
        deadline: datetime,
        stop_file: Path | None,
        now_fn: Callable[[], datetime],
        worker_client_factory=None,
    ) -> None:
        self.client = client
        self.trino = trino
        self.conn = conn
        self.denominator = denominator
        self.scope = tuple(scope)
        if hasattr(client, "attempt_journal"):
            client.run_id, client.task_id = run_id, task_id
        self.run_id = run_id
        self.task_id = task_id
        self.deadline = deadline
        self.stop_file = stop_file
        self.now_fn = now_fn
        self.live_targets = sorted(denominator.live_targets())
        self.matches = self.failed = self.batches = 0
        self._journalled = 0
        self.worker_client_factory = worker_client_factory
        self._workers = []
        self._worker_slots = {}
        self._worker_journalled = {}
        self._cancel = threading.Event()
        self._pace_store = (PaceStore(client.gate.state_path.with_name('pace.sqlite3'))
                            if hasattr(client, 'gate') else None)
        self._seasons: dict[tuple[str, int], _Season] = {}
        # Event ids of a season handled by an earlier type in this run.
        self._seen: dict[tuple[str, int], set[int]] = {}
        self._touched: set[tuple[str, int, int]] = set()
        if hasattr(client, 'before_attempt'):
            client.before_attempt = self._worker_check
            client.sleep_fn = lambda seconds: interruptible_sleep(seconds, self._worker_check)
            client.gate.sleep_fn = client.sleep_fn

    # -------------------------------------------------------------- checks

    def _between(self) -> None:
        if history_stopped(self.stop_file):
            raise _Stop(STOPPED, f"stop file {self.stop_file}")
        if self.now_fn() >= self.deadline:
            raise _Stop(BUDGET)

    def _worker_check(self):
        self._between()
        if self._cancel.is_set():
            raise _Stop(STOPPED, 'batch interrupted')
        if hasattr(self.client, 'gate'):
            snapshot = self.client.gate.snapshot()
            if protected(snapshot, self.now_fn().timestamp()):
                raise LaneClosed('history protection active')

    def _worker_limit(self):
        if not hasattr(self.client, 'gate'):
            return 1
        return WORKERS[self.client.gate.snapshot()['step']]

    def _new_worker(self, slot):
        if slot in self._worker_slots:
            return self._worker_slots[slot]
        if self.worker_client_factory is not None:
            client = self.worker_client_factory(slot, self._worker_check)
        elif hasattr(self.client, 'worker'):
            client = self.client.worker(
                before_attempt=self._worker_check,
                sleep_fn=lambda seconds: interruptible_sleep(seconds, self._worker_check),
            )
        else:
            # Recorded-fixture clients have no parallel transport state.
            return self.client
        self._workers.append(client)
        self._worker_slots[slot] = client
        return client

    def _before_batch(self) -> None:
        self._between()
        debt = live_debt(self.trino, self.live_targets, self.now_fn())
        if debt:
            raise _Stop(LIVE_DEBT, f"{debt} live match(es) 14-72 h after kickoff unpublished")

    # -------------------------------------------------------------- helpers

    def _row(self, row: QueueRow, **changes) -> QueueRow:
        return replace(row, updated_at=self.now_fn(), run_id=self.run_id, **changes)

    def _save(self, queue, rows: Sequence[QueueRow], drop=()) -> None:
        save_rows(self.conn, rows, drop)
        for key in drop:
            queue.pop(key, None)
        for row in rows:
            queue[row.key] = row

    def flush_journal(self) -> None:
        if hasattr(self.client, "flush_attempts"):
            self.client.flush_attempts(self.conn)
        entries = self.client.ledger
        new = entries[self._journalled :]
        if new:
            flush_journal(
                self.conn, journal_rows(new, run_id=self.run_id, task_id=self.task_id)
            )
        self._journalled = len(entries)
        for worker in self._workers:
            entries = worker.ledger
            offset = self._worker_journalled.get(id(worker), 0)
            if entries[offset:]:
                flush_journal(self.conn, journal_rows(entries[offset:], run_id=self.run_id,
                                                       task_id=self.task_id))
            self._worker_journalled[id(worker)] = len(entries)

    def _season(self, slug: str, year: int) -> _Season:
        key = (slug, year)
        if key not in self._seasons:
            request = urls.season(slug, year)
            result = _fetch(self.client, request, force_refresh=False)
            core, types = parse_season(result.json_data)
            today = self.now_fn().astimezone(timezone.utc).date()
            closed = core.end < today - CLOSED_AFTER
            if not closed and result.cache_hit:
                core, types = parse_season(
                    _fetch(self.client, request, force_refresh=True).json_data
                )
            if core.year != year:
                raise EspnParseError(f"{slug} season {year} answers year {core.year}")
            self._seasons[key] = _Season(core, types, closed)
        return self._seasons[key]

    def _type_ids(self, slug: str, year: int, type_id: int, closed: bool) -> tuple[int, ...]:
        return core_event_ids(
            collect_refs(
                lambda page: _fetch(
                    self.client,
                    urls.type_events(slug, year, type_id, page, limit=LIST_LIMIT),
                    force_refresh=not closed,
                ).body,
                f"{slug} {year} type {type_id} events",
            )
        )

    def _summary(self, slug: str, event_id: int, closed: bool, client=None):
        """A closed season replays a stored Summary with a terminal status (a
        pre-match body of the live lane is read again); an open season always
        downloads it."""

        client = client or self.client
        request = urls.summary(slug, event_id)
        if not closed:
            return _fetch(client, request, force_refresh=True)
        try:
            result = client.replay_json(request.url, request.endpoint, request.params)
        except RawStoreError:
            return _fetch(client, request, force_refresh=True)
        status = STATUS_MAP.get(_header_status(result.json_data) or "")
        if status is not None and status.terminal:
            return result
        return _fetch(client, request, force_refresh=True)

    def _payload(
        self, event_id: int, competition: Competition, edition: Edition, closed: bool, client=None
    ) -> MatchPayload:
        result = self._summary(competition.slug, event_id, closed, client)
        schedule = schedule_row_from_header(
            result.body, competition=competition, edition=edition
        )
        if schedule.event_id != event_id:
            raise EspnParseError(f"Summary of {event_id} is event {schedule.event_id}")
        raw = _raw_ref(result)
        parsed = (
            parse_summary(result.body, competition=competition, edition=edition, event=schedule)
            if schedule.played_final
            else None
        )
        return MatchPayload(schedule, parsed, raw, status_checked_at=raw.fetched_at)

    def _write(self, slug: str, year: int, payloads: list[MatchPayload]) -> None:
        owners, retake = duplicate_owners(
            self.trino, slug, year, [payload.schedule.event_id for payload in payloads]
        )
        # Before the write: a failed write leaves the _qual rows pointing at an
        # owner the next run writes (its matches stay uncollected until then).
        for (other_slug, other_year), ids in sorted(retake.items()):
            mark_duplicates(self.trino, f"{slug}:{year}", other_slug, other_year, ids)
        payloads = [
            replace(payload, schedule=replace(payload.schedule, duplicate_of=owners[eid]))
            if (eid := payload.schedule.event_id) in owners
            else payload
            for payload in payloads
        ]
        # Bronze rows never point at a raw body that is not written yet.
        self.client.flush()
        for worker in self._workers:
            worker.flush()
        write_started = time.monotonic()
        step = self.client.gate.snapshot()['step'] if hasattr(self.client, 'gate') else 0
        succeeded = False
        try:
            write_tournament_batch(TournamentBatch(slug, year, payloads), trino=self.trino)
            succeeded = True
        finally:
            if self._pace_store is not None:
                self._pace_store.record(
                    'write', self.now_fn().timestamp(), run_id=self.run_id,
                    slug=slug, year=year, step=step, matches=len(payloads),
                    write_seconds=time.monotonic() - write_started,
                    batch_seconds=time.monotonic() - self._batch_started,
                    success=succeeded, isolated=False,
                )
        self.flush_journal()
        self.matches += len(payloads)
        self.batches += 1

    # -------------------------------------------------------------- steps

    def _slugs(self) -> list[str]:
        if self.scope:
            return sorted({slug for slug, _ in self.scope})
        return sorted(self.denominator.targets())

    def _in_scope(self, slug: str, year: int) -> bool:
        if self.scope:
            return (slug, year) in self.scope
        return self.denominator.is_target(slug)

    def _inventory(self, queue) -> None:
        """List the seasons of every scope slug the queue does not cover yet.

        A non-empty ``allow`` needs its (slug, year) pairs in the queue; an
        empty one needs an ``ALL_SEASONS`` inventory row per target slug, so
        widening the scope file lists what is new and nothing twice.
        """

        queued = {key[:2] for key in queue}
        for slug in self._slugs():
            if self.scope:
                wanted = {year for other, year in self.scope if other == slug}
                key = (slug, INVENTORY, SEASON)
                if all((slug, year) in queued for year in wanted):
                    continue
            else:
                key = (slug, INVENTORY, ALL_SEASONS)
                if key in queue and queue[key].state == DONE:
                    continue
            row = queue.get(key)
            if key in self._touched or (
                row is not None and row.state == RED and row.attempts >= MAX_ATTEMPTS
            ):
                continue
            self._touched.add(key)
            self._between()
            attempts = (row.attempts if row is not None else 0) + 1
            registry = self.denominator.row(slug)
            base = row or QueueRow(*key, PENDING)
            if registry is None or not registry.in_target or registry.espn_id is None:
                raise ValueError(f"history scope names {slug}, not an ESPN target")
            current = registry.current_season_year
            try:
                if current is None:
                    raise EspnParseError(f"{slug} has no current season in the registry")
                refs = collect_refs(
                    lambda page: _fetch(
                        self.client,
                        urls.league_seasons(slug, page, limit=LIST_LIMIT),
                        force_refresh=True,
                    ).body,
                    f"{slug} seasons",
                )
            except _STATUS_ERRORS as exc:
                if isinstance(exc, AllOriginsBlocked):
                    raise
                error = f"{type(exc).__name__}: {exc}"
                logger.warning("ESPN history inventory of %s: %s", slug, error)
                self._save(queue, [self._row(base, state=RED, attempts=attempts, last_error=error)])
                continue
            listed = set()
            for ref in refs:
                match = _SEASON_REF_RE.search(ref)
                if match is None:
                    raise EspnParseError(f"{slug} seasons: not a season $ref {ref!r}")
                listed.add(int(match.group(1)))
            rows = []
            if self.scope:
                years = tuple(
                    sorted((year for year in wanted if year in listed and year < current), reverse=True)
                )
                # An allowed season core does not list (or not a past one) is a
                # red season row: visible, never tried again.
                rows += [
                    self._row(
                        QueueRow(slug, year, SEASON, RED),
                        attempts=MAX_ATTEMPTS,
                        last_error=f"core lists no past season {year} (current {current})",
                    )
                    for year in sorted(wanted - set(years))
                    if (slug, year) not in queued
                ]
            else:
                years = history_years(listed, current)
            rows.append(self._row(base, state=DONE, matches=len(years), attempts=attempts,
                                  last_error=None))
            rows += [
                self._row(QueueRow(slug, year, SEASON, PENDING))
                for year in years
                if (slug, year) not in queued
            ]
            self._save(queue, rows)
            queued.update(row.key[:2] for row in rows)

    def _order(self, row: QueueRow) -> tuple:
        priority = self.denominator.queue_priority(row.slug)
        return (
            priority if priority else 99,
            _is_qual(row.slug),
            -row.season_year,
            row.slug,
            row.season_type,
        )

    def _next(self, queue) -> QueueRow | None:
        work = [
            row
            for row in queue.values()
            if row.season_year != INVENTORY
            and row.key not in self._touched
            and self._in_scope(row.slug, row.season_year)
            and (
                row.state in (PENDING, LISTED)
                or (row.state == RED and row.attempts < MAX_ATTEMPTS)
            )
        ]
        return min(work, key=self._order) if work else None

    def _expand(self, row: QueueRow, queue) -> None:
        self._before_batch()
        try:
            season = self._season(row.slug, row.season_year)
        except _STATUS_ERRORS as exc:
            if isinstance(exc, AllOriginsBlocked):
                raise
            self._save(
                queue,
                [self._row(row, state=RED, attempts=row.attempts + 1,
                           last_error=f"season: {type(exc).__name__}: {exc}")],
            )
            return
        if not season.types:
            self._save(queue, [self._row(row, state=EMPTY, attempts=row.attempts + 1)])
            return
        self._save(
            queue,
            [
                self._row(QueueRow(row.slug, row.season_year, item.id, PENDING))
                for item in season.types
            ],
            drop=[row.key],
        )

    def _run_type(self, row: QueueRow, queue) -> None:
        slug, year = row.slug, row.season_year
        attempts = row.attempts + 1

        def red(error: str) -> None:
            self._save(queue, [self._row(row, state=RED, attempts=attempts, last_error=error)])

        self._before_batch()
        try:
            season = self._season(slug, year)
            listed = self._type_ids(slug, year, row.season_type, season.closed)
        except _STATUS_ERRORS as exc:
            if isinstance(exc, AllOriginsBlocked):
                raise
            red(f"list: {type(exc).__name__}: {exc}")
            return
        seen = self._seen.setdefault((slug, year), set())
        ids = [event_id for event_id in listed if event_id not in seen]
        seen.update(ids)
        if not ids:
            self._save(queue, [self._row(row, state=EMPTY, matches=0, done=0,
                                         failed=0, attempts=attempts, last_error=None)])
            return
        registry = self.denominator.row(slug)
        competition, edition = build_competition(
            registry,
            EditionState(slug, year, season.core.display_name, season.core.start, season.core.end),
        )
        stored = bronze_matches(self.trino, slug, year, ids)
        todo = [event_id for event_id in ids if not _collected(stored.get(event_id))]
        done = len(ids) - len(todo)
        failed = 0
        first_error: str | None = None

        def progress(**changes) -> QueueRow:
            return self._row(
                row, matches=len(ids), done=done, failed=failed, last_error=first_error, **changes
            )

        self._save(queue, [progress(state=LISTED)])
        for start in range(0, len(todo), BATCH_MATCHES):
            if start:
                self._before_batch()
            payloads: list[MatchPayload] = []
            interrupted: BaseException | None = None
            self._batch_started = time.monotonic()
            self._cancel.clear()
            fetched = bounded_fetch(
                todo[start : start + BATCH_MATCHES],
                lambda client, event_id: self._payload(event_id, competition, edition,
                                                       season.closed, client),
                limit=self._worker_limit, check=self._worker_check,
                client_factory=self._new_worker,
            )
            try:
                for event_id, payload, error in fetched:
                    if error is not None:
                        if isinstance(error, (_Stop, LaneClosed, AllOriginsBlocked)):
                            interrupted = interrupted or error
                            self._cancel.set()
                        elif isinstance(error, _STATUS_ERRORS):
                            failed += 1
                            self.failed += 1
                            first_error = first_error or f"{event_id}: {type(error).__name__}: {error}"
                        else:
                            interrupted = interrupted or error
                            self._cancel.set()
                        continue
                    known = stored.get(event_id)
                    payloads.append(known.carried(payload) if known is not None else payload)
                    if not payload.schedule.terminal:
                        failed += 1
                        self.failed += 1
                        first_error = first_error or (
                            f"{event_id}: status {payload.schedule.status} is not terminal"
                        )
            except BaseException as exc:
                interrupted = interrupted or exc
            finally:
                fetched.close()
            if payloads:
                self._write(slug, year, payloads)
                done += sum(payload.schedule.terminal for payload in payloads)
            self._save(queue, [progress(state=LISTED)])
            if interrupted is not None:
                raise interrupted
        self._save(queue, [progress(state=DONE if failed == 0 else RED, attempts=attempts)])

    def run(self) -> HistoryRun:
        reason, detail = IDLE, None
        try:
            self._before_batch()
            queue = load_queue(self.conn)
            self._inventory(queue)
            while (row := self._next(queue)) is not None:
                self._touched.add(row.key)
                if row.season_type == SEASON:
                    self._expand(row, queue)
                else:
                    self._run_type(row, queue)
        except _Stop as stop:
            reason, detail = stop.reason, stop.detail
        except (LaneClosed, AllOriginsBlocked) as exc:
            reason, detail = LANE_CLOSED, f"{type(exc).__name__}: {exc}"
        except BaseException as exc:
            reason, detail = ERROR, f"{type(exc).__name__}: {exc}"
            raise
        finally:
            try:
                self._finish(reason, detail)
            finally:
                for worker in self._workers:
                    worker.close()
                    if hasattr(worker, 'session'):
                        worker.session.close()
        logger.info(
            "ESPN history run %s: %s, %d match(es) in %d batch(es), %d failed%s",
            self.run_id, reason, self.matches, self.batches, self.failed,
            f" ({detail})" if detail else "",
        )
        return HistoryRun(reason, self.matches, self.failed, self.batches, detail)

    def _finish(self, reason: str, detail: str | None) -> None:
        try:
            self.client.flush()
            self.flush_journal()
            append_run_row(
                self.conn,
                QueueRow(
                    RUN_ROW, INVENTORY, SEASON, reason,
                    matches=self.matches, done=self.batches, failed=self.failed,
                    last_error=detail, updated_at=self.now_fn(), run_id=self.run_id,
                ),
            )
        except Exception:
            if reason != ERROR:
                raise
            logger.exception("ESPN history run row of %s not written", self.run_id)


def run_history(
    *,
    client,
    trino,
    conn,
    denominator: Denominator,
    scope: Sequence[tuple[str, int]],
    run_id: str,
    deadline: datetime,
    stop_file: Path | None = None,
    task_id: str = "run_history",
    now_fn: Callable[[], datetime] = _utcnow,
    worker_client_factory=None,
) -> HistoryRun:
    """One run of the history lane until the queue, the budget or the gate ends it.

    ``client`` is an ``EspnHttpClient`` on ``TransportGate(lane="history")``,
    ``trino`` an ``EspnTrinoTableManager`` and ``conn`` its connection (queue,
    request journal).  ``LaneClosed``, the live debt, the stop file and the
    budget end the run cleanly; any other error is raised after the run row.
    """

    if deadline.tzinfo is None:
        raise ValueError("deadline must be timezone-aware")
    return _Runner(
        client=client,
        trino=trino,
        conn=conn,
        denominator=denominator,
        scope=scope,
        run_id=run_id,
        task_id=task_id,
        deadline=deadline,
        stop_file=stop_file,
        now_fn=now_fn,
        worker_client_factory=worker_client_factory,
    ).run()


__all__ = [
    "BATCH_MATCHES",
    "CLOSED_AFTER",
    "HISTORY_YEARS",
    "HistoryRun",
    "MAX_ATTEMPTS",
    "QUEUE_COLUMNS",
    "QUEUE_TABLE",
    "QueueRow",
    "append_run_row",
    "default_stop_file",
    "duplicate_owners",
    "mark_duplicates",
    "ensure_queue_table",
    "history_stopped",
    "history_years",
    "live_debt",
    "load_queue",
    "load_scope",
    "run_history",
    "save_rows",
]
