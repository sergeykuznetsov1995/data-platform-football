#!/usr/bin/env python3
"""Refresh ``bronze.sofascore_schedule`` from season pages of the campaign.

Refresh lane (#1218, lane F): the source has no by-date event list (a live
probe on 2026-08-25 answered 404 for every by-date shape), so the lane walks
the current season of every ready campaign tournament instead, through the
metered discovery client.  A visit is NOT one request: a tail visit chases up
to ``--chase-pages`` pages while what it reads is newer than Bronze, a seeded
chain walks up to ``--seed-pages`` plus a page of overlap and up to three steps
back, and ``stale``/``seed`` also take a page of fixtures.

Every run works three classes of target, in this order of priority:

* ``due`` — seasons Bronze says are playing right now (a match inside the
  schedule window): their tail pages on every run, up to ``--max-due`` of them
  (beyond that the class rotates on its cursor like the others).  This is the
  fast path.
* ``stale`` — seasons already in Bronze but outside that window: their tail
  page too, but only a cursor slice per run.  ``schedule_last`` serves finished
  matches only, so a season whose last known match has aged out of the window
  would otherwise never be asked again — and a weekly league leaves the window
  four days before its next match.  The cursor slice is what bounds the lag.
* ``seed`` — seasons Bronze has never seen: their whole page chain, a cursor
  slice per run, resumed across runs when the chain is longer than the bound.

The cursor walks a stable target sequence, so consecutive runs cover the
campaign and then start over.  Rows are MERGEd into Bronze under the writer
lock; the JSON report carries the row counters and the discovery client's byte
accounting.  Any failure exits 1.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Collection, Iterable, Mapping, Optional, Sequence

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "dags") not in sys.path:
    sys.path.insert(0, str(ROOT / "dags"))

from scrapers.sofascore.catalog import SofaScoreCatalog  # noqa: E402
from scrapers.sofascore.discovery import (  # noqa: E402
    DISCOVERY_LEASE_MAX_BYTES,
    DISCOVERY_LEASE_TTL_SECONDS,
    LeaseBrowserSofaScoreClient,
)
from dags.utils.sofascore_dq import SofaScoreDQViolation  # noqa: E402
from scrapers.sofascore.raw_store import RawPayloadStore  # noqa: E402
from scrapers.sofascore.schedule_refresh import (  # noqa: E402
    MAX_BACKTRACK_PAGES,
    SweepVerdictError,
    fetch_season_fixtures,
    fetch_season_schedules,
    schedule_rows_from_events,
)

DAG_ID = "dag_refresh_sofascore_all_mens"
TASK_ID = "refresh_season_schedules"
# A ``schedule_last`` page measured on the campaign's own traffic accounting is
# ~20-27 KB, plus ~75-80 KB once per lease for the browser warm-up.  The worst
# case of a plan is computed, not guessed — see ``worst_case_pages`` — and the
# run refuses to start when it does not fit the cap, because a sweep that dies
# fail-closed on bytes halfway is a run that paid for pages and kept nothing.
DEFAULT_BUDGET_CAP_BYTES = 64 * 1024 * 1024
_PAGE_BYTES = 27 * 1024
_LEASE_WARMUP_BYTES = 80 * 1024
# The client re-mints its lease at 90 % of the lease ceiling
# (``_LEASE_BYTE_HEADROOM`` in ``discovery``), so a sweep pays one warm-up per
# that many bytes — not a handful of times as the first estimate assumed (Sol
# round 7, finding 5).
_LEASE_BYTE_HEADROOM = 0.9
# Seasons whose results move today: their tail pages, on every run — up to
# this many of them.  A slice larger than the cap rotates on the cursor like
# the other classes; the cap is what the byte budget is sized on.
DEFAULT_MAX_DUE = 150
# Seasons already in Bronze but not playing in the window: their tail page too,
# but a slice per run.  This is what bounds the lag — a league that plays once a
# week leaves the ``due`` window long before its next match, and without a round
# trip over everything known it would never be looked at again.
DEFAULT_MAX_STALE = 200
# How far a tail visit may walk back when the page it read is entirely newer
# than what Bronze has: 3 pages is ~90 finished matches between two visits.
DEFAULT_CHASE_PAGES = 3
# Seasons Bronze has never seen: the whole page chain, a slice per run.
DEFAULT_MAX_SEED = 40
DEFAULT_SEED_PAGES = 12
DEFAULT_WINDOW_HOURS = 36
# Runs in a row that may write nothing before the lane is called broken.  Six
# is two days at the DAG's three runs a day: a lane pointed at seasons the
# source really serves cannot go two days without a single finished match
# across ~1.4k campaign seasons, while a single quiet run is unremarkable.
DEFAULT_MAX_IDLE_RUNS = 6
# Visits in a row that may leave a queued chain on the very same page before it
# is given up on.  A chain goes back on the queue whenever it did not finish —
# a page that breaks the endpoint contract puts it there at the page it still
# owes — and a page that is broken for good would otherwise be paid for on
# every run for ever, and pile up until broken chains dominate a slice and fail
# it (the same self-arming shape as the muteness check of round 11).  The chain
# is visited three times over three runs, and on the third it is given up.
MAX_CHAIN_ATTEMPTS = 3
# Runs in a row a class may keep its anchor because its walk was CUT SHORT (a
# transport failure, a schema surprise — see ``interrupted``).  Holding the
# anchor is right for a wobble: the slice was not covered, and the next run
# should cover it.  Holding it for ever is the wedge the whole review was about
# — one season whose calendar answers 403 on every run would pin ``stale`` on
# the same 200 targets and starve ``seed`` completely (audit after Sol round
# 28).  After this many fruitless attempts the class moves on and says so in
# the report; the slice it skipped comes back on the next lap of the cursor.
MAX_INTERRUPTED_RUNS = 3

# Campaign partitions the lane has already written: a season missing here has
# never been seeded, so it needs the whole page chain rather than the tail.
# The newest FINISHED kick-off of each is how far a tail visit may have to
# chase: everything older than it is already in Bronze.  It must exclude the
# scheduled rows — Bronze carries fixtures too, and a future anchor would make
# every page look "already seen" and kill the chase on page 0 (Sol round 5,
# finding 3).
#
# ``played`` is the newest kick-off of a row whose time has passed by more than
# the grace period: such a season owes a ``schedule_last`` page whether or not
# Bronze got to mark the match finished.  Without it a season Bronze knows only
# through its fixtures could answer 404 forever and stay green — it never gets a
# finished row, so it never enters the denominator of the absence check (Sol
# round 7, finding 2).  A ``postponed``/``canceled`` game is terminally unplayed
# and buys no page — the same exclusion the capture probe already makes in
# ``run_sofascore_scraper.py`` (Sol round 8, finding 2).  A NULL kick-off counts
# as owed for the same reason it does there: only evidence buys a quiet zero,
# and a NULL is the absence of evidence (Sol round 9, finding 3).
KNOWN_PARTITIONS_SQL = """
SELECT league,
       CAST(season AS varchar) AS season,
       max(CASE WHEN status_type = 'finished' THEN start_timestamp END) AS newest,
       count_if((start_timestamp IS NULL
                 OR start_timestamp
                    < to_unixtime(current_timestamp) - {grace} * 3600)
                AND coalesce(status_type, 'unknown')
                    NOT IN ('postponed', 'canceled')) AS owed
FROM iceberg.bronze.sofascore_schedule
WHERE league LIKE 'SS-%'
GROUP BY league, CAST(season AS varchar)
"""
# How long after kick-off a match is expected to be over and on the tail page.
PLAYED_GRACE_HOURS = 6

# Campaign partitions with a match around now: their results change today.
# Kick-off lives in ``start_timestamp`` (epoch seconds as the source ships it);
# the ``date`` column of this table is empty for every SofaScore row.
DUE_PARTITIONS_SQL = """
SELECT DISTINCT league, CAST(season AS varchar) AS season
FROM iceberg.bronze.sofascore_schedule
WHERE league LIKE 'SS-%'
  AND start_timestamp >= to_unixtime(current_timestamp) - {hours} * 3600
  AND start_timestamp <= to_unixtime(current_timestamp) + 6 * 3600
"""


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode()
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _configured_tournament_ids() -> frozenset[int]:
    """Source ids of the leagues ``dag_ingest_sofascore`` already covers."""

    return frozenset(SofaScoreCatalog.load().tournament_map(enabled_only=True).values())


@dataclass(frozen=True)
class SeasonTarget:
    """One sweep target and the Bronze partition it writes into."""

    tournament_id: int
    season_id: int
    league: str
    canonical_season: str

    @property
    def pair(self) -> tuple[int, int]:
        return (self.tournament_id, self.season_id)

    @property
    def partition(self) -> tuple[str, str]:
        return (self.league, self.canonical_season)


def current_season_targets(
    snapshot: Mapping[str, Any], exclude_tournament_ids: frozenset[int]
) -> list[SeasonTarget]:
    """Newest non-excluded season of every ready tournament, by tournament id.

    The order is stable so the cursor walks a fixed sequence.  An ``excluded``
    season is never a target (the campaign will not take it either); a
    tournament whose seasons are all excluded is skipped.
    """

    targets: list[SeasonTarget] = []
    for tournament in snapshot.get("tournaments", ()):
        if tournament.get("metadata_status") != "ready":
            continue
        tournament_id = int(tournament["unique_tournament_id"])
        if tournament_id in exclude_tournament_ids:
            continue
        newest: Optional[tuple[int, SeasonTarget]] = None
        for season in tournament.get("seasons", ()):
            if season.get("metadata_status") == "excluded":
                continue
            start_year = season.get("start_year")
            season_id = season.get("source_season_id")
            canonical = season.get("canonical_season")
            if not isinstance(start_year, int) or season_id is None or not canonical:
                continue
            if newest is None or start_year > newest[0]:
                newest = (
                    start_year,
                    SeasonTarget(
                        tournament_id=tournament_id,
                        season_id=int(season_id),
                        league=str(tournament["capture_key"]),
                        canonical_season=str(canonical),
                    ),
                )
        if newest is not None:
            targets.append(newest[1])
    targets.sort(key=lambda target: target.pair)
    return targets


def targets_digest(targets: Sequence[SeasonTarget]) -> str:
    """Identity of the sweep sequence, for the operator reading the state.

    NOT what the cursor is keyed on: the cursor is a stable ``[tournament,
    season]`` anchor and survives a reissued snapshot on purpose (Sol round 16,
    finding 4 — this line still called it an index).
    """

    payload = json.dumps([target.pair for target in targets], separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


SWEEP_CLASSES = ("due", "stale", "seed")


def read_cursor(path: Path) -> dict[str, Optional[tuple[int, int]]]:
    """Where each class stopped last run, as a ``(tournament, season)`` pair.

    The cursor is an ANCHOR, not an index.  An index only means anything for
    the exact target sequence it was taken from, so it had to be thrown away
    whenever the campaign snapshot was reissued — and the campaign rewrites its
    snapshot continuously (the metadata wave enriches the same file in place),
    which reset every class to the head of the sequence.  ``stale`` needs seven
    consecutive runs to walk the ~1.4k campaign seasons; resetting it more often
    than that meant the same first few hundred were refreshed forever and the
    rest never (cross-check, keys lens).  A source-id pair keeps its meaning
    across snapshots: the walk simply resumes at the first target that is not
    behind it, exactly as the unfinished-chain file already does with its ids.
    """

    restart: dict[str, Optional[tuple[int, int]]] = {
        name: None for name in SWEEP_CLASSES
    }
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return restart
    if not isinstance(value, Mapping):
        return restart
    index = value.get("index")
    if not isinstance(index, Mapping):
        return restart
    cursors: dict[str, Optional[tuple[int, int]]] = {}
    for name in SWEEP_CLASSES:
        entry = index.get(name)
        if (
            isinstance(entry, list)
            and len(entry) == 2
            and all(
                isinstance(item, int) and not isinstance(item, bool)
                for item in entry
            )
        ):
            cursors[name] = (entry[0], entry[1])
        else:
            cursors[name] = None
    return cursors


def read_interrupted_runs(path: Path) -> dict[str, int]:
    """How many runs in a row each class has kept its anchor after a cut-short
    walk.  A missing or malformed counter reads as zero: this guard must never
    be the reason a healthy lane cannot start.
    """

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    counters = value.get("interrupted_runs") if isinstance(value, Mapping) else None
    if not isinstance(counters, Mapping):
        return {}
    return {
        name: counters[name]
        for name in SWEEP_CLASSES
        if isinstance(counters.get(name), int)
        and not isinstance(counters.get(name), bool)
        and counters[name] >= 0
    }


def read_idle_runs(path: Path) -> int:
    """How many committed runs in a row have written no schedule row at all.

    Removing the per-slice muteness check left one hole open: a bootstrap whose
    snapshot points at seasons the source does not serve writes nothing, moves
    its cursors and reports success, run after run, and nothing ever complains
    (Sol round 12, finding 1).  A per-slice check cannot close it — mute seed
    targets accumulate until an all-mute slice is normal — but the LANE writing
    nothing at all is never normal: some campaign season somewhere finishes a
    match every hour, and ``stale`` alone visits 200 of them per run.  So the
    count is kept across runs and a lane that stays empty for too long fails.
    A missing or malformed counter reads as zero: this guard must never be the
    reason a healthy lane cannot start.
    """

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0
    if not isinstance(value, Mapping):
        return 0
    count = value.get("idle_runs")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        return 0
    return count


def read_incomplete(
    path: Path,
) -> tuple[dict[tuple[int, int], tuple[int, int, int]], Optional[tuple[int, int]]]:
    """Chains cut short, and where the retry queue stopped last run.

    The mapping is ``(tournament, season) -> (next page, anchor, attempts)``,
    the last being how many runs in a row have visited that chain without
    moving it forward — a fifth field an older state file does not have, which
    reads as zero.  Their first
    pages are already in Bronze, so ``known`` would hide them from the seed
    slice for good; this list is what brings them back.  It is NOT tied to the
    campaign snapshot: the ids are the source's own, and dropping the list
    whenever the snapshot is reissued would lose the unfinished tail of every
    season that survived into the new one.

    The queue has a cursor of its own because the retry slice is capped: taking
    the first ``max_seed`` entries every run means a single entry that keeps
    failing hides everything behind it forever (Sol round 7, finding 3).  It
    lives here next to the list it walks, and like every other id in this file
    it is the source's own: a ``(tournament, season)`` pair keeps its meaning
    when the queue grows or shrinks between runs, an index does not.
    """

    if not path.exists():
        return {}, None
    # An unreadable or malformed file is NOT "no unfinished chains": treating it
    # as empty would let this run overwrite the state and lose those tails for
    # good, and nothing else ever asks for them (Sol round 4, finding 6).
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"{path} is unreadable: {exc}") from exc
    entries = value.get("seasons") if isinstance(value, Mapping) else None
    if not isinstance(entries, list):
        raise ValueError(f"{path} has no list of unfinished seasons")
    resume: dict[tuple[int, int], tuple[int, int, int]] = {}
    for entry in entries:
        # Every field has to be an honest int: a string, a bool or a float
        # would be coerced by ``int()`` and a page of 0 or less silently
        # dropped, and this run then overwrites the file without that tail
        # (Sol round 5, finding 7).  The fourth field is the oldest kick-off
        # that chain has already read — the anchor a resumed walk needs to tell
        # whether the source repacked its pages underneath it.  The fifth is
        # the count of fruitless visits; a file written before it existed is
        # read as a chain that has just been queued.
        if (
            not isinstance(entry, list)
            or len(entry) not in (4, 5)
            or any(isinstance(item, bool) or not isinstance(item, int) for item in entry)
            or entry[2] <= 0
            or entry[3] < 0
            or (len(entry) == 5 and entry[4] < 0)
        ):
            raise ValueError(f"{path} holds a malformed entry: {entry!r}")
        resume[(entry[0], entry[1])] = (
            entry[2], entry[3], entry[4] if len(entry) == 5 else 0,
        )
    cursor = value.get("cursor")
    if cursor is None:
        return resume, None
    if (
        not isinstance(cursor, list)
        or len(cursor) != 2
        or any(isinstance(item, bool) or not isinstance(item, int) for item in cursor)
    ):
        raise ValueError(f"{path} holds a malformed queue cursor: {cursor!r}")
    return resume, (cursor[0], cursor[1])


def rolled_over_chains(
    queued: Collection[tuple[int, int]],
    targets: Sequence[SeasonTarget],
    snapshot: Mapping[str, Any],
) -> set[tuple[int, int]]:
    """Queued chains whose season the snapshot dates BEFORE the current target.

    That is the only proof of a rollover this lane accepts.  ``season_id`` alone
    says nothing about time — the ids are the source's own — so the start year
    is read from the snapshot for BOTH seasons, and an entry is retired only
    when both are known and the target's year is greater.  Everything else keeps
    its chain: a tournament that is not ready in this snapshot, a season whose
    ``canonical_season`` is missing, two seasons sharing a start year (Sol round
    28).
    """

    years: dict[int, dict[int, int]] = {}
    for tournament in snapshot.get("tournaments", ()):
        try:
            tournament_id = int(tournament["unique_tournament_id"])
        except (KeyError, TypeError, ValueError):
            continue
        seasons: dict[int, int] = {}
        for season in tournament.get("seasons", ()):
            start_year = season.get("start_year")
            season_id = season.get("source_season_id")
            if isinstance(start_year, bool) or not isinstance(start_year, int):
                continue
            try:
                seasons[int(season_id)] = start_year
            except (TypeError, ValueError):
                continue
        years[tournament_id] = seasons
    current = {
        target.tournament_id: years.get(target.tournament_id, {}).get(target.season_id)
        for target in targets
    }
    retired: set[tuple[int, int]] = set()
    for tournament_id, season_id in queued:
        target_year = current.get(tournament_id)
        queued_year = years.get(tournament_id, {}).get(season_id)
        if target_year is None or queued_year is None:
            continue
        if queued_year < target_year:
            retired.add((tournament_id, season_id))
    return retired


def requeue_chains(
    previous: Mapping[tuple[int, int], tuple[int, int, int]],
    attempted: Collection[tuple[int, int]],
    fresh: Iterable[tuple[int, int, int, int]],
    rolled_over: Collection[tuple[int, int]] = (),
) -> tuple[list[list[int]], list[list[int]]]:
    """Rebuild the resume queue, and name the chains this run gives up on.

    ``previous`` is the queue as it was read, ``attempted`` the chains this run
    has actually visited (they are dropped and re-added only if they came back
    unfinished), and ``fresh`` the ``(tournament, season, page, anchor)`` tuples
    the fetcher returned — a chain cut short by the page bound, or one whose
    page broke the endpoint contract, which comes back at the page it still
    owes.

    A chain that comes back on a LATER page than it was queued with made
    progress, so its attempt count starts over.  One that comes back on the
    same page (or earlier) did not: a page that is broken for good, or a lease
    that keeps running out in the same place, would otherwise be paid for on
    every run for ever and pile up until such chains dominate a slice and fail
    it.  After ``MAX_CHAIN_ATTEMPTS`` fruitless visits the chain is dropped
    from the queue and returned as abandoned, for the report to carry: the rest
    of that season stays unread, and saying so is the whole point — the
    alternative is a queue that never drains.

    ``rolled_over`` names the entries whose season is PROVEN to be over: the
    snapshot dates it before the season the lane now works for that tournament.
    Such an entry can never be attempted again — the retry slice is taken from
    the current target list — so it would age neither forward nor out, and the
    file would grow by a campaign every year (code review after Sol round 26).
    Nothing weaker may be used as proof: "the pair is not a target" also happens
    when a tournament is momentarily not ready, when the snapshot lacks a
    canonical season, or when two seasons share a start year — and dropping the
    chain there loses the tail of a season that is merely out of sight (Sol
    rounds 27 and 28).  The queue is otherwise NOT tied to the snapshot: the ids
    are the source's own and survive its reissue.
    """

    queued: dict[tuple[int, int], list[int]] = {}
    abandoned: list[list[int]] = []
    retired = set(rolled_over)
    for pair, (page, anchor, attempts) in previous.items():
        if pair in attempted:
            continue
        if pair in retired:
            # A season the snapshot dates BEFORE the one the lane works now:
            # its history belongs to the campaign, not to this lane.
            abandoned.append([pair[0], pair[1], page, anchor])
            continue
        queued[pair] = [pair[0], pair[1], page, anchor, attempts]
    for tournament, season, page, anchor in fresh:
        pair = (int(tournament), int(season))
        prior = previous.get(pair)
        attempts = 0 if prior is None or page > prior[0] else prior[2] + 1
        if attempts >= MAX_CHAIN_ATTEMPTS:
            queued.pop(pair, None)
            abandoned.append([pair[0], pair[1], int(page), int(anchor)])
            continue
        queued[pair] = [pair[0], pair[1], int(page), int(anchor), attempts]
    return sorted(queued.values()), sorted(abandoned)


def _trino_rows(sql: str) -> list[tuple]:
    """Run one read-only query at task runtime; never at DAG parse."""

    from utils.silver_tasks import _get_trino_connection

    conn = _get_trino_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        return list(cursor.fetchall())
    finally:
        conn.close()


def bronze_partitions(
    window_hours: int,
) -> tuple[
    dict[tuple[str, str], Optional[int]],
    set[tuple[str, str]],
    set[tuple[str, str]],
]:
    """``(known, due, played)`` partitions of ``bronze.sofascore_schedule``.

    ``known`` maps every partition that holds a match the source has already
    PLAYED to its newest FINISHED kick-off; that timestamp is what tells a
    one-page visit whether the page reached back into what Bronze already has.
    A partition whose only rows are the lane's own FIXTURE rows is NOT known:
    those rows say nothing about the tail chain, and treating them as evidence
    took a season out of ``seed`` before a single tail page had been written
    for it — after which the tail classes read page 0, the chase settled there
    at once, and the middle of that season was never asked for again (audit
    after Sol round 28).  ``due`` is the partitions whose matches fall in
    the window around now — those are the seasons whose results change today
    and are refreshed first on every run.  ``played`` is the partitions that hold
    a match whose kick-off has passed: the source owes them a tail page, and
    they are the denominator of the absence check.
    """

    known: dict[tuple[str, str], Optional[int]] = {}
    played: set[tuple[str, str]] = set()
    rows = _trino_rows(KNOWN_PARTITIONS_SQL.format(grace=PLAYED_GRACE_HOURS))
    for league, season, newest, owed in rows:
        partition = (str(league), str(season))
        has_played = newest is not None or int(owed or 0) > 0
        if has_played:
            played.add(partition)
            known[partition] = int(newest) if newest is not None else None
    due = {
        (str(league), str(season))
        for league, season in _trino_rows(DUE_PARTITIONS_SQL.format(hours=int(window_hours)))
    }
    return known, due, played


def write_schedule_rows(rows: list[dict]) -> str:
    """MERGE the rows into ``bronze.sofascore_schedule`` under the writer lock."""

    import pandas as pd

    from scrapers.sofascore.scraper import SofaScoreScraper
    from scrapers.sofascore.writer_lock import bronze_writer_lock

    with SofaScoreScraper() as scraper, bronze_writer_lock():
        frame = scraper._add_metadata(pd.DataFrame(rows), "schedule")
        return scraper.save_to_iceberg(
            df=frame,
            table_name="sofascore_schedule",
            partition_cols=["league", "season"],
            natural_keys=["league", "season", "game_id"],
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--cursor", required=True,
        help="JSON file holding the sweep position across runs.",
    )
    parser.add_argument(
        "--incomplete", required=True,
        help="JSON file listing seasons whose page chain is not finished yet.",
    )
    parser.add_argument(
        "--max-due", type=int,
        default=int(
            os.environ.get("SOFASCORE_REFRESH_MAX_DUE", "").strip() or DEFAULT_MAX_DUE
        ),
        help="Cap on seasons playing in the window (a chased tail visit each).",
    )
    parser.add_argument(
        "--max-stale", type=int,
        default=int(
            os.environ.get("SOFASCORE_REFRESH_MAX_STALE", "").strip()
            or DEFAULT_MAX_STALE
        ),
        help="Known seasons outside the window per run (tail visit + fixtures).",
    )
    parser.add_argument(
        "--max-seed", type=int,
        default=int(
            os.environ.get("SOFASCORE_REFRESH_MAX_SEED", "").strip() or DEFAULT_MAX_SEED
        ),
        help="Never-seeded seasons to pull per run (whole page chain).",
    )
    parser.add_argument(
        "--chase-pages", type=int,
        default=int(
            os.environ.get("SOFASCORE_REFRESH_CHASE_PAGES", "").strip()
            or DEFAULT_CHASE_PAGES
        ),
        help="Page bound for a tail visit that has to catch up with Bronze.",
    )
    parser.add_argument(
        "--seed-pages", type=int,
        default=int(
            os.environ.get("SOFASCORE_REFRESH_SEED_PAGES", "").strip()
            or DEFAULT_SEED_PAGES
        ),
        help="Page-chain bound for a never-seeded season.",
    )
    parser.add_argument(
        "--window-hours", type=int,
        default=int(
            os.environ.get("SOFASCORE_REFRESH_WINDOW_HOURS", "").strip()
            or DEFAULT_WINDOW_HOURS
        ),
        help="How far back a match counts as 'playing now'.",
    )
    parser.add_argument(
        "--max-idle-runs", type=int,
        default=int(
            os.environ.get("SOFASCORE_REFRESH_MAX_IDLE_RUNS", "").strip()
            or DEFAULT_MAX_IDLE_RUNS
        ),
        help=(
            "Consecutive runs that may write no schedule row before the lane "
            "is declared broken; 0 disables the guard."
        ),
    )
    parser.add_argument(
        "--control-url", default=os.environ.get("SOFASCORE_PROXY_CONTROL_URL", "")
    )
    parser.add_argument(
        "--budget-cap-bytes", type=int,
        default=int(
            os.environ.get("SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES", "").strip()
            or DEFAULT_BUDGET_CAP_BYTES
        ),
    )
    parser.add_argument(
        # The gateway may hand out a smaller lease than the client's default,
        # and the preflight counts warm-ups per lease — so this has to be
        # settable from the environment like every other knob, or the estimate
        # silently assumes 8 MiB leases (Sol round 9, finding 4).
        "--per-lease-max-bytes", type=int,
        default=int(
            os.environ.get("SOFASCORE_REFRESH_PER_LEASE_MAX_BYTES", "").strip()
            or DISCOVERY_LEASE_MAX_BYTES
        ),
    )
    parser.add_argument(
        "--lease-ttl-seconds", type=int, default=DISCOVERY_LEASE_TTL_SECONDS
    )
    parser.add_argument("--max-attempts", type=int, default=12)
    parser.add_argument(
        "--raw-store-uri", help="Override SOFASCORE_RAW_STORE_URI."
    )
    parser.add_argument(
        "--dag-id", default=os.environ.get("AIRFLOW_CTX_DAG_ID") or DAG_ID
    )
    parser.add_argument(
        "--run-id", default=os.environ.get("AIRFLOW_CTX_DAG_RUN_ID") or "manual"
    )
    parser.add_argument(
        "--task-id", default=os.environ.get("AIRFLOW_CTX_TASK_ID") or TASK_ID
    )
    return parser


def _cursor_index(targets: Sequence[SeasonTarget], start: Optional[Any]) -> int:
    """First position at or after the anchor pair, in the sorted sequence."""

    if start is None:
        return 0
    anchor = (int(start[0]), int(start[1]))
    for index, target in enumerate(targets):
        if target.pair >= anchor:
            return index
    # The anchor is past the end of the sequence: wrap to the head.
    return 0


def take_slice(
    targets: Sequence[SeasonTarget],
    start: Optional[tuple[int, int]],
    limit: int,
    wanted: Any,
) -> tuple[list[SeasonTarget], Optional[tuple[int, int]]]:
    """One class's slice from its own cursor, and where that cursor lands.

    The cursor is the ``(tournament, season)`` pair to resume at, and the walk
    wraps once around the sequence.  Filling the slice leaves the cursor at the
    target AFTER the last one taken, so the next run continues down the class's
    own queue; going the whole way round without filling it means the class has
    fewer members than the cap, and the cursor stays put — those few are simply
    taken again next run, which is exactly right for seasons that are playing
    now or still waiting to be seeded.
    """

    picked: list[SeasonTarget] = []
    if not targets:
        return picked, start
    total = len(targets)
    begin = _cursor_index(targets, start)
    if limit <= 0:
        return picked, targets[begin].pair
    for step in range(total):
        index = (begin + step) % total
        target = targets[index]
        if not wanted(target):
            continue
        picked.append(target)
        if len(picked) >= limit:
            return picked, targets[(index + 1) % total].pair
    return picked, targets[begin].pair


def worst_case_pages(
    max_due: int,
    max_stale: int,
    max_seed: int,
    chase_pages: int,
    seed_pages: int,
) -> int:
    """Requests one run may make in the worst case, all classes included.

    Every ``due``/``stale`` target may have to chase up to ``chase_pages``
    tail pages; ``stale`` and ``seed`` targets also get one fixture page each;
    a seeded chain may walk ``seed_pages``, read one page of OVERLAP before
    them (a resumed chain starts a page early on purpose) and, when the source
    repacked its pages under it, step back up to ``MAX_BACKTRACK_PAGES`` more.
    So a seeded target costs ``seed_pages + backtrack + overlap + fixture``.
    The fixture pages and the backtrack allowance were missing from the first
    estimate, which is how a "1980 page" plan turned out to be 2440 (Sol round
    6, finding 5); the overlap page was still missing from the second, worth
    another ``max_seed`` requests (Sol round 13, finding 1).
    """

    return (
        max_due * chase_pages
        + max_stale * (chase_pages + 1)
        + max_seed * (seed_pages + MAX_BACKTRACK_PAGES + 2)
    )


def worst_case_bytes(
    max_due: int,
    max_stale: int,
    max_seed: int,
    chase_pages: int,
    seed_pages: int,
    per_lease_max_bytes: int = DISCOVERY_LEASE_MAX_BYTES,
    budget_cap_bytes: Optional[int] = None,
) -> int:
    """Bytes that worst case costs at the measured upper page size.

    The warm-ups are counted, not guessed: the client mints a new lease every
    ``_LEASE_BYTE_HEADROOM`` of the lease ceiling and pays a browser warm-up
    each time, so a small lease ceiling means many warm-ups.  And the ceiling
    is not constant — the client asks for ``min(per_lease_max_bytes, what is
    left of the budget)``, so the last leases of a run are SMALLER and serve
    fewer pages each.  Dividing the payload by one fixed lease size therefore
    undercounted the warm-ups of exactly the plans that sit near the cap (Sol
    round 14, finding 1), so the leases are walked one by one instead, against
    ``budget_cap_bytes`` when it is known.

    Retries are NOT in here — the client may repeat a page up to
    ``--max-attempts`` times — and neither is the lease the client re-mints
    when its TTL is nearly up, which depends on how slow the source is.  This
    is the floor the plan cannot go under, which is what a preflight can
    honestly check.
    """

    pages = worst_case_pages(max_due, max_stale, max_seed, chase_pages, seed_pages)
    ceiling = int(per_lease_max_bytes)
    cap = int(budget_cap_bytes) if budget_cap_bytes else None

    def _pages_per_lease(room: int) -> tuple[int, bool]:
        """Pages one lease of ``room`` bytes serves, and whether it burns out.

        Two things bound a lease and the smaller one wins.  The client re-mints
        at ``_LEASE_BYTE_HEADROOM`` of the ceiling, checked BEFORE the request,
        so the page that crosses the mark is still served by the old lease —
        that is the rounding UP.  But the gateway also refuses anything past
        ``lease.max_bytes`` outright (``filter_proxy.py``), so a page that does
        not FIT is not served at all — that is the rounding down.  Taking only
        the first gave a lease of 128 KiB two pages when the second one had
        21 504 bytes of room and needed 27 648 (Sol round 16, finding 1).
        """

        if room <= _LEASE_WARMUP_BYTES:
            return 0, False
        fits = (room - _LEASE_WARMUP_BYTES) // _PAGE_BYTES
        # Pages the client serves before it re-mints, in EXACT arithmetic.
        # ``int(0.9 * room)`` truncated the mark down onto the byte the client
        # is still below, so a lease of 121 743 bytes looked like a clean
        # re-mint after one page while the real client (109 568 against a mark
        # of 109 568.7) asks for another and burns the lease out (Sol round 18,
        # finding 1).  The mark is 9/10 of the ceiling and the comparison is
        # ``>=``, so the last page that starts UNDER it is the one that counts.
        margin = 9 * room - 10 * _LEASE_WARMUP_BYTES
        before_remint = margin // (10 * _PAGE_BYTES) + 1 if margin > 0 else 0
        # When the hard limit bites FIRST, the client does not know it: it is
        # still under the re-mint mark, so it asks for one more page, and the
        # gateway serves the prefix that fits and cuts the connection — the
        # lease ends at ``lease.max_bytes``, billed in full (``filter_proxy``
        # ``_pump``; ``test_lease_pump_pre_reads_only_the_remaining_provider_
        # window`` asserts ``total_bytes == max_bytes``).  Treating that page as
        # free let the estimate accept a cap the run would burn through halfway
        # (Sol round 17, finding 1).
        return max(0, min(fits, before_remint)), fits < before_remint

    if _pages_per_lease(ceiling)[0] < 1:
        # A response cannot be split across two leases, so a lease that has no
        # room for a page after its warm-up can never serve one (Sol round 8,
        # finding 3).
        raise ValueError(
            f"a lease of {per_lease_max_bytes} bytes has no room for a "
            f"{_PAGE_BYTES} byte page after its warm-up"
        )
    spent = 0
    left = pages
    while left > 0:
        room = ceiling if cap is None else min(ceiling, cap - spent)
        served, burns_out = _pages_per_lease(room)
        if served < 1:
            # What is left of the budget cannot even warm a lease up and serve
            # one page through it: the plan does not fit, and saying so needs a
            # number above the cap.
            return spent + _LEASE_WARMUP_BYTES + left * _PAGE_BYTES
        served = min(left, served)
        if _LEASE_WARMUP_BYTES + served * _PAGE_BYTES == room:
            # A lease that ends EXACTLY on its ceiling never lets the gateway
            # read the provider EOF: the down pump takes the allowance check at
            # the top of its loop, finds nothing left and breaks before the
            # zero-length read that would prove the response ended, so the
            # ``finally`` latches ``accounting_uncertain`` and the lease is
            # closed 409 with its escrow retained (``filter_proxy`` ``_pump``,
            # "Only an observed provider EOF may release the lifecycle").  It
            # can happen on ANY lease of the plan, not just the last one, so
            # the whole layout is refused (Sol rounds 19 and 20).
            raise ValueError(
                f"a lease of {room} bytes would end exactly on its ceiling "
                f"after {served} pages: the gateway cannot observe the EOF and "
                f"closes it accounting-uncertain"
            )
        left -= served
        # A lease that runs into the hard limit with pages still to fetch is
        # drained to its ceiling by the cut-off attempt; one that re-mints on
        # the mark, or that has nothing left to ask for, is not.
        spent += room if (burns_out and left) else (
            _LEASE_WARMUP_BYTES + served * _PAGE_BYTES
        )
    return spent


def sweep_predicates(
    known: set[tuple[str, str]],
    due: set[tuple[str, str]],
    pinned: Optional[set[tuple[int, int]]] = None,
) -> dict[str, Any]:
    """What makes a target a member of each class.

    One definition, used both to take the slice and to count the class: two
    copies of these three conditions would drift apart, and the count is what
    the acceptance criterion reads.
    """

    held = pinned or set()
    return {
        "due": lambda target: (
            target.partition in due and target.pair not in held
        ),
        "stale": lambda target: (
            target.partition not in due
            and target.partition in known
            and target.pair not in held
        ),
        "seed": lambda target: (
            target.partition not in due and target.partition not in known
        ),
    }


def plan_sweep(
    targets: Sequence[SeasonTarget],
    known: set[tuple[str, str]],
    due: set[tuple[str, str]],
    cursors: Mapping[str, int],
    max_due: int,
    max_stale: int,
    max_seed: int,
    pinned: Optional[set[tuple[int, int]]] = None,
) -> tuple[
    dict[str, list[SeasonTarget]],
    dict[str, Optional[tuple[int, int]]],
    dict[str, int],
]:
    """Split the sweep into the seasons playing now, the known ones and the new.

    ``due`` seasons are refreshed on every run — up to ``max_due`` of them,
    beyond which the class rotates on its own cursor like the others, because
    the byte budget is sized on the cap (Sol round 18, finding 3).  That is the
    fast path for a match that has just finished.  ``stale`` is every other
    season Bronze already has: a cursor slice of them gets its tail page too,
    which is what bounds the lag for a league that plays once a week and spends
    most of the time outside the window.  The seed slice walks over the seasons
    Bronze has never seen and pulls their whole page chain.

    Each class walks its OWN cursor.  A single shared cursor starves the
    classes against each other: one lone ``due`` season far down the sequence
    drags the cursor past hundreds of unvisited ones, and from then on every
    run replays the same slice while the rest is never looked at again (Sol
    round 4, finding 1).

    ``pinned`` are the seasons whose page chain was cut short by an earlier run.
    Bronze already holds their first pages, so the tail classes would happily
    take them — and a tail visit walks from page 0, not from where the chain
    stopped, yet it would still count as "this season was served" and drop the
    resume point.  They belong to the seed phase, which is the only one that
    carries on from the saved page (Sol round 6, finding 1).
    """

    wanted = sweep_predicates(known, due, pinned)
    limits = {"due": max_due, "stale": max_stale, "seed": max_seed}
    plan: dict[str, list[SeasonTarget]] = {}
    next_cursors: dict[str, Optional[tuple[int, int]]] = {}
    members: dict[str, int] = {}
    for name in SWEEP_CLASSES:
        plan[name], next_cursors[name] = take_slice(
            targets, cursors.get(name), limits[name], wanted[name]
        )
        # How many targets the class holds ALTOGETHER, not just this slice.
        # Without it the acceptance criterion cannot tell a class that was
        # covered whole from one whose anchor is stuck: a class of exactly
        # ``limit`` members walks the entire sequence and lands back on its
        # own anchor, which looks identical to no progress (Sol round 12,
        # finding 9).  The rule holds ONE WAY only: a class with more members
        # than its slice must move its anchor.  The converse is false — a
        # class of exactly ``limit`` members scattered through the sequence is
        # walked whole and still leaves the anchor elsewhere (Sol round 13,
        # finding 2).
        members[name] = sum(1 for target in targets if wanted[name](target))
    return plan, next_cursors, members


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    report: dict[str, Any] = {"status": "running", "errors": []}
    client: Optional[LeaseBrowserSofaScoreClient] = None
    cursor_state: Optional[dict[str, Any]] = None
    incomplete_state: Optional[dict[str, Any]] = None
    committed = False
    try:
        if not str(args.control_url).strip():
            raise ValueError("--control-url is required")
        for name in (
            "max_due", "max_stale", "max_seed", "seed_pages", "chase_pages",
        ):
            if getattr(args, name) <= 0:
                raise ValueError(f"--{name.replace('_', '-')} must be positive")
        # Zero switches the emptiness guard off; a NEGATIVE limit made every
        # run trip it, healthy ones included, and only after the whole paid
        # sweep had been walked (Sol round 13, finding 4).
        if args.max_idle_runs < 0:
            raise ValueError("--max-idle-runs cannot be negative")
        # The knobs are read from the environment, so an override can ask for
        # more pages than the byte cap admits.  Refuse before a single paid
        # request instead of dying fail-closed with the sweep half done.
        knobs = (
            args.max_due, args.max_stale, args.max_seed, args.chase_pages,
            args.seed_pages,
        )
        needed = worst_case_bytes(
            *knobs, args.per_lease_max_bytes, args.budget_cap_bytes
        )
        # ``>=``, not ``>``: a plan that lands EXACTLY on the cap leaves its
        # last lease drained to its final byte, and a lease that ends on its
        # ceiling never lets the gateway see EOF — it is closed
        # ``accounting_uncertain`` and the close answers 409 (``filter_proxy``;
        # lesson #7 on lost leases).  The cap has to keep a byte of slack
        # (Sol round 19).
        if needed >= args.budget_cap_bytes:
            raise ValueError(
                f"worst case of this plan is {worst_case_pages(*knobs)} pages "
                f"~ {needed} bytes, over the {args.budget_cap_bytes} byte cap"
            )
        snapshot = json.loads(Path(args.snapshot).read_text(encoding="utf-8"))
        snapshot_id = str(snapshot.get("snapshot_id") or "")
        raw_store = (
            RawPayloadStore.from_uri(args.raw_store_uri)
            if args.raw_store_uri
            else RawPayloadStore.from_env(optional=False)
        )
        exclude = _configured_tournament_ids()
        all_targets = current_season_targets(snapshot, exclude)
        if not all_targets:
            raise ValueError("campaign snapshot has no ready tournament season")
        digest = targets_digest(all_targets)
        cursor_path = Path(args.cursor)
        cursors = read_cursor(cursor_path)
        idle_runs = read_idle_runs(cursor_path)
        interrupted_runs = read_interrupted_runs(cursor_path)
        known, due, played = bronze_partitions(args.window_hours)
        # Seasons cut short by the page bound last run come first: Bronze
        # already knows their partition, so nothing else would pick them up.
        # The queue rotates on a cursor of its own — a head entry that keeps
        # failing must not hide the rest of the queue behind it.
        unfinished, retry_cursor = read_incomplete(Path(args.incomplete))
        queued = [target for target in all_targets if target.pair in unfinished]
        retry_targets, next_retry_cursor = take_slice(
            queued, retry_cursor, args.max_seed, lambda target: True,
        )
        plan, next_cursors, members = plan_sweep(
            all_targets, set(known), due, cursors, args.max_due, args.max_stale,
            args.max_seed - len(retry_targets),
            pinned=set(unfinished),
        )
        retried = {target.pair for target in retry_targets}
        plan["seed"] = retry_targets + [
            target for target in plan["seed"] if target.pair not in retried
        ]
        # The page each resumed chain owes.  The fetcher itself re-reads the one
        # before it (the source repacks its pages, and a page fetched twice is a
        # MERGE no-op) while counting its bound from the owed page — doing the
        # step here made ``--seed-pages 1`` walk the same page forever.
        start_pages = {
            target.pair: unfinished[target.pair][0]
            for target in retry_targets
        }
        resume_anchors = {
            target.pair: unfinished[target.pair][1]
            for target in retry_targets
            if unfinished[target.pair][1]
        }
        # Only a partition Bronze already has can be chased: for the rest the
        # whole chain is new, and the seed bound is what stops it.
        chase_before = {
            target.pair: known[target.partition]
            for target in plan["due"] + plan["stale"]
            if known.get(target.partition) is not None
        }
        # Bronze speaks (league, canonical season); the source — and therefore
        # the fetcher — speaks (tournament id, season id).  Handing the fetcher
        # the Bronze keys made every membership test false, so the absence check
        # silently had an empty denominator (Sol round 8, finding 1).
        owed_pages = {
            target.pair
            for target in plan["due"] + plan["stale"]
            if target.partition in played
        }
        report.update(
            {
                "snapshot_id": snapshot_id,
                "targets_total": len(all_targets),
                "known_partitions": len(known),
                "due_partitions": len(due),
                "due_targets": len(plan["due"]),
                "stale_targets": len(plan["stale"]),
                "seed_targets": len(plan["seed"]),
                # The whole class, and the limit its slice was actually taken
                # with: a class bigger than its limit is the one whose anchor
                # has to keep moving.  ``seed``'s limit is NOT ``--max-seed``
                # — the retry queue takes its share first — so reporting the
                # configured number would compare against the wrong one (Sol
                # round 13, finding 3).
                "class_members": members,
                "class_limits": {
                    "due": args.max_due,
                    "stale": args.max_stale,
                    "seed": args.max_seed - len(retry_targets),
                },
                # Resumed chains ride ahead of the seed slice, so they are in
                # ``seed_targets`` without being in ``class_members["seed"]``.
                "retry_targets": len(retry_targets),
                "resumed_targets": len(start_pages),
                # With ``stale`` in the plan every campaign season is either
                # playing, known or new, so an empty plan over a non-empty
                # target list means the split itself is broken.  It stays in
                # the report because a run that fetches nothing must not be
                # merely green and silent.
                "idle": not any(plan.values()),
                "cursor_start": {
                    name: list(pair) if pair else None
                    for name, pair in cursors.items()
                },
                "excluded_tournaments": len(exclude),
            }
        )
        if report["idle"]:
            raise ValueError(
                f"sweep plan is empty for {len(all_targets)} campaign seasons "
                f"(known={len(known)}, due={len(due)}, cursors={cursors})"
            )
        client = LeaseBrowserSofaScoreClient(
            control_url=str(args.control_url).strip(),
            budget_cap_bytes=args.budget_cap_bytes,
            per_lease_max_bytes=args.per_lease_max_bytes,
            lease_ttl_seconds=args.lease_ttl_seconds,
            max_attempts=args.max_attempts,
            dag_id=args.dag_id,
            run_id=args.run_id,
            task_id=args.task_id,
        )
        # Class by class, each MERGEd before the next one is fetched: the
        # priority of ``due`` has to survive a failure further down, and a
        # single MERGE at the end would throw away rows already paid for
        # (Sol round 4, finding 4).
        rows_written = 0
        unfinished_now: list[tuple[int, int, int, int]] = []
        # Every class that finishes commits its own cursor and its own attempted
        # targets, whatever happens to the classes after it: a class that keeps
        # failing must not freeze the ones that work (Sol round 5, finding 1).
        # ``snapshot_id``/``targets`` are kept for the operator to read, NOT to
        # invalidate the cursor: the anchors are source ids and survive a
        # reissued snapshot on purpose.
        cursor_state = {
            "snapshot_id": snapshot_id,
            "targets": digest,
            # Carried over untouched until the sweep gets all the way through:
            # a run that died halfway is a FAILED run, not an idle one, and
            # must not count towards the emptiness alarm either way.
            "idle_runs": idle_runs,
            # Per class, how many runs in a row it kept its anchor after a walk
            # that was cut short.  Carried over untouched until each class
            # decides its own below.
            "interrupted_runs": dict(interrupted_runs),
            "index": {
                name: list(pair) if pair else None
                for name, pair in cursors.items()
            },
        }
        attempted: set[tuple[int, int]] = set()
        # Verdicts about slices, gathered across classes: the sweep finishes,
        # then the run fails on the first of them.
        verdicts: list[Exception] = []
        rolled_over = rolled_over_chains(set(unfinished), all_targets, snapshot)
        queued_seasons, abandoned = requeue_chains(
            unfinished, attempted, (), rolled_over
        )
        incomplete_state = {
            "seasons": queued_seasons,
            "cursor": list(retry_cursor) if retry_cursor else None,
        }
        committed = False
        for name in SWEEP_CLASSES:
            pairs = [target.pair for target in plan[name]]
            # Verdicts about this slice, in the order they were reached.  A
            # verdict is not a transport failure: the walk finished, and what it
            # collected is banked (rows, cursor, resume queue) BEFORE the run
            # fails on the first of them.  A class that fails before its cursor
            # moves rebuilds the identical slice on the next run and fails
            # identically, for ever (Sol rounds 24-27).
            deferred: list[Exception] = []
            # A failure that is NOT a verdict about this slice: the walk did not
            # finish, so the class banks its rows and its queue but leaves its
            # cursor where it was — the slice has to be walked again.
            interrupted: Optional[Exception] = None
            if name == "seed":
                # The queue cursor moves as soon as its slice is TAKEN, not when
                # the phase succeeds: the entries stay on the list until they are
                # really finished, but a head entry that keeps failing must not
                # hide the rest of the queue behind it (Sol round 7, finding 3).
                incomplete_state["cursor"] = (
                    list(next_retry_cursor) if next_retry_cursor else None
                )
                try:
                    events, counters, cut_short = fetch_season_schedules(
                        client, pairs, raw_store,
                        max_pages=args.seed_pages,
                        start_pages=start_pages,
                        resume_anchors=resume_anchors,
                        # A season that has not kicked off yet legitimately has
                        # no page and the seed slice is full of them, so
                        # absences here are not evidence of anything; failing on
                        # them would pin the cursor to that slice for good.
                        missing_fail_share=None,
                    )
                except SweepVerdictError as exc:
                    events, counters, cut_short = (
                        exc.fetched, exc.counters, exc.incomplete
                    )
                    deferred.append(exc)
            else:
                # ``chase_before`` turns the single tail page into "as many
                # pages as it takes to reach what Bronze already has": more
                # than a page of matches can finish between two visits.
                try:
                    events, counters, cut_short = fetch_season_schedules(
                        client, pairs, raw_store,
                        max_pages=args.chase_pages,
                        chase_before=chase_before,
                        # Only a season whose match has already kicked off owes
                        # a page; the rest may answer 404 for as long as they
                        # like.
                        owed_pages=owed_pages,
                    )
                except SweepVerdictError as exc:
                    events, counters, cut_short = (
                        exc.fetched, exc.counters, exc.incomplete
                    )
                    deferred.append(exc)
            # ``extend``, never assignment: the classes are fetched in turn and
            # each can leave a chain cut short (Sol round 5, finding 2).
            unfinished_now.extend(cut_short)
            # What the source actually served this class, in EVENTS: an empty
            # 200 is a legitimate answer for a season that owes no page (the
            # contract says so, and the fetcher fails the owed ones itself), so
            # counting pages turned a normal pre-season answer into a run that
            # died on its first class and never wrote its state — the lane then
            # rebuilt the same plan and died again, forever.
            served = counters["events"]
            if name in ("stale", "seed"):
                # The seasons Bronze has never seen, and the ones it has not
                # looked at for a while, also get their FIXTURE page: without a
                # future match in Bronze the ``due`` window can never open for
                # them, so a season that has not kicked off yet would wait for
                # the whole seed round to come back (Sol round 5, finding 4).
                try:
                    fixture_events, fixture_counters = fetch_season_fixtures(
                        client, pairs, raw_store
                    )
                except SweepVerdictError as exc:
                    # The calendar walk is an EXTRA on top of the tail pages,
                    # and those are already paid for.  Raising here threw them
                    # away and, worse, left the class before its cursor moved
                    # and before the resume queue was written — so the next run
                    # rebuilt the same slice and broke on the same page, for
                    # ever (Sol round 24).  The drift is reported and the run
                    # still fails, but only once this class has banked what it
                    # has: rows, cursor and queue.  EVERY failure of this walk is
                    # banked the same way, not just the drift verdict: a 200
                    # whose body is not an object, or a transport failure that is
                    # not a 404, escaped the same way and cost the class the
                    # same work (code review after Sol round 26).
                    deferred.append(exc)
                    fixture_events = list(exc.fetched)
                    fixture_counters = dict(exc.counters)
                    fixture_counters.setdefault("events", 0)
                    fixture_counters["error"] = str(exc)
                except Exception as exc:
                    # NOT a verdict: a transport failure, or a bug.  The slice
                    # was not walked to the end, so the cursor may NOT move —
                    # but the tail pages this class already paid for still go to
                    # Bronze, and the resume queue is still written, because
                    # throwing them away costs the run twice (Sol rounds 26-28).
                    interrupted = exc
                    fixture_events = []
                    fixture_counters = {"events": 0, "error": str(exc)}
                events.extend(fixture_events)
                # The whole fixture counter set goes to the report: dropping
                # everything but two fields hid ``foreign_season`` (Sol round 7,
                # finding 4).
                report[f"{name}_fixtures"] = fixture_counters
                served += fixture_counters["events"]
                # A seed slice mute on BOTH endpoints is NOT fatal, however
                # much it looks like the snapshot pointing at seasons the
                # source does not have (Sol round 11, finding 1).  A season
                # mute on both never produces a row, so its partition never
                # becomes ``known`` and it never leaves ``seed``: the servable
                # targets graduate and the mute ones ACCUMULATE, until a slice
                # is legitimately all-mute and the check fires on every run for
                # good.  The drift it was meant to catch is already caught
                # where it cannot false-positive — ``owed_pages`` over the
                # seasons Bronze holds a kicked-off match of.  What is left
                # here is evidence for the operator, and both ``missing``
                # counters are already in the report above.
            report[name] = counters
            try:
                rows, row_counters = schedule_rows_from_events(
                    events, snapshot, exclude
                )
            except (SweepVerdictError, SofaScoreDQViolation) as exc:
                # A verdict about the events of this slice — or a DQ gate that
                # refused them — is banked exactly like the ones above: nothing
                # is written (these rows never passed validation), but the class
                # still records its cursor and its resume queue, because a run
                # that fails before that rebuilds the identical slice for ever
                # (Sol round 27).
                # Only these two: a verdict about the events of this slice, or
                # the DQ gate refusing them.  Anything else is a bug and must
                # not be dressed up as a verdict that banks a cursor (Sol round
                # 28).
                rows, row_counters = [], dict(getattr(exc, "counters", ()) or {})
                row_counters["error"] = str(exc)
                deferred.append(exc)
            report[f"{name}_rows"] = len(rows)
            report[f"{name}_counters"] = row_counters
            if served and not rows and not deferred:
                # The lane asks for the pages of ITS OWN targets, so every page
                # it gets belongs to a ready campaign season: pages without a
                # single row means the snapshot and the source have drifted
                # apart, not a quiet day.  Fixture pages count too — a season
                # whose calendar came back full of another season's events would
                # otherwise move the cursor on and stay green.
                #
                # DEFERRED like the calendar drift above, and for the same
                # reason: ``served`` counts events the fetcher itself drops —
                # a page carrying only a neighbour season's tail
                # (``foreign_season``), a season the snapshot does not know yet
                # — so a thin slice can hit this legitimately, and raising here
                # left the class before its cursor moved.  The next run then
                # rebuilt the identical slice and failed identically, for ever
                # (code review after Sol round 26).
                deferred.append(ValueError(
                    f"{name}: {counters['pages']} tail pages and {served} events "
                    f"produced no schedule row: {row_counters}"
                ))
            if rows:
                report["table"] = write_schedule_rows(rows)
                rows_written += len(rows)
            # The resume queue may only move forward on evidence that is IN
            # Bronze.  A class that read pages and wrote no row (a verdict about
            # its events, the DQ gate) must leave its chains where they were:
            # the fresh entry points at the page AFTER the ones just read, so
            # banking it would skip a prefix that was never written (Sol round
            # 28).  A class that legitimately had nothing to serve is free to
            # move its chains — there is no unwritten prefix to skip.
            queue_safe = bool(rows) or not served
            # This class is done: its cursor may move, and the chains it just
            # walked drop off the retry list while fresh truncations join it.
            # Only the seed phase resumes a saved chain, so only it may retire
            # one: a tail visit reads from page 0 and leaves the rest of the
            # chain owed (Sol round 6, finding 1).  ``pinned`` keeps such a
            # season out of the tail classes, so this is also the only place
            # where a retry target can be attempted at all.
            held = interrupted_runs.get(name, 0)
            if interrupted is None:
                # A class that was walked moves its anchor and forgets the
                # wobbles it survived on the way.
                cursor_state["index"][name] = (
                    list(next_cursors[name]) if next_cursors[name] else None
                )
                held = 0
            else:
                held += 1
                if held >= MAX_INTERRUPTED_RUNS:
                    # The walk keeps being cut short in the same slice: holding
                    # the anchor any longer starves every class after this one
                    # (and every target after this slice) for good.  Move on and
                    # say so; the skipped slice returns on the next lap.
                    cursor_state["index"][name] = (
                        list(next_cursors[name]) if next_cursors[name] else None
                    )
                    report.setdefault("skipped_slices", []).append(name)
                    held = 0
            cursor_state["interrupted_runs"][name] = held
            if name == "seed" and queue_safe:
                attempted.update(retried)
            committed = True
            if queue_safe:
                queued_seasons, abandoned = requeue_chains(
                    unfinished, attempted, unfinished_now, rolled_over
                )
                incomplete_state["seasons"] = queued_seasons
            # A chain the lane has stopped asking for: the rest of that season
            # stays unread, so it goes in the report by ``(tournament, season,
            # page)`` — an operator can put it back by hand, and nothing else
            # would ever say the tail is missing.  Recorded HERE, inside the
            # loop: a deferred verdict raises before the end of the sweep, and
            # writing it after the loop meant the queue was already trimmed on
            # disk while the report said nothing (Sol round 27).
            report["abandoned_chains"] = abandoned
            report["rows_written"] = rows_written
            report["incomplete_seasons"] = len(incomplete_state["seasons"])
            # The emptiness alarm has to be honest on EVERY exit: a run that
            # MERGEd rows and then failed used to write the OLD counter back,
            # so a lane that was working could still trip the alarm, and one
            # that wrote nothing for days could hide behind a stale zero (audit
            # after Sol round 28).
            cursor_state["idle_runs"] = 0 if rows_written else idle_runs + 1
            report["idle_runs"] = cursor_state["idle_runs"]
            # Written NOW, not left to ``finally``: the rows of this class are
            # already in Bronze, which makes their partition "known", and a
            # lease that then fails to close would take the rest of the chain
            # with it — nothing else ever asks for those pages again
            # (cross-check, state lens).
            _atomic_json(Path(args.incomplete), incomplete_state)
            if deferred:
                # Everything this class earned is on disk now.  The verdict is
                # about THIS class and its own cursor, so the classes after it
                # still run: they have their own anchors, and letting a verdict
                # in ``due`` stop ``stale`` and ``seed`` starved them of every
                # run for as long as it lasted (audit after Sol round 28).  The
                # run still fails, on the first verdict, once the sweep is done.
                report[f"{name}_deferred"] = [
                    f"{type(exc).__name__}: {exc}" for exc in deferred
                ]
                verdicts.extend(deferred)
            if interrupted is not None:
                # NOT a verdict: the walk was cut short, so the byte accounting
                # of this run is in doubt and the sweep stops here.  Rows and
                # queue are on disk; the anchor stayed put (unless this class
                # has been stuck too long — see above), so the next run walks
                # this slice again.
                raise interrupted
        report["rows_written"] = rows_written
        report["incomplete_seasons"] = len(incomplete_state["seasons"])
        report["abandoned_chains"] = abandoned
        if verdicts:
            raise verdicts[0]
        # A single empty run is unremarkable; a lane that has written nothing
        # for days is a snapshot that has drifted off the source, and nothing
        # else would ever say so (Sol round 12, finding 1).  The counter is
        # saved either way, so the alarm clears itself the moment a run writes
        # a row again — and it is raised AFTER the state above is built, so
        # the failing run still records everything it did.
        idle_runs = 0 if rows_written else idle_runs + 1
        cursor_state["idle_runs"] = idle_runs
        report["idle_runs"] = idle_runs
        if args.max_idle_runs and idle_runs >= args.max_idle_runs:
            raise ValueError(
                f"the lane has written no schedule row for {idle_runs} runs in "
                f"a row (limit {args.max_idle_runs}): the campaign snapshot and "
                f"the source have drifted apart"
            )
        report["status"] = "success"
        exit_code = 0
    except Exception as exc:
        report["status"] = "failed"
        report["errors"].append(f"{type(exc).__name__}: {exc}")
        exit_code = 1
    finally:
        if client is not None:
            # Close first: the bytes of the open lease are billed to
            # paid_proxy_bytes only when the lease closes.
            try:
                client.close()
            except Exception as exc:
                report["status"] = "failed"
                report["errors"].append(f"{type(exc).__name__}: {exc}")
                exit_code = 1
            report["discovery"] = dict(client.stats)
        # The CURSOR of the classes that DID finish is saved whatever happened
        # afterwards — a later class failing, or the lease failing to close.
        # A run is judged by the work it RECORDED (lesson #11): the rows of
        # those classes are in Bronze, MERGE makes a repeat harmless but not
        # free, and repeating a slice pays for the same pages again while the
        # byte accounting a failed close leaves uncertain stays uncertain
        # either way (Sol round 25).  The unfinished chains are written the
        # same way, right after each MERGE, because their rows already make the
        # partition "known" and nothing else would ever ask for the rest of
        # those chains (Sol round 17, finding 3).
        if committed:
            # The unfinished chains go first and the cursor second: the two
            # renames cannot be one transaction, and a crash between them must
            # leave the sweep repeating a slice (harmless — MERGE) rather than
            # stepping over a chain whose tail nothing else would ask for.
            if incomplete_state is not None:
                _atomic_json(Path(args.incomplete), incomplete_state)
            _atomic_json(Path(args.cursor), cursor_state)
            report["cursor_next"] = cursor_state["index"]
        _atomic_json(Path(args.output), report)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "main",
    "SeasonTarget",
    "current_season_targets",
    "targets_digest",
    "plan_sweep",
    "take_slice",
    "worst_case_pages",
    "worst_case_bytes",
    "SWEEP_CLASSES",
    "read_cursor",
    "read_idle_runs",
    "read_interrupted_runs",
    "MAX_INTERRUPTED_RUNS",
    "read_incomplete",
    "requeue_chains",
    "rolled_over_chains",
    "MAX_CHAIN_ATTEMPTS",
    "sweep_predicates",
    "bronze_partitions",
    "write_schedule_rows",
]
