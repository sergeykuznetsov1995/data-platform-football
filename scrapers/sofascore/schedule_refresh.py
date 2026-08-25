"""Season schedule pages as ``bronze.sofascore_schedule`` evidence.

The all-men refresh lane (#1218, lane F) needs to know which matches of the
~1490 tournaments outside ``competitions.yaml`` have finished, so the existing
match phase can pick them straight from Bronze.

The lane was designed around the source's daily list
(``/sport/football/scheduled-events/{date}``); a live probe on 2026-08-25
answered **404** for that path and for every other by-date shape, while
``/sport/football/events/live`` and the season pages kept answering 200 — the
by-date selection simply does not exist on the SPA host.  The lane therefore
walks season pages instead (``schedule_last``, the class the campaign already
measures), in a fixed order with a cursor so every run covers the next slice.
One visit is not one request: the walk chases further pages while what it reads
is newer than Bronze, a resumed chain reads a page of overlap and may step back
up to ``MAX_BACKTRACK_PAGES``, and the caller adds a ``schedule_next`` page for
the classes that need one.

Every page is fetched through the metered discovery client (source
``sofascore_discovery``) and kept in the raw store first, as the exact
response bytes the browser received.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Collection, Iterable, Mapping, Optional

from dags.utils.sofascore_dq import validate_schedule_rows
from scrapers.sofascore.camoufox_capture import normalize_event
from scrapers.sofascore.discovery import DiscoveryHTTPError, DiscoverySchemaError
from scrapers.sofascore.raw_store import (
    PayloadTarget,
    RawPayloadRecord,
    RawPayloadStore,
)


log = logging.getLogger(__name__)

SCHEDULE_PAGE_ENDPOINT = "schedule_last"
FIXTURE_PAGE_ENDPOINT = "schedule_next"
SCHEDULE_PAGE_TARGET_TYPE = "season_page"
SCHEDULE_PAGE_FRESHNESS_KEY = "refresh"
_REQUEST_BASE_URL = "https://www.sofascore.com/api/v1"
# A season page may carry an event that cannot be attributed to a tournament
# (no ``tournament.uniqueTournament.id``, or not an object).  One such event
# must not fail a slice of hundreds of tournaments, but a slice where they
# dominate is schema drift, not exotica.
_UNSCOPED_FAIL_SHARE = 0.5
# A season that has not started answers 404 — normal for a handful of targets,
# but a slice where absences dominate means the snapshot or the source moved.
_MISSING_FAIL_SHARE = 0.5
# A page that breaks the endpoint contract costs its season, not the slice —
# but a slice where such pages dominate is schema drift, not one odd season.
_MALFORMED_FAIL_SHARE = 0.5
# ...over at least this many broken seasons.  A share taken over ONE target is
# not evidence of anything, and the failure it causes is forever: the class
# commits its cursor only after it is walked, so the next run rebuilds the same
# slice and breaks on the same page, while a queued chain never reaches the
# caller that ages it out.  Two broken seasons are the least that can be called
# a pattern; a slice that serves nothing at all is caught by the lane's
# ``idle_runs`` guard instead (Sol round 24).
_FAIL_MIN_TARGETS = 2
# How far a resumed chain may step back when the source has repacked its pages
# under it: three pages is ~90 matches of overlap, far more than a repack drops.
MAX_BACKTRACK_PAGES = 3


class DailyEventsSchemaError(DiscoverySchemaError):
    """Season-page events no longer carry the tournament/season/id shape we map."""


class ScheduleSweepError(DiscoverySchemaError):
    """The sweep itself is unsound: the source served almost no season page."""


@dataclass(frozen=True)
class FetchedEvent:
    """One source event together with the raw record of the list it came from.

    ``event`` is whatever the list element was; anything but a mapping with
    the expected ids is classified as malformed downstream.
    """

    event: Any
    raw: RawPayloadRecord


def schedule_page_path(tournament_id: int, season_id: int, page: int = 0) -> str:
    """Path of one ``schedule_last`` page (the freshest finished events)."""

    return (
        f"/unique-tournament/{int(tournament_id)}"
        f"/season/{int(season_id)}/events/last/{int(page)}"
    )


def fixture_page_path(tournament_id: int, season_id: int, page: int = 0) -> str:
    """Path of one ``schedule_next`` page (the next scheduled events)."""

    return (
        f"/unique-tournament/{int(tournament_id)}"
        f"/season/{int(season_id)}/events/next/{int(page)}"
    )


def fetch_season_fixtures(
    client: Any,
    targets: Iterable[tuple[int, int]],
    raw_store: RawPayloadStore,
) -> tuple[list[FetchedEvent], dict[str, int]]:
    """Fetch the first ``schedule_next`` page of every target.

    ``schedule_last`` only ever shows matches that have finished, so a season
    whose next round is a week away is invisible to a window query over Bronze.
    One fixture page per visit puts the upcoming matches in Bronze, which is
    what lets the ``due`` window open on the day they are played — and what
    makes a season that has not kicked off yet visible at all.  Both consumers
    of ``bronze.sofascore_schedule`` select ``status_type = 'finished'``, so the
    scheduled rows sit there without giving anyone work.

    A season with no fixtures answers 404 (or an empty list) — normal, counted
    in ``missing``, never fatal: this page is an extra on top of the tail walk,
    and losing it must not cost the run.
    """

    fetched: list[FetchedEvent] = []
    counters = {
        "targets": 0, "pages": 0, "events": 0, "missing": 0, "foreign_season": 0,
        "truncated": 0, "malformed": 0,
    }
    for tournament_id, season_id in targets:
        counters["targets"] += 1
        path = fixture_page_path(tournament_id, season_id)
        try:
            body, payload = client.get_json_bytes(path)
        except DiscoveryHTTPError as exc:
            if exc.status_code != 404:
                raise
            counters["missing"] += 1
            continue
        record = raw_store.store_bytes(
            PayloadTarget(
                source_tournament_id=str(int(tournament_id)),
                source_season_id=str(int(season_id)),
                target_type=SCHEDULE_PAGE_TARGET_TYPE,
                target_id="next-0",
                endpoint=FIXTURE_PAGE_ENDPOINT,
                freshness_key=SCHEDULE_PAGE_FRESHNESS_KEY,
            ),
            body,
            request_url=f"{_REQUEST_BASE_URL}{path}",
            http_status=200,
        )
        events = payload.get("events") if isinstance(payload, Mapping) else None
        has_next = payload.get("hasNextPage") if isinstance(payload, Mapping) else None
        if not isinstance(events, list) or not isinstance(has_next, bool):
            # Both are required paths of this endpoint, and both are checked
            # for the same reason: a contract this page silently breaks is
            # drift, not a quiet calendar (Sol round 20).  Per SEASON, not per
            # slice: this walk runs after the tail pages of the same class have
            # been paid for and before their MERGE, so one permanently broken
            # calendar used to cancel all of them, every run (Sol round 21).
            counters["malformed"] += 1
            log.warning(
                "calendar page breaks its contract, skipping: %s (%r/%r)",
                path, type(events).__name__, has_next,
            )
            continue
        if has_next:
            # ONE fixture page on purpose: this walk exists to open the ``due``
            # window, and the matches beyond the first page are far enough away
            # that the next visit will carry them.  Counted so the operator can
            # see how often a calendar is longer than one page — the budget is
            # sized for one.
            counters["truncated"] += 1
        counters["pages"] += 1
        for event in events:
            # Counted before the season filter: a page that served events and
            # produced no row at all is the caller's evidence of drift, and
            # dropping them all as ``foreign_season`` must not look like an
            # empty calendar (Sol round 7, finding 4).
            counters["events"] += 1
            event_season = _nested_int(event, "season", "id")
            if event_season is not None and event_season != int(season_id):
                counters["foreign_season"] += 1
                continue
            fetched.append(FetchedEvent(event=event, raw=record))
    # Per season the broken contract costs only that calendar (above), but a
    # slice where such pages dominate is schema drift and has to fail here:
    # a calendar page that breaks its contract writes NO row, so a slice of
    # nothing but broken calendars ends with ``events`` at zero — which is the
    # legitimate answer of a slice whose seasons simply have no fixture, and
    # the caller cannot tell the two apart.  The ``due`` window opens off these
    # rows alone, so without this the lane would keep paying for a required
    # endpoint (``endpoint_coverage.yaml``) that has stopped answering and stay
    # green while every future match went missing (Sol round 22, finding 2).
    # The denominator is the seasons that ANSWERED: a season with no fixture
    # answers 404, and before a season starts those are most of a seed slice —
    # counting them would make one broken calendar look like drift.  And the
    # share has to be exceeded, not merely reached: one broken calendar beside
    # one good one is the per-season cost round 21 asked for, not a slice going
    # bad.
    answered = counters["pages"] + counters["malformed"]
    if (
        counters["malformed"] >= _FAIL_MIN_TARGETS
        and counters["malformed"] > answered * _MALFORMED_FAIL_SHARE
    ):
        raise ScheduleSweepError(
            f"{counters['malformed']} of {answered} calendar pages that were "
            f"served broke the endpoint contract: {counters}"
        )
    return fetched, counters


def fetch_season_schedules(
    client: Any,
    targets: Iterable[tuple[int, int]],
    raw_store: RawPayloadStore,
    *,
    max_pages: int = 1,
    start_pages: Optional[Mapping[tuple[int, int], int]] = None,
    resume_anchors: Optional[Mapping[tuple[int, int], int]] = None,
    chase_before: Optional[Mapping[tuple[int, int], int]] = None,
    owed_pages: Optional[Collection[tuple[int, int]]] = None,
    missing_fail_share: Optional[float] = _MISSING_FAIL_SHARE,
) -> tuple[list[FetchedEvent], dict[str, int], list[tuple[int, int, int, int]]]:
    """Fetch the ``schedule_last`` chain of every ``(tournament_id, season_id)``.

    ``max_pages`` bounds the chain: one page is the freshest tail (a season
    already in Bronze), a larger bound walks ``hasNextPage`` to fill a season
    Bronze has never seen.  A chain cut short by the bound is counted in
    ``truncated`` and returned as the third value — ``(tournament, season,
    next_page, anchor)``, the anchor being the oldest kick-off this visit saw,
    which is what tells the resumed walk whether the source repacked its pages
    underneath it — so the caller can finish it on the next run: the first pages
    already make the partition look known, and starting the chain over from
    page 0 every run would never reach a season longer than the bound.
    ``start_pages`` is where such a resumed chain picks up.  A resumed chain
    whose page breaks the contract is returned in that same list at the page it
    still owes — the caller is what counts the attempts and eventually gives
    up (Sol round 22, finding 1).

    ``chase_before`` makes a one-page visit self-correcting: it maps a target to
    the newest kick-off Bronze already has, and the walk goes on while the page
    it just read is entirely NEWER than that (the matches in between have to be
    on the next page).  Without it a season that finished more than a page of
    matches since its last visit would silently lose the overflow — page 0 is
    only the freshest 30.  ``max_pages`` is the bound on that chase.

    The exact response bytes are stored before the payload is validated so a
    schema surprise from the source leaves replayable evidence behind.

    A season the source does not serve (404 — the usual answer for a season
    that has not started) is counted in ``missing`` and skipped: one absent
    season must not fail a slice of hundreds.  A slice where absences dominate
    is not "nothing to do", it is the source or the campaign snapshot drifting,
    and it fails the run — but only absences of the seasons in ``owed_pages``,
    the ones Bronze holds a match of whose kick-off has passed.  A season whose
    first match is still ahead answers 404 by definition, and before the season
    starts those dominate a slice; the old share over ALL targets would have
    failed the lane through every pre-season (Sol round 6, finding 2).  The list
    is passed in rather than derived from ``chase_before``, whose anchor exists
    only once a match is FINISHED: a season stuck at 404 after its first
    kick-off would never earn a finished row, never enter the denominator, and
    stay green forever (Sol round 7, finding 2).  ``missing_fail_share=None``
    disables the check outright for a slice where absences are the normal
    answer — the
    seed slice is full of seasons that have not kicked off, and failing it would
    freeze the cursor on them forever.  Any other transport failure propagates:
    it is a property of the run, not of the season.

    ``schedule_last`` is the season's own page, so an event of another season
    (the source occasionally carries a neighbour's tail) is dropped and counted
    in ``foreign_season`` — Bronze rows must match the season that was asked
    for.
    """

    fetched: list[FetchedEvent] = []
    incomplete: list[tuple[int, int, int, int]] = []
    starts = dict(start_pages or {})
    anchors = dict(resume_anchors or {})
    counters = {
        "targets": 0, "expected": 0, "pages": 0, "events": 0, "missing": 0,
        "missing_expected": 0, "empty_expected": 0, "truncated": 0,
        "foreign_season": 0, "resumed": 0, "chased": 0, "malformed": 0, "chase_settled": 0,
        "backtracked": 0, "malformed_resumed": 0,
    }
    # Chains resumed from the retry queue, and how many of them broke.  They are
    # kept OUT of the drift threshold below: a queued chain whose page breaks is
    # a poison the caller already tracks, one visit at a time, and failing the
    # sweep on it means the fetcher never returns the queue entry — so the
    # attempt count never grows, the chain never ages out and the same page is
    # paid for on every run for ever.  With the last such chain alone in a slice
    # that reads "1 of 1" and fails the lane outright (Sol round 23).
    resumed_targets = 0
    resumed_malformed = 0
    for tournament_id, season_id in targets:
        counters["targets"] += 1
        pair = (int(tournament_id), int(season_id))
        # A target is "expected" when the source owes it a page: Bronze holds a
        # match of it whose kick-off has passed (``owed_pages``).  A season
        # whose first match is still ahead legitimately answers 404, and before
        # a season starts those dominate a slice.  The caller decides; with no
        # list at all every target owes a page.
        expected = owed_pages is None or pair in owed_pages
        counters["expected"] += int(expected)
        # Everything this season goes on to count is rolled back with its rows
        # if one of its pages breaks the contract (below), so the counters the
        # caller reads describe only what actually survived the sweep.
        counters_before = dict(counters)
        # ``start_pages`` holds the page the chain owes.  The walk starts ONE
        # page earlier (the source repacks its pages, and a page fetched twice
        # is a MERGE no-op), but the bound is counted from the owed page — an
        # overlap that ate the budget made ``max_pages=1`` walk the same page
        # forever, writing back the very page it started from.
        owed_page = max(0, int(starts.get(pair, 0)))
        resumed_targets += int(bool(owed_page))
        first = max(0, owed_page - 1) if owed_page else 0
        anchor = int(anchors.get(pair, 0))
        stop = owed_page + max(1, int(max_pages))
        backtracked = 0
        if first:
            counters["resumed"] += 1
        # The chain's oldest match seen in THIS visit: it becomes the anchor of
        # the next one, which is how a resumed chain knows whether the source
        # repacked its pages underneath it.
        chain_oldest: Optional[int] = None
        # Pages this visit has already read: after a step back the walk goes
        # forward again, and re-fetching what it just read would be paid twice.
        seen: set[int] = set()
        # Where this season's events start in the shared list, so a broken page
        # can take the whole season's prefix out with it.
        collected = len(fetched)
        page = first - 1
        bounded = False
        try:
            while True:
                page += 1
                if page >= stop:
                    bounded = True
                    break
                if page in seen:
                    continue
                path = schedule_page_path(tournament_id, season_id, page)
                try:
                    body, payload = client.get_json_bytes(path)
                except DiscoveryHTTPError as exc:
                    if exc.status_code != 404:
                        raise
                    # Only the head of a chain says "this season does not exist";
                    # a 404 further in means the chain simply ended (it can even be
                    # shorter than last run, the source drops pages as it repacks).
                    if page == first == 0:
                        counters["missing"] += 1
                        counters["missing_expected"] += int(expected)
                        log.info("season page absent at source: %s", path)
                    break
                record = raw_store.store_bytes(
                    PayloadTarget(
                        source_tournament_id=str(int(tournament_id)),
                        source_season_id=str(int(season_id)),
                        target_type=SCHEDULE_PAGE_TARGET_TYPE,
                        target_id=f"last-{page}",
                        endpoint=SCHEDULE_PAGE_ENDPOINT,
                        freshness_key=SCHEDULE_PAGE_FRESHNESS_KEY,
                    ),
                    body,
                    request_url=f"{_REQUEST_BASE_URL}{path}",
                    http_status=200,
                )
                events = payload.get("events") if isinstance(payload, Mapping) else None
                if not isinstance(events, list):
                    raise DiscoverySchemaError(f"{path} has no events list")
                # Validated HERE, next to the events list, and not where the walk
                # decides to go on: the backtracking branch below ``continue``s
                # first and puts the page in ``seen``, so a page whose contract is
                # broken slipped through unchecked and was never looked at again
                # (Sol round 20).
                has_next = payload.get("hasNextPage")
                if not isinstance(has_next, bool):
                    # ``/hasNextPage`` is a required path of this endpoint
                    # (``endpoint_coverage.yaml``) and the ONLY thing that says a
                    # chain goes on.  Reading a missing or non-boolean field as "no
                    # more pages" ended the walk quietly, with ``truncated`` at zero
                    # and nothing queued to resume (Sol round 19).
                    raise DailyEventsSchemaError(
                        f"{path} has no boolean hasNextPage: {has_next!r}"
                    )
                counters["pages"] += 1
                counters["events"] += len(events)
                seen.add(page)
                if not events and page == first == 0 and expected:
                    # The season holds a match that has already kicked off, so its
                    # freshest page cannot be empty.  An empty 200 is the same
                    # evidence of drift a 404 would be — and unlike a 404 it used to
                    # pass silently (Sol round 9, finding 2).
                    counters["empty_expected"] += 1
                    log.warning("season page is empty though a match was played: %s", path)
                oldest: Optional[int] = None
                newest: Optional[int] = None
                for event in events:
                    event_season = _nested_int(event, "season", "id")
                    if event_season is not None and event_season != int(season_id):
                        counters["foreign_season"] += 1
                        continue
                    kick_off = _nested_int(event, "startTimestamp")
                    if kick_off is not None:
                        if oldest is None or kick_off < oldest:
                            oldest = kick_off
                        if newest is None or kick_off > newest:
                            newest = kick_off
                    # Non-object elements are kept: ``schedule_rows_from_events``
                    # counts them as malformed rather than silently dropping them.
                    fetched.append(FetchedEvent(event=event, raw=record))
                if oldest is not None and (chain_oldest is None or oldest < chain_oldest):
                    chain_oldest = oldest
                if (
                    page == first > 0
                    and anchor
                    and newest is not None
                    and newest < anchor
                    # The allowance is per target: a slice walks many chains, and
                    # the first one to be repacked must not spend the steps the
                    # later ones need (Sol round 6, finding 3).
                    and backtracked < MAX_BACKTRACK_PAGES
                ):
                    # The whole page is older than what this chain had already read,
                    # so the source dropped pages and the resume point jumped over
                    # the matches in between.  Step back a page and read again — a
                    # page fetched twice is a MERGE no-op, a page skipped is lost
                    # (Sol round 5, finding 6).
                    backtracked += 1
                    counters["backtracked"] += 1
                    first = max(0, first - 1)
                    page = first - 1
                    continue
                if not has_next:
                    break
                if chase_before is not None:
                    # Stop as soon as the page reaches back into what Bronze
                    # already knows: everything older than that is already there.
                    known_until = chase_before.get(pair)
                    if known_until is None or oldest is None or oldest <= known_until:
                        counters["chase_settled"] += 1
                        break
                    counters["chased"] += 1
        except DiscoverySchemaError as exc:
            # One season whose page breaks the contract must not throw away
            # the pages the rest of the slice has already paid for, and must
            # not be able to block the lane for good: a permanently broken
            # page would fail every run before the class ever MERGEd, so the
            # other targets were never refreshed either (Sol round 20).  The
            # season is skipped and counted; a slice where such pages dominate
            # still fails, below.
            #
            # Everything this season DID serve is dropped with it (Sol round
            # 21): a valid page 0 followed by a broken page 1 would otherwise
            # write half a season, which makes its partition ``known`` and
            # takes it out of ``seed`` for good — the pages after the break
            # would never be asked for again.  Nothing written, nothing known,
            # and the season stays in the class that walks whole chains.  Its
            # counters go back with its rows: ``events`` survived the rollback
            # and the caller reads it as "the source served this class", so a
            # tolerated per-season break made the whole class fail as "pages
            # produced no schedule row" and pinned the cursor on that slice
            # (Sol round 22, finding 3).
            #
            # A RESUMED chain is queued again, at the page it already owed: its
            # earlier pages are in Bronze, which makes the partition ``known``,
            # so nothing but this queue would ever ask for the rest of it and
            # dropping the entry lost the tail of that season for good (Sol
            # round 22, finding 1).  The caller counts the attempts and gives
            # up on a chain that keeps breaking in the same place, which is
            # what keeps a permanently broken page from being paid for forever.
            # A chain that owes page 0 needs no entry: nothing of it is in
            # Bronze, so its partition stays unknown and the seed class walks
            # it again on its own.
            del fetched[collected:]
            counters.clear()
            counters.update(counters_before)
            counters["malformed"] += 1
            if owed_page:
                incomplete.append((pair[0], pair[1], owed_page, anchor))
                resumed_malformed += 1
                counters["malformed_resumed"] += 1
            log.warning("season page breaks its contract, skipping: %s", exc)
            continue
        if bounded:
            counters["truncated"] += 1
            incomplete.append((pair[0], pair[1], stop, chain_oldest or 0))
    # An absent page and an empty one are the same evidence, so they share one
    # threshold: a single season of a slice of hundreds must not throw away the
    # pages everybody else already paid for, and a slice where most of the owed
    # seasons served nothing is the source or the snapshot drifting.
    # Over the targets that are NOT resumed chains, for the reason above: a
    # queued chain that breaks has to reach the caller, which is what ages it
    # out.  Drift of the source itself shows up here all the same — it breaks
    # the pages of the seasons this slice visits for the first time too.
    fresh_targets = counters["targets"] - resumed_targets
    fresh_malformed = counters["malformed"] - resumed_malformed
    if (
        fresh_malformed >= _FAIL_MIN_TARGETS
        and fresh_malformed >= fresh_targets * _MALFORMED_FAIL_SHARE
    ):
        raise ScheduleSweepError(
            f"{fresh_malformed} of {fresh_targets} season pages "
            f"broke the endpoint contract: {counters}"
        )
    unserved = counters["missing_expected"] + counters["empty_expected"]
    # The two kinds of unusable target share a threshold as well as having one
    # each: a slice of 49 broken pages, 49 unserved owed seasons and 2 good
    # ones passed BOTH checks and then cleared the lane's emptiness alarm with
    # its two rows, so 98 % of the slice being unusable read as a healthy run
    # (Sol round 21).  ``missing_fail_share=None`` still turns the whole
    # question off for ``seed``, where absences are the normal answer.

    if (
        missing_fail_share is not None
        # The same minimum mass, for the same reason: the denominator here is
        # the seasons that OWE a page, not the size of the slice, so a slice of
        # 200 with one owed season that answers nothing reads "1 of 1".  And it
        # is forever — a season that has ever had a finished match owes a page
        # for good (``played``), so it stays in ``stale`` and keeps failing the
        # class before its cursor moves (Sol round 25).
        and unserved >= _FAIL_MIN_TARGETS
        and counters["expected"]
        and unserved >= counters["expected"] * missing_fail_share
    ):
        raise ScheduleSweepError(
            f"{unserved} of {counters['expected']} seasons that owe a page "
            f"served none (missing or empty): {counters}"
        )
    # And the two kinds of unusable target share a threshold as well as having
    # one each.  A slice of 49 broken pages, 49 unserved owed seasons and 2 good
    # ones passed BOTH checks above and then cleared the lane's emptiness alarm
    # with its two rows, so 98 % of the slice being unusable read as a healthy
    # run (Sol round 21).  The denominator is the seasons the source owes an
    # answer for: one that legitimately has nothing to say is evidence of
    # nothing, either way.
    # The minimum mass applies here too, and for the same reason: one unusable
    # season out of one that owed a page would fail the class before its cursor
    # moved, and the next run would rebuild the very same slice for ever (Sol
    # rounds 24 and 25).
    unusable = fresh_malformed + unserved
    if (
        missing_fail_share is not None
        and unusable >= _FAIL_MIN_TARGETS
        and counters["expected"]
        and unusable >= counters["expected"] * missing_fail_share
    ):
        raise ScheduleSweepError(
            f"{unusable} of {counters['expected']} seasons that owe a page "
            f"were unusable (broken pages or none served): {counters}"
        )
    return fetched, counters, incomplete


def _identity(row: Mapping[str, Any]) -> tuple:
    """What may never change between two copies of the same game."""

    return (
        row.get("league"),
        row.get("season"),
        row.get("home_team_id"),
        row.get("away_team_id"),
    )


def _nested_int(value: Any, *keys: str) -> Optional[int]:
    for key in keys:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _snapshot_index(
    snapshot: Mapping[str, Any],
) -> dict[int, tuple[str, dict[int, str]]]:
    """Map ready ``unique_tournament_id`` -> (capture_key, {season_id: canonical})."""

    index: dict[int, tuple[str, dict[int, str]]] = {}
    for tournament in snapshot.get("tournaments", ()):
        if tournament.get("metadata_status") != "ready":
            continue
        seasons = {
            int(season["source_season_id"]): str(season["canonical_season"])
            for season in tournament.get("seasons", ())
            if season.get("canonical_season")
        }
        index[int(tournament["unique_tournament_id"])] = (
            str(tournament["capture_key"]),
            seasons,
        )
    return index


def schedule_rows_from_events(
    events: Iterable[FetchedEvent],
    snapshot: Mapping[str, Any],
    exclude_leagues: Collection[int],
) -> tuple[list[dict], dict[str, int]]:
    """Turn season-page events into schedule rows for ready snapshot tournaments.

    ``exclude_leagues`` holds the source ``unique_tournament_id`` values of the
    configured leagues that the daily ingest already covers.  Those events and
    the ones of tournaments absent from the snapshot (other genders, youth,
    amateur) are out of scope and counted in ``excluded``; a ``season.id`` the
    snapshot does not know yet is counted in ``unknown_seasons`` for the
    metadata wave to pick up.  Scope is decided first, from
    ``tournament.uniqueTournament.id`` alone: an event that cannot be placed
    (not an object, or no tournament id) is ``unscoped`` — tolerated with a
    warning as long as such events stay a minority (``_UNSCOPED_FAIL_SHARE``),
    because a season page legitimately carries the odd entry whose tournament
    the source does not attribute; a slice they dominate is schema drift and
    fails the sweep.  An event of a ready snapshot tournament that lacks ``id``
    or ``season.id`` is ``malformed`` and fails the sweep unconditionally (the
    raw page is already kept).  A game seen more than once — the tail page and
    the fixture page of the same season can both carry it, and a resumed chain
    re-reads one page of overlap — keeps its last, freshest copy, unless the
    two copies disagree about which teams are playing: then the FIRST is kept
    and the disagreement counted, because last-wins would glue two different
    matches together under one game id.
    """

    index = _snapshot_index(snapshot)
    excluded_ids = {int(value) for value in exclude_leagues}
    counters = {
        "events": 0,
        "matched": 0,
        "excluded": 0,
        "unknown_seasons": 0,
        "unscoped": 0,
        "malformed": 0,
        "identity_conflict": 0,
    }
    rows_by_game: dict[int, dict] = {}
    for item in events:
        counters["events"] += 1
        tournament_id = _nested_int(item.event, "tournament", "uniqueTournament", "id")
        if tournament_id is None:
            counters["unscoped"] += 1
            continue
        entry = None if tournament_id in excluded_ids else index.get(tournament_id)
        if entry is None:
            counters["excluded"] += 1
            continue
        event_id = _nested_int(item.event, "id")
        season_id = _nested_int(item.event, "season", "id")
        if event_id is None or season_id is None:
            counters["malformed"] += 1
            continue
        capture_key, seasons = entry
        canonical_season = seasons.get(season_id)
        if canonical_season is None:
            counters["unknown_seasons"] += 1
            continue
        row = normalize_event(dict(item.event))
        row.update(
            {
                "league": capture_key,
                "season": canonical_season,
                "source_tournament_id": str(tournament_id),
                "source_season_id": str(season_id),
                "raw_content_hash": item.raw.content_hash,
                "raw_blob_key": item.raw.blob_key,
                "raw_request_url": item.raw.request_url,
                "raw_fetched_at": item.raw.fetched_at,
                "raw_endpoint": item.raw.endpoint,
                "raw_target_id": item.raw.target_id,
            }
        )
        previous = rows_by_game.get(row["game_id"])
        if previous is not None and _identity(previous) != _identity(row):
            # Same game id, different match.  The freshest copy wins only when
            # the two copies are the SAME game — a repeat is normal (the tail
            # page and the calendar overlap, and a resumed chain re-reads a
            # page), but a repeat that disagrees about who is playing is the
            # source contradicting itself, and last-wins would silently glue
            # two matches together (Sol round 21).  The first copy is kept, the
            # disagreement is counted, and a slice full of them fails below.
            counters["identity_conflict"] += 1
            log.warning(
                "two different matches share game_id %s: %s vs %s",
                row["game_id"], _identity(previous), _identity(row),
            )
            continue
        rows_by_game[row["game_id"]] = row
        counters["matched"] += 1
    if counters["malformed"]:
        raise DailyEventsSchemaError(
            f"{counters['malformed']} of {counters['events']} season-page events of "
            f"ready tournaments lack id or season.id: {counters}"
        )
    if counters["identity_conflict"] and (
        counters["identity_conflict"]
        >= (counters["matched"] + counters["identity_conflict"]) * _UNSCOPED_FAIL_SHARE
    ):
        raise DailyEventsSchemaError(
            f"{counters['identity_conflict']} of "
            f"{counters['matched'] + counters['identity_conflict']} games came "
            f"back twice with different teams: {counters}"
        )
    if counters["unscoped"] and (
        counters["unscoped"] >= counters["events"] * _UNSCOPED_FAIL_SHARE
    ):
        raise DailyEventsSchemaError(
            f"{counters['unscoped']} of {counters['events']} season-page events cannot "
            f"be placed (no object or tournament.uniqueTournament.id): {counters}"
        )
    if counters["unscoped"]:
        log.warning(
            "%d of %d season-page events could not be placed in a tournament: %s",
            counters["unscoped"],
            counters["events"],
            counters,
        )
    if counters["events"] and not counters["matched"]:
        # Legitimate (a slice whose seasons all belong to configured leagues
        # or to tournaments outside the campaign), but a "green and empty"
        # outcome must be visible.
        log.warning(
            "no season-page event matched a ready campaign tournament: %s", counters
        )
    rows = list(rows_by_game.values())
    validate_schedule_rows(rows).require()
    return rows, counters
