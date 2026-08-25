"""Season schedule pages as ``bronze.sofascore_schedule`` evidence (lane F)."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path

import pytest
from pyarrow import fs

from dags.utils.sofascore_dq import SofaScoreDQViolation
from scrapers.sofascore.schedule_refresh import (
    SCHEDULE_PAGE_ENDPOINT,
    DailyEventsSchemaError,
    ScheduleSweepError,
    FIXTURE_PAGE_ENDPOINT,
    MAX_BACKTRACK_PAGES,
    fetch_season_fixtures,
    fetch_season_schedules,
    fixture_page_path,
    schedule_page_path,
    schedule_rows_from_events,
)
from scrapers.sofascore.discovery import DiscoveryHTTPError
from scrapers.sofascore.raw_store import PayloadTarget, RawPayloadStore


FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"

READY_TOURNAMENT = 7
CONFIGURED_TOURNAMENT = 17
EXCLUDED_TOURNAMENT = 799
FOREIGN_TOURNAMENT = 555

READY_SEASON = 96518
READY_PREVIOUS_SEASON = 76953
CONFIGURED_SEASON = 76986

SNAPSHOT = {
    "schema_version": 1,
    "tournaments": [
        {
            "capture_key": f"SS-{READY_TOURNAMENT}",
            "unique_tournament_id": READY_TOURNAMENT,
            "metadata_status": "ready",
            "seasons": [
                {
                    "canonical_season": "2627",
                    "metadata_status": "pending",
                    "source_season_id": READY_SEASON,
                    "start_year": 2026,
                },
                {
                    "canonical_season": "2526",
                    "metadata_status": "ready",
                    "source_season_id": READY_PREVIOUS_SEASON,
                    "start_year": 2025,
                },
            ],
        },
        {
            "capture_key": f"SS-{CONFIGURED_TOURNAMENT}",
            "unique_tournament_id": CONFIGURED_TOURNAMENT,
            "metadata_status": "ready",
            "seasons": [
                {
                    "canonical_season": "2526",
                    "metadata_status": "ready",
                    "source_season_id": CONFIGURED_SEASON,
                    "start_year": 2025,
                }
            ],
        },
        {
            "capture_key": f"SS-{EXCLUDED_TOURNAMENT}",
            "unique_tournament_id": EXCLUDED_TOURNAMENT,
            "metadata_status": "excluded",
            "seasons": [
                {
                    "canonical_season": "2526",
                    "metadata_status": "pending",
                    "source_season_id": 76139,
                    "start_year": 2025,
                }
            ],
        },
    ],
}

READY_TARGET = (READY_TOURNAMENT, READY_SEASON)
PREVIOUS_TARGET = (READY_TOURNAMENT, READY_PREVIOUS_SEASON)
CONFIGURED_TARGET = (CONFIGURED_TOURNAMENT, CONFIGURED_SEASON)


def _fixture_events() -> list[dict]:
    payload = json.loads(
        (FIXTURES / "sofascore_season_76986_schedule_last_0.json").read_text(
            encoding="utf-8"
        )
    )
    return payload["events"]


def _event(
    template: dict, *, event_id: int, tournament_id: int, season_id: int,
    kick_off: int | None = None,
):
    event = json.loads(json.dumps(template))
    event["id"] = event_id
    event["tournament"] = {"uniqueTournament": {"id": tournament_id}}
    event["season"] = {"id": season_id, "name": f"season {season_id}"}
    if kick_off is not None:
        event["startTimestamp"] = kick_off
    return event


def _page(*events, has_next: bool = False) -> dict:
    return {"events": list(events), "hasNextPage": has_next}


def _sweep_payloads() -> dict[str, dict]:
    first, second = _fixture_events()
    return {
        schedule_page_path(*READY_TARGET): _page(
            _event(first, event_id=1, tournament_id=READY_TOURNAMENT,
                   season_id=READY_SEASON),
            # A neighbour season's tail on the same page: dropped by the fetch,
            # because Bronze rows must match the season that was asked for.
            _event(second, event_id=3, tournament_id=READY_TOURNAMENT,
                   season_id=424242),
        ),
        schedule_page_path(*CONFIGURED_TARGET): _page(
            _event(second, event_id=2, tournament_id=CONFIGURED_TOURNAMENT,
                   season_id=CONFIGURED_SEASON),
        ),
        schedule_page_path(*PREVIOUS_TARGET): _page(
            _event(second, event_id=6, tournament_id=READY_TOURNAMENT,
                   season_id=READY_PREVIOUS_SEASON),
            _event(first, event_id=7, tournament_id=FOREIGN_TOURNAMENT,
                   season_id=READY_PREVIOUS_SEASON),
        ),
    }


class _Client:
    """Metered browser client stub: the wire bytes are NOT the canonical dump."""

    def __init__(self, payloads, missing=()):
        self.payloads = payloads
        self.missing = set(missing)
        self.paths: list[str] = []

    def body(self, path) -> bytes:
        return json.dumps(self.payloads[path], indent=1).encode("utf-8")

    def get_json(self, path):
        self.paths.append(path)
        if path in self.missing:
            raise DiscoveryHTTPError(
                f"metered browser request failed: HTTP 404 {path}", status_code=404
            )
        return self.payloads[path]

    def get_json_bytes(self, path):
        payload = self.get_json(path)
        return self.body(path), payload


def _store(tmp_path):
    return RawPayloadStore(fs.LocalFileSystem(), str(tmp_path / "raw"))


def test_fetch_stores_every_season_page_and_returns_events_with_lineage(tmp_path):
    payloads = _sweep_payloads()
    client = _Client(payloads)
    store = _store(tmp_path)

    fetched, counters, _ = fetch_season_schedules(
        client, [READY_TARGET, CONFIGURED_TARGET, PREVIOUS_TARGET], store
    )

    assert client.paths == list(payloads)
    # Event 3 belongs to a neighbour season and never leaves the fetch.
    assert [item.event["id"] for item in fetched] == [1, 2, 6, 7]
    assert counters == {
        # Nothing is known about what Bronze holds, so every target is expected
        # to serve its page — that is the strict reading of the absence share.
        "targets": 3, "expected": 3, "pages": 3, "events": 5, "missing": 0,
        "missing_expected": 0, "empty_expected": 0, "truncated": 0,
        "foreign_season": 1, "resumed": 0, "chased": 0, "chase_settled": 0,
        "backtracked": 0, "malformed": 0, "malformed_resumed": 0,
    }
    first = fetched[0].raw
    assert first.endpoint == SCHEDULE_PAGE_ENDPOINT
    assert first.target_type == "season_page"
    assert first.target_id == "last-0"
    assert first.source_tournament_id == str(READY_TOURNAMENT)
    assert first.source_season_id == str(READY_SEASON)
    assert first.request_url.endswith(schedule_page_path(*READY_TARGET))
    stored, record = store.load_json(
        PayloadTarget(
            source_tournament_id=str(READY_TOURNAMENT),
            source_season_id=str(READY_SEASON),
            target_type="season_page",
            target_id="last-0",
            endpoint=SCHEDULE_PAGE_ENDPOINT,
            freshness_key=first.freshness_key,
        )
    )
    assert stored == payloads[schedule_page_path(*READY_TARGET)]
    assert record.content_hash == first.content_hash


def test_fetch_keeps_the_exact_response_bytes_as_raw_evidence(tmp_path):
    # Coverage contract: raw lineage is the HTTP body as received, so the
    # blob and its content hash witness the source answer byte for byte.
    payloads = _sweep_payloads()
    client = _Client(payloads)
    store = _store(tmp_path)

    fetched, _, _ = fetch_season_schedules(client, [READY_TARGET], store)

    path = schedule_page_path(*READY_TARGET)
    body, record = store.load_bytes(
        PayloadTarget(
            source_tournament_id=str(READY_TOURNAMENT),
            source_season_id=str(READY_SEASON),
            target_type="season_page",
            target_id="last-0",
            endpoint=SCHEDULE_PAGE_ENDPOINT,
            freshness_key="refresh",
        )
    )
    assert body == client.body(path)
    assert record.content_hash == fetched[0].raw.content_hash
    assert record.content_hash == hashlib.sha256(client.body(path)).hexdigest()


def test_fetch_walks_the_page_chain_up_to_the_bound(tmp_path):
    # A season Bronze has never seen needs more than the freshest 30 events:
    # the chain is walked while ``hasNextPage`` holds and the bound allows.
    first, second = _fixture_events()
    payloads = {
        schedule_page_path(*READY_TARGET, page): _page(
            _event(first, event_id=100 + page, tournament_id=READY_TOURNAMENT,
                   season_id=READY_SEASON),
            has_next=page < 2,
        )
        for page in range(3)
    }
    store = _store(tmp_path)

    fetched, counters, _ = fetch_season_schedules(
        _Client(payloads), [READY_TARGET], store, max_pages=5
    )

    assert [item.event["id"] for item in fetched] == [100, 101, 102]
    assert counters["pages"] == 3
    assert counters["truncated"] == 0
    assert [item.raw.target_id for item in fetched] == ["last-0", "last-1", "last-2"]
    assert store.has_payload(
        PayloadTarget(
            source_tournament_id=str(READY_TOURNAMENT),
            source_season_id=str(READY_SEASON),
            target_type="season_page",
            target_id="last-2",
            endpoint=SCHEDULE_PAGE_ENDPOINT,
            freshness_key="refresh",
        )
    )


def test_fetch_marks_a_chain_cut_short_by_the_bound(tmp_path):
    # A season that still has pages behind the bound must stay visible: a
    # truncated chain is not a complete season.
    first, _ = _fixture_events()
    payloads = {
        schedule_page_path(*READY_TARGET, page): _page(
            _event(first, event_id=200 + page, tournament_id=READY_TOURNAMENT,
                   season_id=READY_SEASON),
            has_next=True,
        )
        for page in range(3)
    }

    fetched, counters, incomplete = fetch_season_schedules(
        _Client(payloads), [READY_TARGET], _store(tmp_path), max_pages=2
    )

    assert [item.event["id"] for item in fetched] == [200, 201]
    assert counters["pages"] == 2
    assert counters["truncated"] == 1
    # The page to resume from, not just the pair: starting over at page 0 every
    # run would never reach a chain longer than the bound.
    # The fourth field is the oldest kick-off this visit read: the anchor the
    # next visit needs to notice a repacked chain.
    assert incomplete == [(READY_TOURNAMENT, READY_SEASON, 2, 1754751600)]


def test_a_resumed_chain_carries_on_from_the_page_it_stopped_at(tmp_path):
    # Sol r3 #2: two runs against the SOURCE stub (no fetch-level mock), the
    # second one continuing the chain the bound cut short.
    first, _ = _fixture_events()
    payloads = {
        schedule_page_path(*READY_TARGET, page): _page(
            _event(first, event_id=400 + page, tournament_id=READY_TOURNAMENT,
                   season_id=READY_SEASON),
            has_next=page < 3,
        )
        for page in range(4)
    }
    client = _Client(payloads)
    store = _store(tmp_path)

    _, _, incomplete = fetch_season_schedules(
        client, [READY_TARGET], store, max_pages=2
    )
    resume = {
        (tournament, season): page
        for tournament, season, page, _ in incomplete
    }
    fetched, counters, still_open = fetch_season_schedules(
        client, [READY_TARGET], store, max_pages=2, start_pages=resume
    )

    # The walk re-reads the page before the one it owes (the source repacks its
    # pages and a page read twice is a MERGE no-op), but the bound is counted
    # from the OWED page — an overlap that ate the budget left ``max_pages=1``
    # walking the same page forever (cross-check, seed_pages=1).
    assert client.paths == [
        schedule_page_path(*READY_TARGET, page) for page in (0, 1, 1, 2, 3)
    ]
    assert [item.event["id"] for item in fetched] == [401, 402, 403]
    assert counters["resumed"] == 1
    assert counters["truncated"] == 0
    assert still_open == []


def test_a_resumed_chain_steps_back_when_the_source_repacked_its_pages(tmp_path):
    # Sol r5 #6: ``last/N`` counts from the freshest page, so when the source
    # drops pages the content slides towards SMALLER numbers and a resume at
    # the saved page reads past the matches it still owes.  One page of overlap
    # only covers a one-page repack; the walk has to notice and step back.
    first, _ = _fixture_events()
    # Page 3 now holds what used to be page 5: everything on it is older than
    # the anchor the last visit stopped at.
    payloads = {
        schedule_page_path(*READY_TARGET, page): _page(
            _event(first, event_id=600 + page, tournament_id=READY_TOURNAMENT,
                   season_id=READY_SEASON, kick_off=1_000_000 - page * 86_400),
            has_next=True,
        )
        for page in range(6)
    }
    client = _Client(payloads)

    fetched, counters, _ = fetch_season_schedules(
        client, [READY_TARGET], _store(tmp_path), max_pages=1,
        start_pages={READY_TARGET: 3},
        # The last visit had already read down to the freshest kick-off: even
        # the page BEFORE the owed one is older than that, so the built-in
        # overlap is not enough and the walk has to keep stepping back.
        resume_anchors={READY_TARGET: 1_000_000},
    )

    # Two steps back, then forward again — and never re-reading a page it has
    # already paid for.
    assert client.paths == [
        schedule_page_path(*READY_TARGET, page) for page in (2, 1, 0, 3)
    ]
    assert counters["backtracked"] == 2
    assert [item.event["id"] for item in fetched] == [602, 601, 600, 603]


def test_every_chain_gets_its_own_step_back_allowance(tmp_path):
    # Sol r6 #3: the allowance used to be shared by the whole slice, so the
    # first repacked chain could spend all of it and every chain after it
    # walked past the matches it still owed.
    first, _ = _fixture_events()
    payloads = {}
    for tournament, season in (READY_TARGET, PREVIOUS_TARGET):
        payloads.update({
            schedule_page_path(tournament, season, page): _page(
                _event(first, event_id=700 + page, tournament_id=tournament,
                       season_id=season, kick_off=1_000_000 - page * 86_400),
                has_next=True,
            )
            for page in range(6)
        })
    client = _Client(payloads)

    # Both chains owe page 4 and both were repacked: the anchor is newer than
    # everything the page they land on holds.
    _, counters, _ = fetch_season_schedules(
        client, [READY_TARGET, PREVIOUS_TARGET], _store(tmp_path), max_pages=1,
        start_pages={READY_TARGET: 4, PREVIOUS_TARGET: 4},
        resume_anchors={
            READY_TARGET: 1_000_000, PREVIOUS_TARGET: 1_000_000,
        },
    )

    # Three steps back each, not three between them.
    assert client.paths == [
        schedule_page_path(*READY_TARGET, page) for page in (3, 2, 1, 0, 4)
    ] + [
        schedule_page_path(*PREVIOUS_TARGET, page) for page in (3, 2, 1, 0, 4)
    ]
    assert counters["backtracked"] == 6


def test_a_resumed_chain_that_ran_out_of_pages_is_not_a_missing_season(tmp_path):
    # A chain shrinks when the source repacks its pages; that 404 says "the
    # chain ended", not "this season does not exist", and must not count
    # towards the absence share that fails the whole slice.
    payloads = _sweep_payloads()
    client = _Client(
        payloads,
        missing={
            schedule_page_path(*READY_TARGET, 1),
            schedule_page_path(*READY_TARGET, 2),
        },
    )

    fetched, counters, incomplete = fetch_season_schedules(
        client, [READY_TARGET], _store(tmp_path), max_pages=2,
        start_pages={READY_TARGET: 2},
    )

    assert fetched == [] and incomplete == []
    assert counters["missing"] == 0 and counters["resumed"] == 1


def test_fetch_default_bound_is_the_freshest_page_only(tmp_path):
    first, _ = _fixture_events()
    payloads = {
        schedule_page_path(*READY_TARGET, page): _page(
            _event(first, event_id=300 + page, tournament_id=READY_TOURNAMENT,
                   season_id=READY_SEASON),
            has_next=True,
        )
        for page in range(2)
    }

    fetched, counters, _ = fetch_season_schedules(
        _Client(payloads), [READY_TARGET], _store(tmp_path)
    )

    assert [item.event["id"] for item in fetched] == [300]
    assert counters["truncated"] == 1


def _chain(pages: int, *, base_kick_off: int, has_next_last: bool = False):
    """A ``last`` chain: page 0 is the freshest, each next one older."""

    first, _ = _fixture_events()
    return {
        schedule_page_path(*READY_TARGET, page): _page(
            _event(first, event_id=500 + page, tournament_id=READY_TOURNAMENT,
                   season_id=READY_SEASON,
                   kick_off=base_kick_off - page * 86_400),
            has_next=has_next_last or page < pages - 1,
        )
        for page in range(pages)
    }


def test_a_tail_visit_chases_the_pages_bronze_has_not_seen(tmp_path):
    # Sol r4 #3: more than a page of matches can finish between two visits of
    # the same season.  Reading page 0 and stopping would drop the overflow
    # silently, so the walk goes on while the page is entirely newer than the
    # newest kick-off Bronze holds.
    payloads = _chain(4, base_kick_off=1_000_000)
    client = _Client(payloads)

    fetched, counters, incomplete = fetch_season_schedules(
        client, [READY_TARGET], _store(tmp_path), max_pages=4,
        # Bronze knows everything up to two days before the freshest match.
        chase_before={READY_TARGET: 1_000_000 - 2 * 86_400},
    )

    assert client.paths == [
        schedule_page_path(*READY_TARGET, page) for page in (0, 1, 2)
    ]
    assert [item.event["id"] for item in fetched] == [500, 501, 502]
    assert counters["chased"] == 2 and counters["chase_settled"] == 1
    assert counters["truncated"] == 0 and incomplete == []


def test_a_tail_visit_stops_at_the_first_page_it_has_already_seen(tmp_path):
    payloads = _chain(3, base_kick_off=1_000_000)
    client = _Client(payloads)

    fetched, counters, _ = fetch_season_schedules(
        client, [READY_TARGET], _store(tmp_path), max_pages=3,
        # The freshest page already reaches into what Bronze has.
        chase_before={READY_TARGET: 1_000_000},
    )

    assert client.paths == [schedule_page_path(*READY_TARGET, 0)]
    assert [item.event["id"] for item in fetched] == [500]
    assert counters["chased"] == 0 and counters["chase_settled"] == 1


def test_a_chase_that_hits_its_bound_is_left_for_the_next_run(tmp_path):
    payloads = _chain(4, base_kick_off=1_000_000, has_next_last=True)
    client = _Client(payloads)

    _, counters, incomplete = fetch_season_schedules(
        client, [READY_TARGET], _store(tmp_path), max_pages=2,
        chase_before={READY_TARGET: 0},
    )

    assert counters["pages"] == 2 and counters["truncated"] == 1
    assert incomplete == [
        (READY_TOURNAMENT, READY_SEASON, 2, 1_000_000 - 86_400)
    ]


def test_fetch_skips_a_season_the_source_does_not_serve(tmp_path):
    # A season that has not started answers 404: one absent season must not
    # fail a slice of hundreds, and the sweep goes on to the next target.
    payloads = _sweep_payloads()
    client = _Client(payloads, missing={schedule_page_path(*READY_TARGET)})
    store = _store(tmp_path)

    fetched, counters, _ = fetch_season_schedules(
        client, [READY_TARGET, PREVIOUS_TARGET, CONFIGURED_TARGET], store
    )

    assert counters["missing"] == 1 and counters["pages"] == 2
    assert [item.event["id"] for item in fetched] == [6, 7, 2]
    assert not store.has_payload(
        PayloadTarget(
            source_tournament_id=str(READY_TOURNAMENT),
            source_season_id=str(READY_SEASON),
            target_type="season_page",
            target_id="last-0",
            endpoint=SCHEDULE_PAGE_ENDPOINT,
            freshness_key="refresh",
        )
    )


def test_fetch_fails_when_absences_dominate_the_slice(tmp_path):
    # "Nothing was served" is not an empty day: it is the snapshot or the
    # source moving under the lane, and a green empty run would hide it.
    payloads = _sweep_payloads()
    client = _Client(payloads, missing=set(payloads))

    with pytest.raises(ScheduleSweepError, match="served none"):
        fetch_season_schedules(
            client, [READY_TARGET, CONFIGURED_TARGET, PREVIOUS_TARGET],
            _store(tmp_path),
        )


def test_absences_of_seasons_that_have_not_kicked_off_do_not_fail_a_slice(tmp_path):
    # Sol r6 #2: a season whose first match is still ahead answers 404 to
    # ``schedule_last`` by definition.  Before a season starts a tail slice is
    # full of them, and counting them against the whole slice would fail the
    # lane through every pre-season.  Only the seasons Bronze holds a match of
    # whose kick-off has passed owe a page.
    payloads = _sweep_payloads()
    client = _Client(
        payloads,
        missing={
            schedule_page_path(*CONFIGURED_TARGET),
            schedule_page_path(*PREVIOUS_TARGET),
        },
    )

    fetched, counters, _ = fetch_season_schedules(
        client, [READY_TARGET, CONFIGURED_TARGET, PREVIOUS_TARGET],
        _store(tmp_path),
        owed_pages={READY_TARGET},
    )

    assert [item.event["id"] for item in fetched] == [1]
    assert counters["missing"] == 2 and counters["missing_expected"] == 0
    assert counters["expected"] == 1


def test_a_slice_fails_when_the_seasons_that_owe_a_page_do_not_serve_it(tmp_path):
    # The other side of the same rule: a season whose match has kicked off owes
    # a tail page, and its absence is the snapshot or the source moving.
    payloads = _sweep_payloads()
    client = _Client(payloads, missing={
        schedule_page_path(*READY_TARGET), schedule_page_path(*PREVIOUS_TARGET),
    })

    with pytest.raises(ScheduleSweepError, match="served none"):
        fetch_season_schedules(
            client, [READY_TARGET, CONFIGURED_TARGET, PREVIOUS_TARGET],
            _store(tmp_path), owed_pages={READY_TARGET, PREVIOUS_TARGET},
        )


def test_a_season_owes_a_page_once_its_match_kicked_off_even_without_a_result(
    tmp_path,
):
    # Sol r7 #2: the anchor of ``chase_before`` only exists once a match is
    # FINISHED in Bronze.  A season stuck at 404 after its first kick-off would
    # never earn one, so deriving "owes a page" from the anchor kept it out of
    # the denominator forever and the lane stayed green while collecting
    # nothing.  ``owed_pages`` is decided by kick-off time instead.
    payloads = _sweep_payloads()
    client = _Client(payloads, missing={
        schedule_page_path(*READY_TARGET), schedule_page_path(*PREVIOUS_TARGET),
    })

    with pytest.raises(ScheduleSweepError, match="served none"):
        fetch_season_schedules(
            client, [READY_TARGET, CONFIGURED_TARGET, PREVIOUS_TARGET],
            _store(tmp_path),
            # Bronze holds only a scheduled row of them — no anchor to chase.
            chase_before={}, owed_pages={READY_TARGET, PREVIOUS_TARGET},
        )


def test_exactly_half_the_owed_seasons_missing_already_fails(tmp_path):
    # Sol r8 #4: the plan says "absences dominate" at half the slice, the code
    # used a strict >.  One owed 404 out of two stayed green.
    first, _ = _fixture_events()
    spare = [(904, 9004), (905, 9005)]
    payloads = _sweep_payloads() | {
        schedule_page_path(*pair): _page(
            _event(first, event_id=80 + index, tournament_id=pair[0],
                   season_id=pair[1]),
        )
        for index, pair in enumerate(spare)
    }
    client = _Client(payloads, missing={
        schedule_page_path(*READY_TARGET), schedule_page_path(*PREVIOUS_TARGET),
    })

    # Two of the four seasons that owe a page serve none: exactly half, which
    # already fails — and two is the minimum mass, so the share is what decides.
    with pytest.raises(ScheduleSweepError, match="served none"):
        fetch_season_schedules(
            client, [READY_TARGET, PREVIOUS_TARGET, *spare], _store(tmp_path),
            owed_pages={READY_TARGET, PREVIOUS_TARGET, *spare},
        )


def test_an_empty_page_from_a_season_that_owes_one_fails_the_slice(tmp_path):
    # Sol r9 #2: a 200 with an empty event list used to pass silently, so a
    # season with a played match could serve nothing, write no row and still
    # move the cursor on.  An empty page is the same evidence a 404 would be.
    payloads = {
        schedule_page_path(*READY_TARGET): _page(),
        schedule_page_path(*PREVIOUS_TARGET): _page(),
    }
    client = _Client(payloads)

    with pytest.raises(ScheduleSweepError, match="served none"):
        fetch_season_schedules(
            client, [READY_TARGET, PREVIOUS_TARGET], _store(tmp_path),
            owed_pages={READY_TARGET, PREVIOUS_TARGET},
        )


def test_one_owed_season_serving_nothing_does_not_fail_the_slice(tmp_path):
    # Sol r25: the denominator here is the seasons that OWE a page, not the size
    # of the slice, so a slice of hundreds with one owed season that answers
    # nothing reads "1 of 1" — and it is forever: a season that has ever had a
    # finished match owes a page for good, so it stays in ``stale`` and fails
    # the class before its cursor moves, every run.
    payloads = _sweep_payloads()
    client = _Client(payloads, missing={schedule_page_path(*READY_TARGET)})

    fetched, counters, _ = fetch_season_schedules(
        client, [READY_TARGET, CONFIGURED_TARGET, PREVIOUS_TARGET],
        _store(tmp_path), owed_pages={READY_TARGET},
    )

    assert counters["missing_expected"] == 1 and counters["expected"] == 1
    assert [item.event["id"] for item in fetched] == [2, 6, 7]


def test_an_empty_page_is_fine_for_a_season_that_has_not_kicked_off(tmp_path):
    # The other half: a season whose first match is still ahead may legitimately
    # serve an empty tail page, and failing on it would freeze the seed slice.
    payloads = {schedule_page_path(*READY_TARGET): _page()}
    client = _Client(payloads)

    fetched, counters, _ = fetch_season_schedules(
        client, [READY_TARGET], _store(tmp_path), owed_pages=set(),
    )

    assert fetched == [] and counters["empty_expected"] == 0
    assert counters["pages"] == 1 and counters["events"] == 0


def test_a_slice_where_every_season_is_absent_can_be_tolerated(tmp_path):
    # Sol r4 #5: the seed slice is full of seasons that have not kicked off, so
    # "all 404" is the normal answer there.  Failing it would pin the cursor to
    # that slice forever, and the sweep would never move again.
    payloads = _sweep_payloads()
    client = _Client(payloads, missing=set(payloads))

    fetched, counters, _ = fetch_season_schedules(
        client, [READY_TARGET, CONFIGURED_TARGET, PREVIOUS_TARGET],
        _store(tmp_path), missing_fail_share=None,
    )

    assert fetched == [] and counters["missing"] == 3


def test_fetch_propagates_a_transport_failure_that_is_not_a_missing_season(tmp_path):
    client = _Client({})

    def explode(path):
        raise DiscoveryHTTPError("gateway said no", status_code=502)

    client.get_json_bytes = explode

    with pytest.raises(DiscoveryHTTPError, match="gateway"):
        fetch_season_schedules(client, [READY_TARGET], _store(tmp_path))


def test_fetch_rejects_a_page_without_events_after_keeping_the_raw_evidence(
    tmp_path,
):
    first, _ = _fixture_events()
    path = schedule_page_path(*READY_TARGET)
    client = _Client({
        path: {"nope": 1},
        schedule_page_path(*CONFIGURED_TARGET): _page(
            _event(first, event_id=71, tournament_id=CONFIGURED_TOURNAMENT,
                   season_id=CONFIGURED_SEASON),
        ),
    })
    store = _store(tmp_path)

    # A page with no ``events`` list breaks the contract, so it costs its season
    # (a slice of them still fails — see below); the raw bytes are kept BEFORE
    # the payload is validated, which is what makes the surprise replayable.
    _, counters, _ = fetch_season_schedules(
        client, [READY_TARGET, CONFIGURED_TARGET], store,
    )

    assert counters["malformed"] == 1 and counters["pages"] == 1
    assert store.has_payload(
        PayloadTarget(
            source_tournament_id=str(READY_TOURNAMENT),
            source_season_id=str(READY_SEASON),
            target_type="season_page",
            target_id="last-0",
            endpoint=SCHEDULE_PAGE_ENDPOINT,
            freshness_key="refresh",
        )
    )


def test_rows_keep_ready_snapshot_tournaments_only_and_tag_league_season(
    tmp_path,
):
    client = _Client(_sweep_payloads())
    fetched, _, _ = fetch_season_schedules(
        client, [READY_TARGET, CONFIGURED_TARGET, PREVIOUS_TARGET], _store(tmp_path)
    )

    rows, counters = schedule_rows_from_events(
        fetched, SNAPSHOT, exclude_leagues={CONFIGURED_TOURNAMENT}
    )

    # Out of scope (excluded): the configured league (2) and the tournament the
    # snapshot does not know (7).
    assert counters == {
        "events": 4,
        "matched": 2,
        "excluded": 2,
        "unknown_seasons": 0,
        "unscoped": 0,
        "malformed": 0,
        "identity_conflict": 0,
    }
    assert [(row["game_id"], row["league"], row["season"]) for row in rows] == [
        (1, "SS-7", "2627"),
        (6, "SS-7", "2526"),
    ]
    by_id = {row["game_id"]: row for row in rows}
    assert by_id[1]["source_tournament_id"] == "7"
    assert by_id[1]["source_season_id"] == str(READY_SEASON)
    for row in rows:
        assert row["raw_endpoint"] == SCHEDULE_PAGE_ENDPOINT
        assert row["raw_content_hash"]
        assert row["raw_blob_key"]
        assert row["raw_request_url"]
        assert row["raw_fetched_at"]
        assert row["home_team_name"] and row["away_team_name"]
        assert row["start_timestamp"]


def test_rows_keep_the_freshest_copy_of_a_game_listed_twice(tmp_path):
    # The same game can appear on two pages of one chain (the source shifts
    # the window between requests): the last copy wins, lineage included.
    # Both copies are the SAME match — a repeat that disagrees about the teams
    # is a different matter, tested below.
    first, _second = _fixture_events()
    payloads = {
        schedule_page_path(*READY_TARGET, 0): _page(
            _event(first, event_id=55, tournament_id=READY_TOURNAMENT,
                   season_id=READY_SEASON),
            has_next=True,
        ),
        schedule_page_path(*READY_TARGET, 1): _page(
            _event(first, event_id=55, tournament_id=READY_TOURNAMENT,
                   season_id=READY_SEASON),
        ),
    }
    fetched, _, _ = fetch_season_schedules(
        _Client(payloads), [READY_TARGET], _store(tmp_path), max_pages=3
    )

    rows, counters = schedule_rows_from_events(fetched, SNAPSHOT, exclude_leagues=())

    assert counters["events"] == 2 and counters["matched"] == 2
    assert [row["game_id"] for row in rows] == [55]
    assert rows[0]["raw_target_id"] == "last-1"


def test_rows_count_a_season_the_snapshot_does_not_know_yet(tmp_path):
    # The metadata wave has not reached this season: its events are counted,
    # not written, and not treated as drift.
    first, _ = _fixture_events()
    unknown = (READY_TOURNAMENT, 424242)
    payloads = {
        schedule_page_path(*unknown): _page(
            _event(first, event_id=61, tournament_id=READY_TOURNAMENT,
                   season_id=424242),
        )
    }
    fetched, _, _ = fetch_season_schedules(
        _Client(payloads), [unknown], _store(tmp_path)
    )

    rows, counters = schedule_rows_from_events(fetched, SNAPSHOT, exclude_leagues=())

    assert rows == []
    assert counters["unknown_seasons"] == 1
    assert counters["matched"] == 0


def test_rows_reject_skeleton_events(tmp_path):
    first, _ = _fixture_events()
    skeleton = _event(first, event_id=9, tournament_id=READY_TOURNAMENT,
                      season_id=READY_PREVIOUS_SEASON)
    skeleton.pop("homeTeam")
    client = _Client({schedule_page_path(*PREVIOUS_TARGET): _page(skeleton)})
    fetched, _, _ = fetch_season_schedules(client, [PREVIOUS_TARGET], _store(tmp_path))

    with pytest.raises(SofaScoreDQViolation, match="skeleton"):
        schedule_rows_from_events(fetched, SNAPSHOT, exclude_leagues=())


def test_rows_reject_events_whose_tournament_or_season_shape_drifted(tmp_path):
    # Schema drift must not masquerade as "foreign tournaments": an event of a
    # READY tournament without season.id / id is malformed and fails the run
    # (the raw page is already kept), never dropped silently.  An event that
    # cannot even be placed (renamed tournament key) is a minority "unscoped"
    # entry here and only warns — see the exotic-event test below.
    first, second = _fixture_events()
    renamed = _event(first, event_id=11, tournament_id=READY_TOURNAMENT,
                     season_id=READY_PREVIOUS_SEASON)
    renamed["tournament"] = {"unique_tournament": {"id": READY_TOURNAMENT}}
    no_season = _event(second, event_id=12, tournament_id=READY_TOURNAMENT,
                       season_id=READY_PREVIOUS_SEASON)
    no_season.pop("season")
    fine = _event(first, event_id=13, tournament_id=READY_TOURNAMENT,
                  season_id=READY_PREVIOUS_SEASON)
    client = _Client({
        schedule_page_path(*PREVIOUS_TARGET): _page(renamed, no_season, fine)
    })
    fetched, _, _ = fetch_season_schedules(client, [PREVIOUS_TARGET], _store(tmp_path))

    with pytest.raises(DailyEventsSchemaError, match="1 of 3 .*ready tournaments"):
        schedule_rows_from_events(fetched, SNAPSHOT, exclude_leagues=())


def test_rows_tolerate_a_minority_of_exotic_unscoped_events(tmp_path, caplog):
    # Sol r3: a page legitimately carries exotic entries without a
    # tournament.uniqueTournament.id — one of them must not fail the slice;
    # it is counted as ``unscoped`` and warned about.
    first, second = _fixture_events()
    exotic = _event(first, event_id=41, tournament_id=READY_TOURNAMENT,
                    season_id=READY_PREVIOUS_SEASON)
    exotic["tournament"] = {"name": "Friendly XI"}
    fine = [
        _event(second, event_id=42 + n, tournament_id=READY_TOURNAMENT,
               season_id=READY_PREVIOUS_SEASON)
        for n in range(3)
    ]
    client = _Client({
        schedule_page_path(*PREVIOUS_TARGET): _page(exotic, *fine)
    })
    fetched, _, _ = fetch_season_schedules(client, [PREVIOUS_TARGET], _store(tmp_path))

    with caplog.at_level(
        logging.WARNING, logger="scrapers.sofascore.schedule_refresh"
    ):
        rows, counters = schedule_rows_from_events(
            fetched, SNAPSHOT, exclude_leagues=()
        )

    assert sorted(row["game_id"] for row in rows) == [42, 43, 44]
    assert counters["unscoped"] == 1 and counters["malformed"] == 0
    assert any(
        "could not be placed" in record.getMessage() for record in caplog.records
    )


@pytest.mark.parametrize("junk", [None, 5])
def test_rows_reject_non_object_events_as_malformed(tmp_path, junk):
    # Sol r2 #2: ``{"events": [null]}`` is schema drift like any other —
    # it must fail the slice after the raw page is kept, not be filtered out
    # into a green empty result.  Here the junk is half of the page, i.e. at
    # the ``_UNSCOPED_FAIL_SHARE`` threshold.
    first, _ = _fixture_events()
    fine = _event(first, event_id=31, tournament_id=READY_TOURNAMENT,
                  season_id=READY_PREVIOUS_SEASON)
    client = _Client({schedule_page_path(*PREVIOUS_TARGET): _page(junk, fine)})
    store = _store(tmp_path)
    fetched, _, _ = fetch_season_schedules(client, [PREVIOUS_TARGET], store)

    assert store.has_payload(
        PayloadTarget(
            source_tournament_id=str(READY_TOURNAMENT),
            source_season_id=str(READY_PREVIOUS_SEASON),
            target_type="season_page",
            target_id="last-0",
            endpoint=SCHEDULE_PAGE_ENDPOINT,
            freshness_key="refresh",
        )
    )
    with pytest.raises(DailyEventsSchemaError, match="1 of 2 .*cannot be placed"):
        schedule_rows_from_events(fetched, SNAPSHOT, exclude_leagues=())


def test_rows_reject_a_page_made_only_of_junk(tmp_path):
    # ``{"events": [null]}`` alone: nothing can be placed — fully malformed.
    client = _Client({schedule_page_path(*PREVIOUS_TARGET): _page(None)})
    fetched, _, _ = fetch_season_schedules(client, [PREVIOUS_TARGET], _store(tmp_path))

    with pytest.raises(DailyEventsSchemaError, match="1 of 1 .*cannot be placed"):
        schedule_rows_from_events(fetched, SNAPSHOT, exclude_leagues=())


def test_rows_warn_when_no_event_is_in_scope(tmp_path, caplog):
    # A slice where every event belongs to configured/foreign tournaments is a
    # legitimate empty result, but "green and empty" must be visible in logs.
    fetched, _, _ = fetch_season_schedules(
        _Client(_sweep_payloads()), [CONFIGURED_TARGET], _store(tmp_path)
    )

    with caplog.at_level(
        logging.WARNING, logger="scrapers.sofascore.schedule_refresh"
    ):
        rows, counters = schedule_rows_from_events(
            fetched, SNAPSHOT, exclude_leagues={CONFIGURED_TOURNAMENT}
        )

    assert rows == []
    assert counters["events"] == counters["excluded"] == 1
    assert counters["matched"] == 0
    assert any(
        "no season-page event matched" in record.getMessage()
        for record in caplog.records
    )


def _fixture_payloads() -> dict[str, dict]:
    first, second = _fixture_events()
    return {
        fixture_page_path(*READY_TARGET): _page(
            _event(first, event_id=11, tournament_id=READY_TOURNAMENT,
                   season_id=READY_SEASON),
            # A neighbour season's fixture on the same calendar: dropped, like
            # on the tail page — a Bronze row must belong to the season asked for.
            _event(second, event_id=12, tournament_id=READY_TOURNAMENT,
                   season_id=424242),
        ),
        fixture_page_path(*CONFIGURED_TARGET): _page(
            _event(second, event_id=13, tournament_id=CONFIGURED_TOURNAMENT,
                   season_id=CONFIGURED_SEASON),
        ),
    }


def test_fixture_pages_are_stored_under_their_own_endpoint_with_lineage(tmp_path):
    # Sol r12 #6: the fixture walk is what opens the ``due`` window for a season
    # that has not kicked off, and it was only ever exercised through a stub.
    payloads = _fixture_payloads()
    client = _Client(payloads)
    store = _store(tmp_path)

    fetched, counters = fetch_season_fixtures(
        client, [READY_TARGET, CONFIGURED_TARGET], store
    )

    assert client.paths == list(payloads)
    assert [item.event["id"] for item in fetched] == [11, 13]
    assert counters == {
        "targets": 2, "pages": 2, "events": 3, "missing": 0, "foreign_season": 1,
        "truncated": 0, "malformed": 0,
    }
    record = fetched[0].raw
    # The calendar is its OWN endpoint: sharing ``schedule_last``'s target id
    # would have one page overwrite the other in the raw store.
    assert record.endpoint == FIXTURE_PAGE_ENDPOINT != SCHEDULE_PAGE_ENDPOINT
    assert record.target_type == "season_page"
    assert record.target_id == "next-0"
    assert record.source_tournament_id == str(READY_TOURNAMENT)
    assert record.source_season_id == str(READY_SEASON)
    assert record.request_url.endswith(fixture_page_path(*READY_TARGET))
    stored, saved = store.load_json(
        PayloadTarget(
            source_tournament_id=str(READY_TOURNAMENT),
            source_season_id=str(READY_SEASON),
            target_type="season_page",
            target_id="next-0",
            endpoint=FIXTURE_PAGE_ENDPOINT,
            freshness_key=record.freshness_key,
        )
    )
    assert stored == payloads[fixture_page_path(*READY_TARGET)]
    assert saved.content_hash == record.content_hash


def test_a_season_without_a_calendar_is_counted_and_never_fatal(tmp_path):
    # A season with no fixtures answers 404, and this page is an extra on top
    # of the tail walk: losing it must not cost the run.
    payloads = _fixture_payloads()
    client = _Client(payloads, missing={fixture_page_path(*READY_TARGET)})

    fetched, counters = fetch_season_fixtures(
        client, [READY_TARGET, CONFIGURED_TARGET], _store(tmp_path)
    )

    assert [item.event["id"] for item in fetched] == [13]
    assert counters["missing"] == 1 and counters["targets"] == 2
    assert counters["pages"] == 1


def test_a_calendar_that_is_not_a_page_is_skipped_but_transport_still_fails(tmp_path):
    # A page that is not a page costs its season (Sol r21); anything but a 404
    # from the transport is a property of the RUN, not of the season, and still
    # propagates.
    first, _ = _fixture_events()
    client = _Client({
        fixture_page_path(*READY_TARGET): {"events": "soon"},
        # A healthy calendar beside it: a slice of nothing but broken pages is
        # drift and fails on its own (see below), so the per-season tolerance
        # is only visible next to a page that worked.
        fixture_page_path(*CONFIGURED_TARGET): _page(
            _event(first, event_id=61, tournament_id=CONFIGURED_TOURNAMENT,
                   season_id=CONFIGURED_SEASON),
        ),
    })

    _, counters = fetch_season_fixtures(
        client, [READY_TARGET, CONFIGURED_TARGET], _store(tmp_path)
    )
    assert counters["malformed"] == 1 and counters["pages"] == 1

    class _Boom(_Client):
        def get_json(self, path):
            raise DiscoveryHTTPError("HTTP 503", status_code=503)

    with pytest.raises(DiscoveryHTTPError):
        fetch_season_fixtures(
            _Boom({fixture_page_path(*READY_TARGET): {}}), [READY_TARGET],
            _store(tmp_path),
        )


def test_the_worst_case_formula_bounds_what_a_resumed_seed_really_requests(tmp_path):
    # Sol r13 #1: the estimate is only worth anything if it is checked against
    # the fetcher instead of against itself.  A seeded chain in its worst case
    # is resumed (one page of OVERLAP before the owed page), repacked (up to
    # MAX_BACKTRACK_PAGES steps back), walks its whole ``seed_pages`` bound and
    # takes a fixture page — the overlap page is what the formula used to miss.
    from dags.scripts.run_sofascore_schedule_refresh import worst_case_pages

    first, _ = _fixture_events()
    seed_pages = 4
    owed = 8
    payloads = {
        schedule_page_path(*READY_TARGET, page): _page(
            _event(first, event_id=800 + page, tournament_id=READY_TOURNAMENT,
                   season_id=READY_SEASON, kick_off=1_000_000 - page * 86_400),
            has_next=True,
        )
        for page in range(owed + seed_pages + 1)
    }
    payloads[fixture_page_path(*READY_TARGET)] = _page(
        _event(first, event_id=899, tournament_id=READY_TOURNAMENT,
               season_id=READY_SEASON, kick_off=2_000_000),
    )
    client = _Client(payloads)
    store = _store(tmp_path)

    _, counters, _ = fetch_season_schedules(
        client, [READY_TARGET], store, max_pages=seed_pages,
        start_pages={READY_TARGET: owed},
        # Everything the walk lands on is older than where the last visit
        # stopped, so it spends the whole step-back allowance.
        resume_anchors={READY_TARGET: 1_000_000},
        missing_fail_share=None,
    )
    fetch_season_fixtures(client, [READY_TARGET], store)

    assert counters["backtracked"] == MAX_BACKTRACK_PAGES
    # One target, one class: the seed term of the formula is the whole budget.
    per_seed_target = worst_case_pages(0, 0, 1, 1, seed_pages)
    assert len(client.paths) == per_seed_target == seed_pages + MAX_BACKTRACK_PAGES + 2


@pytest.mark.parametrize("page_body", [
    {"events": []},                        # поля нет вовсе
    {"events": [], "hasNextPage": "yes"},   # поле есть, но не булево
    {"events": [], "hasNextPage": None},
])
def test_a_page_without_a_boolean_has_next_costs_its_season_not_the_slice(
    page_body, tmp_path
):
    # Sol r19: ``hasNextPage`` is a required path of the endpoint and the only
    # thing that says the chain goes on, so reading its absence as "no more
    # pages" ended the walk with ``truncated`` at zero and nothing queued — the
    # rest of that season was never asked for again.
    # Sol r20: but failing the whole slice on it let one permanently broken
    # page block every other target of the class, every run.  The season is
    # skipped and counted instead, and the walk goes on.
    first, _ = _fixture_events()
    good = _page(
        _event(first, event_id=21, tournament_id=CONFIGURED_TOURNAMENT,
               season_id=CONFIGURED_SEASON),
    )
    client = _Client({
        schedule_page_path(*READY_TARGET): page_body,
        schedule_page_path(*CONFIGURED_TARGET): good,
        schedule_page_path(*PREVIOUS_TARGET): good,
    })

    fetched, counters, incomplete = fetch_season_schedules(
        client, [READY_TARGET, CONFIGURED_TARGET, PREVIOUS_TARGET],
        _store(tmp_path), max_pages=5, missing_fail_share=None,
    )

    assert counters["malformed"] == 1
    # A chain that owed page 0 needs no resume entry: nothing of it reached
    # Bronze, so its partition stays unknown and the seed class walks it again.
    assert incomplete == []
    # And the seasons after it were still fetched and still count.
    assert counters["pages"] == 2
    assert [item.event["id"] for item in fetched] == [21]


def test_a_broken_resumed_chain_goes_back_on_the_queue_at_the_page_it_owes(tmp_path):
    # Sol r22 #1: a RESUMED chain has its earlier pages in Bronze already, which
    # makes the partition ``known`` — the seed class will not take it and a tail
    # visit only reads the head.  Dropping its queue entry when a page broke
    # therefore lost the rest of that season for good.
    first, _ = _fixture_events()
    good = _page(
        _event(first, event_id=41, tournament_id=CONFIGURED_TOURNAMENT,
               season_id=CONFIGURED_SEASON),
    )
    client = _Client({
        # The page of overlap a resumed chain re-reads, then the page it owed.
        schedule_page_path(*READY_TARGET, 3): _page(
            _event(first, event_id=42, tournament_id=READY_TOURNAMENT,
                   season_id=READY_SEASON, kick_off=1_700_500),
            has_next=True,
        ),
        schedule_page_path(*READY_TARGET, 4): {"events": [], "hasNextPage": "yes"},
        schedule_page_path(*CONFIGURED_TARGET): good,
        schedule_page_path(*PREVIOUS_TARGET): good,
    })

    fetched, counters, incomplete = fetch_season_schedules(
        client, [READY_TARGET, CONFIGURED_TARGET, PREVIOUS_TARGET],
        _store(tmp_path), max_pages=5, start_pages={READY_TARGET: 4},
        resume_anchors={READY_TARGET: 1_700_000}, missing_fail_share=None,
    )

    assert counters["malformed"] == 1
    # At the page it still owes, with the anchor it was resumed from: this
    # visit is rolled back whole, so it has read nothing to move either.
    assert incomplete == [(READY_TOURNAMENT, READY_SEASON, 4, 1_700_000)]
    assert 42 not in [item.event["id"] for item in fetched]


def test_the_last_broken_chain_alone_in_a_slice_is_returned_not_raised(tmp_path):
    # Sol r23: the drift threshold used to fire before the queue entry was
    # returned — with the last broken chain alone in a slice that reads "1 of 1"
    # and fails the sweep.  The caller then never reaches the attempt count, the
    # chain never ages out, and the same page is paid for on every run for ever.
    # A queued chain that breaks is a poison the CALLER tracks; drift of the
    # source shows up on the seasons a slice visits for the first time.
    client = _Client({
        schedule_page_path(*READY_TARGET, 3): {"events": [], "hasNextPage": "yes"},
    })

    fetched, counters, incomplete = fetch_season_schedules(
        client, [READY_TARGET], _store(tmp_path), max_pages=5,
        start_pages={READY_TARGET: 4}, resume_anchors={READY_TARGET: 1_700_000},
        missing_fail_share=None,
    )

    assert fetched == []
    assert incomplete == [(READY_TOURNAMENT, READY_SEASON, 4, 1_700_000)]
    assert counters["malformed"] == 1 and counters["malformed_resumed"] == 1


def test_a_slice_of_new_seasons_that_all_break_still_fails(tmp_path):
    # The other side of the same rule: a season this slice visits for the first
    # time has no queue entry to age out, so nothing but the threshold stands
    # between a source that changed its schema and a lane that keeps paying.
    broken = {"events": [], "hasNextPage": "yes"}
    client = _Client({
        schedule_page_path(*READY_TARGET, 3): broken,
        schedule_page_path(*CONFIGURED_TARGET): broken,
        schedule_page_path(*PREVIOUS_TARGET): broken,
    })

    with pytest.raises(ScheduleSweepError):
        fetch_season_schedules(
            client, [READY_TARGET, CONFIGURED_TARGET, PREVIOUS_TARGET],
            _store(tmp_path), max_pages=5, start_pages={READY_TARGET: 4},
            resume_anchors={READY_TARGET: 1_700_000}, missing_fail_share=None,
        )


def test_one_broken_page_alone_in_a_slice_does_not_fail_the_sweep(tmp_path):
    # Sol r24: a share taken over ONE target is evidence of nothing, and the
    # failure it caused was forever — the class commits its cursor only once it
    # is walked, so the next run rebuilt the same slice and broke on the same
    # page.  Two broken seasons are the least that can be called a pattern; a
    # slice that serves nothing at all is caught by ``idle_runs`` instead.
    client = _Client({
        schedule_page_path(*READY_TARGET): {"events": [], "hasNextPage": "yes"},
    })

    fetched, counters, incomplete = fetch_season_schedules(
        client, [READY_TARGET], _store(tmp_path), max_pages=5,
        missing_fail_share=None,
    )

    assert fetched == [] and incomplete == []
    assert counters["malformed"] == 1


def test_the_drift_share_is_counted_among_the_seasons_visited_fresh(tmp_path):
    # Not just the numerator: the share only means anything among comparable
    # targets.  Queued chains sit in neither half, so a slice held up by them
    # still fails on the one season it visited for the first time.
    broken = {"events": [], "hasNextPage": "yes"}
    queued = [(901, 9001), (902, 9002), (903, 9003)]
    client = _Client({
        schedule_page_path(*pair, 3): broken for pair in queued
    } | {
        schedule_page_path(*READY_TARGET): broken,
        schedule_page_path(*CONFIGURED_TARGET): broken,
    })

    # Three queued chains and two seasons seen for the first time, all broken:
    # 2 of 2 fresh targets fails, while 5 of 5 over the whole slice would let
    # the share pass at 2 of 5.
    with pytest.raises(ScheduleSweepError):
        fetch_season_schedules(
            client, [*queued, READY_TARGET, CONFIGURED_TARGET],
            _store(tmp_path), max_pages=5,
            start_pages={pair: 4 for pair in queued},
            missing_fail_share=None,
        )


def test_a_broken_season_takes_its_counters_back_with_its_rows(tmp_path):
    # Sol r22 #3: the rollback dropped the season's rows but left ``events``
    # standing, and the caller reads that counter as "the source served this
    # class" — so a valid page 0 followed by a broken page 1, next to seasons
    # that legitimately served nothing, failed the whole class as "pages
    # produced no schedule row" and pinned the cursor on that slice.
    first, _ = _fixture_events()
    client = _Client({
        schedule_page_path(*READY_TARGET, 0): _page(
            _event(first, event_id=51, tournament_id=READY_TOURNAMENT,
                   season_id=READY_SEASON),
            has_next=True,
        ),
        schedule_page_path(*READY_TARGET, 1): {"events": [], "hasNextPage": "yes"},
        # Neighbours whose season has not kicked off: an empty page is their
        # legitimate answer.
        schedule_page_path(*CONFIGURED_TARGET): _page(),
        schedule_page_path(*PREVIOUS_TARGET): _page(),
    })

    fetched, counters, incomplete = fetch_season_schedules(
        client, [READY_TARGET, CONFIGURED_TARGET, PREVIOUS_TARGET],
        _store(tmp_path), max_pages=5, missing_fail_share=None,
    )

    assert fetched == [] and counters["malformed"] == 1
    # Nothing of that season survived, so nothing of it is counted as served.
    assert counters["events"] == 0
    assert counters["pages"] == 2


def test_a_slice_of_pages_that_all_break_the_contract_fails_the_sweep(tmp_path):
    # The tolerance is per season, not per slice: pages that break the contract
    # everywhere are schema drift and must stop the run.
    broken = {"events": [], "hasNextPage": "yes"}
    client = _Client({
        schedule_page_path(*READY_TARGET): broken,
        schedule_page_path(*CONFIGURED_TARGET): broken,
    })

    with pytest.raises(ScheduleSweepError):
        fetch_season_schedules(
            client, [READY_TARGET, CONFIGURED_TARGET], _store(tmp_path),
            max_pages=5, missing_fail_share=None,
        )


def test_a_calendar_longer_than_one_page_is_counted_not_walked(tmp_path):
    # Sol r20: ``schedule_next`` declares ``/hasNextPage`` too, and the lane
    # ignored it entirely.  One page is deliberate — this walk exists to open
    # the ``due`` window and the budget is sized for one — but the contract is
    # checked and a longer calendar is visible in the counters.
    first, _ = _fixture_events()
    client = _Client({
        fixture_page_path(*READY_TARGET): _page(
            _event(first, event_id=31, tournament_id=READY_TOURNAMENT,
                   season_id=READY_SEASON),
            has_next=True,
        ),
    })

    fetched, counters = fetch_season_fixtures(
        client, [READY_TARGET], _store(tmp_path)
    )

    assert client.paths == [fixture_page_path(*READY_TARGET)]  # one page only
    assert counters["truncated"] == 1 and counters["pages"] == 1
    assert [item.event["id"] for item in fetched] == [31]


def test_a_broken_calendar_costs_its_season_not_the_class(tmp_path):
    # Sol r21: this walk runs after the tail pages of the same class are paid
    # for and before their MERGE, so raising here cancelled all of them — one
    # permanently broken calendar blocked the class every run.
    first, _ = _fixture_events()
    client = _Client({
        fixture_page_path(*READY_TARGET): {"events": []},  # no hasNextPage
        fixture_page_path(*CONFIGURED_TARGET): _page(
            _event(first, event_id=41, tournament_id=CONFIGURED_TOURNAMENT,
                   season_id=CONFIGURED_SEASON),
        ),
    })

    fetched, counters = fetch_season_fixtures(
        client, [READY_TARGET, CONFIGURED_TARGET], _store(tmp_path)
    )

    assert counters["malformed"] == 1 and counters["pages"] == 1
    assert [item.event["id"] for item in fetched] == [41]


def test_a_slice_of_calendars_that_all_break_the_contract_fails_the_sweep(tmp_path):
    # Sol r22 #2: a broken calendar writes no row, so a slice of nothing but
    # broken calendars ends with ``events`` at zero — which is also the
    # legitimate answer of seasons that simply have no fixture, and the caller
    # cannot tell the two apart.  The ``due`` window opens off these rows
    # alone, so a required endpoint that stopped answering has to fail here
    # instead of passing as a quiet calendar.  The denominator is the seasons
    # that ANSWERED: before a season starts 404 is its normal answer, and
    # counting those would make one broken calendar look like drift.
    broken = {"events": [], "hasNextPage": "yes"}
    absent = [(901, 9001), (902, 9002)]
    client = _Client(
        {
            fixture_page_path(*READY_TARGET): broken,
            fixture_page_path(*CONFIGURED_TARGET): broken,
        },
        missing=[fixture_page_path(*pair) for pair in absent],
    )

    # Two broken calendars out of two that answered fails; over all four targets
    # the same two would be exactly half and pass.
    with pytest.raises(ScheduleSweepError):
        fetch_season_fixtures(
            client, [READY_TARGET, CONFIGURED_TARGET, *absent], _store(tmp_path),
        )


def test_one_broken_calendar_alone_in_a_slice_does_not_fail_the_sweep(tmp_path):
    # The minimum mass holds on this side too (Sol r24): the only season that
    # answered having a broken calendar is one target, not a pattern — and the
    # failure would be forever, because the class never reaches the cursor.
    client = _Client(
        {fixture_page_path(*READY_TARGET): {"events": [], "hasNextPage": "yes"}},
        missing=[fixture_page_path(*CONFIGURED_TARGET)],
    )

    fetched, counters = fetch_season_fixtures(
        client, [READY_TARGET, CONFIGURED_TARGET], _store(tmp_path),
    )

    assert fetched == []
    assert counters["malformed"] == 1 and counters["missing"] == 1


def test_broken_and_unserved_seasons_share_a_threshold_as_well(tmp_path):
    # Sol r21: 49 broken pages + 49 unserved owed seasons + 2 good ones cleared
    # BOTH per-kind thresholds and then cleared the lane's emptiness alarm with
    # its two rows — 98 % of the slice unusable, reported as a healthy run.
    first, _ = _fixture_events()
    broken = {"events": [], "hasNextPage": "yes"}
    good = _page(
        _event(first, event_id=51, tournament_id=READY_TOURNAMENT,
               season_id=READY_SEASON),
    )
    targets = [(1000 + index, 2000 + index) for index in range(100)]
    payloads, missing = {}, set()
    for index, (tournament, season) in enumerate(targets):
        path = schedule_page_path(tournament, season)
        if index < 49:
            payloads[path] = broken
        elif index < 98:
            payloads[path] = broken  # placeholder, replaced by ``missing``
            missing.add(path)
        else:
            payloads[path] = good
    client = _Client(payloads, missing=missing)

    with pytest.raises(ScheduleSweepError, match="were unusable"):
        fetch_season_schedules(
            client, targets, _store(tmp_path), max_pages=2,
            owed_pages=set(targets),
        )


def test_two_different_matches_under_one_game_id_do_not_overwrite_each_other(tmp_path):
    # Sol r21: a repeat is normal (pages overlap), but a repeat that disagrees
    # about who is playing is the source contradicting itself, and last-wins
    # glued two matches together under one id.  The first copy is kept and the
    # disagreement is counted.
    first, second = _fixture_events()
    payloads = {
        schedule_page_path(*READY_TARGET, 0): _page(
            _event(first, event_id=77, tournament_id=READY_TOURNAMENT,
                   season_id=READY_SEASON),
            _event(second, event_id=78, tournament_id=READY_TOURNAMENT,
                   season_id=READY_SEASON),
            has_next=True,
        ),
        schedule_page_path(*READY_TARGET, 1): _page(
            _event(second, event_id=77, tournament_id=READY_TOURNAMENT,
                   season_id=READY_SEASON),
        ),
    }
    fetched, _, _ = fetch_season_schedules(
        _Client(payloads), [READY_TARGET], _store(tmp_path), max_pages=3
    )

    rows, counters = schedule_rows_from_events(fetched, SNAPSHOT, exclude_leagues=())

    assert counters["identity_conflict"] == 1
    assert counters["matched"] == 2
    # The first copy of 77 survived, not the contradicting one.
    kept = {row["game_id"]: row for row in rows}
    assert kept[77]["raw_target_id"] == "last-0"
