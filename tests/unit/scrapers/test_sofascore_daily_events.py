"""Daily source event lists as ``bronze.sofascore_schedule`` evidence (lane F)."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import date
from pathlib import Path

import pytest
from pyarrow import fs

from dags.utils.sofascore_dq import SofaScoreDQViolation
from scrapers.sofascore.daily_events import (
    SCHEDULED_EVENTS_ENDPOINT,
    DailyEventsSchemaError,
    fetch_daily_events,
    schedule_rows_from_events,
)
from scrapers.sofascore.discovery import DiscoverySchemaError
from scrapers.sofascore.raw_store import PayloadTarget, RawPayloadStore


FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"

READY_TOURNAMENT = 7
CONFIGURED_TOURNAMENT = 17
EXCLUDED_TOURNAMENT = 799
FOREIGN_TOURNAMENT = 555

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
                    "source_season_id": 96518,
                    "start_year": 2026,
                },
                {
                    "canonical_season": "2526",
                    "metadata_status": "ready",
                    "source_season_id": 76953,
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
                    "source_season_id": 76986,
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


def _fixture_events() -> list[dict]:
    payload = json.loads(
        (FIXTURES / "sofascore_season_76986_schedule_last_0.json").read_text(
            encoding="utf-8"
        )
    )
    return payload["events"]


def _event(template: dict, *, event_id: int, tournament_id: int, season_id: int):
    event = json.loads(json.dumps(template))
    event["id"] = event_id
    event["tournament"] = {"uniqueTournament": {"id": tournament_id}}
    event["season"] = {"id": season_id, "name": f"season {season_id}"}
    return event


def _daily_payloads() -> dict[str, dict]:
    first, second = _fixture_events()
    return {
        "/sport/football/scheduled-events/2026-08-23": {
            "events": [
                _event(first, event_id=1, tournament_id=READY_TOURNAMENT,
                       season_id=96518),
                _event(second, event_id=2, tournament_id=CONFIGURED_TOURNAMENT,
                       season_id=76986),
            ]
        },
        "/sport/football/scheduled-events/2026-08-23/inverse": {
            "events": [
                _event(first, event_id=3, tournament_id=READY_TOURNAMENT,
                       season_id=424242),
                _event(second, event_id=4, tournament_id=FOREIGN_TOURNAMENT,
                       season_id=1),
            ]
        },
        "/sport/football/scheduled-events/2026-08-24": {
            "events": [
                _event(first, event_id=5, tournament_id=EXCLUDED_TOURNAMENT,
                       season_id=76139),
                _event(second, event_id=6, tournament_id=READY_TOURNAMENT,
                       season_id=76953),
            ]
        },
        "/sport/football/scheduled-events/2026-08-24/inverse": {
            "events": [
                _event(first, event_id=1, tournament_id=READY_TOURNAMENT,
                       season_id=96518),
            ]
        },
    }


class _Client:
    """Metered browser client stub: the wire bytes are NOT the canonical dump."""

    def __init__(self, payloads):
        self.payloads = payloads
        self.paths: list[str] = []

    def body(self, path) -> bytes:
        return json.dumps(self.payloads[path], indent=1).encode("utf-8")

    def get_json(self, path):
        self.paths.append(path)
        return self.payloads[path]

    def get_json_bytes(self, path):
        return self.body(path), self.get_json(path)


def _store(tmp_path):
    return RawPayloadStore(fs.LocalFileSystem(), str(tmp_path / "raw"))


def test_fetch_stores_every_daily_list_and_returns_events_with_lineage(tmp_path):
    payloads = _daily_payloads()
    client = _Client(payloads)
    store = _store(tmp_path)

    fetched = fetch_daily_events(
        client, [date(2026, 8, 23), date(2026, 8, 24)], store
    )

    assert client.paths == list(payloads)
    assert [item.event["id"] for item in fetched] == [1, 2, 3, 4, 5, 6, 1]
    first = fetched[0].raw
    assert first.endpoint == SCHEDULED_EVENTS_ENDPOINT
    assert first.target_type == "date"
    assert first.target_id == "2026-08-23"
    assert first.request_url.endswith("/sport/football/scheduled-events/2026-08-23")
    assert fetched[2].raw.target_id == "2026-08-23-inverse"
    stored, record = store.load_json(
        PayloadTarget(
            source_tournament_id=first.source_tournament_id,
            source_season_id=first.source_season_id,
            target_type="date",
            target_id="2026-08-23",
            endpoint=SCHEDULED_EVENTS_ENDPOINT,
            freshness_key=first.freshness_key,
        )
    )
    assert stored == payloads["/sport/football/scheduled-events/2026-08-23"]
    assert record.content_hash == first.content_hash


def test_fetch_keeps_the_exact_response_bytes_as_raw_evidence(tmp_path):
    # Coverage contract: raw lineage is the HTTP body as received, so the
    # blob and its content hash witness the source answer byte for byte.
    payloads = _daily_payloads()
    client = _Client(payloads)
    store = _store(tmp_path)

    fetched = fetch_daily_events(client, [date(2026, 8, 23)], store)

    path = "/sport/football/scheduled-events/2026-08-23"
    body, record = store.load_bytes(
        PayloadTarget(
            source_tournament_id="0",
            source_season_id="0",
            target_type="date",
            target_id="2026-08-23",
            endpoint=SCHEDULED_EVENTS_ENDPOINT,
            freshness_key="daily",
        )
    )
    assert body == client.body(path)
    assert record.content_hash == fetched[0].raw.content_hash
    assert record.content_hash == hashlib.sha256(client.body(path)).hexdigest()


def test_fetch_rejects_a_list_without_events_after_keeping_the_raw_evidence(
    tmp_path,
):
    client = _Client({"/sport/football/scheduled-events/2026-08-24": {"nope": 1}})
    store = _store(tmp_path)

    with pytest.raises(DiscoverySchemaError, match="events"):
        fetch_daily_events(client, [date(2026, 8, 24)], store)

    assert store.has_payload(
        PayloadTarget(
            source_tournament_id="0",
            source_season_id="0",
            target_type="date",
            target_id="2026-08-24",
            endpoint=SCHEDULED_EVENTS_ENDPOINT,
            freshness_key="daily",
        )
    )


def test_rows_keep_ready_snapshot_tournaments_only_and_tag_league_season(
    tmp_path,
):
    client = _Client(_daily_payloads())
    fetched = fetch_daily_events(
        client, [date(2026, 8, 23), date(2026, 8, 24)], _store(tmp_path)
    )

    rows, counters = schedule_rows_from_events(
        fetched, SNAPSHOT, exclude_leagues={CONFIGURED_TOURNAMENT}
    )

    # Out of scope (excluded): the configured league (2), the tournament the
    # snapshot does not know (4) and the excluded-status tournament (5).
    assert counters == {
        "events": 7,
        "matched": 3,
        "excluded": 3,
        "unknown_seasons": 1,
        "unscoped": 0,
        "malformed": 0,
    }
    assert [(row["game_id"], row["league"], row["season"]) for row in rows] == [
        (1, "SS-7", "2627"),
        (6, "SS-7", "2526"),
    ]
    by_id = {row["game_id"]: row for row in rows}
    assert by_id[1]["source_tournament_id"] == "7"
    assert by_id[1]["source_season_id"] == "96518"
    # The duplicate of event 1 in the D inverse list wins: freshest lineage.
    assert by_id[1]["raw_target_id"] == "2026-08-24-inverse"
    assert by_id[6]["raw_target_id"] == "2026-08-24"
    for row in rows:
        assert row["raw_endpoint"] == SCHEDULED_EVENTS_ENDPOINT
        assert row["raw_content_hash"]
        assert row["raw_blob_key"]
        assert row["raw_request_url"]
        assert row["raw_fetched_at"]
        assert row["home_team_name"] and row["away_team_name"]
        assert row["start_timestamp"]


def test_rows_reject_skeleton_events(tmp_path):
    first, _ = _fixture_events()
    skeleton = _event(first, event_id=9, tournament_id=READY_TOURNAMENT,
                      season_id=76953)
    skeleton.pop("homeTeam")
    client = _Client({
        "/sport/football/scheduled-events/2026-08-24": {"events": [skeleton]},
        "/sport/football/scheduled-events/2026-08-24/inverse": {"events": []},
    })
    fetched = fetch_daily_events(client, [date(2026, 8, 24)], _store(tmp_path))

    with pytest.raises(SofaScoreDQViolation, match="skeleton"):
        schedule_rows_from_events(fetched, SNAPSHOT, exclude_leagues=())


def test_rows_reject_events_whose_tournament_or_season_shape_drifted(tmp_path):
    # Schema drift must not masquerade as "foreign tournaments": an event of a
    # READY tournament without season.id / id is malformed and fails the run
    # (the raw list is already kept), never dropped silently.  An event that
    # cannot even be placed (renamed tournament key) is a minority "unscoped"
    # entry here and only warns — see the exotic-event test below.
    first, second = _fixture_events()
    renamed = _event(first, event_id=11, tournament_id=READY_TOURNAMENT,
                     season_id=76953)
    renamed["tournament"] = {"unique_tournament": {"id": READY_TOURNAMENT}}
    no_season = _event(second, event_id=12, tournament_id=READY_TOURNAMENT,
                       season_id=76953)
    no_season.pop("season")
    fine = _event(first, event_id=13, tournament_id=READY_TOURNAMENT,
                  season_id=76953)
    client = _Client({
        "/sport/football/scheduled-events/2026-08-24": {
            "events": [renamed, no_season, fine]
        },
        "/sport/football/scheduled-events/2026-08-24/inverse": {"events": []},
    })
    fetched = fetch_daily_events(client, [date(2026, 8, 24)], _store(tmp_path))

    with pytest.raises(DailyEventsSchemaError, match="1 of 3 .*ready tournaments"):
        schedule_rows_from_events(fetched, SNAPSHOT, exclude_leagues=())


def test_rows_tolerate_a_minority_of_exotic_unscoped_events(tmp_path, caplog):
    # Sol r3: a global day list legitimately carries exotic entries without a
    # tournament.uniqueTournament.id — one of them must not fail the day of
    # ~1500 tournaments; it is counted as ``unscoped`` and warned about.
    first, second = _fixture_events()
    exotic = _event(first, event_id=41, tournament_id=READY_TOURNAMENT,
                    season_id=76953)
    exotic["tournament"] = {"name": "Friendly XI"}
    exotic.pop("season")
    fine = [
        _event(second, event_id=42 + n, tournament_id=READY_TOURNAMENT,
               season_id=76953)
        for n in range(3)
    ]
    client = _Client({
        "/sport/football/scheduled-events/2026-08-24": {"events": [exotic, *fine]},
        "/sport/football/scheduled-events/2026-08-24/inverse": {"events": []},
    })
    fetched = fetch_daily_events(client, [date(2026, 8, 24)], _store(tmp_path))

    with caplog.at_level(logging.WARNING, logger="scrapers.sofascore.daily_events"):
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
    # it must fail the day after the raw list is kept, not be filtered out
    # into a green empty result.  Here the junk is half of the list, i.e. at
    # the ``_UNSCOPED_FAIL_SHARE`` threshold.
    first, _ = _fixture_events()
    fine = _event(first, event_id=31, tournament_id=READY_TOURNAMENT,
                  season_id=76953)
    client = _Client({
        "/sport/football/scheduled-events/2026-08-24": {"events": [junk, fine]},
        "/sport/football/scheduled-events/2026-08-24/inverse": {"events": []},
    })
    store = _store(tmp_path)
    fetched = fetch_daily_events(client, [date(2026, 8, 24)], store)

    assert store.has_payload(
        PayloadTarget(
            source_tournament_id="0",
            source_season_id="0",
            target_type="date",
            target_id="2026-08-24",
            endpoint=SCHEDULED_EVENTS_ENDPOINT,
            freshness_key="daily",
        )
    )
    with pytest.raises(DailyEventsSchemaError, match="1 of 2 .*cannot be placed"):
        schedule_rows_from_events(fetched, SNAPSHOT, exclude_leagues=())


def test_rows_reject_a_list_made_only_of_junk(tmp_path):
    # ``{"events": [null]}`` alone: nothing can be placed — fully malformed.
    client = _Client({
        "/sport/football/scheduled-events/2026-08-24": {"events": [None]},
        "/sport/football/scheduled-events/2026-08-24/inverse": {"events": []},
    })
    fetched = fetch_daily_events(client, [date(2026, 8, 24)], _store(tmp_path))

    with pytest.raises(DailyEventsSchemaError, match="1 of 1 .*cannot be placed"):
        schedule_rows_from_events(fetched, SNAPSHOT, exclude_leagues=())


def test_rows_warn_when_no_event_is_in_scope(tmp_path, caplog):
    # A day where every event belongs to configured/foreign tournaments is a
    # legitimate empty result, but "green and empty" must be visible in logs.
    first, second = _fixture_events()
    client = _Client({
        "/sport/football/scheduled-events/2026-08-24": {
            "events": [
                _event(first, event_id=21, tournament_id=CONFIGURED_TOURNAMENT,
                       season_id=76986),
                _event(second, event_id=22, tournament_id=FOREIGN_TOURNAMENT,
                       season_id=1),
            ]
        },
        "/sport/football/scheduled-events/2026-08-24/inverse": {"events": []},
    })
    fetched = fetch_daily_events(client, [date(2026, 8, 24)], _store(tmp_path))

    with caplog.at_level(logging.WARNING, logger="scrapers.sofascore.daily_events"):
        rows, counters = schedule_rows_from_events(
            fetched, SNAPSHOT, exclude_leagues={CONFIGURED_TOURNAMENT}
        )

    assert rows == []
    assert counters["events"] == counters["excluded"] == 2
    assert counters["matched"] == 0
    assert any(
        "no daily event matched" in record.getMessage()
        and "'excluded': 2" in record.getMessage()
        for record in caplog.records
    )
