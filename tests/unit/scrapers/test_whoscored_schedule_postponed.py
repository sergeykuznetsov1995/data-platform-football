"""#1059 / #1475: a postponed match and an empty bet market must not fail a scope.

Fixtures are real ``schedule_month`` documents from the raw store, trimmed to
the affected game:

* game 1958270 (Primera B Nacional, stage 25050): the closed July document
  froze it as postponed on 20.07 (status 2), the August document carries the
  new date 19.08 -> the old code raised ``conflicting stage data``;
* game 1961066 (stage 25068): ``bets.home`` is ``null`` -> the old code raised
  ``bets.home must be an object``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path

from scrapers.whoscored.parsers import parse_schedule_bets, parse_schedule_json
from scrapers.whoscored.service import ACTIVE_SCHEDULE_CACHE_TTL
from scrapers.whoscored.transport import TransportResponse, TransportRoute

from tests.unit.scrapers.test_whoscored_service import _service

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "whoscored"
POSTPONED_JULY = FIXTURES / "whoscored_schedule_25050_202607_game1958270.json"
POSTPONED_AUGUST = FIXTURES / "whoscored_schedule_25050_202608_game1958270.json"
NULL_HOME_MARKET = FIXTURES / "whoscored_schedule_25068_202608_game1961066.json"


def _set_status(payload: bytes, status: int) -> bytes:
    document = json.loads(payload)
    for tournament in document["tournaments"]:
        for match in tournament["matches"]:
            match["status"] = status
    return json.dumps(document).encode()


def _sync_schedule(tmp_path, monkeypatch, *, today, months, active=True):
    """Run ``sync_schedule`` for one stage whose months are served from bytes."""

    class _FixedDate(date):
        @classmethod
        def today(cls):
            return cls(*today)

    monkeypatch.setattr("scrapers.whoscored.service.date", _FixedDate)
    service, repository, _ = _service(tmp_path)
    service.catalog_season = replace(
        service.catalog_season,
        end=None if active else date(2020, 1, 1),
        stage_ids=(700,),
    )
    service._source_season_id = lambda: 9001
    mask = {}
    for token in months:
        mask.setdefault(int(token[:4]), {})[int(token[4:]) - 1] = {1: 1}
    calendar_html = (
        f"<script>var wsCalendar = {{mask:{json.dumps(mask)}}};</script>"
    ).replace('"', "")
    stage_html = """
    <select id="stages"><option
      value="/Regions/247/Tournaments/36/Seasons/9001/Stages/700/Fixtures/world-cup-2026">
      Group Stage</option></select>
    """
    calls = []

    def fake_fetch(target, **kwargs):
        calls.append((target.page_kind, dict(target.source_ids), kwargs))
        if target.page_kind == "season_stages":
            content = stage_html.encode()
        elif target.page_kind == "stage_calendar":
            content = calendar_html.encode()
        elif target.page_kind == "team_stage_statistics":
            content = b'{"teamTableStats": []}'
        elif target.page_kind == "player_stage_statistics":
            content = b'{"playerTableStats": []}'
        elif target.page_kind == "team_stage_feed":
            content = b"[[]]"
        elif target.page_kind == "referee_stage_statistics":
            content = b"<html><body><h1>Referee Statistics</h1></body></html>"
        else:
            content = months[target.source_ids["month"]]
        response = TransportResponse(
            url=target.canonical_url,
            content=content,
            status_code=200,
            headers={},
            route=TransportRoute.DIRECT_HTTP,
            wire_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        )
        assert kwargs["validator"](response) is True
        return response, f"s3://raw/{target.target_id}/{response.sha256}"

    def fetch_parsed_many(specs):
        return [
            service._fetch_parsed(
                spec.target,
                parser=spec.parser,
                content_type=spec.content_type,
                allow_cache=spec.allow_cache,
                cache_ttl=spec.cache_ttl,
                browser_bootstrap_url=spec.browser_bootstrap_url,
            )
            for spec in specs
        ]

    service._fetch = fake_fetch
    service._fetch_parsed_many = fetch_parsed_many
    result = service.sync_schedule()
    return result, repository, calls


def _month_ttls(calls, month):
    return [
        kwargs.get("cache_ttl")
        for kind, ids, kwargs in calls
        if kind == "schedule_month" and ids.get("month") == month
    ]


def test_postponed_match_takes_the_row_of_its_kickoff_month(tmp_path, monkeypatch):
    result, repository, _ = _sync_schedule(
        tmp_path,
        monkeypatch,
        today=(2026, 8, 20),
        months={
            "202607": POSTPONED_JULY.read_bytes(),
            "202608": POSTPONED_AUGUST.read_bytes(),
        },
    )

    assert result.status == "success", result.as_dict()
    assert result.counts["schedule"] == 1
    (snapshot,) = repository.scope_snapshots
    (row,) = snapshot["datasets"]["whoscored_schedule"]
    assert row["game_id"] == 1958270
    assert row["date"] == datetime(2026, 8, 19, 18, 0)
    assert row["status"] == 1


def _with_kickoff(payload: bytes, kickoff: str, *, status: int) -> bytes:
    document = json.loads(payload)
    for tournament in document["tournaments"]:
        for match in tournament["matches"]:
            match["startTimeUtc"] = kickoff
            match["status"] = status
    return json.dumps(document).encode()


def test_moved_match_outside_every_month_takes_the_later_document(
    tmp_path, monkeypatch
):
    result, repository, _ = _sync_schedule(
        tmp_path,
        monkeypatch,
        today=(2026, 8, 20),
        months={
            "202607": _with_kickoff(
                POSTPONED_JULY.read_bytes(), "2026-09-02T18:00:00Z", status=2
            ),
            "202608": _with_kickoff(
                POSTPONED_AUGUST.read_bytes(), "2026-09-05T18:00:00Z", status=1
            ),
        },
    )

    assert result.status == "success", result.as_dict()
    (snapshot,) = repository.scope_snapshots
    (row,) = snapshot["datasets"]["whoscored_schedule"]
    assert row["date"] == datetime(2026, 9, 5, 18, 0)
    assert row["status"] == 1


def test_matching_month_wins_even_when_traversed_before_a_stale_one(
    tmp_path, monkeypatch
):
    result, repository, _ = _sync_schedule(
        tmp_path,
        monkeypatch,
        today=(2026, 8, 20),
        months={
            "202607": _with_kickoff(
                POSTPONED_JULY.read_bytes(), "2026-07-20T18:00:00Z", status=6
            ),
            "202608": _with_kickoff(
                POSTPONED_AUGUST.read_bytes(), "2026-09-05T18:00:00Z", status=1
            ),
        },
    )

    assert result.status == "success", result.as_dict()
    (snapshot,) = repository.scope_snapshots
    (row,) = snapshot["datasets"]["whoscored_schedule"]
    assert row["date"] == datetime(2026, 7, 20, 18, 0)
    assert row["status"] == 6


def test_closed_month_with_unplayed_postponed_match_keeps_the_active_ttl(
    tmp_path, monkeypatch
):
    months = {
        "202607": POSTPONED_JULY.read_bytes(),
        "202608": POSTPONED_AUGUST.read_bytes(),
    }
    result, _, calls = _sync_schedule(
        tmp_path, monkeypatch, today=(2026, 8, 20), months=months
    )

    assert result.status == "success", result.as_dict()
    # July closed on 08.08: first read from the permanent cache, then the
    # postponed, still unplayed game forces the short TTL.
    assert _month_ttls(calls, "202607") == [None, ACTIVE_SCHEDULE_CACHE_TTL]
    assert _month_ttls(calls, "202608") == [ACTIVE_SCHEDULE_CACHE_TTL]


def test_closed_month_returns_to_permanent_cache_once_the_match_is_played(
    tmp_path, monkeypatch
):
    months = {
        "202607": POSTPONED_JULY.read_bytes(),
        "202608": _set_status(POSTPONED_AUGUST.read_bytes(), 6),
    }
    result, repository, calls = _sync_schedule(
        tmp_path, monkeypatch, today=(2026, 9, 20), months=months
    )

    assert result.status == "success", result.as_dict()
    assert _month_ttls(calls, "202607") == [None]
    assert _month_ttls(calls, "202608") == [None]
    (snapshot,) = repository.scope_snapshots
    (row,) = snapshot["datasets"]["whoscored_schedule"]
    assert row["status"] == 6


def test_closed_postponed_month_of_a_finished_season_stays_permanent(
    tmp_path, monkeypatch
):
    result, _, calls = _sync_schedule(
        tmp_path,
        monkeypatch,
        today=(2026, 8, 20),
        months={
            "202607": POSTPONED_JULY.read_bytes(),
            "202608": POSTPONED_AUGUST.read_bytes(),
        },
        active=False,
    )

    assert result.status == "success", result.as_dict()
    assert _month_ttls(calls, "202607") == [None]


def test_null_bet_market_is_skipped(tmp_path, monkeypatch):
    content = NULL_HOME_MARKET.read_bytes()
    schedule = parse_schedule_json(
        content,
        scope=_service(tmp_path)[0].scope,
        stage_id=25068,
    )
    raw_bets = json.loads(schedule.rows[0]["bets"])
    assert raw_bets["home"] is None

    bets = parse_schedule_bets(schedule)

    assert {row["source_outcome"] for row in bets.rows} == {"draw", "away"}

    result, repository, _ = _sync_schedule(
        tmp_path / "service",
        monkeypatch,
        today=(2026, 7, 29),
        months={"202608": content},
    )
    assert result.status == "success", result.as_dict()
    assert result.counts["match_bets"] == 2
