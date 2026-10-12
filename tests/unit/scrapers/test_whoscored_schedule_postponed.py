"""#1059 / #1475: a postponed match and an empty bet market must not fail a scope.

Fixtures are real ``schedule_month`` documents from the raw store, trimmed to
the affected game:

* game 1958270 (Primera B Nacional, stage 25050): the closed July document
  froze it as postponed on 20.07 (status 2), the August document carries the
  new date 19.08 -> the old code raised ``conflicting stage data``;
* game 1961066 (stage 25068): ``bets.home`` is ``null`` -> the old code raised
  ``bets.home must be an object``;
* stage 25590, October 2026: real raw hashes 985178ec.. (08.10) and
  51094524.. (11.10), trimmed to games 1990155 and 1990104. The postponed
  1990155 disappears while 1990104 acquires its final score. The December
  relocation below is synthetic, not evidence of a source-provided date.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path

import pytest

from scrapers.whoscored.parsers import parse_schedule_bets, parse_schedule_json
from scrapers.whoscored.service import ACTIVE_SCHEDULE_CACHE_TTL
from scrapers.whoscored.transport import TransportResponse, TransportRoute

from tests.unit.scrapers.test_whoscored_service import _service

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "whoscored"
POSTPONED_JULY = FIXTURES / "whoscored_schedule_25050_202607_game1958270.json"
POSTPONED_AUGUST = FIXTURES / "whoscored_schedule_25050_202608_game1958270.json"
NULL_HOME_MARKET = FIXTURES / "whoscored_schedule_25068_202608_game1961066.json"
SCHEDULE_GAP_BEFORE = FIXTURES / "whoscored_schedule_25590_202610_gap_before.json"
SCHEDULE_GAP_AFTER = FIXTURES / "whoscored_schedule_25590_202610_gap_after.json"


def _set_status(payload: bytes, status: int) -> bytes:
    document = json.loads(payload)
    for tournament in document["tournaments"]:
        for match in tournament["matches"]:
            match["status"] = status
    return json.dumps(document).encode()


def _sync_schedule(
    tmp_path, monkeypatch, *, today, months, active=True,
    refreshed_months=None, previous_schedule_ids=None, publication_error=None,
):
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
    if previous_schedule_ids is not None:
        from scrapers.whoscored.repository import ScheduleCompletenessError

        original_commit = repository.commit_scope_bundle

        def guarded_commit(**snapshot):
            rows = snapshot["datasets"]["whoscored_schedule"]
            if publication_error is not None:
                raise publication_error
            if len(rows) < len(previous_schedule_ids):
                raise ScheduleCompletenessError(
                    new_count=len(rows), old_count=len(previous_schedule_ids)
                )
            missing = set(previous_schedule_ids) - {r["game_id"] for r in rows}
            if snapshot.get("require_schedule_game_ids") and missing:
                raise ScheduleCompletenessError(
                    new_count=len(rows), old_count=len(previous_schedule_ids),
                    missing_game_ids=missing,
                )
            return original_commit(**snapshot)

        repository.commit_scope_bundle = guarded_commit
        repository.missing_schedule_game_ids = lambda _league, _season, observed: (
            tuple(sorted(set(previous_schedule_ids) - set(observed)))
        )

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
            source_months = (
                refreshed_months
                if refreshed_months is not None and not kwargs.get("allow_cache", True)
                else months
            )
            content = source_months[target.source_ids["month"]]
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


def _gap_months(*, recover):
    """Real October omission; a synthetic December relocation tests recovery."""
    old = json.loads(SCHEDULE_GAP_BEFORE.read_bytes())
    fresh = json.loads(SCHEDULE_GAP_AFTER.read_bytes())
    future = json.loads(SCHEDULE_GAP_AFTER.read_bytes())
    future["tournaments"][0]["matches"] = []
    if recover:
        moved = next(m for m in old["tournaments"][0]["matches"] if m["id"] == 1990155)
        moved["startTimeUtc"] = "2026-12-01T15:00:00Z"
        moved["status"] = 1
        future["tournaments"][0]["matches"].append(moved)
    empty = json.loads(SCHEDULE_GAP_AFTER.read_bytes())
    empty["tournaments"][0]["matches"] = []
    return (
        {"202610": json.dumps(fresh).encode(), "202612": json.dumps(empty).encode()},
        {"202610": json.dumps(fresh).encode(), "202612": json.dumps(future).encode()},
    )


def test_schedule_shrink_rechecks_far_month_and_publishes_only_complete_bundle(tmp_path, monkeypatch):
    months, fresh = _gap_months(recover=True)
    result, repository, calls = _sync_schedule(
        tmp_path, monkeypatch, today=(2026, 10, 12), months=months,
        refreshed_months=fresh, previous_schedule_ids=(1990155, 1990104),
    )
    assert result.status == "success", result.errors
    assert result.counts["schedule"] == 2
    assert len(repository.scope_snapshots) == 1
    rows = repository.scope_snapshots[0]["datasets"]["whoscored_schedule"]
    moved = next(r for r in rows if r["game_id"] == 1990155)
    assert moved["date"].month == 12
    forced = [(ids["month"], kw) for kind, ids, kw in calls
              if kind == "schedule_month" and not kw.get("allow_cache", True)]
    assert [month for month, _ in forced] == ["202610", "202612"]
    assert result.metadata["schedule_recheck"]["attempted"] is True
    snapshot = repository.scope_snapshots[0]
    assert any(hashlib.sha256(fresh["202612"]).hexdigest() in uri for uri in snapshot["raw_uris"])
    assert not any(hashlib.sha256(months["202612"]).hexdigest() in uri for uri in snapshot["raw_uris"])


@pytest.mark.parametrize("active", [True, False])
def test_schedule_missing_postponed_game_stays_red_without_partial_publication(tmp_path, monkeypatch, active):
    months, fresh = _gap_months(recover=False)
    result, repository, calls = _sync_schedule(
        tmp_path, monkeypatch, today=(2026, 10, 12), months=months,
        refreshed_months=fresh, previous_schedule_ids=(1990155, 1990104), active=active,
    )
    assert result.status == "failed"
    assert "published snapshot cannot shrink" in result.errors[0]
    assert not repository.scope_snapshots
    forced = [1 for kind, _, kw in calls if kind == "schedule_month" and not kw.get("allow_cache", True)]
    assert len(forced) == (2 if active else 0)
    assert result.metadata["schedule_recheck"]["missing_game_ids"] == [1990155]


def test_schedule_other_publication_errors_do_not_force_month_recheck(tmp_path, monkeypatch):
    months, fresh = _gap_months(recover=True)
    result, repository, calls = _sync_schedule(
        tmp_path, monkeypatch, today=(2026, 10, 12), months=months,
        refreshed_months=fresh, previous_schedule_ids=(1990155, 1990104),
        publication_error=ValueError("other dataset completeness guard"),
    )
    assert result.status == "failed"
    assert not repository.scope_snapshots
    assert all(kw.get("allow_cache", True) for _, _, kw in calls)


def test_new_game_cannot_compensate_missing_postponed_id_on_recheck(tmp_path, monkeypatch):
    months, fresh = _gap_months(recover=True)
    future = json.loads(fresh["202612"])
    future["tournaments"][0]["matches"][0]["id"] = 2999999
    fresh["202612"] = json.dumps(future).encode()
    result, repository, _ = _sync_schedule(
        tmp_path, monkeypatch, today=(2026, 10, 12), months=months,
        refreshed_months=fresh, previous_schedule_ids=(1990155, 1990104),
    )
    assert result.status == "failed"
    assert not repository.scope_snapshots
    assert result.metadata["schedule_recheck"]["missing_game_ids"] == [1990155]


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
