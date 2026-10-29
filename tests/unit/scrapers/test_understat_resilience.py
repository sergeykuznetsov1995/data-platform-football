"""#1437: additive schema drift and bounded, observable HTTP retries (offline)."""

from copy import deepcopy
from datetime import date, datetime, timezone
import json
from unittest.mock import MagicMock

import pandas as pd
import pytest

from dags.scripts import run_understat_scraper as runner
from scrapers.understat import UnderstatClient, UnderstatHTTPError, UnderstatSchemaDrift, UnderstatSource
from tests.unit.scrapers.test_understat_native import (
    _Clock, _Response, _Session, _SourceClient,
)
from tests.unit.scrapers.test_run_understat_scraper import (
    _args, _complete_frames, _hash_scraper, _LEAGUE_HASHES, _published_baseline,
    _Repository, _scraper_factory,
)


class _ExtraClient(_SourceClient):
    def __init__(self, endpoint, path):
        super().__init__()
        self.endpoint = endpoint
        self.path = path
        self.enabled = True

    def _extra(self, endpoint, payload):
        payload = deepcopy(payload)
        if self.enabled and endpoint == self.endpoint:
            target = payload
            for key in self.path:
                target = target[key]
            target["new_metric"] = {"private_value": "never log payload values"}
        return payload

    def get_league_data(self, *args, **kwargs):
        return self._extra("league", super().get_league_data(*args, **kwargs))

    def get_match_data(self, *args, **kwargs):
        return self._extra("match", super().get_match_data(*args, **kwargs))

    def get_team_data(self, *args, **kwargs):
        return self._extra("team", super().get_team_data(*args, **kwargs))


@pytest.mark.parametrize(("endpoint", "path"), [
    ("league", ()), ("league", ("dates", 0)), ("league", ("dates", 0, "h")),
    ("league", ("dates", 0, "goals")), ("league", ("players", 0)),
    ("league", ("teams", "1")), ("league", ("teams", "1", "history", 0)),
    ("league", ("teams", "1", "history", 0, "ppda")),
    ("match", ()), ("match", ("shots",)), ("match", ("shots", "h", 0)),
    ("match", ("rosters", "h", 0)), ("team", ()), ("team", ("players", 0)),
    ("team", ("statistics",)), ("team", ("statistics", "situation", "Penalty")),
    ("team", ("statistics", "situation", "Penalty", "against")),
])
def test_additive_fields_preserve_frames_publish_and_serialize_warning(endpoint, path, caplog):
    client = _ExtraClient(endpoint, path)
    source = UnderstatSource(client, today=date(2026, 7, 27))
    expected = UnderstatSource(_SourceClient(), today=date(2026, 7, 27)).scrape_scope(
        "ENG-Premier League", "2526", 2025
    )
    frames = source.scrape_scope("ENG-Premier League", "2526", 2025)
    for entity, frame in expected.items():
        pd.testing.assert_frame_equal(frame, frames[entity])
    assert source.schema_warnings
    assert all(warning["fields"] == ["new_metric"] for warning in source.schema_warnings)
    assert "Understat unknown fields" in caplog.text
    assert "private_value" not in caplog.text

    factory, scraper = _scraper_factory(_complete_frames())
    scraper.source = source
    repository = _Repository()
    result, code = runner.run_scope(_args(), scraper_factory=factory, repository=repository)
    assert code == 0
    assert scraper.save_to_iceberg.call_count == 7
    quality = json.loads(json.dumps(result["scope_attempt"]["quality"]))
    warnings = [item for item in quality["issues"] if item["code"] == "unknown_source_fields"]
    assert len(warnings) == len(source.schema_warnings)
    assert all(item["status"] == "warning" for item in warnings)
    assert "private_value" not in json.dumps(warnings)

    client.enabled = False
    source.league_snapshot("ENG-Premier League", "2526", 2025)
    assert source.schema_warnings == []
    source.scrape_scope("ENG-Premier League", "2526", 2025)
    assert source.schema_warnings == []


@pytest.mark.parametrize("invalid", ["missing", "type"])
def test_extra_fields_do_not_mask_required_contract_error(invalid):
    client = _ExtraClient("league", ())
    original = client.get_league_data
    def broken(*args, **kwargs):
        payload = original(*args, **kwargs)
        if invalid == "missing":
            del payload["players"]
        else:
            payload["players"] = "wrong type"
        return payload
    client.get_league_data = broken
    with pytest.raises(UnderstatSchemaDrift) as caught:
        UnderstatSource(client).scrape_scope("ENG-Premier League", "2526", 2025)
    assert runner._classify_exception(caught.value)[1] == 2
    assert client.match_calls == []


@pytest.mark.parametrize(("header", "expected"), [
    ("3600", 120), ("Mon, 27 Jul 2026 01:00:00 GMT", 120),
    ("3", 3), ("invalid", 1), ("-2", 1),
])
def test_retry_after_cap_and_counts(header, expected, caplog):
    clock = _Clock()
    session = _Session([_Response(200), _Response(429, headers={"Retry-After": header}), _Response(200)])
    client = UnderstatClient(session=session, requests_per_minute=600,
                            sleep=clock.sleep, monotonic=clock.monotonic,
                            now=lambda: datetime(2026, 7, 27, tzinfo=timezone.utc),
                            jitter=lambda *_: 0)
    client.get_stat_data()
    assert clock.sleeps == [0.1, expected]
    assert (client.request_count, client.retry_count) == (3, 1)
    assert "reason=HTTP 429 attempt=2/5" in caplog.text
    assert f"delay={float(expected)}" in caplog.text


@pytest.mark.parametrize("exhaust", [False, True])
@pytest.mark.parametrize("transport", [False, True])
def test_retry_transport_and_exhaustion(transport, exhaust, caplog):
    clock = _Clock()
    items = [ConnectionError("offline")] if transport else [_Response(503)]
    items = items * 3 if exhaust else items + [_Response(200)]
    class Session(_Session):
        def get(self, *args, **kwargs):
            item = super().get(*args, **kwargs)
            if isinstance(item, Exception):
                raise item
            return item
    client = UnderstatClient(session=Session([_Response(200)] + items), max_attempts=3,
                            sleep=clock.sleep, monotonic=clock.monotonic, jitter=lambda *_: 0)
    if exhaust:
        with pytest.raises(ConnectionError if transport else UnderstatHTTPError):
            client.get_stat_data()
    else:
        client.get_stat_data()
    assert (client.request_count, client.retry_count) == ((4, 2) if exhaust else (3, 1))
    assert len([record for record in caplog.records if "Understat retry:" in record.message]) == (2 if exhaust else 1)


@pytest.mark.parametrize("fail", [False, True])
def test_runner_reports_http_totals_on_success_and_failure(fail, caplog):
    caplog.set_level("INFO")
    factory, scraper = _scraper_factory(_complete_frames())
    scraper.client.request_count = 12
    scraper.client.retry_count = 3
    if fail:
        scraper.scrape_scope.side_effect = ConnectionError("offline")
    result, code = runner.run_scope(_args(), scraper_factory=factory, repository=_Repository())
    assert code == (1 if fail else 0)
    assert (result["request_count"], result["retry_count"]) == (12, 3)
    assert result["scope_attempt"]["quality"]["retry_count"] == 3
    assert "request_count=12 retry_count=3" in caplog.text


def test_closed_check_warning_forces_publication_without_mutating_baseline():
    repository = _Repository()
    baseline = _published_baseline(repository)
    before = deepcopy(baseline.to_dict())
    probe = _hash_scraper(_complete_frames(), hashes=_LEAGUE_HASHES, request_count=3)
    full = _hash_scraper(_complete_frames(), hashes=_LEAGUE_HASHES, request_count=10)
    probe.client.retry_count = 1
    full.client.retry_count = 2
    probe.source.schema_warnings = [{"path": "getLeagueData", "fields": ["new_metric"], "response": "league_EPL_2025.json"}]
    full.source.schema_warnings = []
    result, code = runner.run_scope(_args(mode="closed_check"),
                                   scraper_factory=MagicMock(side_effect=[probe, full]),
                                   repository=repository)
    assert code == 0
    assert full.save_to_iceberg.call_count == 7
    assert baseline.to_dict() == before
    assert (result["request_count"], result["retry_count"]) == (13, 3)
    assert result["scope_attempt"]["quality"]["issues"][0]["code"] == "unknown_source_fields"


def test_unknown_only_dimension_keeps_empty_statistics_valid():
    from scrapers.understat.parsers import validate_team_payload
    warnings = []
    validate_team_payload({"dates": [], "players": [], "statistics": {"new_metric": None}},
                          warnings=warnings)
    assert warnings == [{"path": "getTeamData.statistics", "fields": ["new_metric"]}]


@pytest.mark.parametrize("probe_fails", [False, True])
def test_closed_check_failure_counts_each_client_once(probe_fails):
    repository = _Repository()
    _published_baseline(repository)
    probe = _hash_scraper(_complete_frames(), hashes={**_LEAGUE_HASHES, "schedule": "changed"}, request_count=3)
    full = _hash_scraper(_complete_frames(), hashes=_LEAGUE_HASHES, request_count=10)
    probe.client.retry_count = 1
    full.client.retry_count = 2
    if probe_fails:
        probe.league_snapshot.side_effect = ConnectionError("offline")
    else:
        full.scrape_scope.side_effect = ConnectionError("offline")
    result, code = runner.run_scope(_args(mode="closed_check"),
                                   scraper_factory=MagicMock(side_effect=[probe, full]),
                                   repository=repository)
    assert code == 1
    assert (result["request_count"], result["retry_count"]) == ((3, 1) if probe_fails else (13, 3))


def test_closed_check_repeated_warning_is_recorded_once():
    repository = _Repository()
    _published_baseline(repository)
    probe = _hash_scraper(_complete_frames(), hashes=_LEAGUE_HASHES, request_count=3)
    full = _hash_scraper(_complete_frames(), hashes=_LEAGUE_HASHES, request_count=10)
    warning = {"path": "getLeagueData", "fields": ["new_metric"], "response": "league_EPL_2025.json"}
    probe.source.schema_warnings = [warning]
    full.source.schema_warnings = [dict(warning)]
    result, code = runner.run_scope(_args(mode="closed_check"),
                                   scraper_factory=MagicMock(side_effect=[probe, full]),
                                   repository=repository)
    assert code == 0
    assert sum(issue["code"] == "unknown_source_fields"
               for issue in result["scope_attempt"]["quality"]["issues"]) == 1


def test_backfill_skip_reports_zero_current_http_calls(caplog):
    caplog.set_level("INFO")
    repository = _Repository()
    baseline = _published_baseline(repository)
    # _published_baseline records a successful previous run with 412 requests.
    assert baseline.quality["request_count"] == 412
    factory = MagicMock(side_effect=AssertionError("skip must not create a scraper"))
    result, code = runner.run_scope(_args(mode="backfill"), scraper_factory=factory,
                                   repository=repository)
    assert code == 0
    assert (result["request_count"], result["retry_count"]) == (0, 0)
    assert result["scope_attempt"] == baseline.to_dict()
    assert "request_count=0 retry_count=0" in caplog.text
