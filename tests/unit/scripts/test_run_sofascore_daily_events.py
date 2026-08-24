"""CLI of the refresh lane's daily-list fetch (lane F, issue #1218)."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date

import pytest

from dags.scripts import run_sofascore_daily_events as daily


class _FakeClient:
    created: list[dict] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.closed = False
        _FakeClient.created.append(kwargs)

    @property
    def stats(self):
        return {"requests": 4, "paid_proxy_bytes": 2_500_000}

    def close(self):
        self.closed = True


@pytest.fixture
def offline(monkeypatch, tmp_path):
    """No network, no gateway, no Trino: every collaborator is a stub."""

    _FakeClient.created.clear()
    monkeypatch.delenv("SOFASCORE_PROXY_CONTROL_URL", raising=False)
    monkeypatch.setattr(daily, "LeaseBrowserSofaScoreClient", _FakeClient)
    monkeypatch.setattr(daily, "_configured_tournament_ids", lambda: frozenset({17}))
    calls = {}

    def fetch(client, dates, raw_store):
        calls["fetch"] = (client, list(dates), raw_store)
        return ["event-a", "event-b"]

    def rows(events, snapshot, exclude_leagues):
        calls["rows"] = (events, snapshot, exclude_leagues)
        return (
            [{"game_id": 1, "league": "SS-7"}],
            {"events": 2, "matched": 1, "excluded": 1, "unknown_seasons": 0},
        )

    def write(rows):
        calls["write"] = rows
        return "bronze.sofascore_schedule"

    monkeypatch.setattr(daily, "fetch_daily_events", fetch)
    monkeypatch.setattr(daily, "schedule_rows_from_events", rows)
    monkeypatch.setattr(daily, "write_schedule_rows", write)
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(json.dumps({"campaign_id": "c", "tournaments": []}))
    return {
        "calls": calls,
        "snapshot": snapshot,
        "output": tmp_path / "daily-events.json",
        "raw_store": f"file://{tmp_path / 'raw'}",
    }


def _argv(offline, *extra):
    return [
        "--snapshot", str(offline["snapshot"]),
        "--output", str(offline["output"]),
        "--raw-store-uri", offline["raw_store"],
        *extra,
    ]


@pytest.mark.unit
def test_main_fetches_both_days_writes_rows_and_reports_counters(offline):
    exit_code = daily.main(_argv(
        offline,
        "--dates", "2026-08-23", "2026-08-24",
        "--control-url", "http://sofascore-gw:8899",
        "--budget-cap-bytes", "1000",
        "--run-id", "scheduled__2026-08-24T00:30:00+00:00",
    ))

    assert exit_code == 0
    calls = offline["calls"]
    client, dates, raw_store = calls["fetch"]
    assert isinstance(client, _FakeClient)
    assert client.closed is True
    assert dates == [date(2026, 8, 23), date(2026, 8, 24)]
    assert raw_store.root.endswith("/raw")
    assert client.kwargs["control_url"] == "http://sofascore-gw:8899"
    assert client.kwargs["budget_cap_bytes"] == 1000
    assert client.kwargs["dag_id"] == "dag_refresh_sofascore_all_mens"
    assert client.kwargs["task_id"] == "fetch_daily_events"
    assert client.kwargs["run_id"] == "scheduled__2026-08-24T00:30:00+00:00"
    events, snapshot, exclude = calls["rows"]
    assert events == ["event-a", "event-b"]
    assert snapshot["campaign_id"] == "c"
    assert exclude == frozenset({17})
    assert calls["write"] == [{"game_id": 1, "league": "SS-7"}]
    report = json.loads(offline["output"].read_text())
    assert report["status"] == "success"
    assert report["dates"] == ["2026-08-23", "2026-08-24"]
    assert report["events"] == 2
    assert report["matched"] == 1
    assert report["excluded"] == 1
    assert report["rows_written"] == 1
    assert report["table"] == "bronze.sofascore_schedule"
    assert report["discovery"]["paid_proxy_bytes"] == 2_500_000
    assert report["errors"] == []


@pytest.mark.unit
def test_default_dates_are_yesterday_and_today_utc():
    assert daily._default_dates(today=date(2026, 8, 24)) == [
        date(2026, 8, 23), date(2026, 8, 24)
    ]


@pytest.mark.unit
def test_control_url_and_budget_default_from_the_environment(offline, monkeypatch):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_URL", "http://env-gw:8899")
    monkeypatch.setenv("SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES", "2048")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "manual__env")

    assert daily.main(_argv(offline, "--dates", "2026-08-24")) == 0
    client = _FakeClient.created[0]
    assert client["control_url"] == "http://env-gw:8899"
    assert client["budget_cap_bytes"] == 2048
    assert client["run_id"] == "manual__env"


@pytest.mark.unit
def test_main_fails_closed_without_a_gateway_url(offline):
    exit_code = daily.main(_argv(offline, "--dates", "2026-08-24"))

    assert exit_code == 1
    assert _FakeClient.created == []
    assert "fetch" not in offline["calls"]
    report = json.loads(offline["output"].read_text())
    assert report["status"] == "failed"
    assert "control-url" in report["errors"][0]


@pytest.mark.unit
def test_fetch_failure_is_reported_and_nothing_is_written(offline, monkeypatch):
    def fetch(client, dates, raw_store):
        raise RuntimeError("gateway said 429")

    monkeypatch.setattr(daily, "fetch_daily_events", fetch)

    exit_code = daily.main(_argv(
        offline, "--dates", "2026-08-24", "--control-url", "http://gw",
    ))

    assert exit_code == 1
    assert "write" not in offline["calls"]
    report = json.loads(offline["output"].read_text())
    assert report["status"] == "failed"
    assert report["errors"] == ["RuntimeError: gateway said 429"]
    assert report["discovery"]["requests"] == 4


@pytest.mark.unit
def test_no_matching_events_writes_nothing(offline, monkeypatch):
    monkeypatch.setattr(
        daily, "schedule_rows_from_events",
        lambda *a: ([], {"events": 5, "matched": 0, "excluded": 5, "unknown_seasons": 0}),
    )

    assert daily.main(_argv(
        offline, "--dates", "2026-08-24", "--control-url", "http://gw",
    )) == 0
    assert "write" not in offline["calls"]
    report = json.loads(offline["output"].read_text())
    assert report["rows_written"] == 0
    assert "table" not in report


@pytest.mark.unit
def test_write_schedule_rows_merges_by_game_inside_the_writer_lock(monkeypatch):
    events = []

    class _Scraper:
        def __init__(self, **kwargs):
            events.append(("scraper", kwargs))

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            events.append("scraper:exit")
            return False

        def _add_metadata(self, frame, entity_type):
            events.append(("metadata", entity_type, len(frame)))
            return frame

        def save_to_iceberg(self, **kwargs):
            events.append(("save", kwargs["table_name"], kwargs["natural_keys"],
                           kwargs["partition_cols"], len(kwargs["df"])))
            return "bronze.sofascore_schedule"

    @contextmanager
    def lock():
        events.append("lock:enter")
        yield True
        events.append("lock:exit")

    import scrapers.sofascore.scraper as scraper_module
    import scrapers.sofascore.writer_lock as writer_lock

    monkeypatch.setattr(scraper_module, "SofaScoreScraper", _Scraper)
    monkeypatch.setattr(writer_lock, "bronze_writer_lock", lock)

    table = daily.write_schedule_rows([
        {"game_id": 1, "league": "SS-7", "season": "2627"},
        {"game_id": 2, "league": "SS-7", "season": "2627"},
    ])

    assert table == "bronze.sofascore_schedule"
    assert events == [
        ("scraper", {}),
        "lock:enter",
        ("metadata", "schedule", 2),
        ("save", "sofascore_schedule", ["league", "season", "game_id"],
         ["league", "season"], 2),
        "lock:exit",
        "scraper:exit",
    ]
