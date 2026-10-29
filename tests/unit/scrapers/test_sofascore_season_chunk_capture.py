"""Offline runner integration: real raw checks, budget ledger and scope lock."""
from __future__ import annotations

import copy
import json
import sys
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock
from urllib.parse import urlsplit

import pytest
from pyarrow import fs

from dags.scripts import run_sofascore_scraper as runner
from scrapers.sofascore import live_capture, writer_lock
from scrapers.sofascore.adapters import MANIFEST_KEY_COLUMNS, TrinoManifestStore
from scrapers.sofascore.capture_engine import HttpPayload, RetryPolicy, SofaScoreCaptureEngine
from scrapers.sofascore.manifest import BatchingManifestStore
from scrapers.sofascore.pipeline import CaptureRuntime, DeferredCaptureSink, finalize_materialized_results
from scrapers.sofascore.raw_store import RawPayloadStore
from scrapers.sofascore.season_pipeline import (
    build_schedule_page_spec, materialize_season_partition, plan_season_partition,
)
from scrapers.sofascore.workload_plan import (
    SEASON_DYNAMIC_ENDPOINTS, SEASON_STATIC_ENDPOINTS, SeasonWorkload,
    WorkloadBudgetPolicy, WorkloadClassBudget, production_season_shape,
    season_workload_class, workload_shape_digest,
)
from scrapers.sofascore.workload_runtime import PartitionWorkload, build_partitioned_plan
from scripts.proxy_filter.budget import BudgetPolicy, SharedBudgetLedger


pytestmark = pytest.mark.unit
TOKEN = "offline-concurrent-chunk-control-token-at-least32bytes"
FRESHNESS = "same-native-final"
COMMON = dict(source_tournament_id=17, source_season_id=76986, freshness_key=FRESHNESS)
SHAPE = production_season_shape(season_format="split_year", team_count_band="16_20", max_pages_per_direction=50)
CLASS = season_workload_class(SHAPE)


class _FakePostgres:
    """Only SQL/session boundary is fake; production scope-lock code runs."""
    def __init__(self):
        self.guard = threading.Lock()
        self.locks = {}
        self.local = threading.local()
        self.contended = threading.Event()

    def connect(self, dsn):
        assert dsn == "postgresql://offline.invalid/metadata"
        owner = self

        class Connection:
            held = None

            def cursor(self):
                return self

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, sql, params):
                assert sql == "SELECT pg_try_advisory_lock(%s)"
                with owner.guard:
                    lock = owner.locks.setdefault(params[0], threading.Lock())
                self.acquired = lock.acquire(blocking=False)
                if self.acquired:
                    self.held = lock
                    owner.local.held = getattr(owner.local, "held", 0) + 1
                else:
                    owner.contended.set()

            def fetchone(self):
                return (self.acquired,)

            def close(self):
                if self.held is not None:
                    owner.local.held -= 1
                    self.held.release()
                    self.held = None

        return Connection()


class _FakeTrino:
    catalog = "iceberg"

    def __init__(self, pg):
        self.pg = pg
        self.rows = {}
        self.reads_under_lock = 0

    def _execute(self, sql, *, fetch=False, params=()):
        assert sql.startswith("SELECT ") and fetch
        if getattr(self.pg.local, "held", False):
            self.reads_under_lock += 1
        rows = list(self.rows.values())
        if len(params) == 2:
            return [row for row in rows if (row["source_tournament_id"], row["source_season_id"]) == tuple(map(str, params))]
        if len(params) == len(MANIFEST_KEY_COLUMNS):
            return [row for row in rows if tuple(row[key] for key in MANIFEST_KEY_COLUMNS) == tuple(params)]
        raise AssertionError((sql, params))

    def insert_dataframe_atomic(self, schema, table, frame, *, merge_keys):
        assert getattr(self.pg.local, "held", False), "manifest flush escaped capture lock"
        for row in frame.to_dict("records"):
            self.rows[tuple(row[key] for key in merge_keys)] = row


class _Source:
    def __init__(self, pg, *, overlap=False):
        self.pg = pg
        self.overlap = overlap
        self.calls = Counter()
        fixture = Path(__file__).resolve().parents[2] / "fixtures/sofascore_season_76986_schedule_last_0.json"
        self.event = json.loads(fixture.read_text())["events"][0]

    def response(self, url):
        path = urlsplit(url).path
        if "/events/last/" in path:
            page = int(path.rsplit("/", 1)[1])
            event = copy.deepcopy(self.event)
            event["id"] = 14000000 + page
            event.pop("referee", None)
            return 200, json.dumps({"events": [event], "hasNextPage": page < 54}).encode()
        return 204, b""

    def transport(self, engine, **kwargs):
        source = self

        class Transport:
            spent = 0

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def request(self, url, *, provider_budget):
                assert getattr(source.pg.local, "held", False), "paid request preceded scope lock"
                assert provider_budget is not None
                if source.overlap and not source.calls:
                    assert source.pg.contended.wait(3), "second runner never contended"
                source.calls[urlsplit(url).path] += 1
                cost = min(10, provider_budget.max_provider_bytes)
                self.spent += cost
                status, body = source.response(url)
                return HttpPayload(status, body, provider_bytes=cost)

            def provider_snapshot(self):
                return dict(provider_total_bytes=self.spent, provider_up_bytes=0,
                    provider_down_bytes=self.spent, provider_budget_bytes=engine.budget.policy.hard_run_bytes,
                    provider_budget_exceeded=False)

        return Transport()


@pytest.fixture
def environment(tmp_path, monkeypatch):
    pg = _FakePostgres()
    monkeypatch.setitem(sys.modules, "psycopg2", SimpleNamespace(connect=pg.connect))
    monkeypatch.setenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "postgresql+psycopg2://offline.invalid/metadata")
    monkeypatch.setenv("SOFASCORE_WRITER_LOCK", "1")
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    monkeypatch.setenv("SOFASCORE_SEASON_FRESHNESS_KEY", FRESHNESS)
    monkeypatch.delenv("SOFASCORE_REFRESH_EVIDENCE_DIR", raising=False)
    monkeypatch.setattr("utils.proxy_traffic.record_traffic_run", Mock())
    monkeypatch.setattr(writer_lock, "WRITER_LOCK_POLL_SECONDS", 0.001)
    monkeypatch.setattr(runner, "_source_context", lambda *args: (17, 76986))
    monkeypatch.setattr(runner, "_tournament_canonical_url", lambda *args: "https://www.sofascore.com/tournament/17")
    publication = Mock(side_effect=AssertionError("partial season must not publish"))
    monkeypatch.setattr("scrapers.sofascore.season_pipeline.materialize_season_partition", publication)
    monkeypatch.setattr("scrapers.sofascore.pipeline.finalize_materialized_results", publication)
    manager = _FakeTrino(pg)
    source = _Source(pg)
    original_live = live_capture.capture_live_dynamic_specs

    def capture(*args, **kwargs):
        return original_live(*args, transport_factory=source.transport, **kwargs)

    monkeypatch.setattr(live_capture, "capture_live_dynamic_specs", capture)
    raw = RawPayloadStore(fs.LocalFileSystem(), str(tmp_path / "raw"))
    initial = plan_season_partition(raw, BatchingManifestStore(TrinoManifestStore(manager, ensure_table=False)), **COMMON)
    chunk = initial.schedule_chunk

    def build(name, *, hard_task_bytes=5000, schedule_chunk=chunk,
              dag_id="dag_backfill_sofascore_all_mens"):
        policy = WorkloadBudgetPolicy("a" * 64, {CLASS: WorkloadClassBudget(
            CLASS, "season", 1, hard_task_bytes, tuple(SEASON_STATIC_ENDPOINTS + SEASON_DYNAMIC_ENDPOINTS),
            workload_shape_digest(SHAPE),
        )})
        signed = build_partitioned_plan(policy, dag_id=dag_id,
            run_id=f"{name}::season", control_token=TOKEN,
            freshness_keys={"season": FRESHNESS, "match": "final", "player": "final"},
            partitions=[PartitionWorkload("ENG-Premier League", "2526", 17,
                season_workload=SeasonWorkload(17, 76986, SHAPE, schedule_chunk=schedule_chunk))])
        budget = SharedBudgetLedger(tmp_path / "budget.json", BudgetPolicy(
            artifact_id="a" * 64, parent_artifact_id="a" * 64,
            hard_run_bytes=signed.allocations[0].budget_bytes,
            endpoint_reservation_bytes={key: 10 for key in SEASON_STATIC_ENDPOINTS + SEASON_DYNAMIC_ENDPOINTS},
            sample_count=20, distinct_proxy_exits=5, workload_class=CLASS,
        ))
        manifest = BatchingManifestStore(TrinoManifestStore(manager, ensure_table=False), max_pending=200)
        manifest.preload_scope(17, 76986)  # Deliberately stale before the capture lock.
        engine = SofaScoreCaptureEngine(raw_store=raw, manifest_store=manifest,
            transport=SimpleNamespace(request=lambda *args, **kw: pytest.fail("unbound transport")),
            sink=DeferredCaptureSink(), run_id=signed.run_id, task_id="initial",
            budget=budget, rate_limiter=SimpleNamespace(acquire=lambda: True),
            retry_policy=RetryPolicy(max_attempts=1), max_workers=1)
        runtime = CaptureRuntime(engine, manifest, raw)
        output = tmp_path / f"{name}.json"

        def run():
            code = runner._run_season_capture_engine(["ENG-Premier League"], 2025, str(output),
                force_replace=False, entity="all", capture_runtime=runtime, offline_replay=False,
                workload_plan=signed, workload_allocations=signed.allocations)
            return code, json.loads(output.read_text())

        return SimpleNamespace(run=run, runtime=runtime, signed=signed, output=output)

    return SimpleNamespace(pg=pg, manager=manager, source=source, raw=raw, chunk=chunk,
                           build=build, publication=publication, directory=tmp_path)


def test_two_dagruns_share_raw_under_real_scope_lock_and_never_pay_twice(environment, monkeypatch):
    env = environment
    first, second = env.build("first"), env.build("second")
    env.source.overlap = True
    with ThreadPoolExecutor(max_workers=2) as workers:
        futures = [workers.submit(item.run) for item in (first, second)]
        results = [future.result(timeout=15) for future in futures]
    assert env.pg.contended.is_set()
    assert len(env.pg.locks) == 1 and writer_lock.WRITER_LOCK_KEY not in env.pg.locks
    assert env.manager.reads_under_lock == 2
    assert all(code == 0 for code, report in results), results
    for _, report in results:
        assert report["partial"]["continuation"] == {"last": 25, "next": None}
        assert report["endpoint_completeness"] == 0
        assert report["manifest_committed"] is False
    assert len([path for path in env.source.calls if "/events/last/" in path]) == 25
    assert all(count == 1 for count in env.source.calls.values())
    counts = sorted(item.runtime.engine.metrics.snapshot()["request_count"] for item in (first, second))
    assert counts[0] == 0 < counts[1]
    env.publication.assert_not_called()

    # The next ordinary run completes all 55 pages through real aggregation,
    # real manifest finalization, and a fake Bronze storage boundary.
    next_plan = plan_season_partition(env.raw, first.runtime.manifest_store, **COMMON)
    completion = env.build("completion", schedule_chunk=next_plan.schedule_chunk)
    monkeypatch.setattr("scrapers.sofascore.season_pipeline.materialize_season_partition", materialize_season_partition)
    monkeypatch.setattr("scrapers.sofascore.pipeline.finalize_materialized_results", finalize_materialized_results)
    scraper = MagicMock()
    scraper.__enter__.return_value = scraper
    scraper._add_metadata.side_effect = lambda frame, entity: frame
    scraper.save_to_iceberg.side_effect = lambda **kwargs: "iceberg.bronze." + kwargs["table_name"]
    monkeypatch.setattr("scrapers.sofascore.SofaScoreScraper", lambda **kwargs: scraper)
    code, report = completion.run()
    assert code == 0, report
    assert "partial" not in report
    assert report["schedule_rows"] == 55
    assert report["endpoint_completeness"] == 1.0
    assert scraper.save_to_iceberg.call_count == 1
    assert len([path for path in env.source.calls if "/events/last/" in path]) == 55
    assert all(count == 1 for count in env.source.calls.values())
    committed = plan_season_partition(env.raw, completion.runtime.manifest_store, **COMMON)
    assert committed.complete and committed.match_phase_ready


def test_raw_before_manifest_retry_replays_saved_prefix_without_source_payment(environment):
    env = environment
    attempt = env.build("raw-before-manifest")
    for page in range(20):
        spec = build_schedule_page_spec(direction="last", page=page, **COMMON)
        status, body = env.source.response(spec.url)
        env.raw.store_bytes(spec.raw_target, body, request_url=spec.url, http_status=status)
    code, report = attempt.run()
    assert code == 0, report
    assert report["partial"]["raw_manifest_committed"]
    assert all(int(path.rsplit("/", 1)[1]) >= 20 for path in env.source.calls if "/events/last/" in path)
    assert len([path for path in env.source.calls if "/events/last/" in path]) == 5
    assert attempt.runtime.engine.metrics.snapshot()["replay_hits"] >= 20
    assert len(env.manager.rows) >= 26
    env.publication.assert_not_called()


def test_budget_exhaustion_keeps_saved_raw_and_same_cap_without_healthy_partial(environment):
    env = environment
    attempt = env.build("budget-exhausted", hard_task_bytes=20)
    original_cap = attempt.signed.run_cap_bytes
    code, report = attempt.run()
    assert code == 1, report
    assert "partial" not in report
    assert any("budget exhausted" in error for error in report["errors"])
    assert env.raw.has_payload(build_schedule_page_spec(direction="last", page=0, **COMMON).raw_target)
    calls = env.source.calls.copy()
    code, retried = attempt.run()
    assert code == 1 and "partial" not in retried
    assert env.source.calls == calls
    ledger = json.loads((env.directory / "budget.json").read_text())
    assert all(run["hard_run_bytes"] == original_cap for run in ledger["runs"].values())
    assert attempt.signed.run_cap_bytes == original_cap == 23
    env.publication.assert_not_called()


@pytest.mark.parametrize("dag_id", ["dag_ingest_sofascore", "dag_refresh_sofascore_all_mens"])
def test_non_history_lane_rejects_incomplete_schedule(environment, dag_id):
    env = environment
    attempt = env.build("incomplete", dag_id=dag_id)
    code, report = attempt.run()
    assert code == 1, report
    assert any("season schedule exceeds one chunk on a non-history lane" in error
               for error in report["errors"])
    assert "partial" not in report
    env.publication.assert_not_called()


def test_daily_current_season_publishes_both_schedule_directions(environment, monkeypatch):
    env = environment

    def response(url):
        if "/events/" not in url:
            return 204, b""
        direction, page = url.rsplit("/", 2)[-2:]
        page = int(page)
        event = copy.deepcopy(env.source.event)
        event["id"] = 14000000 + (page if direction == "last" else 100 + page)
        event.pop("referee", None)
        return 200, json.dumps({
            "events": [event],
            "hasNextPage": page < (7 if direction == "last" else 2),
        }).encode()

    env.source.response = response
    monkeypatch.setattr("scrapers.sofascore.season_pipeline.materialize_season_partition", materialize_season_partition)
    monkeypatch.setattr("scrapers.sofascore.pipeline.finalize_materialized_results", finalize_materialized_results)
    scraper = MagicMock()
    scraper.__enter__.return_value = scraper
    scraper._add_metadata.side_effect = lambda frame, entity: frame
    scraper.save_to_iceberg.side_effect = lambda **kwargs: "iceberg.bronze." + kwargs["table_name"]
    monkeypatch.setattr("scrapers.sofascore.SofaScoreScraper", lambda **kwargs: scraper)
    attempt = env.build("daily", dag_id="dag_ingest_sofascore")
    code, report = attempt.run()
    assert code == 0, report
    assert "partial" not in report
    assert report["schedule_rows"] == 11
    scraper.save_to_iceberg.assert_called_once()
    committed = plan_season_partition(env.raw, attempt.runtime.manifest_store, **COMMON)
    assert committed.schedule_chain_complete and committed.match_phase_ready
    assert len([path for path in env.source.calls if "/events/last/" in path]) == 8
    assert len([path for path in env.source.calls if "/events/next/" in path]) == 3
