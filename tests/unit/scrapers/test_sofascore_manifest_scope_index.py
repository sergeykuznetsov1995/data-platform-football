"""#1357: the refresh plan reads the manifest once per scope, not per endpoint."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pyarrow import fs

from scrapers.sofascore import trino_accounting
from scrapers.sofascore.adapters import (
    MANIFEST_COLUMNS,
    TrinoManifestStore,
    manifest_to_row,
)
from scrapers.sofascore.capture_engine import RetryPolicy, SofaScoreCaptureEngine
from scrapers.sofascore.manifest import (
    BatchingManifestStore,
    EndpointManifest,
    InMemoryManifestStore,
    ManifestStatus,
    preload_manifest_scope,
)
from scrapers.sofascore.pipeline import (
    EVENT_PATHS,
    CaptureRuntime,
    build_event_spec,
)
from scrapers.sofascore.raw_store import RawPayloadStore
from tests.unit.scrapers.test_sofascore_pipeline import (
    NoNetworkTransport,
    SuccessSink,
    UnlimitedLimiter,
)

TOURNAMENT, SEASON = "17", "76986"
MATCH_IDS = [str(14_000_000 + index) for index in range(30)]


class FakeTrinoManager:
    """Filters stored rows by the SQL's key columns and counts every call.

    ``insert_dataframe_atomic`` sends the same statement sequence as the base
    manager's staged batch MERGE through ``_execute``, so the accounting sees
    the hidden per-flush statements too.
    """

    catalog = "iceberg"
    BATCH_STATEMENTS = (
        "CREATE TABLE iceberg.ops.stage_x (LIKE iceberg.ops.sofascore_capture_manifest)",
        "INSERT INTO iceberg.ops.stage_x VALUES (?)",
        "SELECT count(*) FROM iceberg.ops.stage_x",
        "MERGE INTO iceberg.ops.sofascore_capture_manifest t USING iceberg.ops.stage_x s",
        "DROP TABLE IF EXISTS iceberg.ops.stage_x",
    )

    def __init__(self):
        self.rows: list[dict] = []
        self.selects = 0
        self.merges = 0

    def create_schema(self, schema):
        self._execute(f"CREATE SCHEMA IF NOT EXISTS iceberg.{schema}")

    def _execute(self, sql, fetch=False, params=None):
        if "sofascore_capture_manifest WHERE" not in sql:
            return [(1,)] if fetch else None
        self.selects += 1
        where = sql.split(" WHERE ", 1)[1]
        columns = [part.split('"')[1] for part in where.split(" AND ")]
        matched = [
            row
            for row in self.rows
            if all(str(row[column]) == str(value) for column, value in zip(columns, params))
        ]
        return [tuple(row[column] for column in MANIFEST_COLUMNS) for row in matched]

    def insert_dataframe_atomic(self, schema, table, df, *, merge_keys):
        self.merges += 1
        for statement in self.BATCH_STATEMENTS:
            self._execute(statement, fetch=statement.startswith("SELECT"))
        for record in df.to_dict("records"):
            self.rows = [
                row
                for row in self.rows
                if any(row[key] != record[key] for key in merge_keys)
            ]
            self.rows.append(record)
        return len(df)


def _spec(match_id, endpoint, *, tournament=TOURNAMENT, season=SEASON):
    return build_event_spec(
        source_tournament_id=tournament,
        source_season_id=season,
        target_id=match_id,
        endpoint=endpoint,
        freshness_key="final",
        paid_proxy=False,
    )


def _success(spec):
    return EndpointManifest(
        key=spec.key,
        status=ManifestStatus.SUCCESS,
        run_id="run",
        task_id="capture",
        attempts=1,
        row_count=1,
        http_status=200,
        raw_content_hash="a" * 64,
        raw_blob_key=f"blobs/{spec.key.target_id}/{spec.key.endpoint}.json.gz",
    )


def _seeded_manager(match_ids=MATCH_IDS):
    manager = FakeTrinoManager()
    manager.rows = [
        manifest_to_row(_success(_spec(match_id, endpoint)))
        for match_id in match_ids
        for endpoint in EVENT_PATHS
    ]
    return manager


@pytest.fixture(autouse=True)
def _reset_counter():
    trino_accounting.reset()
    yield
    trino_accounting.reset()


@pytest.mark.unit
def test_preload_is_one_select_and_scope_reads_never_query_trino():
    manager = _seeded_manager()
    store = TrinoManifestStore(manager)

    assert store.preload_scope(TOURNAMENT, SEASON) == len(MATCH_IDS) * len(EVENT_PATHS)
    assert manager.selects == 1
    for match_id in MATCH_IDS:
        for endpoint in EVENT_PATHS:
            assert store.get(_spec(match_id, endpoint).key).status == ManifestStatus.SUCCESS
    # A miss inside the scope is authoritative: never committed -> None.
    assert store.get(_spec("99999999", "lineups").key) is None
    assert manager.selects == 1
    assert trino_accounting.snapshot()["select"] == 1


@pytest.mark.unit
def test_key_outside_the_preloaded_scope_keeps_the_point_select():
    manager = _seeded_manager()
    store = TrinoManifestStore(manager)
    store.preload_scope(TOURNAMENT, SEASON)

    assert store.get(_spec(MATCH_IDS[0], "lineups", season="1").key) is None
    assert manager.selects == 2


@pytest.mark.unit
def test_commits_update_the_index_through_the_batching_wrapper():
    manager = _seeded_manager(match_ids=[])
    inner = TrinoManifestStore(manager)
    store = BatchingManifestStore(inner, max_pending=200)
    assert store.preload_scope(TOURNAMENT, SEASON) == 0

    specs = [_spec(match_id, "lineups") for match_id in MATCH_IDS]
    for spec in specs:
        store.upsert(_success(spec))
    store.flush()

    assert manager.merges == 1
    assert all(inner.get(spec.key) is not None for spec in specs)
    assert manager.selects == 1
    # init: CREATE SCHEMA + DDL; preload: 1 SELECT; one flush: the five hidden
    # statements of the staged batch MERGE (its count SELECT belongs to the
    # write -> other; MERGE -> merge).
    assert trino_accounting.snapshot() == {"select": 1, "merge": 1, "other": 6}


@pytest.mark.unit
def test_duplicate_natural_key_in_the_scope_still_fails_loudly():
    manager = _seeded_manager(match_ids=MATCH_IDS[:1])
    manager.rows.append(dict(manager.rows[0]))
    store = TrinoManifestStore(manager)

    with pytest.raises(RuntimeError, match="natural key is duplicated"):
        store.preload_scope(TOURNAMENT, SEASON)


@pytest.mark.unit
def test_in_memory_stores_need_no_preload():
    assert preload_manifest_scope(InMemoryManifestStore(), TOURNAMENT, SEASON) == 0
    batching = BatchingManifestStore(InMemoryManifestStore())
    assert batching.preload_scope(TOURNAMENT, SEASON) == 0


def _runtime(tmp_path, manager, *, sink=None, max_pending=200):
    store = BatchingManifestStore(TrinoManifestStore(manager), max_pending=max_pending)
    raw = RawPayloadStore(fs.LocalFileSystem(), str(tmp_path / "raw"))
    engine = SofaScoreCaptureEngine(
        raw_store=raw,
        manifest_store=store,
        transport=NoNetworkTransport(),
        run_id="fixture-run",
        task_id="match-capture",
        sink=sink or SuccessSink(),
        rate_limiter=UnlimitedLimiter(),
        retry_policy=RetryPolicy(max_attempts=1),
        max_workers=2,
    )
    return CaptureRuntime(engine, store, raw)


@pytest.mark.unit
def test_refresh_plan_of_30_matches_reads_the_manifest_once(tmp_path, monkeypatch):
    from dags.scripts.prepare_sofascore_workload import (
        CompetitionSeason,
        prepare_workload_plan,
    )
    from tests.unit.scripts.test_prepare_sofascore_workload import TOKEN, _policy

    monkeypatch.setenv("SOFASCORE_PROXY_BUDGET_ARTIFACT_ID", "b" * 64)
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    # Half the season is already terminal, half still needs the network.
    manager = _seeded_manager(match_ids=MATCH_IDS[:15])
    runtime = _runtime(tmp_path, manager)
    catalog = MagicMock()
    catalog.competition.return_value = SimpleNamespace(
        capture_allowed=True, unique_tournament_id=int(TOURNAMENT)
    )
    catalog.resolve_source_season.return_value = SimpleNamespace(
        season_id=int(SEASON), format="split_year"
    )
    module = "dags.scripts.prepare_sofascore_workload"
    with (
        patch(f"{module}.load_static_workload_policy", return_value=_policy()),
        patch(f"{module}.build_capture_runtime", return_value=runtime),
        patch(f"{module}.SofaScoreCatalog.load", return_value=catalog),
        patch(f"{module}._finished_match_ids", return_value=set(MATCH_IDS)),
    ):
        prepare_workload_plan(
            dag_id="dag_refresh_sofascore_all_mens",
            base_run_id="refresh-1",
            phase="targets",
            competition_seasons=[CompetitionSeason("ENG-Premier League", "2526")],
            artifact_path=tmp_path / "artifact.json",
            output_path=tmp_path / "target-plan.json",
            allow_inactive_season=True,
            season_freshness_key="final",
            season_evidence="bronze",
        )

    # Without the scope index this plan cost 90 point SELECTs (any() stops at
    # the first pending endpoint of a match); the no-op capture below cost 150.
    assert manager.selects <= 10
    assert trino_accounting.snapshot()["select"] <= 10


@pytest.mark.unit
def test_match_capture_of_30_matches_reads_the_manifest_once(tmp_path, monkeypatch):
    from dags.scripts import run_sofascore_scraper as runner

    manager = _seeded_manager()
    runtime = _runtime(tmp_path, manager)
    monkeypatch.setattr(
        runner, "_resolve_match_ids_from_bronze", lambda *args, **kwargs: MATCH_IDS
    )
    monkeypatch.setattr(
        runner, "_source_context", lambda *args: (int(TOURNAMENT), int(SEASON))
    )
    output = tmp_path / "match-capture.json"
    browser = MagicMock(side_effect=AssertionError("no-op opened a scraper"))

    with patch("scrapers.sofascore.SofaScoreScraper", browser):
        rc = runner._run_match_capture(
            leagues=["ENG-Premier League"],
            season=2025,
            limit=None,
            output_path=str(output),
            capture_runtime=runtime,
            workload_plan=None,
            offline_replay=False,
        )

    assert rc == 0
    browser.assert_not_called()
    assert manager.selects <= 10
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["traffic"]["trino_queries"]["select"] <= 10


@pytest.mark.unit
def test_counted_connection_classifies_every_cursor_execute():
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value = cursor
    counted = trino_accounting.counted_connection(connection)

    counted.cursor().execute("SELECT 1")
    counted.cursor().execute("  merge INTO t USING s ON 1=1")
    counted.cursor().execute("CREATE TABLE t (x int)")
    counted.close()

    assert trino_accounting.snapshot() == {"select": 1, "merge": 1, "other": 1}
    assert cursor.execute.call_count == 3
    connection.close.assert_called_once()
    assert trino_accounting.counted_connection(None) is None


@pytest.mark.unit
def test_phase_report_carries_trino_queries(tmp_path):
    from dags.scripts.run_sofascore_scope_cycle import _phase_report

    report_path = tmp_path / "matches.json"
    counts = {"select": 2, "merge": 1, "other": 2}
    report_path.write_text(
        json.dumps({"errors": [], "traffic": {"trino_queries": counts}}),
        encoding="utf-8",
    )

    assert _phase_report(report_path)["trino_queries"] == counts


@pytest.mark.unit
def test_real_manager_counts_every_statement_including_the_connect_probe(
    monkeypatch,
):
    from scrapers.base.trino_manager import TrinoTableManager

    monkeypatch.setattr(TrinoTableManager, "_trino_unreachable", False)
    sent = []

    class Cursor:
        def execute(self, sql, *args):
            sent.append(sql)

        def fetchall(self):
            return []

        def close(self):
            pass

    connection = MagicMock()
    connection.cursor.side_effect = Cursor
    manager = TrinoTableManager()
    manager._create_connection = lambda: connection
    trino_accounting.instrument_manager(manager)
    trino_accounting.instrument_manager(manager)  # idempotent

    manager._execute("SELECT 2", fetch=True)
    manager._execute("MERGE INTO t USING s ON 1=1")
    manager._execute("DROP TABLE IF EXISTS s")

    # The connect probe (SELECT 1) is a real round-trip and is counted too.
    assert sent == ["SELECT 1", "SELECT 2", "MERGE INTO t USING s ON 1=1", "DROP TABLE IF EXISTS s"]
    assert trino_accounting.snapshot() == {"select": 2, "merge": 1, "other": 1}


@pytest.mark.unit
def test_whole_refresh_phase_plan_plus_capture_stays_within_ten_queries(
    tmp_path, monkeypatch
):
    """run_phase's window: the plan and the capture, two stores, one counter."""

    from dags.scripts import run_sofascore_scraper as runner
    from dags.scripts.prepare_sofascore_workload import (
        CompetitionSeason,
        prepare_workload_plan,
    )
    from tests.unit.scripts.test_prepare_sofascore_workload import TOKEN, _policy

    monkeypatch.setenv("SOFASCORE_PROXY_BUDGET_ARTIFACT_ID", "b" * 64)
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    manager = _seeded_manager()
    catalog = MagicMock()
    catalog.competition.return_value = SimpleNamespace(
        capture_allowed=True, unique_tournament_id=int(TOURNAMENT)
    )
    catalog.resolve_source_season.return_value = SimpleNamespace(
        season_id=int(SEASON), format="split_year"
    )
    module = "dags.scripts.prepare_sofascore_workload"
    trino_accounting.reset()
    with (
        patch(f"{module}.load_static_workload_policy", return_value=_policy()),
        patch(
            f"{module}.build_capture_runtime",
            return_value=_runtime(tmp_path / "plan", manager),
        ),
        patch(f"{module}.SofaScoreCatalog.load", return_value=catalog),
        patch(f"{module}._finished_match_ids", return_value=set(MATCH_IDS)),
    ):
        prepare_workload_plan(
            dag_id="dag_refresh_sofascore_all_mens",
            base_run_id="refresh-1",
            phase="targets",
            competition_seasons=[CompetitionSeason("ENG-Premier League", "2526")],
            artifact_path=tmp_path / "artifact.json",
            output_path=tmp_path / "target-plan.json",
            allow_inactive_season=True,
            season_freshness_key="final",
            season_evidence="bronze",
        )
    monkeypatch.setattr(
        runner, "_resolve_match_ids_from_bronze", lambda *args, **kwargs: MATCH_IDS
    )
    monkeypatch.setattr(
        runner, "_source_context", lambda *args: (int(TOURNAMENT), int(SEASON))
    )
    with patch(
        "scrapers.sofascore.SofaScoreScraper",
        MagicMock(side_effect=AssertionError("no-op opened a scraper")),
    ):
        rc = runner._run_match_capture(
            leagues=["ENG-Premier League"],
            season=2025,
            limit=None,
            output_path=str(tmp_path / "matches.json"),
            capture_runtime=_runtime(tmp_path / "capture", manager),
            workload_plan=None,
            offline_replay=False,
        )

    assert rc == 0
    counts = trino_accounting.snapshot()
    # Two stores x (CREATE SCHEMA + DDL + one preload SELECT); every further
    # manifest flush adds the five statements of one staged batch MERGE.
    assert counts == {"select": 2, "merge": 0, "other": 4}
    assert sum(counts.values()) <= 10


@pytest.mark.unit
def test_capture_writing_many_batches_keeps_reads_within_ten(tmp_path, monkeypatch):
    """A real (offline-replay) capture of 30 pending matches: 150 endpoint
    records through a 20-record batch = several flushes, Bronze MERGE and
    finalize.  Reads stay bounded; writes scale only with the batch count."""

    from scrapers.sofascore.pipeline import DeferredCaptureSink, ingest_prefetched_records
    from tests.unit.scrapers.test_sofascore_pipeline import (
        _event_records_for,
        _event_spec,
        _run_match_pass,
    )

    match_ids = [str(20_000_000 + n) for n in range(30)]
    manager = FakeTrinoManager()
    runtime = _runtime(
        tmp_path, manager, sink=DeferredCaptureSink(), max_pending=20
    )
    specs = {
        (match_id, endpoint): _event_spec(match_id, endpoint)
        for match_id in match_ids
        for endpoint in EVENT_PATHS
    }
    records = {}
    for match_id in match_ids:
        records.update(_event_records_for(match_id))
    ingest_prefetched_records(runtime, specs=specs, records=records)
    runtime.manifest_store.flush()
    # Seeding ran without a preload: that is the old per-endpoint read cost.
    assert manager.selects >= 150
    trino_accounting.reset()
    merges_before, selects_before = manager.merges, manager.selects

    rc, result, saved, _events = _run_match_pass(
        runtime, match_ids, tmp_path / "matches.json", monkeypatch
    )

    assert rc == 0, result["errors"]
    assert "sofascore_match_capture_status" in saved
    counts = result["traffic"]["trino_queries"]
    batches = manager.merges - merges_before
    assert batches >= 5  # 150 terminal records through a 20-record buffer
    assert counts["select"] <= 10
    assert counts["merge"] == batches
    assert manager.selects - selects_before <= 10
