"""Offline regression checks for the nightly repeated compaction pass."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from threading import Event

import pytest

from utils import bronze_compaction as compaction
from utils import maintenance_tasks as maintenance


@pytest.fixture
def runner(monkeypatch):
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(
        compaction, "nightly_deadline", lambda: now + timedelta(hours=3)
    )

    class Connection:
        def close(self):
            pass

    monkeypatch.setattr(maintenance, "_get_trino_connection", Connection)
    monkeypatch.setattr(
        maintenance,
        "_list_tables",
        lambda *_: list(compaction.BRONZE_COMPACTION_TABLES),
    )

    @contextmanager
    def fence(enabled, deadline=None):
        yield lambda: None

    monkeypatch.setattr(compaction, "_publication_fence", fence)
    counts = iter([100, 99] * 200)
    monkeypatch.setattr(
        maintenance,
        "_fetch_scalar",
        lambda _conn, sql: 0 if "file_path IN" in sql else next(counts),
    )
    return now + timedelta(hours=1)


def test_discovery_excludes_views_before_probe_and_retention(monkeypatch):
    statements = []

    class Cursor:
        def execute(self, sql):
            statements.append(sql)

        def fetchall(self):
            return (
                [("espn_match",)]
                if "table_type = 'BASE TABLE'" in statements[-1]
                else [("espn_match",), ("espn_match_current",)]
            )

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    monkeypatch.setattr(maintenance, "_connect", Connection)
    probes, retained = [], []

    def probe(_conn, *, table, **_kwargs):
        probes.append(table)
        return maintenance._CompactionProbeResult((), 0)

    monkeypatch.setattr(maintenance, "_compaction_candidates", probe)
    monkeypatch.setattr(
        maintenance, "_maintain_one", lambda _c, fq, _r: retained.append(fq) or {}
    )
    result = maintenance.maintain_iceberg_tables(
        schemas=("bronze",),
        table_filter=("espn_match", "espn_match_current"),
        compact_live_files=True,
    )
    assert probes == ["espn_match"]
    assert retained == ['iceberg."bronze"."espn_match"']
    assert not result["failures"]

    def broken(*_args, **_kwargs):
        raise RuntimeError("TABLE_NOT_FOUND real table disappeared")

    monkeypatch.setattr(maintenance, "_compaction_candidates", broken)
    result = maintenance.maintain_iceberg_tables(
        schemas=("bronze",),
        table_filter=("espn_match",),
        compact_live_files=True,
    )
    assert "TABLE_NOT_FOUND" in result["failures"][0][1]


def test_repeated_portions_fair_queue_and_fresh_metadata(runner, monkeypatch):
    tables = ("fbref_schedule", "espn_match", "sofascore_schedule")
    calls, probes = [], []
    backlog = dict.fromkeys(tables, 4)

    def probe(_conn, *, table, **_kwargs):
        probes.append(table)
        paths = tuple((f"s3://bucket/{table}/{i}", 10) for i in range(backlog[table]))
        return maintenance._CompactionProbeResult(paths if len(paths) > 1 else (), 0)

    def compact(_conn, *, table, **_kwargs):
        calls.append(table)
        backlog[table] -= 1
        return {}

    monkeypatch.setattr(maintenance, "_compaction_candidates", probe)
    monkeypatch.setattr(maintenance, "_compact_exact_files", compact)
    result = compaction.run_compaction(tables=tables, deadline=runner, max_portions=8)
    assert calls[:3] == sorted(tables)
    assert calls[3:6] == sorted(tables)
    assert len(calls) == result["portions"] == 8
    assert result["stop_reason"] == "budget"
    assert set(result["targets"].values()) == {"pending"}
    assert len(probes) == 8
    assert not result["failures"]


def test_bytes_dry_run_and_no_progress(runner, monkeypatch):
    def probe(_conn, *, max_input_bytes, **_kwargs):
        paths = (("s3://a", 10), ("s3://b", 10)) if max_input_bytes >= 20 else ()
        return maintenance._CompactionProbeResult(paths, 0)

    monkeypatch.setattr(maintenance, "_compaction_candidates", probe)

    def unexpected(*_a, **_kw):
        pytest.fail("dry-run must never alter a table")

    monkeypatch.setattr(maintenance, "_compact_exact_files", unexpected)
    result = compaction.run_compaction(
        tables=("espn_match", "sofascore_schedule"),
        deadline=runner,
        max_input_bytes=25,
        dry_run=True,
    )
    assert result["portions"] == 1
    assert result["input_bytes"] == 20
    monkeypatch.setattr(maintenance, "_compact_exact_files", lambda *_a, **_kw: {})
    monkeypatch.setattr(maintenance, "_fetch_scalar", lambda *_: 1)
    result = compaction.run_compaction(tables=("espn_match",), deadline=runner)
    assert result["portions"] == 1
    assert "no proven progress" in result["failures"][0][1]


def test_deadline_stop_and_window():
    with pytest.raises(TimeoutError, match="02:00"):
        compaction.nightly_deadline(datetime(2026, 10, 28, 5, tzinfo=timezone.utc))
    stop = Event()
    conn = compaction._DeadlineConnection(
        None, datetime.now(timezone.utc) - timedelta(seconds=1), stop, lambda: None
    )
    with pytest.raises(TimeoutError):
        conn.check()
    stop.set()
    with pytest.raises(InterruptedError):
        conn.check()


def test_physical_writer_contracts_and_omitted_tables():
    from scrapers.fbref.typed_bronze import MATCH_DATASET_TABLES, SEASON_DATASET_TABLES
    from scrapers.espn.bronze_schema import TABLES

    assert set(MATCH_DATASET_TABLES.values()) | set(
        SEASON_DATASET_TABLES.values()
    ) <= set(compaction.FBREF_TABLES)
    assert set(TABLES) == set(compaction.ESPN_TABLES)
    assert "fbref_schedule" in compaction.BRONZE_COMPACTION_TABLES
    assert "sofascore_schedule" in compaction.BRONZE_COMPACTION_TABLES
    assert "clubelo_ratings_historical" not in compaction.BRONZE_COMPACTION_TABLES
    assert maintenance.COMPACTION_MAX_FILES_PER_PORTION == 1024


def test_busy_lock_reschedules_and_other_conflicts_fail(monkeypatch):
    monkeypatch.setattr(compaction, "nightly_deadline", lambda: None)
    monkeypatch.setattr(
        compaction, "compaction_step", lambda **kw: {"stop_reason": "busy"}
    )
    assert compaction.compact_nightly(reschedule_on_busy=True) is False
    assert compaction.compact_nightly() == {"stop_reason": "busy"}


def test_running_query_is_cancelled_at_deadline():
    from threading import Event

    cancelled = Event()

    class Cursor:
        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            if self.sql.startswith("SET SESSION"):
                return []
            assert cancelled.wait(timeout=2), "deadline did not cancel the query"
            return []

        def cancel(self):
            cancelled.set()

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

    conn = compaction._DeadlineConnection(
        Connection(),
        datetime.now(timezone.utc) + timedelta(seconds=0.3),
        Event(),
        lambda: None,
    )
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT blocked")
        with pytest.raises(TimeoutError):
            cursor.fetchall()
    finally:
        cursor.close()
    assert cancelled.is_set()


def test_publication_ownership_is_renewed_and_loss_fails(monkeypatch):
    from scrapers.fbref.control import ControlStore, StateConflict

    events = []

    class Control:
        def create_run(self, *_a, **_kw):
            return "maintenance"

        def start_run(self, _run):
            pass

        def acquire_publication_lock(self, _run, **_kw):
            events.append("acquire")

        def renew_publication_lock(self, _run, **_kw):
            events.append("renew")

        def assert_publication_lock_owner(self, _run):
            raise StateConflict("ownership lost")

        def release_publication_lock(self, _run):
            events.append("release")

        def finish_run(self, _run, *, succeeded):
            events.append(succeeded)

    monkeypatch.setattr(ControlStore, "from_env", lambda **kw: Control())
    with pytest.raises(StateConflict, match="ownership lost"):
        with compaction._publication_fence(True) as fence:
            fence()
    assert events == ["acquire", "renew", "release", False]


def test_portion_sql_and_python_share_1024_file_limit():
    statements = []

    class Cursor:
        def execute(self, sql):
            statements.append(sql)

        def fetchall(self):
            return [(f"s3://bucket/{i}", 1, 0) for i in range(1024)]

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

    result = maintenance._compaction_candidates(
        Connection(),
        schema="bronze",
        table="fbref_schedule",
        max_input_bytes=512 * 1024**2,
        max_files=1024,
        partition_offset=3,
    )
    assert len(result.candidates) == 1024
    assert "LIMIT 1024" in statements[-1]
    assert "file_rank <= 1024" in statements[-1]
    assert "OFFSET 3" in statements[-1]
    with pytest.raises(ValueError, match="portion bound"):
        maintenance._compact_exact_files(
            Connection(), schema="bronze", table="x", candidates=(), max_files=1025
        )


def test_catchup_cli_queues_instead_of_bypassing_pool(monkeypatch, tmp_path):
    from scripts import compact_bronze as cli

    class Clock:
        @staticmethod
        def now(_tz):
            return datetime(2026, 10, 28, 2, tzinfo=timezone.utc)

    monkeypatch.setattr(cli, "datetime", Clock)
    calls = []
    monkeypatch.setattr(
        cli.subprocess, "run", lambda args, **kw: calls.append((args, kw))
    )
    stop = tmp_path / "stop"
    assert (
        cli.main(
            [
                "--table",
                "fbref_schedule",
                "--journal",
                str(tmp_path / "journal"),
                "--stop-file",
                str(stop),
            ]
        )
        == 0
    )
    import json

    command, kwargs = calls[0]
    assert command[:4] == [
        "airflow",
        "dags",
        "trigger",
        "dag_iceberg_compaction_catchup",
    ]
    conf = json.loads(command[-1])
    assert conf["apply"] is False
    assert conf["deadline"] == "2026-10-28T02:30:00+00:00"
    assert kwargs == {"check": True}
    assert cli.main(["--stop-file", str(stop), "--stop"]) == 0
    assert stop.exists()


def test_catchup_dag_uses_pool_and_preserves_original_deadline(monkeypatch, tmp_path):
    import importlib
    import sys
    from types import SimpleNamespace

    monkeypatch.setitem(
        sys.modules,
        "airflow.sensors.base",
        SimpleNamespace(PokeReturnValue=lambda **kw: kw),
    )
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    mod = importlib.import_module("dag_iceberg_compaction_catchup")
    task = next(
        task for task in PythonOperator._instances if task.task_id == "compact_bronze"
    )
    assert task._init_kwargs["pool"] == "fbref_scraper_pool"
    assert task._init_kwargs["mode"] == "reschedule"
    assert task._init_kwargs["retries"] == 0
    now = datetime.now(timezone.utc)
    deadline = now + timedelta(minutes=5)
    monkeypatch.setattr(
        compaction, "nightly_deadline", lambda: now + timedelta(hours=3)
    )
    calls = []
    monkeypatch.setattr(
        compaction,
        "compaction_step",
        lambda **kw: calls.append(kw) or {"failures": [], "stop_reason": "exhausted"},
    )
    conf = dict(
        deadline=deadline.isoformat(),
        tables=["fbref_schedule"],
        portions=3,
        bytes=1000,
        apply=False,
        stop_file=str(tmp_path / "stop"),
        journal=str(tmp_path / "journal"),
    )
    mod._catchup(dag_run=SimpleNamespace(conf=conf))
    assert calls[0]["deadline"] == deadline
    assert calls[0]["dry_run"] is True


def test_checkpoints_keep_total_budget_and_release_per_portion(runner, monkeypatch):
    from types import SimpleNamespace

    calls, locks = [], []

    @contextmanager
    def fence(enabled, deadline=None):
        locks.append(("acquire", enabled))
        try:
            yield lambda: None
        finally:
            locks.append(("release", enabled))

    monkeypatch.setattr(compaction, "_publication_fence", fence)

    def probe(_conn, *, table, partition_offset=0, **kwargs):
        calls.append((table, partition_offset))
        return maintenance._CompactionProbeResult((("s3://a", 10), ("s3://b", 10)), 0)

    monkeypatch.setattr(maintenance, "_compaction_candidates", probe)
    monkeypatch.setattr(maintenance, "_compact_exact_files", lambda *_a, **kw: {})
    import json
    import sys

    checkpoints = {}

    class Variable:
        @staticmethod
        def get(key, **kwargs):
            return json.loads(checkpoints[key]) if key in checkpoints else None

        @staticmethod
        def set(key, value, **kwargs):
            checkpoints[key] = json.dumps(value)

    monkeypatch.setattr(
        sys.modules["airflow.models"], "Variable", Variable, raising=False
    )
    context = {
        "run_id": "scheduled__2026-10-28",
        "dag": SimpleNamespace(dag_id="daily"),
        "task": SimpleNamespace(task_id="compact_bronze"),
    }
    tables = ("fbref_schedule", "espn_match")
    for _ in range(4):
        result = compaction.compaction_step(
            context=context, tables=tables, deadline=runner, max_portions=3
        )
    assert result["portions"] == 3
    assert result["input_bytes"] == 60
    assert result["stop_reason"] == "budget"
    assert calls == [("espn_match", 0), ("fbref_schedule", 0), ("espn_match", 1)]
    assert locks == [
        ("acquire", False),
        ("release", False),
        ("acquire", True),
        ("release", True),
        ("acquire", False),
        ("release", False),
    ]
    assert result["deadline"] == runner.isoformat()


def test_lost_fence_after_set_never_submits_working_sql():
    from scrapers.fbref.control import StateConflict

    queries = []

    class Cursor:
        def execute(self, sql):
            queries.append(sql)

        def fetchall(self):
            return []

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

    def fence():
        if queries:
            raise StateConflict("ownership lost during SET")

    conn = compaction._DeadlineConnection(
        Connection(), datetime.now(timezone.utc) + timedelta(minutes=1), Event(), fence
    )
    cursor = conn.cursor()
    try:
        with pytest.raises(StateConflict, match="ownership lost"):
            cursor.execute("ALTER TABLE must_never_run")
    finally:
        cursor.close()
    assert len(queries) == 1
    assert queries[0].startswith("SET SESSION")


def test_expired_deadline_still_releases_publication_ownership(monkeypatch):
    import sys
    from types import SimpleNamespace
    from scrapers.fbref.control import ControlStore

    events, factory = [], []
    now = datetime.now(timezone.utc)

    class Clock:
        @classmethod
        def now(cls, _tz):
            return cls.value

    Clock.value = now
    monkeypatch.setattr(compaction, "datetime", Clock)
    monkeypatch.setitem(
        sys.modules,
        "psycopg2",
        SimpleNamespace(connect=lambda dsn, **kw: events.append(("connect", kw))),
    )

    class Control:
        def create_run(self, *_a, **_kw):
            return "maintenance"

        def start_run(self, _run):
            pass

        def acquire_publication_lock(self, _run, *, ttl_seconds, **kw):
            events.append(("acquire", ttl_seconds))

        def renew_publication_lock(self, _run, **kw):
            pass

        def release_publication_lock(self, _run):
            factory[0]("dsn")
            events.append("release")

        def finish_run(self, _run, *, succeeded):
            events.append(("finish", succeeded))

    def control(**kwargs):
        factory.append(kwargs["connection_factory"])
        return Control()

    monkeypatch.setattr(ControlStore, "from_env", control)
    with pytest.raises(TimeoutError, match="control deadline"):
        with compaction._publication_fence(True, now + timedelta(seconds=10)) as fence:
            Clock.value = now + timedelta(seconds=11)
            fence()
    assert events[0] == ("acquire", 70)
    assert events[1][0] == "connect"
    assert events[1][1]["connect_timeout"] == 1
    assert events[-2:] == ["release", ("finish", False)]
