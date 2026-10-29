"""Offline regression checks for the nightly repeated compaction pass."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from threading import Event

import pytest

from utils import bronze_compaction as compaction
from utils import maintenance_tasks as maintenance


@pytest.fixture
def runner(monkeypatch):
    now = datetime(2026, 10, 29, 2, tzinfo=timezone.utc)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now
    monkeypatch.setattr(compaction, "datetime", Clock)
    monkeypatch.setattr(compaction.time, "time", lambda: now.timestamp())
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


def test_checkpoints_keep_total_budget_and_release_per_portion(runner, monkeypatch, shared_db):
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

    def probe(_conn, *, table, partition_cursor=None, **kwargs):
        calls.append((table, partition_cursor))
        return maintenance._CompactionProbeResult((("s3://a", 10), ("s3://b", 10)), 0, "partition-32")

    monkeypatch.setattr(maintenance, "_compaction_candidates", probe)
    monkeypatch.setattr(maintenance, "_compact_exact_files", lambda *_a, **kw: {})
    import json
    import sys

    checkpoints = {}

    class Variable(shared_db[2]):
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
    assert calls == [("espn_match", None), ("fbref_schedule", None), ("espn_match", "partition-32")]
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


@pytest.fixture
def shared_db(monkeypatch):
    """Metadata DB stand-in; exercise the production state mutation/serialization."""
    import sys
    from types import ModuleType

    rows, statements = {}, []

    class Variable:
        key = None

        def __init__(self, *, key, val):
            self.key, self.val = key, val

        @staticmethod
        def get(key, **kwargs):
            import json
            return json.loads(rows[key].val) if key in rows else None

        @staticmethod
        def set(key, value, **kwargs):
            import json
            rows[key] = Variable(key=key, val=json.dumps(value))

    class Session:
        def execute(self, statement):
            statements.append(str(statement))

        def query(self, model):
            return self

        def filter(self, predicate):
            return self

        def one_or_none(self):
            return rows.get("bronze_compaction_shared_budget")

        def add(self, row):
            rows[row.key] = row

    @contextmanager
    def create_session():
        yield Session()

    module = ModuleType("airflow.utils.session")
    module.create_session = create_session
    monkeypatch.setitem(sys.modules, "airflow.utils.session", module)
    monkeypatch.setattr(sys.modules["airflow.models"], "Variable", Variable)
    return rows, statements, Variable


@pytest.mark.parametrize("limit", ["portions", "bytes"])
def test_daily_catchup_and_repeated_cli_share_reserved_budget(
    runner, monkeypatch, shared_db, limit
):
    calls = []
    import json
    from types import SimpleNamespace

    state = {
        "window": runner.date().isoformat(), "partition_offsets": {},
    }
    initial_portions = compaction.MAX_PORTIONS - 2 if limit == "portions" else 0
    initial_bytes = compaction.MAX_INPUT_BYTES - 40 if limit == "bytes" else 0
    state.update(portions=initial_portions, input_bytes=initial_bytes)
    shared_db[0]["bronze_compaction_shared_budget"] = shared_db[2](
        key="bronze_compaction_shared_budget", val=json.dumps(state)
    )
    monkeypatch.setattr(maintenance, "_compaction_candidates", lambda *a, **kw:
        maintenance._CompactionProbeResult((("s3://a", 10), ("s3://b", 10)), 0))

    def compact(*args, **kwargs):
        # Reservation must already be durable when the rewrite starts.
        state = json.loads(shared_db[0]["bronze_compaction_shared_budget"].val)
        calls.append((state["portions"], state["input_bytes"]))
        if len(calls) == 1:
            raise RuntimeError("failed rewrite still spends budget")
        return {}

    monkeypatch.setattr(maintenance, "_compact_exact_files", compact)
    for run in ("daily", "catchup_cli_1", "catchup_cli_2"):
        result = compaction.compaction_step(
            context={
                "dag": SimpleNamespace(dag_id="daily" if run == "daily" else "catchup"),
                "run_id": run,
                "task": SimpleNamespace(task_id="compact_bronze"),
            },
            tables=("espn_match",), deadline=runner,
        )
    assert calls == [
        (initial_portions + 1, initial_bytes + 20),
        (initial_portions + 2, initial_bytes + 40),
    ]
    assert result["stop_reason"] == "budget"
    assert any("pg_advisory_xact_lock" in sql for sql in shared_db[1])


def test_atomic_reservation_rejects_competing_run(runner, monkeypatch, shared_db):
    import json

    state = compaction._shared_budget(runner)
    state["portions"] = compaction.MAX_PORTIONS - 1
    shared_db[0]["bronze_compaction_shared_budget"].val = json.dumps(state)
    rewrites = []

    def probe(*args, **kwargs):
        # A different run spends the last slot after this run read the budget.
        assert compaction._shared_budget(runner, reservation=("espn_match", 20, "partition-32"))
        return maintenance._CompactionProbeResult((("s3://a", 10), ("s3://b", 10)), 0)

    monkeypatch.setattr(maintenance, "_compaction_candidates", probe)
    monkeypatch.setattr(maintenance, "_compact_exact_files", lambda *a, **kw: rewrites.append(kw))
    result = compaction.compaction_step(context={}, tables=("espn_match",), deadline=runner)
    assert result["stop_reason"] == "budget"
    assert not rewrites


def test_partition_cursor_survives_nights_with_replenished_first_partition(
    runner, monkeypatch, shared_db
):
    offsets = []
    clock = [runner]
    monkeypatch.setattr(compaction, "nightly_deadline", lambda: clock[0])

    def probe(*args, partition_cursor=None, **kwargs):
        offsets.append(partition_cursor)
        # Each partition still has small files every night, including the first.
        return maintenance._CompactionProbeResult(
            (("s3://a", 10), ("s3://b", 10)), 0,
            {None: "p32", "p32": "p64", "p64": "p96"}[partition_cursor]
        )

    monkeypatch.setattr(maintenance, "_compaction_candidates", probe)
    monkeypatch.setattr(maintenance, "_compact_exact_files", lambda *a, **kw: {})
    for _ in range(3):
        compaction.compaction_step(
            context={}, tables=("espn_match",), deadline=clock[0], max_portions=1
        )
        clock[0] += timedelta(days=1)
    assert offsets == [None, "p32", "p64"]
    state = compaction._shared_budget(clock[0])
    assert state["portions"] == state["input_bytes"] == 0
    assert state["partition_cursors"]["espn_match"] == "p96"


def test_expired_checkpoint_cannot_reset_current_window_budget(runner, shared_db):
    state = compaction._shared_budget(runner, reservation=("espn_match", 20, "partition-32"))
    with pytest.raises(TimeoutError, match="live nightly deadline"):
        compaction._shared_budget(runner - timedelta(days=1))
    assert compaction._shared_budget(runner) == state


def test_shared_legacy_offsets_migrate_without_refunding_budget(runner, shared_db):
    import json

    old = dict(window=runner.date().isoformat(), portions=7, input_bytes=1234,
               partition_offsets={"espn_match": 42})
    shared_db[0]["bronze_compaction_shared_budget"] = shared_db[2](
        key="bronze_compaction_shared_budget", val=json.dumps(old)
    )
    state = compaction._shared_budget(runner)
    assert state == dict(window=runner.date().isoformat(), portions=7,
                         input_bytes=1234, partition_cursors={})


def test_legacy_checkpoint_preserves_queue_and_charges(runner, monkeypatch):
    checkpoint = dict(portions=7, input_bytes=1234, selected_files=14,
                      failures=[], targets={}, delete_partitions={},
                      queue=["espn_match"], partition_offsets={"espn_match": 42})
    seen = []

    def probe(*args, partition_cursor=None, **kwargs):
        seen.append(partition_cursor)
        return maintenance._CompactionProbeResult((), 0)

    monkeypatch.setattr(maintenance, "_compaction_candidates", probe)
    result = compaction.run_compaction(
        tables=["espn_match"], deadline=runner, checkpoint=checkpoint
    )
    assert seen == [None]
    assert result["portions"] == 7 and result["input_bytes"] == 1234
    assert result["partition_cursors"] == {}
    assert "partition_offsets" not in result


def test_airflow_dry_run_does_not_write_checkpoint(runner, monkeypatch, shared_db):
    from types import SimpleNamespace

    visited = []

    def probe(*args, table, **kwargs):
        visited.append(table)
        return maintenance._CompactionProbeResult((("s3://a", 10), ("s3://b", 10)), 0, "p32")

    monkeypatch.setattr(maintenance, "_compaction_candidates", probe)
    context = dict(dag=SimpleNamespace(dag_id="catchup"), run_id="dry",
                   task=SimpleNamespace(task_id="compact"))
    result = compaction.compaction_step(
        context=context, tables=["espn_match", "sofascore_schedule"],
        deadline=runner, dry_run=True,
    )
    assert visited == ["espn_match", "sofascore_schedule"]
    assert result["portions"] == 2 and result["stop_reason"] == "exhausted"
    assert not shared_db[0]
