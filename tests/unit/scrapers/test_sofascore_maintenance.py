"""Offline compaction safety and single-data-file/delete regression (#1879)."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from scrapers.sofascore import maintenance as m

NOW = datetime(2026, 10, 28, 3, tzinfo=timezone.utc)
PARTITION = ("SS-1", "2026")
ROW = (*PARTITION, 2, 150, 1, 1)


class Queries:
    def __init__(self):
        self.queries = []
        self.rows = [ROW]
        self.locked = False
        self.optimized = []
        self.closed = False

    def execute(self, sql):
        self.queries.append({"query_id": f"q{len(self.queries)}", "sql": sql})
        if sql == m.FILES_SQL:
            return self.rows
        if sql == m.METRICS_SQL:
            return [(0, 1, 100), (1, 1, 50)]
        if sql == m.SNAPSHOT_SQL:
            return [(123,)]
        assert self.locked
        self.optimized.append(sql)
        self.rows = []
        return []

    def close(self):
        self.closed = True

    @contextmanager
    def lock(self):
        self.locked = True
        try:
            yield True
        finally:
            self.locked = False


def run(queries, **kwargs):
    return m.run(query_factory=lambda: queries, writer_lock=queries.lock,
                 refresh_probe=lambda: False, now=lambda: NOW, **kwargs)


def test_single_data_file_with_delete_is_optimized_under_lock():
    queries = Queries()
    result = run(queries)
    assert result["stopped"] == "complete"
    assert len(queries.optimized) == 1
    assert "WHERE (league = 'SS-1' AND season = '2026')" in queries.optimized[0]
    assert "128MB" in queries.optimized[0]
    assert "$path" not in queries.optimized[0]
    assert result["before"]["files"] == [(0, 1, 100), (1, 1, 50)]
    assert result["after"]["snapshot"] == [(123,)]
    assert result["processed"][0]["partitions"] == [PARTITION]
    assert queries.closed


@pytest.mark.parametrize("count,size", [(1001, 100), (2, m.MAX_BYTES + 1)])
def test_oversized_whole_partition_is_reported(count, size):
    chunks, skipped = m.plan_chunks([(*PARTITION, count, size, count - 1, 1)])
    assert chunks == []
    assert skipped[0]["reason"] == "partition_over_limit"


def test_delete_files_count_towards_both_chunk_limits():
    rows = [(f"SS-{n}", "2026", 600, m.MAX_BYTES // 2 + 1, 599, 1)
            for n in range(3)]
    chunks, skipped = m.plan_chunks(rows)
    assert [len(chunk) for chunk in chunks] == [1, 1, 1]
    assert skipped == []


def test_pack_partitions_and_escape_literals():
    rows = [(f"SS-{n}", "2026", 2, 100, 1, 1) for n in range(33)]
    chunks, _ = m.plan_chunks(rows)
    assert [len(chunk) for chunk in chunks] == [32, 1]
    assert "SS-''x" in m.optimize_sql([("SS-'x", "2026")])
    assert m.plan_chunks([(*PARTITION, 1, 100, 0, 1)]) == ([], [])
    assert m.plan_chunks([(*PARTITION, 2, 100, 0, 2)])[0] == [[PARTITION]]


def test_sizes_rechecked_under_lock():
    queries = Queries()

    @contextmanager
    def lock():
        queries.rows = [(*PARTITION, 1001, 100, 1000, 1)]
        yield True

    result = m.run(query_factory=lambda: queries, writer_lock=lock,
                   refresh_probe=lambda: False, now=lambda: NOW)
    assert result["stopped"] == "partial"
    assert result["skipped"]
    assert queries.optimized == []


def test_window_closes_during_lock_wait():
    queries = Queries()
    current = [NOW]

    @contextmanager
    def lock():
        current[0] += timedelta(hours=1)
        yield True

    result = m.run(query_factory=lambda: queries, writer_lock=lock,
                   refresh_probe=lambda: False, now=lambda: current[0])
    assert result["stopped"] == "deadline"
    assert queries.optimized == []


def test_disabled_lock_fails_closed():
    queries = Queries()

    @contextmanager
    def lock():
        yield False

    result = m.run(query_factory=lambda: queries, writer_lock=lock,
                   refresh_probe=lambda: False, now=lambda: NOW)
    assert result["stopped"] == "error"
    assert queries.optimized == []


def test_refresh_starts_while_waiting():
    queries = Queries()
    probes = iter([False, True])
    result = m.run(query_factory=lambda: queries, writer_lock=queries.lock,
                   refresh_probe=lambda: next(probes), now=lambda: NOW)
    assert result["stopped"] == "refresh_running"
    assert queries.optimized == []


@pytest.mark.parametrize("schedule_state", ["success", "running", "queued"])
def test_refresh_probe_only_blocks_active_schedule_task(monkeypatch, schedule_state):
    import sqlite3

    import psycopg2
    from scrapers.sofascore import writer_lock

    database = sqlite3.connect(":memory:")
    dag_id = "dag_refresh_sofascore_all_mens"
    database.execute("CREATE TABLE dag_run (dag_id TEXT, state TEXT)")
    database.execute("INSERT INTO dag_run VALUES (?, 'running')", (dag_id,))
    database.execute(
        "CREATE TABLE task_instance (dag_id TEXT, task_id TEXT, state TEXT)"
    )
    database.executemany("INSERT INTO task_instance VALUES (?, ?, ?)", [
        (dag_id, "refresh_season_schedules", schedule_state),
        (dag_id, "run_refresh_scope", "running"),
        (dag_id, "refill_refresh_window", "running"),
        (dag_id, "enrich_season_metadata", "running"),
        ("other_dag", "refresh_season_schedules", "running"),
    ])
    closed = []

    class Cursor:
        def execute(self, sql, parameters):
            self.result = database.execute(sql.replace("%s", "?"), parameters)

        def fetchone(self):
            return self.result.fetchone()

    @contextmanager
    def cursor():
        yield Cursor()

    monkeypatch.setattr(writer_lock, "_writer_lock_dsn", lambda env: "offline")
    monkeypatch.setattr(psycopg2, "connect", lambda *args, **kwargs:
                        SimpleNamespace(cursor=cursor, close=lambda: closed.append(True)))
    queries = Queries()
    try:
        result = m.run(query_factory=lambda: queries, writer_lock=queries.lock,
                       refresh_probe=m.probe_refresh, now=lambda: NOW)
        assert result["stopped"] == (
            "complete" if schedule_state == "success" else "refresh_running"
        )
        assert len(queries.optimized) == (1 if schedule_state == "success" else 0)
        assert closed
    finally:
        database.close()


def test_outside_window_does_not_connect():
    result = m.run(now=lambda: NOW + timedelta(hours=1),
                   query_factory=lambda: pytest.fail("must not connect"))
    assert result["stopped"] == "out_of_window"


def test_final_metrics_failure_is_error():
    queries = Queries()
    execute = queries.execute

    def failing(sql):
        if sql == m.METRICS_SQL and queries.optimized:
            raise RuntimeError("metrics unavailable")
        return execute(sql)

    queries.execute = failing
    assert run(queries)["stopped"] == "error"


def test_run_budget_stops_new_chunks():
    queries = Queries()
    ticks = iter([0, 0, 0, m.RUN_SECONDS])
    result = run(queries, clock=lambda: next(ticks))
    assert result["stopped"] == "deadline"
    assert queries.optimized == []


@pytest.mark.parametrize("has_id,cancel_ok", [(True, True), (True, False), (False, False)])
def test_query_error_confirms_cancel_or_waits_server_lifetime(monkeypatch, has_id, cancel_ok):
    events = []
    query = SimpleNamespace(finished=False, cancelled=False)

    class Cursor:
        query_id = "q1" if has_id else None
        _query = query

        def execute(self, sql):
            events.append("execute")
            raise TimeoutError("lost response")

        def cancel(self):
            events.append("cancel")
            if not cancel_ok:
                raise RuntimeError("cancel failed")
            query.cancelled = True

        def close(self):
            events.append("close")

    queries = m.Queries.__new__(m.Queries)
    queries.connection = SimpleNamespace(cursor=Cursor)
    queries.session = SimpleNamespace(deadline=0)
    queries.deadline = float("inf")
    queries.queries = []
    monkeypatch.setattr(m.time, "sleep", lambda seconds: events.append(seconds))
    with pytest.raises(TimeoutError):
        queries.execute("OPTIMIZE")
    assert (m.QUERY_SECONDS + m.HTTP_SECONDS in events) == (not cancel_ok)
    assert ("cancel" in events) == has_id
    assert queries.queries[0]["query_id"] == Cursor.query_id


def test_dag_and_release_inventory():
    root = Path(__file__).resolve().parents[3]
    name = "dag_sofascore_schedule_maintenance"
    dag = (root / f"dags/{name}.py").read_text()
    assert 'schedule="0 3 * * *"' in dag
    assert "catchup=False" in dag and "max_active_runs=1" in dag
    assert "dagrun_timeout=timedelta(minutes=60)" in dag
    assert name in (root / "deploy/sofascore/freeze_release.sh").read_text()
    assert name in (root / "deploy/sofascore/postdeploy_checks.sh").read_text()
    assert name in (root / ".github/workflows/sofascore-ci.yml").read_text()


def test_client_bounds_requests_and_disables_spooling(monkeypatch):
    import requests
    import trino.dbapi

    options, requests_seen = {}, []
    monkeypatch.setattr(trino.dbapi, "connect", lambda **kwargs: options.update(kwargs))
    monkeypatch.setattr(requests.Session, "request", lambda self, method, url, **kwargs:
                        requests_seen.append((method, kwargs["timeout"])))
    queries = m.Queries()
    assert options["max_attempts"] == 1
    assert options["encoding"] is None
    assert options["session_properties"]["query_max_run_time"] == "180s"
    with pytest.raises(TimeoutError):
        queries.session.post("http://offline.invalid")
    assert requests_seen == []
    queries.session.delete("http://offline.invalid")
    assert requests_seen == [("DELETE", m.HTTP_SECONDS)]


@pytest.mark.parametrize("mode", ["cancel", "cancel_failed", "lost_id"])
@pytest.mark.parametrize("stage", ["query", "cleanup"])
def test_sigterm_keeps_writer_lock_until_query_stops(tmp_path, mode, stage):
    import select
    import signal
    import subprocess
    import sys
    import textwrap

    events = tmp_path / "events"
    script = textwrap.dedent('''
        import sys
        from contextlib import contextmanager
        from types import SimpleNamespace
        from scrapers.sofascore import maintenance as m

        mode, stage, path = sys.argv[1:]
        def record(event):
            with open(path, "a") as output:
                output.write(event + "\\n")

        def wait_for_parent():
            print("cleanup", flush=True)
            sys.stdin.readline()

        query = SimpleNamespace(finished=False, cancelled=False)
        class Cursor:
            query_id = None if mode == "lost_id" else "q1"
            _query = query
            def execute(self, sql):
                if stage == "cleanup":
                    raise TimeoutError("lost response")
                print("query", flush=True)
                sys.stdin.readline()
            def fetchall(self):
                raise AssertionError("query must be interrupted")
            def cancel(self):
                record("cancel")
                if mode == "cancel_failed":
                    raise RuntimeError("cancel unavailable")
                wait_for_parent()
                query.cancelled = True
                record("cancel_confirmed")

        queries = m.Queries.__new__(m.Queries)
        queries.connection = SimpleNamespace(cursor=Cursor)
        queries.session = SimpleNamespace(deadline=0)
        queries.deadline = float("inf")
        queries.queries = []
        def fallback(seconds):
            assert seconds == m.QUERY_SECONDS + m.HTTP_SECONDS
            record("fallback")
            wait_for_parent()
            record("server_lifetime_elapsed")
        m.time.sleep = fallback

        @contextmanager
        def writer_lock():
            record("locked")
            try:
                yield
            finally:
                record("unlocked")

        def run():
            with writer_lock():
                queries.execute("OPTIMIZE")
        m.run = run
        raise SystemExit(m.main())
    ''')
    stderr_path = tmp_path / "stderr"
    # Cancellation errors log chained tracebacks. Drain them to a file so pipe
    # capacity cannot block cleanup before the parent receives its handshake.
    with stderr_path.open("w") as stderr_file:
        proc = subprocess.Popen(
            [sys.executable, "-c", script, mode, stage, str(events)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr_file,
            text=True,
        )
    try:
        assert select.select([proc.stdout], [], [], 10)[0], "child did not start"
        if stage == "query":
            assert proc.stdout.readline().strip() == "query"
            proc.send_signal(signal.SIGTERM)
            assert select.select([proc.stdout], [], [], 10)[0], "cleanup did not start"
        assert proc.stdout.readline().strip() == "cleanup"
        assert events.read_text().splitlines()[0] == "locked"
        assert "unlocked" not in events.read_text()
        # A repeated Airflow stop must not interrupt cancellation/fallback.
        proc.send_signal(signal.SIGTERM)
        stdout, _ = proc.communicate("continue\n", timeout=10)
        expected = 128 + signal.SIGTERM if stage == "query" else 1
        assert proc.returncode == expected, stdout + stderr_path.read_text()
        stopped = "cancel_confirmed" if mode == "cancel" else "server_lifetime_elapsed"
        recorded = events.read_text().splitlines()
        assert recorded.index(stopped) < recorded.index("unlocked")
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.communicate()
