"""Nightly ops-journal lifecycle: order, retries/busy, dry-run, deadline/stop, CLI window."""

from datetime import datetime, timedelta, timezone
import json

import pytest

from scripts import maintain_ops as cli
from utils import ops_maintenance as ops

NOW = datetime(2026, 11, 4, 0, 30, tzinfo=timezone.utc)
DEADLINE = datetime(2026, 11, 4, 2, tzinfo=timezone.utc)


class CommitConflict(Exception):
    error_name = "ICEBERG_COMMIT_ERROR"


class FakeTrino:
    def __init__(self, files, failures=None):
        self.files = dict(files)
        self.failures = failures or {}
        self.statements = []
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rows = []

    def execute(self, sql):
        self.conn.statements.append(sql)
        if "information_schema.tables" in sql:
            self.rows = [(t,) for t in sorted(self.conn.files)]
        elif '$files"' in sql:
            table = sql.split('"')[3].split("$")[0]
            self.rows = [(self.conn.files[table],)]
        elif '$snapshots"' in sql:
            self.rows = [(7,)]
        elif sql.startswith("ALTER"):
            table = sql.split('"')[3]
            pending = self.conn.failures.get(table)
            if pending:
                raise pending.pop(0)
            if "optimize" in sql:
                self.conn.files[table] = 1
            self.rows = [(1,)]
        else:
            self.rows = []

    def fetchall(self):
        return self.rows

    def close(self):
        pass


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    monkeypatch.setattr(ops, "_now", lambda: NOW)


def _alters(conn):
    return [s for s in conn.statements if s.startswith("ALTER")]


def _run(conn, tmp_path, **kw):
    journal = tmp_path / "journal.jsonl"
    sleeps = []
    result = ops.maintain_ops_tables(lambda: conn, deadline=kw.pop("deadline", DEADLINE),
                                     journal=journal, sleep=sleeps.append, **kw)
    events = [json.loads(line) for line in journal.read_text().splitlines()]
    return result, events, sleeps


def test_fattest_first_full_optimize_then_expire_without_orphans(tmp_path):
    conn = FakeTrino({"small": 10, "espn_http_attempt_v1": 44837, "mid": 900})
    result, events, _ = _run(conn, tmp_path, dry_run=False)
    assert result["exit_code"] == 0 and result["maintained"] == 3
    assert [e["table"] for e in events[:-1]] == ["espn_http_attempt_v1", "mid", "small"]
    alters = _alters(conn)
    assert alters[0] == 'ALTER TABLE iceberg."ops"."espn_http_attempt_v1" EXECUTE optimize'
    assert alters[1] == ('ALTER TABLE iceberg."ops"."espn_http_attempt_v1" EXECUTE '
                         "expire_snapshots(retention_threshold => '3d')")
    assert not any("orphan" in s or "$path" in s for s in conn.statements)
    assert "SET SESSION iceberg.expire_snapshots_min_retention = '1h'" in conn.statements
    assert events[0]["before"] == {"files": 44837, "snapshots": 7}
    assert events[0]["after"] == {"files": 1, "snapshots": 7}
    assert conn.closed


def test_commit_conflict_retried_then_busy_is_not_fatal(tmp_path):
    conn = FakeTrino(
        {"hot": 500, "cold": 5},
        failures={"hot": [CommitConflict("x")] * 4, "cold": [CommitConflict("x")]},
    )
    result, events, sleeps = _run(conn, tmp_path, dry_run=False)
    assert result["tables"] == {"hot": "busy", "cold": "ok"}
    assert result["exit_code"] == 0 and result["busy"] == 1
    # hot optimize: 4 attempts (3 sleeps), hot expire ok, cold: 1 retry.
    assert len(sleeps) == 4
    assert events[0]["optimized"] is False and events[0]["expired"] is True


def test_only_busy_tables_or_real_failure_exit_nonzero_without_leaking(tmp_path):
    conn = FakeTrino({"hot": 5}, failures={"hot": [CommitConflict("x")] * 8})
    assert _run(conn, tmp_path, dry_run=False)[0]["exit_code"] == 1
    conn = FakeTrino({"a": 9, "b": 1}, failures={"a": [RuntimeError("password=hunter2")]})
    (tmp_path / "b").mkdir()
    result, events, _ = _run(conn, tmp_path / "b", dry_run=False)
    assert result["tables"] == {"a": "failed", "b": "ok"}
    assert result["exit_code"] == 1
    assert "hunter2" not in json.dumps(events)
    assert events[0]["error_type"] == "RuntimeError"


def test_dry_run_never_alters(tmp_path):
    conn = FakeTrino({"a": 3, "b": 4})
    result, events, _ = _run(conn, tmp_path, dry_run=True)
    assert not _alters(conn)
    assert result["tables"] == {"b": "planned", "a": "planned"}
    assert result["exit_code"] == 0


def test_stop_file_and_deadline_between_tables(tmp_path, monkeypatch):
    stop = tmp_path / "stop"
    stop.touch()
    conn = FakeTrino({"a": 3})
    result, _, _ = _run(conn, tmp_path, dry_run=False, stop_file=stop)
    assert result["stop_reason"] == "stop" and not _alters(conn)

    conn = FakeTrino({"a": 3})
    result, _, _ = _run(conn, tmp_path, dry_run=False, deadline=NOW)
    assert result["stop_reason"] == "deadline" and not _alters(conn)
    assert result["exit_code"] == 0

    conn = FakeTrino({"a": 3, "b": 2})
    result, _, _ = _run(conn, tmp_path, dry_run=False, deadline=NOW + timedelta(seconds=30))
    assert any("query_max_run_time = '30s'" in s for s in conn.statements)


def test_table_filter_and_exclude(tmp_path):
    conn = FakeTrino({"a": 3, "b": 4, "c": 5})
    result, _, _ = _run(conn, tmp_path, dry_run=True, only=["a"], exclude=["c"])
    assert result["tables"] == {"a": "planned"}
    result, _, _ = _run(FakeTrino({"a": 3}), tmp_path, dry_run=True, only=["zzz"])
    assert result["exit_code"] == 1 and result["error_type"] == "ValueError"


def _cli_args(tmp_path, *extra):
    return ["--direct", "--journal", str(tmp_path / "j"),
            "--stop-file", str(tmp_path / "s"), *extra]


def _clock(monkeypatch, hour):
    class Clock:
        @staticmethod
        def now(_):
            return datetime(2026, 11, 4, hour, 10, tzinfo=timezone.utc)

    monkeypatch.setattr(cli, "datetime", Clock)


@pytest.mark.parametrize("hour", [2, 5, 14, 23])
def test_cli_refuses_outside_window(tmp_path, monkeypatch, hour):
    _clock(monkeypatch, hour)
    with pytest.raises(SystemExit) as exc:
        cli.main(_cli_args(tmp_path))
    assert exc.value.code == 2


def test_cli_in_window_runs_dry_by_default(tmp_path, monkeypatch):
    from utils import bronze_compaction_direct as direct

    _clock(monkeypatch, 0)
    for key in ("TRINO_USER", "TRINO_PASSWORD", "FBREF_CONTROL_DB_URI"):
        monkeypatch.setenv(key, "secret")
    monkeypatch.setattr(direct, "_maintenance_ready", lambda *_: True)
    calls = []
    monkeypatch.setattr(ops, "maintain_ops_tables",
                        lambda f, **kw: calls.append(kw) or {"exit_code": 0})
    assert cli.main(_cli_args(tmp_path, "--table", "a", "--seconds", "600")) == 0
    assert calls[0]["dry_run"] is True and calls[0]["only"] == ["a"]
    assert calls[0]["deadline"] == datetime(2026, 11, 4, 0, 20, tzinfo=timezone.utc)

    monkeypatch.setattr(direct, "_maintenance_ready", lambda *_: False)
    assert cli.main(_cli_args(tmp_path, "--apply")) == 1
    assert len(calls) == 1
