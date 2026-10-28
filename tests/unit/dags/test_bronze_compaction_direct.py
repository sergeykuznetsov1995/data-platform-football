"""Offline host driver: durable reservations, admission and no Airflow."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts import compact_bronze as cli
from utils import bronze_compaction as compaction
from utils import bronze_compaction_direct as direct


@pytest.fixture
def host(tmp_path, monkeypatch):
    now = datetime(2026, 10, 28, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(direct, "_now", lambda: now)
    monkeypatch.setattr(compaction, "nightly_deadline", lambda: now.replace(hour=5))
    # The executor's watchdog uses wall time independently of the driver clock.
    monkeypatch.setattr(compaction.time, "time", lambda: now.timestamp())
    for key in ("TRINO_USER", "TRINO_PASSWORD", "FBREF_CONTROL_DB_URI"):
        monkeypatch.setenv(key, "test-secret-" + key)
    monkeypatch.setattr(direct, "_maintenance_ready", lambda *_: True)
    connections, alters, offsets = [], [], []

    def connect(**kw):
        connections.append(kw)
        return SimpleNamespace(close=lambda: None)

    monkeypatch.setattr(compaction.maintenance, "_get_trino_connection", connect)
    monkeypatch.setattr(compaction.maintenance, "_list_tables", lambda *_: ["espn_match", "fbref_schedule"])

    def probe(*_, partition_offset=0, **kw):
        offsets.append(partition_offset)
        return compaction.maintenance._CompactionProbeResult((("s3://a", 10), ("s3://b", 10)), 0)

    monkeypatch.setattr(compaction.maintenance, "_compaction_candidates", probe)
    counts = iter([100, 99] * 200)
    monkeypatch.setattr(compaction.maintenance, "_fetch_scalar",
                        lambda _, sql: 0 if "file_path IN" in sql else next(counts))
    monkeypatch.setattr(compaction.maintenance, "_compact_exact_files",
                        lambda *_, **kw: alters.append(kw) or {})

    @contextmanager
    def fence(*_):
        yield lambda: None

    monkeypatch.setattr(compaction, "_publication_fence", fence)
    args = SimpleNamespace(table=["espn_match"], apply=True, portions=2, bytes=1000,
                           state_file=tmp_path / "state.json", journal=tmp_path / "journal",
                           stop_file=tmp_path / "stop")
    return args, now.replace(hour=5), connections, alters, offsets


def summary(args):
    return json.loads(args.journal.read_text().splitlines()[-1])


def test_apply_restart_budget_and_next_night_keep_positions(host, monkeypatch):
    args, deadline, connections, alters, offsets = host
    assert direct.run_direct(args, deadline) == 0
    assert len(alters) == 2
    assert summary(args)["tables"]["espn_match"] == dict(files_before=100, files_after=99)
    assert direct.run_direct(args, deadline) == 0
    assert len(alters) == 2
    assert summary(args)["stop_reason"] == "budget"
    tomorrow = deadline + timedelta(days=1)
    monkeypatch.setattr(direct, "_now", lambda: tomorrow.replace(hour=2))
    monkeypatch.setattr(compaction, "nightly_deadline", lambda: tomorrow)
    monkeypatch.setattr(compaction.time, "time", lambda: tomorrow.replace(hour=2).timestamp())
    assert direct.run_direct(args, tomorrow) == 0
    assert offsets == [0, 1, 2, 3]
    assert all(c == dict(host="127.0.0.1", port=8082) for c in connections)
    state = json.loads(args.state_file.read_text())
    assert state["portions"] == 2 and state["partition_offsets"]["espn_match"] == 4
    assert "s3://" not in args.state_file.read_text()


def test_failed_attempt_is_reserved_before_alter_and_secrets_hidden(host, monkeypatch):
    args, deadline, _, _, offsets = host

    def fail(*_, **kw):
        state = json.loads(args.state_file.read_text())
        assert state["portions"] == 1
        assert state["partition_offsets"]["espn_match"] == 1
        raise RuntimeError(os.environ["TRINO_PASSWORD"])

    monkeypatch.setattr(compaction.maintenance, "_compact_exact_files", fail)
    assert direct.run_direct(args, deadline) == 1
    assert summary(args)["portions"] == 1
    assert direct.run_direct(args, deadline) == 1
    assert offsets == [0, 1]
    assert "test-secret" not in args.journal.read_text()
    assert direct.run_direct(args, deadline) == 0
    assert summary(args)["stop_reason"] == "budget"


def test_dry_run_never_alters_or_changes_real_state(host):
    args, deadline, _, alters, _ = host
    assert direct.run_direct(args, deadline) == 0
    before = args.state_file.read_bytes()
    args.apply = False
    alters.clear()
    assert direct.run_direct(args, deadline) == 0
    assert not alters
    assert args.state_file.read_bytes() == before


def test_corrupt_state_fails_closed(host):
    args, deadline, _, alters, _ = host
    args.state_file.write_text('{"version": 1}')
    assert direct.run_direct(args, deadline) == 1
    assert not alters
    assert args.state_file.read_text() == '{"version": 1}'


def test_lock_survives_atomic_replace_across_processes(tmp_path):
    path = tmp_path / "state"
    code = """
import fcntl, sys
with open(sys.argv[1] + '.lock', 'a') as lock:
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit(7)
sys.exit(0)
"""
    with direct._locked(path):
        for n in range(2):
            direct._save(path, {"n": n})
            assert subprocess.run([sys.executable, "-c", code, str(path)]).returncode == 7
    assert subprocess.run([sys.executable, "-c", code, str(path)]).returncode == 0


def test_waits_for_maintenance_and_checks_again_before_next_portion(host, monkeypatch):
    args, deadline, _, alters, _ = host
    readiness = iter([False, True, False, True])
    sleeps = []
    monkeypatch.setattr(direct, "_maintenance_ready", lambda *_: next(readiness))
    monkeypatch.setattr(direct.time, "sleep", lambda _: sleeps.append(len(alters)))
    assert direct.run_direct(args, deadline) == 0
    assert sleeps == [0, 1]
    assert len(alters) == 2


@pytest.mark.parametrize("reason", ["db", "stop", "deadline"])
def test_admission_blocks_next_portion(host, monkeypatch, reason):
    args, deadline, _, alters, _ = host
    calls = []

    def ready(*_):
        calls.append(1)
        if len(calls) == 1:
            return True
        if reason == "db":
            raise RuntimeError(os.environ["FBREF_CONTROL_DB_URI"])
        if reason == "stop":
            args.stop_file.touch()
        else:
            monkeypatch.setattr(direct, "_now", lambda: deadline)
        return False

    monkeypatch.setattr(direct, "_maintenance_ready", ready)
    monkeypatch.setattr(direct.time, "sleep", lambda _: None)
    assert direct.run_direct(args, deadline) == (1 if reason == "db" else 0)
    assert len(alters) == 1
    assert summary(args)["stop_reason"] == ("error" if reason == "db" else reason)
    assert "test-secret" not in args.journal.read_text()


def test_maintenance_query_requires_exact_daily_and_no_running(monkeypatch):
    statements, settings = [], []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, sql, params):
            statements.append((sql, params))

        def fetchone(self):
            return (True,)

    connection = SimpleNamespace(set_session=lambda **kw: settings.append(kw),
                                 cursor=Cursor, close=lambda: None)

    def connect(dsn, **kw):
        assert kw["connect_timeout"] <= 5
        assert "statement_timeout=5000" in kw["options"]
        return connection

    monkeypatch.setitem(sys.modules, "psycopg2", SimpleNamespace(connect=connect))
    assert direct._maintenance_ready("dsn", datetime(2026, 10, 28, 3, tzinfo=timezone.utc))
    sql, params = statements[0]
    assert "NOT EXISTS" in sql and "AND EXISTS" in sql
    assert "'success', 'failed'" in sql and "state = 'running'" in sql
    assert "dag_iceberg_maintenance'" in sql
    assert params == (datetime(2026, 10, 27, 2, tzinfo=timezone.utc),)
    assert settings == [dict(readonly=True, autocommit=True)]


def test_lost_fbref_fence_and_missing_control_env_fail_closed(host, monkeypatch):
    from scrapers.fbref.control import StateConflict

    args, deadline, _, alters, _ = host
    args.table = ["fbref_schedule"]

    @contextmanager
    def lost(*_):
        raise StateConflict("lost test-secret-password")
        yield

    monkeypatch.setattr(compaction, "_publication_fence", lost)
    assert direct.run_direct(args, deadline) == 1
    monkeypatch.delenv("FBREF_CONTROL_DB_URI")
    assert direct.run_direct(args, deadline) == 1
    assert not alters
    assert "test-secret" not in args.journal.read_text()


def test_cli_direct_uses_full_window_by_default(host, monkeypatch):
    args, deadline, *_ = host

    class Clock:
        @staticmethod
        def now(_):
            return deadline.replace(hour=2)

    monkeypatch.setattr(cli, "datetime", Clock)
    calls = []
    monkeypatch.setattr(direct, "run_direct", lambda a, d: calls.append((a, d)) or 0)
    assert cli.main(["--direct", "--table", "espn_match", "--state-file", str(args.state_file),
                     "--journal", str(args.journal), "--stop-file", str(args.stop_file)]) == 0
    assert calls[0][1] == deadline
    assert calls[0][0].apply is False


@pytest.mark.parametrize("apply", [False, True])
@pytest.mark.parametrize("table", ["espn_match", "fbref_schedule"])
def test_direct_subprocess_forbids_every_airflow_import(tmp_path, apply, table):
    # A fresh interpreter avoids the suite's Airflow stubs masking imports.
    root = Path(__file__).resolve().parents[3]
    code = r'''
import builtins, sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from contextlib import contextmanager
original = builtins.__import__
def checked(name, *args, **kwargs):
    if name == 'airflow' or name.startswith('airflow.'):
        raise AssertionError('Airflow import forbidden: ' + name)
    return original(name, *args, **kwargs)
builtins.__import__ = checked
from utils import bronze_compaction as c
from utils import bronze_compaction_direct as d
from scripts import compact_bronze as cli
now = datetime.now(timezone.utc)
end = now + timedelta(hours=1)
c.nightly_deadline = lambda: end
class Clock:
    @staticmethod
    def now(_):
        return now.replace(hour=2)
cli.datetime = Clock
d._maintenance_ready = lambda *_: True
c.maintenance._get_trino_connection = lambda **kw: SimpleNamespace(close=lambda: None)
c.maintenance._list_tables = lambda *_: ['espn_match', 'fbref_schedule']
from scrapers.fbref.control import ControlStore
control = SimpleNamespace(
    create_run=lambda *a, **kw: 'run', start_run=lambda *a: None,
    acquire_publication_lock=lambda *a, **kw: None,
    renew_publication_lock=lambda *a, **kw: None,
    assert_publication_lock_owner=lambda *a: None,
    release_publication_lock=lambda *a: None, finish_run=lambda *a, **kw: None)
ControlStore.from_env = lambda **kw: control
c.maintenance._compaction_candidates = lambda *a, **kw: c.maintenance._CompactionProbeResult((('s3://a', 10), ('s3://b', 10)), 0)
counts = iter([100, 0, 99])
c.maintenance._fetch_scalar = lambda *a: next(counts)
c.maintenance._compact_exact_files = lambda *a, **kw: {}
# Use the real direct runner, with a current executor deadline.
real = d.run_direct
d.run_direct = lambda args, deadline: real(args, end)
raise SystemExit(cli.main(sys.argv[1:]))
'''
    env = dict(os.environ, PYTHONPATH=str(root) + os.pathsep + str(root / "dags"),
               TRINO_USER="secret-user", TRINO_PASSWORD="secret-pass", FBREF_CONTROL_DB_URI="secret-dsn")
    args = ["--direct", "--table", table, "--portions", "1",
            "--state-file", str(tmp_path / "state"), "--journal", str(tmp_path / "journal"),
            "--stop-file", str(tmp_path / "stop")]
    if apply:
        args.append("--apply")
    result = subprocess.run([sys.executable, "-c", code, *args], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr + result.stdout
    assert json.loads((tmp_path / "journal").read_text().splitlines()[-1])["portions"] == 1


def test_process_interruption_keeps_charge_and_rotated_queue(host, monkeypatch):
    args, deadline, _, _, _ = host
    args.table = ["espn_match", "fbref_schedule"]

    def interrupted(*_, **kw):
        raise KeyboardInterrupt()

    monkeypatch.setattr(compaction.maintenance, "_compact_exact_files", interrupted)
    with pytest.raises(KeyboardInterrupt):
        direct.run_direct(args, deadline)
    state = json.loads(args.state_file.read_text())
    assert state["queue"] == ["fbref_schedule", "espn_match"]
    assert state["portions"] == 1 and state["input_bytes"] == 20
    assert state["partition_offsets"] == {"espn_match": 1}
    tables = []
    counts = iter([100, 99])
    monkeypatch.setattr(compaction.maintenance, "_fetch_scalar",
                        lambda _, sql: 0 if "file_path IN" in sql else next(counts))
    monkeypatch.setattr(compaction.maintenance, "_compact_exact_files",
                        lambda *_, **kw: tables.append(kw["table"]) or {})
    assert direct.run_direct(args, deadline) == 0
    assert tables == ["fbref_schedule"]


def test_missing_daily_waits_until_deadline_without_work(host, monkeypatch):
    args, deadline, connections, alters, _ = host
    monkeypatch.setattr(direct, "_maintenance_ready", lambda *_: False)
    monkeypatch.setattr(direct.time, "sleep",
                        lambda _: monkeypatch.setattr(direct, "_now", lambda: deadline))
    assert direct.run_direct(args, deadline) == 0
    assert not connections and not alters
    assert summary(args)["stop_reason"] == "deadline"


@pytest.mark.parametrize("apply", [False, True])
def test_remaining_byte_limit_is_passed_to_each_portion(host, monkeypatch, apply):
    args, deadline, _, alters, _ = host
    args.apply = apply
    args.table = ["espn_match", "fbref_schedule"]
    args.bytes = 25
    budgets = []

    def probe(*_, max_input_bytes, **kw):
        budgets.append(max_input_bytes)
        paths = (("s3://a", 10), ("s3://b", 10)) if max_input_bytes >= 20 else ()
        return compaction.maintenance._CompactionProbeResult(paths, 0)

    monkeypatch.setattr(compaction.maintenance, "_compaction_candidates", probe)
    assert direct.run_direct(args, deadline) == 0
    assert summary(args)["input_bytes"] == 20
    assert budgets[0] == 25 and set(budgets[1:]) == {5}
    assert len(alters) == (1 if apply else 0)
