"""#1474: the WhoScored delivery automat accepts both shapes of the daily DAG."""

from __future__ import annotations

from pathlib import Path
import os
import re
import subprocess
import sys

import pytest

SCRIPT = Path(__file__).resolve().parents[3] / "deploy" / "whoscored" / "auto_deliver.sh"


def _tasks_ok(tasks: str) -> bool:
    text = SCRIPT.read_text(encoding="utf-8")
    function = re.search(r"^tasks_ok\(\) \{\n.*?^\}\n", text, re.S | re.M)
    assert function is not None
    completed = subprocess.run(
        ["bash", "-c", function.group(0) + 'tasks_ok "$1"', "tasks_ok", tasks],
        check=False,
    )
    return completed.returncode == 0


@pytest.mark.unit
@pytest.mark.parametrize(
    ("tasks", "accepted"),
    [
        # Before #1474: one daily task, discover every run.
        ("discover_catalog=success ingest_daily=success", True),
        # After #1474: renamed task; discover outside Monday exits via the
        # weekly gate with rc=0, so the task is still success.
        ("discover_catalog=success ingest_matches=success", True),
        ("discover_catalog=success ingest_matches=failed", False),
        ("discover_catalog=failed ingest_matches=success", False),
        ("discover_catalog=success ingest_daily=failed", False),
        ("discover_catalog=upstream_failed ingest_matches=success", False),
        ("discover_catalog=success", False),
        ("", False),
    ],
)
def test_acceptance_takes_the_old_and_the_new_daily_dag(tasks, accepted):
    assert _tasks_ok(tasks) is accepted


@pytest.mark.unit
def test_acceptance_query_reads_both_task_names():
    text = SCRIPT.read_text(encoding="utf-8")
    assert "task_id in ('discover_catalog','ingest_daily','ingest_matches')" in text
    assert 'if ! tasks_ok "$TASKS"' in text


# --- measure(): fixed Trino SELECT through docker exec, fail closed (#1473).

# Fake `docker`: records argv, then feeds the embedded program from stdin to a
# real interpreter where scrapers.base.trino_manager is a recording stub.
FAKE_DOCKER = '''\
import os
import subprocess
import sys

mode = os.environ.get("FAKE_DOCKER_MODE", "ok")
args = sys.argv[1:]
with open(os.environ["DOCKER_ARGS_CAPTURE"], "w") as fh:
    fh.write("\\n".join(args))
if mode == "sleep":
    import time
    time.sleep(120)
if mode == "fail":
    sys.stdout.write("4,5,6\\n")
    sys.stderr.write("SECRET-STDERR-MARKER\\n")
    sys.exit(1)
if mode == "malformed":
    sys.stdout.write("not-a-count,?,x\\n")
    sys.exit(0)
program = sys.stdin.read()
prog = os.path.join(os.environ["FAKE_WORK"], "embedded.py")
with open(prog, "w") as fh:
    fh.write(program)
env = dict(os.environ)
env["PYTHONPATH"] = os.environ["TRINO_STUB_DIR"]
completed = subprocess.run([sys.executable, "-B", prog, args[-1]], env=env, check=False)
sys.exit(completed.returncode)
'''

# Recording stub of get_trino_connection; the embedded program drives it for real.
TRINO_MANAGER_STUB = '''\
import os


def get_trino_connection(session_properties=None):
    with open(os.environ["SQL_CAPTURE"], "a") as fh:
        fh.write("session_properties=%r\\n" % (session_properties,))
    return _Connection()


class _Connection:
    def cursor(self):
        return _Cursor()

    def close(self):
        pass


class _Cursor:
    def execute(self, sql):
        with open(os.environ["SQL_CAPTURE"], "a") as fh:
            fh.write("sql=%s\\n" % sql)

    def fetchall(self):
        counts = os.environ.get("FAKE_COUNTS", "1,2,3")
        return [tuple(int(v) for v in counts.split(","))]
'''


def _measure_function() -> str:
    text = SCRIPT.read_text(encoding="utf-8")
    function = re.search(r"^measure\(\) \{\n.*?^\}\n", text, re.S | re.M)
    assert function is not None
    return function.group(0)


def _measure_program() -> str:
    text = SCRIPT.read_text(encoding="utf-8")
    program = re.search(r"<<'PYMEASURE'\n(.*?)\nPYMEASURE\n", text, re.S)
    assert program is not None
    return program.group(1)


def _executable(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    path.chmod(0o755)


class _MeasureStand:
    """Extracted measure() plus a fake bin dir; the real docker stays untouched."""

    def __init__(self, tmp_path: Path, *, docker_mode: str | None = "ok", counts: str = "1,2,3"):
        self.bin = tmp_path / "bin"
        self.bin.mkdir()
        work = tmp_path / "work"
        work.mkdir()
        os.symlink(sys.executable, self.bin / "python3")
        self.args_capture = tmp_path / "docker-args.txt"
        self.sql_capture = tmp_path / "sql.txt"
        self.env = {
            "PATH": str(self.bin) + os.pathsep + os.environ.get("PATH", ""),
            "MEASURE_SCHED": "test-scheduler",
            "FAKE_WORK": str(work),
            "FAKE_COUNTS": counts,
            "DOCKER_ARGS_CAPTURE": str(self.args_capture),
            "SQL_CAPTURE": str(self.sql_capture),
            "TRINO_STUB_DIR": str(tmp_path / "trino-stub"),
        }
        if docker_mode is not None:
            _executable(self.bin / "docker", "#!" + sys.executable + "\n" + FAKE_DOCKER)
            self.env["FAKE_DOCKER_MODE"] = docker_mode
        stub = tmp_path / "trino-stub" / "scrapers" / "base"
        stub.mkdir(parents=True)
        (stub.parent / "__init__.py").write_text("", encoding="utf-8")
        (stub / "__init__.py").write_text("", encoding="utf-8")
        (stub / "trino_manager.py").write_text(TRINO_MANAGER_STUB, encoding="utf-8")

    def run(self, timestamp: str, trino_ro: str = "") -> subprocess.CompletedProcess:
        env = dict(self.env, MEASURE_TS=timestamp, MEASURE_TRINO_RO=trino_ro)
        return subprocess.run(
            ["bash", "-c", _measure_function()
             + 'SCHED="$MEASURE_SCHED" TRINO_RO="$MEASURE_TRINO_RO" measure "$MEASURE_TS"'],
            env=env, capture_output=True, text=True, check=False, timeout=30,
        )


@pytest.mark.unit
def test_measure_has_no_default_helper_dependency():
    text = SCRIPT.read_text(encoding="utf-8")
    assert "trino-ro" not in text
    assert 'TRINO_RO="${TRINO_RO:-}"' in text


@pytest.mark.unit
def test_measure_runs_fixed_select_through_docker_exec(tmp_path):
    stand = _MeasureStand(tmp_path)
    completed = stand.run("2026-09-05T10:00:00.123456+05:00")
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "1 2 3"

    args = stand.args_capture.read_text(encoding="utf-8").splitlines()
    assert args[:4] == ["exec", "-i", "-e", "PYTHONPATH=/opt/whoscored-src"]
    assert args[4] == "test-scheduler"
    assert args[5:8] == ["python", "-B", "-"]
    # The container receives the timestamp as an argument, normalized to UTC.
    assert args[8] == "2026-09-05 05:00:00.123456"

    captured = stand.sql_capture.read_text(encoding="utf-8").splitlines()
    session = next(line for line in captured if line.startswith("session_properties="))
    assert "query_max_run_time" in session and "50s" in session
    sql = next(line for line in captured if line.startswith("sql="))[len("sql="):]
    assert sql.startswith("select ")
    assert sql.count("count(*)") == 3
    for table in ("whoscored_matches", "whoscored_events", "whoscored_match_ingest_manifest"):
        assert "iceberg.bronze." + table in sql
    assert sql.count("_ingested_at > timestamp '2026-09-05 05:00:00.123456'") == 3
    # Nothing but the validated, normalized timestamp entered the query.
    assert "+05:00" not in sql and "10:00:00" not in sql


@pytest.mark.unit
def test_measure_helper_output_is_accepted(tmp_path):
    helper = tmp_path / "trino-ro-helper"
    _executable(helper, "#!/bin/sh\nprintf '7,8,9\\n'\n")
    stand = _MeasureStand(tmp_path)
    completed = stand.run("2026-09-05 03:27:55.446584+00", trino_ro=str(helper))
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "7 8 9"
    assert not stand.args_capture.exists()  # helper mode never touches docker


@pytest.mark.unit
def test_measure_missing_helper_fails_closed(tmp_path):
    stand = _MeasureStand(tmp_path)
    completed = stand.run("2026-09-05T03:27:55Z", trino_ro=str(tmp_path / "no-such-helper"))
    assert completed.returncode != 0
    assert completed.stdout == ""
    assert "helper executable missing" in completed.stderr
    assert not stand.args_capture.exists()


@pytest.mark.unit
def test_measure_missing_docker_fails_closed(tmp_path):
    stand = _MeasureStand(tmp_path, docker_mode=None)
    _executable(stand.bin / "bash", "#!/bin/sh\nexec /usr/bin/bash \"$@\"\n")
    env_path_only = str(stand.bin)
    completed = subprocess.run(
        ["bash", "-c", _measure_function()
         + 'SCHED="$MEASURE_SCHED" TRINO_RO="" measure "$MEASURE_TS"'],
        env={"PATH": env_path_only, "MEASURE_SCHED": "test-scheduler",
             "MEASURE_TS": "2026-09-05T03:27:55Z"},
        capture_output=True, text=True, check=False, timeout=30,
    )
    assert completed.returncode != 0
    assert completed.stdout == ""
    assert "docker executable missing" in completed.stderr


@pytest.mark.unit
def test_measure_nonzero_exit_with_valid_stdout_still_fails(tmp_path):
    stand = _MeasureStand(tmp_path, docker_mode="fail")
    completed = stand.run("2026-09-05T03:27:55Z")
    assert completed.returncode != 0
    # Valid-looking stdout of a failed query is never adopted.
    assert completed.stdout == ""
    assert "query execution failed" in completed.stderr


@pytest.mark.unit
def test_measure_never_leaks_subprocess_stderr(tmp_path):
    stand = _MeasureStand(tmp_path, docker_mode="fail")
    completed = stand.run("2026-09-05T03:27:55Z")
    assert "SECRET-STDERR-MARKER" not in completed.stderr


@pytest.mark.unit
@pytest.mark.parametrize("line", ["not-a-count,?,x", "1,2", "1,2,x", "", "1,2,3,4", "-1,2,3"])
def test_measure_malformed_output_fails_closed(tmp_path, line):
    helper = tmp_path / "trino-ro-helper"
    _executable(helper, "#!/bin/sh\nprintf '%s\\n' '" + line + "'\n")
    stand = _MeasureStand(tmp_path)
    completed = stand.run("2026-09-05T03:27:55Z", trino_ro=str(helper))
    assert completed.returncode != 0
    assert completed.stdout == ""
    assert "invalid query result" in completed.stderr


@pytest.mark.unit
def test_measure_malformed_container_output_fails_closed(tmp_path):
    stand = _MeasureStand(tmp_path, docker_mode="malformed")
    completed = stand.run("2026-09-05T03:27:55Z")
    assert completed.returncode != 0
    assert "invalid query result" in completed.stderr


@pytest.mark.unit
def test_measure_negative_count_from_trino_fails_closed(tmp_path):
    stand = _MeasureStand(tmp_path, counts="-1,2,3")
    completed = stand.run("2026-09-05T03:27:55Z")
    assert completed.returncode != 0
    assert completed.stdout == ""
    assert "query execution failed" in completed.stderr


@pytest.mark.unit
@pytest.mark.parametrize(
    "timestamp",
    [
        "not-a-timestamp",
        "2026-13-01 00:00:00",
        "2026-09-05",
        "2026-09-05T03:27:55+00:99",
        "2026-09-05 03:27:55'; DROP TABLE whoscored_matches; --",
        "2026-09-05T03:27:55Z OR 1=1",
        "",
    ],
)
def test_measure_rejects_invalid_timestamps(tmp_path, timestamp):
    stand = _MeasureStand(tmp_path)
    completed = stand.run(timestamp)
    assert completed.returncode != 0
    assert completed.stdout == ""
    assert "invalid timestamp" in completed.stderr
    assert not stand.args_capture.exists()  # no subprocess is ever spawned


@pytest.mark.unit
def test_measure_timeout_fails_closed(monkeypatch, capsys, tmp_path):
    helper = tmp_path / "trino-ro-helper"
    _executable(helper, "#!/bin/sh\n")
    calls = []

    def hung(*args, **kwargs):
        calls.append(kwargs.get("timeout"))
        raise subprocess.TimeoutExpired(cmd="helper", timeout=kwargs.get("timeout"))

    monkeypatch.setattr(subprocess, "run", hung)
    monkeypatch.setattr(
        sys, "argv", ["-", "2026-09-05 03:27:55.446584+00", "test-scheduler", str(helper)]
    )
    with pytest.raises(SystemExit) as excinfo:
        exec(compile(_measure_program(), "measure", "exec"), {})
    assert excinfo.value.code == 1
    assert calls == [60]  # client-side bound
    assert "query timeout" in capsys.readouterr().err


@pytest.mark.unit
def test_measure_bounds_client_and_container_time():
    program = _measure_program()
    assert "timeout=60" in program
    assert "signal.alarm(55)" in program
    assert '"query_max_run_time": "50s"' in program
