"""#1923: full delivery script against local git and an SQLite metabase stub."""
from __future__ import annotations

from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[3]
AUTO = ROOT / "deploy/whoscored/auto_deliver.sh"
DAY = "20261029"
NOW = 1793241000  # 2026-10-29T02:30:00Z


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", *args],
        cwd=repo, check=True, capture_output=True, text=True,
    ).stdout.strip()


def executable(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    path.chmod(0o755)


class Stand:
    def __init__(self, tmp: Path):
        self.tmp = tmp
        self.bin = tmp / "bin"
        self.bin.mkdir()
        self.state = tmp / "state"
        self.state.mkdir()
        self.out = tmp / "out"
        self.runtime = tmp / "runtime"
        self.runtime.mkdir()
        self.source = tmp / "source"
        self.source.mkdir()
        self.tree = self.runtime / "src"
        self.dags = self.runtime / "dags"
        self.dags.mkdir()
        self.installed = tmp / "auto-deliver.sh"
        text = AUTO.read_text(encoding="utf-8")
        path_line = "export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        assert text.count(path_line) == 1
        text = text.replace(path_line, f"export PATH={self.bin}:/usr/bin:/bin")
        executable(self.installed, text)
        git(self.source, "init", "-q", "-b", "master")
        files = {
            "deploy/whoscored/auto_deliver.sh": text,
            "deploy/whoscored/airflow.compose.yaml": "image: data-platform-airflow:test\n",
            "dags/dag_ingest_whoscored.py": "VERSION = 1\n",
            "dags/dag_backfill_whoscored.py": "VERSION = 1\n",
        }
        for rel, body in files.items():
            path = self.source / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(body, encoding="utf-8")
        git(self.source, "add", ".")
        git(self.source, "commit", "-qm", "base")
        self.base = git(self.source, "rev-parse", "HEAD")
        subprocess.run(["git", "clone", "-q", str(self.source), str(self.tree)], check=True)
        git(self.tree, "checkout", "-q", "--detach", self.base)
        for path in (self.source / "dags").iterdir():
            (self.dags / path.name).write_text(path.read_text(), encoding="utf-8")
        (self.source / "dags/dag_ingest_whoscored.py").write_text("VERSION = 2\n")
        git(self.source, "add", ".")
        git(self.source, "commit", "-qm", "new daily")
        self.target = git(self.source, "rev-parse", "HEAD")
        git(self.tree, "fetch", "-q", "origin")
        (self.state / "whoscored-accepted").write_text(self.base + "\n")
        self.db = tmp / "metabase.db"
        with sqlite3.connect(self.db) as db:
            db.executescript("""
                create table dag_run (dag_id text, run_id text, state text);
                create table task_instance (dag_id text, run_id text, task_id text, state text);
            """)
        self.stubs()

    def stubs(self):
        # SQLite executes the actual busy SQL, including the dag_id/run_id join.
        executable(self.bin / "docker", f'''#!{sys.executable}
from pathlib import Path
import sqlite3
import sys
root = Path({str(self.tmp)!r})
a = sys.argv[1:]
sql = a[-1]
if a[0] == 'image' or (a[0] == 'inspect' and a[2] == '{{{{.Image}}}}'):
    print('image-id')
elif a[0] == 'inspect':
    print('true')
elif 'psql' in a:
    if 'from dag_run' in sql:
        with (root / 'queries').open('a') as f: f.write(sql + '\\n')
        if (root / 'reply').exists():
            print((root / 'reply').read_text(), end='')
            sys.exit(1 if (root / 'query-error').exists() else 0)
        with sqlite3.connect(root / 'metabase.db') as db:
            print('|'.join(str(x) for x in db.execute(sql).fetchone()))
    elif 'next_dagrun_create_after' in sql:
        p = root / 'next-run'
        print(p.read_text() if p.exists() else 0)
    elif 'select now()' in sql:
        print('2026-10-29 02:30:00+00')
    elif 'has_import_errors' in sql:
        print(2)
    elif 'import_error' in sql:
        print(0)
    else:
        raise AssertionError(sql)
elif 'python' not in a:
    raise AssertionError(a)
''')
        executable(self.bin / "date", f'''#!/bin/bash
case "${{@: -1}}" in
  +%s) echo {NOW} ;;
  +%Y%m%d) echo {DAY} ;;
  *) echo 2026-10-29T02:30:00Z ;;
esac
''')
        executable(self.bin / "pgrep", f"#!/bin/bash\ntest -f {self.tmp}/driver\n")
        executable(self.bin / "curl", "#!/bin/bash\necho 'network forbidden' >&2; exit 99\n")
        executable(self.bin / "sleep", f'''#!{sys.executable}
from pathlib import Path
import sqlite3
root = Path({str(self.tmp)!r})
p = root / 'sleeps'
n = int(p.read_text()) + 1 if p.exists() else 1
p.write_text(str(n))
if (root / 'transition').exists():
    after, action = (root / 'transition').read_text().split()
    if n == int(after):
        with sqlite3.connect(root / 'metabase.db') as db:
            if action == 'free': db.execute("update task_instance set state='success'")
            elif action in ('daily', 'other'):
                dag = 'dag_ingest_whoscored' if action == 'daily' else 'other'
                db.execute("insert into dag_run values (?, 'new', 'running')", (dag,))
            elif action == 'driver': (root / 'driver').touch()
            elif action == 'schedule':
                db.execute("update task_instance set state='success'")
                (root / 'next-run').write_text(str({NOW} + 600))
''')

    def run(self, *args):
        return subprocess.run(
            ["bash", str(self.installed), *args], capture_output=True, text=True, timeout=20,
            env={"HOME": str(self.tmp), "WHOSCORED_RUNTIME_DIR": str(self.runtime),
                 "STATE": str(self.state), "OUT": str(self.out),
                 "TG_ENV": str(self.tmp / "no-telegram")},
        )

    def history(self, task="wait_before_next_continuous_run", state="up_for_reschedule",
                run_state="running", task_dag="dag_backfill_whoscored", task_run="history"):
        with sqlite3.connect(self.db) as db:
            db.execute("insert into dag_run values ('dag_backfill_whoscored', 'history', ?)", (run_state,))
            db.execute("insert into task_instance values (?, ?, ?, ?)", (task_dag, task_run, task, state))

    def transition(self, action, after=2):
        (self.tmp / "transition").write_text(f"{after} {action}")

    def assert_delivered(self, result):
        assert result.returncode == 0, result.stdout + result.stderr
        assert git(self.tree, "rev-parse", "HEAD") == self.target, result.stdout
        assert "phase=delivered" in (self.state / "whoscored-inflight").read_text()
        assert (self.state / "whoscored-accepted").read_text().strip() == self.base

    def assert_blocked(self, result):
        assert result.returncode == 0, result.stdout + result.stderr
        assert "контур занят" in result.stdout or "до запуска сбора" in result.stdout
        assert git(self.tree, "rev-parse", "HEAD") == self.base
        assert not (self.state / "whoscored-inflight").exists()
        assert not (self.state / f"whoscored-auto-deliver-attempted-{self.target[:12]}").exists()


@pytest.fixture
def stand(tmp_path):
    return Stand(tmp_path)


@pytest.mark.parametrize("sensor", [None, "running", "up_for_reschedule"])
def test_cooldown_or_no_run_delivers(stand, sensor):
    if sensor:
        stand.history(state=sensor)
    stand.assert_delivered(stand.run())


@pytest.mark.parametrize("task", ["run_backfill_chunk", "finalize_chunk"])
@pytest.mark.parametrize("state", ["running", "queued", "scheduled", "up_for_retry"])
@pytest.mark.parametrize("run_state", ["running", "queued"])
def test_active_history_times_out_without_attempt(stand, task, state, run_state):
    stand.history(task, state, run_state)
    stand.assert_blocked(stand.run())
    assert (stand.tmp / "sleeps").read_text() == "30"


@pytest.mark.parametrize("kind", ["daily", "other", "driver"])
@pytest.mark.parametrize("during_wait", [False, True])
def test_other_work_blocks_even_during_wait(stand, kind, during_wait):
    if during_wait:
        stand.history("run_backfill_chunk", "running")
        stand.transition(kind)
    elif kind == "driver":
        (stand.tmp / "driver").touch()
    else:
        with sqlite3.connect(stand.db) as db:
            dag = "dag_ingest_whoscored" if kind == "daily" else "other"
            db.execute("insert into dag_run values (?, 'new', 'queued')", (dag,))
    stand.assert_blocked(stand.run())
    if during_wait:
        assert (stand.tmp / "sleeps").read_text() == "2"
    else:
        assert not (stand.tmp / "sleeps").exists()


def test_window_opens_on_third_poll(stand):
    stand.history("run_backfill_chunk", "running")
    stand.transition("free")
    stand.assert_delivered(stand.run())
    assert (stand.tmp / "sleeps").read_text() == "3"  # two polls + DAG reread


def test_scheduled_run_due_after_wait_blocks(stand):
    stand.history("run_backfill_chunk", "running")
    stand.transition("schedule")
    result = stand.run()
    stand.assert_blocked(result)
    assert "до запуска сбора по расписанию 10 мин" in result.stdout


def test_scheduled_run_within_margin_blocks_without_wait(stand):
    (stand.tmp / "next-run").write_text(str(NOW + 600))
    result = stand.run()
    stand.assert_blocked(result)
    assert "до запуска сбора по расписанию 10 мин" in result.stdout
    assert not (stand.tmp / "sleeps").exists()


@pytest.mark.parametrize("reply, error", [("", False), ("junk", False), ("0|0\n", True), ("0|0\n0|0\n", False)])
def test_metabase_invalid_or_failed_reply_blocks(stand, reply, error):
    (stand.tmp / "reply").write_text(reply)
    if error:
        (stand.tmp / "query-error").touch()
    stand.assert_blocked(stand.run())


@pytest.mark.parametrize("kwargs", [
    {"run_state": "success"}, {"task_dag": "other"}, {"task_run": "finished-run"},
    {"state": "success"},
])
def test_only_tasks_of_active_history_run_block(stand, kwargs):
    stand.history("run_backfill_chunk", **{"state": "running", **kwargs})
    stand.assert_delivered(stand.run())


def test_check_does_not_wait_or_write(stand):
    stand.history("run_backfill_chunk", "running")
    before = {p.relative_to(stand.state): p.read_bytes() for p in stand.state.iterdir()}
    result = stand.run("--check")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "заметка: контур занят" in result.stdout
    assert "проверки пройдены" in result.stdout
    assert not (stand.tmp / "sleeps").exists()
    assert not stand.out.exists()
    assert git(stand.tree, "rev-parse", "HEAD") == stand.base
    assert {p.relative_to(stand.state): p.read_bytes() for p in stand.state.iterdir()} == before
