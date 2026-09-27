"""#1474: the WhoScored delivery automat accepts both shapes of the daily DAG."""

from __future__ import annotations

from pathlib import Path
import re
import subprocess

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
