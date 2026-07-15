from __future__ import annotations

import os
import signal
import subprocess
import sys
import time

import pytest

from dags.scripts.run_fbref_live_waves import _arm_parent_death_containment


def test_parent_death_signal_is_armed_before_parent_identity_is_checked():
    events = []

    _arm_parent_death_containment(
        123,
        set_signal=lambda signum: events.append(("armed", signum)),
        get_parent_pid=lambda: events.append(("checked", 123)) or 123,
    )

    assert events == [("armed", signal.SIGKILL), ("checked", 123)]


def test_parent_death_race_fails_before_any_paid_work():
    with pytest.raises(RuntimeError, match="parent disappeared"):
        _arm_parent_death_containment(
            123,
            set_signal=lambda _signum: None,
            get_parent_pid=lambda: 456,
        )


def _dead_or_zombie(pid: int) -> bool:
    try:
        state = open(f"/proc/{pid}/stat", encoding="utf-8").read().split()[2]
    except FileNotFoundError:
        return True
    return state == "Z"


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux containment")
def test_watchdog_kills_exec_descendant_after_runner_sigkill():
    code = """
import subprocess
import sys
import time
from dags.scripts.run_fbref_live_waves import _ProcessGroupWatchdog

watchdog = _ProcessGroupWatchdog.start()
child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
print(child.pid, flush=True)
time.sleep(60)
"""
    runner = subprocess.Popen(
        [sys.executable, "-c", code],
        cwd=os.getcwd(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    assert runner.stdout is not None
    line = runner.stdout.readline().strip()
    assert line.isdigit(), (line, runner.stderr.read() if runner.stderr else "")
    descendant_pid = int(line)
    try:
        os.kill(runner.pid, signal.SIGKILL)
        runner.wait(timeout=5)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not _dead_or_zombie(descendant_pid):
            time.sleep(0.02)
        assert _dead_or_zombie(descendant_pid)
    finally:
        try:
            os.killpg(runner.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            runner.wait(timeout=1)
        except subprocess.TimeoutExpired:
            pass
