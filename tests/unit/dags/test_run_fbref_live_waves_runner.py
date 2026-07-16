from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from argparse import Namespace
from types import SimpleNamespace

import pytest

from dags.scripts import run_fbref_live_waves as runner
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


def test_bootstrap_control_run_is_allowed_through_live_transport(
    monkeypatch,
    capsys,
):
    control = SimpleNamespace(
        get_run=lambda _run_id: {
            "run_type": "current",
            "metadata": {"dag_id": "dag_bootstrap_fbref"},
        }
    )
    result = SimpleNamespace(as_dict=lambda: {"status": "complete"})
    pipeline = SimpleNamespace(
        control=control,
        fetcher_factory=None,
        run_live_waves=lambda *_args, **_kwargs: result,
    )
    fetcher_kwargs = {}

    monkeypatch.setenv(
        "FBREF_PROXY_CONTROL_URL",
        "http://fbref_proxy_filter:8899",
    )
    monkeypatch.setattr(runner.FBrefPipeline, "from_env", lambda: pipeline)
    monkeypatch.setattr(
        runner,
        "FBrefFetcher",
        lambda **kwargs: fetcher_kwargs.update(kwargs) or object(),
    )
    args = Namespace(
        control_run_id="control-run",
        worker_id="bootstrap-live",
        page_kinds="competition_index,competition",
        run_type="current",
        request_limit=200,
        byte_limit_mb=100,
        shard_size=25,
        reservation_mb=3,
        domain_interval_seconds=3.0,
        max_batches=16,
    )

    assert runner._run(args) == 0
    pipeline.fetcher_factory(None, 200, 100 * 1024 * 1024)

    assert fetcher_kwargs["provider_context"]["dag_id"] == (
        "dag_bootstrap_fbref"
    )
    assert fetcher_kwargs["provider_max_bytes"] == 100 * 1024 * 1024
    assert '"status": "complete"' in capsys.readouterr().out


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
