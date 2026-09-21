from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import time
from argparse import Namespace
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dags.scripts import run_fbref_live_waves as runner
from dags.scripts.run_fbref_live_waves import _arm_parent_death_containment
from scrapers.fbref.settings import DEFAULT_DOMAIN_INTERVAL_SECONDS


@pytest.fixture(autouse=True)
def _healthy_proxy_meter(monkeypatch):
    monkeypatch.setattr(
        runner,
        "validate_fbref_proxy_meter",
        lambda _url, *, required_bytes, **_kwargs: {
            # Above the run's own cap on purpose: with the two equal, the daily
            # term always binds and the dagrun-remainder arithmetic of #1107
            # stops being observable in any assertion.
            "daily_remaining_bytes": required_bytes * 3,
        },
        raising=False,
    )


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


def test_parser_defaults_to_eighty_live_batches():
    action = next(
        item
        for item in runner.build_parser()._actions
        if item.dest == "max_batches"
    )
    assert action.default == 80
    assert tuple(action.choices) == tuple(range(1, 81))


def test_parser_accepts_a_wave_deadline():
    action = next(
        item
        for item in runner.build_parser()._actions
        if item.dest == "deadline_seconds"
    )
    assert action.default == 0
    assert action.type is float


def _live_run_environment(monkeypatch, *, run_live_waves):
    """One accepted live run, with everything paid stubbed out."""

    control = SimpleNamespace(
        get_run=lambda _run_id: {
            "run_type": "current",
            "request_limit": 4096,
            "byte_limit": 2048 * 1024 * 1024,
            "metadata": {"dag_id": "dag_ingest_fbref"},
        }
    )
    pipeline = SimpleNamespace(
        control=control,
        fetcher_factory=None,
        run_live_waves=run_live_waves,
    )
    monkeypatch.setenv(
        "FBREF_PROXY_CONTROL_URL",
        "http://fbref_proxy_filter:8899",
    )
    monkeypatch.setattr(runner.ControlStore, "from_env", lambda: control)
    monkeypatch.setattr(runner.FBrefPipeline, "from_env", lambda: pipeline)
    monkeypatch.setattr(runner, "FBrefFetcher", lambda **_kwargs: object())
    return Namespace(
        control_run_id="control-run",
        worker_id="live",
        page_kinds="match",
        run_type="current",
        request_limit=4096,
        byte_limit_mb=2048,
        shard_size=25,
        reservation_mb=3,
        domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
        max_batches=14,
        deadline_seconds=0,
    )


def test_runner_converts_deadline_seconds_into_a_monotonic_deadline(
    monkeypatch, capsys
):
    seen = {}

    def run_live_waves(*_args, **kwargs):
        seen.update(kwargs)
        return SimpleNamespace(as_dict=lambda: {"batches": 1})

    args = _live_run_environment(monkeypatch, run_live_waves=run_live_waves)
    args.deadline_seconds = 19800.0
    monkeypatch.setattr(runner.time, "monotonic", lambda: 1000.0)

    assert runner._run(args) == 0
    capsys.readouterr()

    # Counted from the runner's own start, so the meter check and fetcher
    # construction are spent out of the budget rather than added to it.
    assert seen["deadline_monotonic"] == 1000.0 + 19800.0


def test_runner_without_a_deadline_passes_none(monkeypatch, capsys):
    seen = {}

    def run_live_waves(*_args, **kwargs):
        seen.update(kwargs)
        return SimpleNamespace(as_dict=lambda: {"batches": 1})

    args = _live_run_environment(monkeypatch, run_live_waves=run_live_waves)

    assert runner._run(args) == 0
    capsys.readouterr()

    assert seen["deadline_monotonic"] is None


def test_runner_prints_a_progress_document_after_each_batch(
    monkeypatch, capsys
):
    def run_live_waves(*_args, **kwargs):
        on_batch = kwargs["on_batch"]
        on_batch({"batches": 1, "deadline_reached": False})
        on_batch({"batches": 2, "deadline_reached": False})
        return SimpleNamespace(
            as_dict=lambda: {"batches": 2, "deadline_reached": False}
        )

    args = _live_run_environment(monkeypatch, run_live_waves=run_live_waves)

    assert runner._run(args) == 0

    out = capsys.readouterr().out
    progress = [
        json.loads(line[len(runner.PROGRESS_PREFIX):])
        for line in out.splitlines()
        if line.startswith(runner.PROGRESS_PREFIX)
    ]
    assert [document["batches"] for document in progress] == [1, 2]
    # The final result keeps its own prefix, so the parent can tell a complete
    # run from the last thing a killed one managed to say.
    assert f"{runner.RESULT_PREFIX}" in out


def test_progress_documents_also_reach_the_run_log_file(monkeypatch, tmp_path):
    """The pipe dies with a SIGKILLed parent; the log file does not."""

    monkeypatch.setattr(runner, "LOG_DIRECTORY", str(tmp_path / "live"))
    root = logging.getLogger()
    before = list(root.handlers)
    before_level = root.level
    try:
        runner._attach_run_log_file("control-run")
        runner._print_progress({"batches": 2, "deadline_reached": False})
        for handler in root.handlers:
            handler.flush()
        written = (tmp_path / "live" / "control-run.log").read_text()
    finally:
        root.setLevel(before_level)
        for handler in list(root.handlers):
            if handler not in before:
                handler.close()
                root.removeHandler(handler)
    assert runner.PROGRESS_PREFIX in written
    assert '"batches": 2' in written


def test_runner_log_file_receives_the_run_log(monkeypatch, tmp_path):
    monkeypatch.setattr(runner, "LOG_DIRECTORY", str(tmp_path / "live"))
    root = logging.getLogger()
    before = list(root.handlers)
    before_level = root.level
    try:
        runner._attach_run_log_file("control-run")
        logging.getLogger("fbref.test").warning("hello from the runner")
        for handler in root.handlers:
            handler.flush()
        written = (tmp_path / "live" / "control-run.log").read_text()
    finally:
        root.setLevel(before_level)
        for handler in list(root.handlers):
            if handler not in before:
                handler.close()
                root.removeHandler(handler)
    assert "hello from the runner" in written


def test_unwritable_log_directory_does_not_stop_the_runner(
    monkeypatch, tmp_path, capsys
):
    blocked = tmp_path / "blocked"
    blocked.write_text("not a directory")
    monkeypatch.setattr(runner, "LOG_DIRECTORY", str(blocked / "live"))
    root = logging.getLogger()
    before = list(root.handlers)

    runner._attach_run_log_file("control-run")

    assert list(root.handlers) == before
    assert "could not open the runner log file" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("request_limit", "byte_limit_mb"),
    [
        (4096, 50),
        (100, 2048),
        (4097, 2048),
        (4096, 2049),
        (200, 100),
    ],
)
def test_runner_rejects_non_profile_pair_before_pipeline_construction(
    monkeypatch, request_limit, byte_limit_mb
):
    constructed = []
    monkeypatch.setenv("FBREF_PROXY_CONTROL_URL", "http://fbref_proxy_filter:8899")
    monkeypatch.setattr(
        runner.FBrefPipeline,
        "from_env",
        lambda: constructed.append(True),
    )
    args = Namespace(
        control_run_id="control-run",
        worker_id="live",
        page_kinds="match",
        run_type="current",
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=25,
        reservation_mb=3,
        domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
        max_batches=80,
        deadline_seconds=0,
    )

    with pytest.raises(ValueError, match="Unsupported FBref live"):
        runner._run(args)

    assert constructed == []


def test_bootstrap_control_run_is_allowed_through_live_transport(
    monkeypatch,
    capsys,
):
    control = SimpleNamespace(
        get_run=lambda _run_id: {
            "run_type": "current",
            "request_limit": 4096,
            "byte_limit": 2048 * 1024 * 1024,
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
    monkeypatch.setattr(runner.ControlStore, "from_env", lambda: control)
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
        request_limit=4096,
        byte_limit_mb=2048,
        shard_size=25,
        reservation_mb=3,
        domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
        max_batches=16,
        deadline_seconds=0,
    )

    assert runner._run(args) == 0
    pipeline.fetcher_factory(None, 4096, 2048 * 1024 * 1024)

    assert pipeline.finalization_guard is (
        runner._defer_sigterm_during_finalization
    )
    assert fetcher_kwargs["provider_context"]["dag_id"] == (
        "dag_bootstrap_fbref"
    )
    # The daily allowance minus the lease-extension race headroom (#1188): the
    # meter recomputes its ceiling at the warm-HTTP boundary, so a run that
    # claims the whole remainder gets a 409 that kills the wave.
    assert fetcher_kwargs["provider_max_bytes"] == (
        2048 * 1024 * 1024 - runner.LEASE_EXTENSION_RACE_HEADROOM_BYTES
    )
    assert '"status": "complete"' in capsys.readouterr().out


@pytest.mark.parametrize(
    ("bytes_used", "bytes_reserved", "expected_provider_max_bytes"),
    [
        # The dagrun remainder binds (2 GiB - spend), and the lease-extension
        # race headroom is held back on top of it (#1188).
        (
            5_400_000,
            100_000,
            2048 * 1024 * 1024 - 5_500_000 - 64 * 1024 * 1024,
        ),
        (2048 * 1024 * 1024, 1, 0),
    ],
)
def test_retry_attempt_requests_only_the_remaining_dagrun_budget(
    monkeypatch,
    capsys,
    bytes_used,
    bytes_reserved,
    expected_provider_max_bytes,
):
    # The filter budget is shared per dagrun across attempts (#1107): a retry
    # subprocess must subtract what earlier attempts already spent instead of
    # re-requesting the full CLI cap and dying on a deterministic extend 409.
    control = SimpleNamespace(
        get_run=lambda _run_id: {
            "run_type": "current",
            "request_limit": 4096,
            "byte_limit": 2048 * 1024 * 1024,
            "metadata": {"dag_id": "dag_ingest_fbref"},
            "bytes_used": bytes_used,
            "bytes_reserved": bytes_reserved,
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
    monkeypatch.setattr(runner.ControlStore, "from_env", lambda: control)
    monkeypatch.setattr(runner.FBrefPipeline, "from_env", lambda: pipeline)
    monkeypatch.setattr(
        runner,
        "FBrefFetcher",
        lambda **kwargs: fetcher_kwargs.update(kwargs) or object(),
    )
    args = Namespace(
        control_run_id="control-run",
        worker_id="retry-live",
        page_kinds="competition_index,competition",
        run_type="current",
        request_limit=4096,
        byte_limit_mb=2048,
        shard_size=25,
        reservation_mb=3,
        domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
        max_batches=16,
        deadline_seconds=0,
    )

    assert runner._run(args) == 0
    pipeline.fetcher_factory(None, 4096, 2048 * 1024 * 1024)

    assert fetcher_kwargs["provider_max_bytes"] == expected_provider_max_bytes
    assert '"status": "complete"' in capsys.readouterr().out


def test_runner_caps_new_run_to_daily_bytes_left_after_another_run(
    monkeypatch,
):
    mib = 1024 * 1024
    control = SimpleNamespace(
        get_run=lambda _run_id: {
            "run_type": "current",
            "request_limit": 4096,
            "byte_limit": 2048 * mib,
            "metadata": {"dag_id": "dag_ingest_fbref"},
            "bytes_used": 5 * mib,
            "bytes_reserved": 3 * mib,
        }
    )
    pipeline = SimpleNamespace(
        control=control,
        fetcher_factory=None,
        run_live_waves=lambda *_args, **_kwargs: SimpleNamespace(
            as_dict=lambda: {"status": "complete"}
        ),
    )
    fetcher_kwargs = {}
    meter = MagicMock(
        return_value={"daily_remaining_bytes": 400 * mib}
    )
    monkeypatch.setenv(
        "FBREF_PROXY_CONTROL_URL",
        "http://fbref_proxy_filter:8899",
    )
    monkeypatch.setenv("FBREF_PROXY_CONTROL_TOKEN", "x" * 32)
    monkeypatch.setattr(runner.ControlStore, "from_env", lambda: control)
    monkeypatch.setattr(runner, "validate_fbref_proxy_meter", meter)
    monkeypatch.setattr(runner.FBrefPipeline, "from_env", lambda: pipeline)
    monkeypatch.setattr(
        runner,
        "FBrefFetcher",
        lambda **kwargs: fetcher_kwargs.update(kwargs) or object(),
    )
    args = Namespace(
        control_run_id="control-run",
        worker_id="another-run-spent",
        page_kinds="match",
        run_type="current",
        request_limit=4096,
        byte_limit_mb=2048,
        shard_size=25,
        reservation_mb=3,
        domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
        max_batches=80,
        deadline_seconds=0,
    )

    assert runner._run(args) == 0
    pipeline.fetcher_factory(None, 4096, 2048 * mib)

    # The daily remainder minus the race headroom, not all of it: the meter
    # recomputes the lease-extension ceiling from its live daily counter, so a
    # run that claims the whole remainder loses the race against its own spend
    # and gets a 409 that kills the wave (#1188).
    assert fetcher_kwargs["provider_max_bytes"] == (
        400 * mib - runner.LEASE_EXTENSION_RACE_HEADROOM_BYTES
    )
    assert fetcher_kwargs["provider_max_bytes"] < 400 * mib
    assert meter.call_args.kwargs["required_bytes"] == 2048 * mib
    assert meter.call_args.kwargs["minimum_configured_exits"] == 4




@pytest.mark.parametrize(
    ("daily_remaining_mib", "expected_withheld_mib"),
    [
        # Far from the cap: the whole 64 MiB race slice is withheld.
        (1024, 64),
        # Exactly the cap boundary (256 MiB // 4 == 64 MiB).
        (256, 64),
        # Below the boundary the quarter-cap binds, so the slice shrinks
        # instead of eating the whole remainder.
        (128, 32),
        (16, 4),
    ],
)
def test_runner_never_claims_the_whole_daily_remainder(
    monkeypatch, daily_remaining_mib, expected_withheld_mib
):
    # #1188: the meter re-derives the lease-extension ceiling from its live
    # daily counter at the browser -> warm-HTTP boundary.  A run that claims
    # every remaining byte therefore gets a 409 the runner classifies as
    # hard_transport_policy, killing the wave with its paid Cloudflare
    # bootstrap already burnt.  The claimed budget must stay STRICTLY under the
    # remainder the meter reported, at every scale of remainder.
    mib = 1024 * 1024
    daily_remaining = daily_remaining_mib * mib
    control = SimpleNamespace(
        get_run=lambda _run_id: {
            "run_type": "current",
            "request_limit": 4096,
            "byte_limit": 2048 * mib,
            "metadata": {"dag_id": "dag_ingest_fbref"},
            "bytes_used": 0,
            "bytes_reserved": 0,
        }
    )
    pipeline = SimpleNamespace(
        control=control,
        fetcher_factory=None,
        run_live_waves=lambda *_args, **_kwargs: SimpleNamespace(
            as_dict=lambda: {"status": "complete"}
        ),
    )
    fetcher_kwargs = {}
    monkeypatch.setenv("FBREF_PROXY_CONTROL_URL", "http://fbref_proxy_filter:8899")
    monkeypatch.setenv("FBREF_PROXY_CONTROL_TOKEN", "x" * 32)
    monkeypatch.setattr(runner.ControlStore, "from_env", lambda: control)
    monkeypatch.setattr(
        runner,
        "validate_fbref_proxy_meter",
        MagicMock(return_value={"daily_remaining_bytes": daily_remaining}),
    )
    monkeypatch.setattr(runner.FBrefPipeline, "from_env", lambda: pipeline)
    monkeypatch.setattr(
        runner,
        "FBrefFetcher",
        lambda **kwargs: fetcher_kwargs.update(kwargs) or object(),
    )
    args = Namespace(
        control_run_id="control-run",
        worker_id="headroom",
        page_kinds="match",
        run_type="current",
        request_limit=4096,
        byte_limit_mb=2048,
        shard_size=25,
        reservation_mb=3,
        domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
        max_batches=80,
        deadline_seconds=0,
    )

    assert runner._run(args) == 0
    pipeline.fetcher_factory(None, 4096, 2048 * mib)

    claimed = fetcher_kwargs["provider_max_bytes"]
    # The invariant, not the arithmetic: never the whole remainder.
    assert 0 < claimed < daily_remaining
    # And the withheld slice is a real wave's worth, not a token byte.
    assert daily_remaining - claimed == expected_withheld_mib * mib

def test_runner_rejects_stored_profile_mismatch_before_fetcher_construction(
    monkeypatch,
):
    fetcher_constructed = []
    control = SimpleNamespace(
        get_run=lambda _run_id: {
            "run_type": "current",
            "request_limit": 100,
            "byte_limit": 50 * 1024 * 1024,
            "metadata": {"dag_id": "dag_ingest_fbref"},
        }
    )
    pipeline = SimpleNamespace(control=control, fetcher_factory=None)
    monkeypatch.setenv("FBREF_PROXY_CONTROL_URL", "http://fbref_proxy_filter:8899")
    monkeypatch.setattr(runner.ControlStore, "from_env", lambda: control)
    monkeypatch.setattr(runner.FBrefPipeline, "from_env", lambda: pipeline)
    monkeypatch.setattr(
        runner,
        "FBrefFetcher",
        lambda **_kwargs: fetcher_constructed.append(True),
    )
    args = Namespace(
        control_run_id="control-run",
        worker_id="live",
        page_kinds="match",
        run_type="current",
        request_limit=4096,
        byte_limit_mb=2048,
        shard_size=25,
        reservation_mb=3,
        domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
        max_batches=80,
        deadline_seconds=0,
    )

    with pytest.raises(RuntimeError, match="profile differs"):
        runner._run(args)

    assert fetcher_constructed == []
    assert pipeline.fetcher_factory is None


def test_runner_rejects_persistent_marker_mismatch_before_pipeline_or_meter(
    monkeypatch,
):
    constructed = []
    meter_calls = []
    control = SimpleNamespace(
        get_run=lambda _run_id: {
            "run_type": "current",
            "request_limit": 100,
            "byte_limit": 50 * 1024 * 1024,
            "metadata": {
                "dag_id": "dag_ingest_fbref",
                "persistent_http_session": False,
            },
        }
    )
    monkeypatch.setenv("FBREF_PROXY_CONTROL_URL", "http://fbref_proxy_filter:8899")
    monkeypatch.setenv("FBREF_PERSISTENT_HTTP_SESSION", "1")
    monkeypatch.setattr(runner.ControlStore, "from_env", lambda: control)
    monkeypatch.setattr(
        runner.FBrefPipeline,
        "from_env",
        lambda: constructed.append("pipeline"),
    )
    monkeypatch.setattr(
        runner,
        "validate_fbref_proxy_meter",
        lambda *_args, **_kwargs: meter_calls.append(True),
    )
    args = Namespace(
        control_run_id="control-run",
        worker_id="live",
        page_kinds="match",
        run_type="current",
        request_limit=100,
        byte_limit_mb=50,
        shard_size=25,
        reservation_mb=3,
        domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
        max_batches=80,
        deadline_seconds=0,
    )

    with pytest.raises(RuntimeError, match="persistent HTTP profile differs"):
        runner._run(args)

    assert constructed == []
    assert meter_calls == []


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux signals")
def test_sigterm_unwinds_strict_tail_and_control_finalizers(tmp_path):
    journal = tmp_path / "finalizers.log"
    code = r'''
import os
import signal
import sys
import time
from types import SimpleNamespace

from dags.scripts import run_fbref_live_waves as runner
from scrapers.fbref.pipeline import _LiveFetchSession

journal = sys.argv[1]

def record(value):
    with open(journal, "a", encoding="utf-8") as stream:
        stream.write(value + "\n")
        stream.flush()
        os.fsync(stream.fileno())

class Fetcher:
    def finalize_metered_session(self):
        record("strict_close")
        # A repeated TERM during exact provider close must be latched, not
        # allowed to interrupt tail/control settlement.
        os.kill(os.getpid(), signal.SIGTERM)
        return SimpleNamespace(session_id="session")

class Control:
    def settle_clearance_session_tail(self, session_id, receipt):
        assert session_id == "session"
        assert receipt.session_id == "session"
        record("tail_settle")
        return {"terminal": False}

    def close_clearance_session(self, session_id, *, status):
        assert session_id == "session"
        assert status == "failed"
        record("control_close")

class Watchdog:
    def disarm(self):
        record("watchdog_disarm")

runner._arm_parent_death_containment = lambda _pid: None
runner._ProcessGroupWatchdog.start = classmethod(lambda _cls: Watchdog())

def run(_args):
    live = _LiveFetchSession(
        fetcher=Fetcher(),
        session_id="session",
        persistent_enabled=True,
        state="active",
        tail_reserved=True,
    )
    try:
        # The parent may signal immediately after READY. Announce readiness
        # only once the session's finally handler is installed.
        print("READY", flush=True)
        while True:
            time.sleep(1)
    finally:
        live.close(Control(), status="failed")

runner._run = run
runner.main([
    "--control-run-id", "control-run",
    "--parent-pid", str(os.getppid()),
    "--worker-id", "live",
    "--page-kinds", "match",
    "--run-type", "current",
    "--request-limit", "100",
    "--byte-limit-mb", "50",
    "--shard-size", "25",
    "--reservation-mb", "3",
    "--domain-interval-seconds", "3",
])
'''
    process = subprocess.Popen(
        [sys.executable, "-c", code, str(journal)],
        cwd=os.getcwd(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    assert process.stdout is not None
    assert process.stdout.readline().strip() == "READY"
    try:
        os.kill(process.pid, signal.SIGTERM)
        _, stderr = process.communicate(timeout=10)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=2)

    assert process.returncode == 128 + signal.SIGTERM, stderr
    assert journal.read_text(encoding="utf-8").splitlines() == [
        "strict_close",
        "tail_settle",
        "control_close",
        "watchdog_disarm",
    ]


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux signals")
def test_first_sigterm_during_existing_finalizer_is_deferred(tmp_path):
    journal = tmp_path / "existing-finalizer.log"
    code = r'''
import os
import signal
import sys
import time
from types import SimpleNamespace

from dags.scripts import run_fbref_live_waves as runner
from scrapers.fbref.pipeline import _LiveFetchSession

journal = sys.argv[1]

def record(value):
    with open(journal, "a", encoding="utf-8") as stream:
        stream.write(value + "\n")
        stream.flush()
        os.fsync(stream.fileno())

class Fetcher:
    def finalize_metered_session(self):
        record("provider_close_started")
        print("FINALIZING", flush=True)
        time.sleep(0.3)
        record("strict_close_finished")
        return SimpleNamespace(session_id="session")

class Control:
    def settle_clearance_session_tail(self, session_id, receipt):
        assert session_id == receipt.session_id == "session"
        record("tail_settle")
        return {"terminal": False}

    def close_clearance_session(self, session_id, *, status):
        assert session_id == "session"
        assert status == "closed"
        record("control_close")

class Watchdog:
    def disarm(self):
        record("watchdog_disarm")

runner._arm_parent_death_containment = lambda _pid: None
runner._ProcessGroupWatchdog.start = classmethod(lambda _cls: Watchdog())

def run(_args):
    live = _LiveFetchSession(
        fetcher=Fetcher(),
        session_id="session",
        persistent_enabled=True,
        state="active",
        tail_reserved=True,
        finalization_guard=runner._defer_sigterm_during_finalization,
    )
    live.close(Control(), status="closed")
    raise AssertionError("deferred SIGTERM was not replayed")

runner._run = run
runner.main([
    "--control-run-id", "control-run",
    "--parent-pid", str(os.getppid()),
    "--worker-id", "live",
    "--page-kinds", "match",
    "--run-type", "current",
    "--request-limit", "100",
    "--byte-limit-mb", "50",
    "--shard-size", "25",
    "--reservation-mb", "3",
    "--domain-interval-seconds", "3",
])
'''
    process = subprocess.Popen(
        [sys.executable, "-c", code, str(journal)],
        cwd=os.getcwd(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    assert process.stdout is not None
    assert process.stdout.readline().strip() == "FINALIZING"
    try:
        os.kill(process.pid, signal.SIGTERM)
        _, stderr = process.communicate(timeout=10)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=2)

    assert process.returncode == 128 + signal.SIGTERM, stderr
    assert journal.read_text(encoding="utf-8").splitlines() == [
        "provider_close_started",
        "strict_close_finished",
        "tail_settle",
        "control_close",
        "watchdog_disarm",
    ]


def _dead_or_zombie(pid: int) -> bool:
    try:
        state = open(f"/proc/{pid}/stat", encoding="utf-8").read().split()[2]
    except FileNotFoundError:
        return True
    except ProcessLookupError:
        # The pid directory outlived the process it describes: /proc answers
        # ESRCH rather than ENOENT for the window between exit and teardown.
        # That is the condition this helper is asked about, so it must report
        # it instead of tearing the test down on its own success.
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
