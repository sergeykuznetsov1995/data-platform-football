"""Run bounded FBref fetch/parse batches in one contained process group.

The runner owns one warm fetcher and metered proxy lease for the live run.
Every batch commits immutable raw evidence before running offline discovery,
then immediately admits newly discovered targets into the next batch.  A tiny
watchdog kills browser descendants if the Airflow task or runner is SIGKILLed.
"""

from __future__ import annotations

import argparse
import ctypes
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import Mapping

from scrapers.fbref.fetcher import FBrefFetcher
from scrapers.fbref.pipeline import FBrefPipeline, PipelineSettings
from scrapers.fbref.proxy_lease import FBREF_DAG_IDS
from scrapers.fbref.settings import MIB


RESULT_PREFIX = "FBREF_LIVE_WAVES_RESULT:"
_PR_SET_PDEATHSIG = 1


def _set_parent_death_signal(signum: int) -> None:
    """Ask Linux to kill this runner when its Airflow task parent dies."""

    if not sys.platform.startswith("linux"):
        raise RuntimeError("FBref live runner requires Linux parent-death support")
    libc = ctypes.CDLL(None, use_errno=True)
    result = int(libc.prctl(_PR_SET_PDEATHSIG, int(signum), 0, 0, 0))
    if result != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))


def _arm_parent_death_containment(
    expected_parent_pid: int,
    *,
    set_signal=_set_parent_death_signal,
    get_parent_pid=os.getppid,
) -> None:
    """Arm PDEATHSIG without the classic parent-died-before-prctl race."""

    expected = int(expected_parent_pid)
    if expected <= 1:
        raise ValueError("parent PID must identify the Airflow task process")
    set_signal(signal.SIGKILL)
    actual = int(get_parent_pid())
    if actual != expected:
        raise RuntimeError(
            "Airflow task parent disappeared before FBref containment armed"
        )


def _watchdog_child(read_fd: int, runner_pid: int, process_group_id: int) -> None:
    """Kill the runner's whole process group after an unexpected pipe EOF."""

    # Do not retain the Popen stdout/stderr pipes if the runner is SIGKILLed.
    for descriptor in (0, 1, 2):
        try:
            os.close(descriptor)
        except OSError:
            pass
    normal_exit = False
    try:
        # If the runner died between fork() and this check, do not wait for a
        # pipe state that another just-forked browser child might still hold.
        if os.getppid() != runner_pid:
            os.killpg(process_group_id, signal.SIGKILL)
            os._exit(2)
        while True:
            try:
                item = os.read(read_fd, 1)
            except InterruptedError:
                continue
            if item == b"N":
                normal_exit = True
                break
            if not item:
                break
    finally:
        try:
            os.close(read_fd)
        except OSError:
            pass
    if not normal_exit:
        os.killpg(process_group_id, signal.SIGKILL)
    os._exit(0 if normal_exit else 2)


@dataclass
class _ProcessGroupWatchdog:
    child_pid: int
    write_fd: int
    process_group_id: int
    disarmed: bool = False

    @classmethod
    def start(cls) -> "_ProcessGroupWatchdog":
        runner_pid = os.getpid()
        process_group_id = os.getpgrp()
        if process_group_id != runner_pid:
            raise RuntimeError(
                "FBref live runner must be the leader of its own process group"
            )
        read_fd, write_fd = os.pipe2(os.O_CLOEXEC)
        child_pid = os.fork()
        if child_pid == 0:  # pragma: no cover - exercised by subprocess test
            try:
                os.close(write_fd)
                _watchdog_child(read_fd, runner_pid, process_group_id)
            finally:
                os._exit(3)
        os.close(read_fd)
        return cls(
            child_pid=child_pid,
            write_fd=write_fd,
            process_group_id=process_group_id,
        )

    def disarm(self) -> None:
        if self.disarmed:
            return
        self.disarmed = True
        try:
            os.write(self.write_fd, b"N")
        except OSError:
            pass
        finally:
            try:
                os.close(self.write_fd)
            except OSError:
                pass
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            try:
                waited, _ = os.waitpid(self.child_pid, os.WNOHANG)
            except ChildProcessError:
                return
            if waited == self.child_pid:
                return
            time.sleep(0.01)
        try:
            os.kill(self.child_pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            os.waitpid(self.child_pid, 0)
        except ChildProcessError:
            pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-run-id", required=True)
    parser.add_argument("--parent-pid", type=int, required=True)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--page-kinds", required=True, help="comma separated")
    parser.add_argument("--run-type", required=True)
    parser.add_argument("--request-limit", type=int, required=True)
    parser.add_argument("--byte-limit-mb", type=int, required=True)
    parser.add_argument("--shard-size", type=int, required=True)
    parser.add_argument("--reservation-mb", type=int, required=True)
    parser.add_argument("--domain-interval-seconds", type=float, required=True)
    parser.add_argument("--max-batches", type=int, default=16)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = build_parser().parse_args(argv)
    _arm_parent_death_containment(args.parent_pid)
    watchdog = _ProcessGroupWatchdog.start()
    try:
        return _run(args)
    finally:
        watchdog.disarm()


def _run(args: argparse.Namespace) -> int:
    page_kinds = [kind for kind in args.page_kinds.split(",") if kind]
    if not page_kinds:
        raise SystemExit("at least one page kind is required")
    proxy_control_url = str(
        os.environ.get("FBREF_PROXY_CONTROL_URL") or ""
    ).strip()
    if not proxy_control_url:
        raise RuntimeError(
            "FBREF_PROXY_CONTROL_URL is required; live FBref cannot run direct"
        )

    settings = PipelineSettings(
        run_type=args.run_type,
        request_limit=args.request_limit,
        byte_limit=args.byte_limit_mb * MIB,
        shard_size=args.shard_size,
        request_reservation_bytes=args.reservation_mb * MIB,
        domain_interval_seconds=args.domain_interval_seconds,
    )
    pipeline = FBrefPipeline.from_env()
    run = pipeline.control.get_run(args.control_run_id)
    if run is None:
        raise RuntimeError("FBref control run does not exist")
    raw_metadata = run.get("metadata") or {}
    if isinstance(raw_metadata, str):
        raw_metadata = json.loads(raw_metadata)
    if not isinstance(raw_metadata, Mapping):
        raise RuntimeError("FBref control run metadata is invalid")
    dag_id = str(raw_metadata.get("dag_id") or "")
    if dag_id not in FBREF_DAG_IDS:
        raise RuntimeError("FBref control run has invalid paid-proxy DAG provenance")
    if str(run.get("run_type") or "") != args.run_type:
        raise RuntimeError("FBref control run type differs from live runner")
    provider_context = {
        "source": "fbref",
        "dag_id": dag_id,
        "run_id": args.control_run_id,
        "task_id": "run_live_waves",
        "scope": args.worker_id,
        "canonical_url": "https://fbref.com/en/",
    }
    pipeline.fetcher_factory = (
        lambda _proxy_file, max_browser_requests, max_browser_bytes: FBrefFetcher(
            max_browser_requests=max_browser_requests,
            max_browser_bytes=max_browser_bytes,
            provider_context=provider_context,
            provider_max_bytes=settings.byte_limit,
            proxy_control_url=proxy_control_url,
        )
    )
    result = pipeline.run_live_waves(
        args.control_run_id,
        worker_id=args.worker_id,
        page_kinds=page_kinds,
        settings=settings,
        max_batches=args.max_batches,
    ).as_dict()
    print(f"{RESULT_PREFIX}{json.dumps(result, sort_keys=True)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
